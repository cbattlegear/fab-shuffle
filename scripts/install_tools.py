"""Install only the reviewed external artifacts in tools.lock.json (Docker build only)."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def verify_hash(path: Path, expected: str, algorithm: str = "sha256") -> None:
    with path.open("rb") as stream:
        actual = hashlib.file_digest(stream, algorithm).hexdigest()
    if actual != expected:
        raise RuntimeError(f"{path.name}: {algorithm} mismatch; expected {expected}, got {actual}")


def download(artifact: dict, path: Path) -> None:
    print(f"Downloading pinned artifact: {artifact['url']}", flush=True)
    with urllib.request.urlopen(artifact["url"], timeout=120) as response, path.open("wb") as stream:
        shutil.copyfileobj(response, stream)
    verify_hash(path, artifact["sha256"])


def install_apt(pins: dict, architecture: str) -> None:
    with tempfile.TemporaryDirectory(prefix="fab-shuffle-apt-") as temporary:
        packages = []
        for name, artifact in pins["apt_artifacts"][architecture].items():
            path = Path(temporary) / f"{name}.deb"
            download(artifact, path)
            packages.append(str(path))
        subprocess.run(
            ["apt-get", "install", "-y", "--no-install-recommends", *packages, "unixodbc", "unixodbc-dev"],
            env={**os.environ, "ACCEPT_EULA": "Y"}, check=True,
        )


def install_azcopy(pins: dict, architecture: str) -> None:
    with tempfile.TemporaryDirectory(prefix="fab-shuffle-azcopy-") as temporary:
        archive = Path(temporary) / "azcopy.tar.gz"
        download(pins["azcopy"]["artifacts"][architecture], archive)
        with tarfile.open(archive, "r:gz") as tar:
            binaries = [
                member for member in tar.getmembers() if member.isfile() and member.name.endswith("/azcopy")
            ]
            if len(binaries) != 1:
                raise RuntimeError(f"Expected one azcopy binary, found {len(binaries)}")
            with tar.extractfile(binaries[0]) as source, Path("/usr/local/bin/azcopy").open("wb") as target:
                shutil.copyfileobj(source, target)
        Path("/usr/local/bin/azcopy").chmod(0o755)


def install_nuget(pins: dict, source: str) -> None:
    # Clearing inherited sources preserves the single-feed corporate proxy override.
    with tempfile.TemporaryDirectory(prefix="fab-shuffle-nuget-") as temporary:
        configuration = ET.Element("configuration")
        sources = ET.SubElement(configuration, "packageSources")
        ET.SubElement(sources, "clear")
        ET.SubElement(sources, "add", {"key": "primary", "value": source})
        config = Path(temporary) / "NuGet.config"
        ET.ElementTree(configuration).write(config, encoding="utf-8", xml_declaration=True)
        for package, pin in pins["nuget_tools"].items():
            subprocess.run(
                [
                    "dotnet", "tool", "install", "--global", package, "--version", pin["version"],
                    "--no-cache", "--configfile", str(config),
                ],
                check=True,
            )
            store = Path.home() / ".dotnet" / "tools" / ".store" / package.lower() / pin["version"]
            archives = list(store.rglob(f"{package.lower()}.{pin['version']}.nupkg"))
            if len(archives) != 1:
                raise RuntimeError(
                    f"Expected one installed package archive for {package}, found {len(archives)}"
                )
            verify_hash(archives[0], pin["sha256"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("component", choices=("apt", "azcopy", "nuget"))
    parser.add_argument("--architecture", choices=("amd64", "arm64"), required=True)
    args = parser.parse_args()
    pins = json.loads((ROOT / "tools.lock.json").read_text(encoding="utf-8"))
    if args.component == "apt":
        install_apt(pins, args.architecture)
    elif args.component == "azcopy":
        install_azcopy(pins, args.architecture)
    else:
        install_nuget(pins, os.environ["NUGET_SOURCE"])


if __name__ == "__main__":
    main()
