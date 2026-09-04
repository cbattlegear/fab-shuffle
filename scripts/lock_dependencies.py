"""Export universal hashed requirements, or check that the committed exports match."""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = {
    "bootstrap": ["--only-group", "lock"],
    "build": ["--only-group", "build"],
    "runtime": ["--no-default-groups"],
    "dev": ["--no-default-groups", "--extra", "dev"],
}
HEADER = "# Generated from uv.lock by scripts/lock_dependencies.py; do not edit.\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail instead of updating stale exports.")
    args = parser.parse_args()
    for name, options in EXPORTS.items():
        result = subprocess.run(
            [
                sys.executable, "-m", "uv", "export", "--locked", "--no-emit-project",
                "--no-header", "--no-annotate", "--quiet", *options,
            ],
            cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True,
        )
        content = HEADER + result.stdout
        path = ROOT / "requirements" / f"{name}.txt"
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                raise SystemExit(f"{path.name} is stale; run python scripts/lock_dependencies.py")
        else:
            path.parent.mkdir(exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")
    print("Dependency exports match uv.lock." if args.check else "Updated dependency exports from uv.lock.")


if __name__ == "__main__":
    main()
