"""``python -m fabshuffle`` starts the web UI."""

from __future__ import annotations

import logging
import os

import uvicorn

from fabshuffle.config import SETTINGS


def configure_logging(mode: str) -> None:
    if mode not in {"basic", "verbose", "debug"}:
        raise ValueError("FAB_SHUFFLE_LOG_LEVEL must be basic, verbose or debug")
    logging.basicConfig(
        level=logging.DEBUG if mode == "debug" else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    logging.getLogger().setLevel(logging.DEBUG if mode == "debug" else logging.INFO)
    logging.getLogger("fabshuffle.activity").setLevel(
        logging.WARNING if mode == "basic" else logging.INFO
    )
    # Third-party HTTP debug output may contain credentials, URLs or response bodies.
    for name in ("httpx", "httpcore", "urllib3", "azure", "msal", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)


def main() -> None:
    mode = os.environ.get("FAB_SHUFFLE_LOG_LEVEL", "basic").strip().lower()
    configure_logging(mode)
    uvicorn.run(
        "fabshuffle.web.app:app",
        host=SETTINGS.host,
        port=SETTINGS.port,
        log_level="debug" if mode == "debug" else "info",
        access_log=False,
    )


if __name__ == "__main__":
    main()
