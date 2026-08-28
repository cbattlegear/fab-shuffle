"""``python -m fabshuffle`` starts the web UI."""

from __future__ import annotations

import logging

import uvicorn

from fabshuffle.config import SETTINGS


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    uvicorn.run(
        "fabshuffle.web.app:app",
        host=SETTINGS.host,
        port=SETTINGS.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
