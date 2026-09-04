"""Shared test setup.

The suite runs the orchestrator end to end against fake Fabric clients, and the orchestrator
makes a scratch directory and a journal file for every run it starts. Without this those land
in the real ``./local``, which is a mounted volume in the container and the developer's
working tree here: the repository had accumulated several thousand empty run directories that
way. Everything is pointed at a temporary directory for the session instead.
"""

from __future__ import annotations

import pytest

from fabshuffle.config import SETTINGS


@pytest.fixture(autouse=True, scope="session")
def scratch_root(tmp_path_factory):
    """Keep every run's scratch directory and journal out of the working tree."""
    previous = SETTINGS.scratch_root
    SETTINGS.scratch_root = tmp_path_factory.mktemp("fab-shuffle-scratch")
    yield SETTINGS.scratch_root
    SETTINGS.scratch_root = previous
