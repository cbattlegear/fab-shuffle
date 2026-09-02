"""Sizing Copy Job concurrency from the target capacity.

A Copy Job runs on the capacity it is copying into, so how many are worth having in flight
is a question about capacity units rather than a fixed number. One per 16 CUs, rounded up.
"""

from __future__ import annotations

import pytest

from fabshuffle.fabric.workspaces import UNKNOWN_SKU_COPY_JOBS, copy_job_concurrency


@pytest.fixture(autouse=True)
def _no_override(monkeypatch):
    # Zero means "work it out from the SKU", which is what these tests are about.
    monkeypatch.setattr("fabshuffle.fabric.workspaces.SETTINGS.copy_job_concurrency", 0)


@pytest.mark.parametrize(
    ("sku", "expected"),
    [
        ("F2", 1),
        ("F4", 1),
        ("F8", 1),
        ("F16", 1),
        ("F32", 2),
        ("F64", 4),
        ("F128", 8),
        ("F256", 16),
        ("F512", 32),
    ],
)
def test_one_job_per_sixteen_capacity_units(sku, expected):
    assert copy_job_concurrency(sku) == expected


def test_a_small_capacity_still_copies():
    """Rounding up matters here: refusing to copy at all is not an answer."""
    assert copy_job_concurrency("F2") == 1


def test_a_size_between_steps_rounds_up():
    # 40 CUs is more than two 16s, so it gets the third slot rather than losing it.
    assert copy_job_concurrency("F40") == 3


def test_the_sku_is_read_case_insensitively():
    assert copy_job_concurrency("f64") == copy_job_concurrency("F64")


@pytest.mark.parametrize("sku", ["P1", "EM2", "A4", "Trial", "", "FSomething"])
def test_a_sku_that_is_not_capacity_units_is_not_guessed_at(sku):
    # P, EM, A and trial SKUs do not report a CU count we can divide, so they get a
    # conservative fixed number rather than an invented one.
    assert copy_job_concurrency(sku) == UNKNOWN_SKU_COPY_JOBS


def test_an_explicit_setting_wins_over_the_capacity(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.workspaces.SETTINGS.copy_job_concurrency", 1)

    assert copy_job_concurrency("F512") == 1


def test_the_override_never_drops_below_one(monkeypatch):
    monkeypatch.setattr("fabshuffle.fabric.workspaces.SETTINGS.copy_job_concurrency", -5)

    assert copy_job_concurrency("F64") == 1
