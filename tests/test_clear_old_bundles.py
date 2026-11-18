import json
import os
import time

import pytest

import JsonExporterForIOS as jefi

SCHEMA = "7"
DAY = 24 * 60 * 60


def make_bundle(bundle_path, name, age_seconds):
    directory = bundle_path / name
    directory.mkdir()
    (directory / "1.zip").write_bytes(b"zip")
    stamp = time.time() - age_seconds
    os.utime(directory, (stamp, stamp))
    return directory


@pytest.fixture()
def bundles_dir(tmp_path, monkeypatch):
    """Create an export tree with a bundles directory and packages.json."""
    export_path = tmp_path / "export"
    bundle_path = export_path / SCHEMA / "bundles"
    bundle_path.mkdir(parents=True)
    packages = [{"en": "Tanakh"}, {"en": "COMPLETE LIBRARY"}]
    (export_path / SCHEMA / "packages.json").write_text(json.dumps(packages))
    for package in packages:
        make_bundle(bundle_path, package["en"], age_seconds=10 * DAY)
    monkeypatch.setattr(jefi, "SEFARIA_EXPORT_PATH", str(export_path))
    return bundle_path


def test_clear_old_bundles_keeps_newest_bundles(bundles_dir):
    # five client bundles older than the download grace, oldest first
    bundles = [make_bundle(bundles_dir, f"bundle{i}", age_seconds=(5 - i) * DAY) for i in range(5)]

    jefi.clear_old_bundles(SCHEMA, max_files=3)

    remaining = {p.name for p in bundles_dir.iterdir()}
    assert {b.name for b in bundles[3:]} <= remaining, "recent bundles were deleted"
    assert not {b.name for b in bundles[:2]} & remaining, "the oldest bundles were not deleted"
    assert {"Tanakh", "COMPLETE LIBRARY"} <= remaining, "package bundles were deleted"


def test_clear_old_bundles_keeps_a_bundle_being_downloaded(bundles_dir):
    # a bundle a client is downloading now, pushed out of the newest bundles by later requests
    in_flight = make_bundle(bundles_dir, "in-flight", age_seconds=60 * 60)
    for i in range(60):
        make_bundle(bundles_dir, f"later{i}", age_seconds=50 * 60 - i)

    jefi.clear_old_bundles(SCHEMA, max_files=3)

    assert in_flight.exists(), "the bundle a client is downloading was deleted"


def test_clear_old_bundles_deletes_only_the_oldest_excess(bundles_dir):
    bundles = [make_bundle(bundles_dir, f"bundle{i}", age_seconds=(60 - i) * DAY) for i in range(60)]

    jefi.clear_old_bundles(SCHEMA, max_files=50)

    remaining = {p.name for p in bundles_dir.iterdir()}
    assert {b.name for b in bundles[-49:]} <= remaining, "a bundle inside the cap was deleted"
    assert not {b.name for b in bundles[:11]} & remaining, "the oldest excess bundles were kept"


def test_clear_old_bundles_below_cap_does_nothing(bundles_dir):
    bundles = [make_bundle(bundles_dir, f"bundle{i}", age_seconds=(i + 1) * DAY) for i in range(2)]

    jefi.clear_old_bundles(SCHEMA, max_files=50)

    assert all(b.exists() for b in bundles)


def test_clear_old_bundles_max_files_zero_clears_every_bundle(bundles_dir):
    # os_error_cleanup asks for a full clear of client bundles, keeping the package bundles
    bundles = [make_bundle(bundles_dir, f"bundle{i}", age_seconds=60 * (i + 1)) for i in range(3)]

    jefi.clear_old_bundles(SCHEMA, max_files=0)

    remaining = {p.name for p in bundles_dir.iterdir()}
    assert not {b.name for b in bundles} & remaining
    assert {"Tanakh", "COMPLETE LIBRARY"} <= remaining
