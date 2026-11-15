"""Focused tests for DownloadServer.create_zip_bundle.

These exercise the bundle-build filesystem logic without the Sefaria-Project
stack: the JsonExporterForIOS and local_settings modules are stubbed before
DownloadServer is imported.
"""
import os
import sys
import threading
import types

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def _install_stubs(export_path):
    sys.modules['local_settings'] = types.ModuleType('local_settings')

    exporter = types.ModuleType('JsonExporterForIOS')
    exporter.build_split_archive = lambda *args, **kwargs: None
    exporter.SEFARIA_EXPORT_PATH = export_path
    exporter.get_export_path = lambda schema_version: export_path
    exporter.updated_books_list = lambda: []
    exporter.new_books_since_last_update = lambda books: []
    exporter.clear_old_bundles = lambda schema_version, max_files: None
    exporter.SCHEMA_VERSION = 7
    exporter.PREV_SCHEMA_VERSION = 6
    sys.modules['JsonExporterForIOS'] = exporter


@pytest.fixture
def server(tmp_path):
    export_path = tmp_path / 'ios-export' / '7'
    (export_path / 'bundles').mkdir(parents=True)
    _install_stubs(str(export_path))
    sys.modules.pop('DownloadServer', None)
    import DownloadServer
    return DownloadServer, str(export_path)


def _bundle_paths(export_path, zip_dirname):
    return (
        os.path.join(export_path, 'bundles', zip_dirname),
        os.path.join(export_path, '.tmp-' + zip_dirname),
    )


def test_build_dir_shares_a_filesystem_with_the_destination(server, monkeypatch):
    DownloadServer, export_path = server
    zip_dirname = 'bundlehash'
    zip_path, _ = _bundle_paths(export_path, zip_dirname)
    seen = {}

    def fake_build(book_list, build_loc, file_locations):
        seen['build_loc'] = build_loc
        os.mkdir(build_loc)
        with open(os.path.join(build_loc, '3.zip'), 'w') as fp:
            fp.write('text')

    monkeypatch.setattr(DownloadServer, 'build_split_archive', fake_build)
    DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)

    build_loc = seen['build_loc']
    # Building on the destination filesystem is what lets the publish be one rename.
    assert os.path.dirname(build_loc) == export_path
    assert os.stat(export_path).st_dev == os.stat(zip_path).st_dev


def test_finished_bundle_is_published_whole_and_build_dir_removed(server, monkeypatch):
    DownloadServer, export_path = server
    zip_dirname = 'bundlehash'
    zip_path, build_loc = _bundle_paths(export_path, zip_dirname)

    def fake_build(book_list, build_loc, file_locations):
        os.mkdir(build_loc)
        with open(os.path.join(build_loc, '3.zip'), 'w') as fp:
            fp.write('text')

    monkeypatch.setattr(DownloadServer, 'build_split_archive', fake_build)
    DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)

    assert os.path.isdir(zip_path)
    assert os.path.isfile(os.path.join(zip_path, '3.zip'))
    assert not os.path.exists(build_loc)


def test_destination_stays_absent_until_the_build_finishes(server, monkeypatch):
    DownloadServer, export_path = server
    zip_dirname = 'bundlehash'
    zip_path, _ = _bundle_paths(export_path, zip_dirname)
    observed = {}

    def fake_build(book_list, build_loc, file_locations):
        observed['partial_present'] = os.path.exists(zip_path)
        os.mkdir(build_loc)
        with open(os.path.join(build_loc, '3.zip'), 'w') as fp:
            fp.write('text')

    monkeypatch.setattr(DownloadServer, 'build_split_archive', fake_build)
    DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)

    assert observed['partial_present'] is False


def test_second_request_for_a_running_bundle_does_not_build_it_again(server, monkeypatch):
    DownloadServer, export_path = server
    zip_dirname = 'bundlehash'
    zip_path, _ = _bundle_paths(export_path, zip_dirname)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def slow_build(book_list, build_loc, file_locations):
        calls.append(build_loc)
        started.set()
        release.wait(5)
        os.mkdir(build_loc)
        with open(os.path.join(build_loc, '3.zip'), 'w') as fp:
            fp.write('text')

    monkeypatch.setattr(DownloadServer, 'build_split_archive', slow_build)
    worker = threading.Thread(
        target=DownloadServer.create_zip_bundle,
        args=(['Genesis'], zip_path, zip_dirname, export_path),
    )
    worker.start()
    assert started.wait(5)

    # A second request for the same bundle while it builds must not start over.
    DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)

    release.set()
    worker.join(5)
    assert len(calls) == 1
    assert os.path.isfile(os.path.join(zip_path, '3.zip'))


def test_failed_build_can_be_retried(server, monkeypatch):
    DownloadServer, export_path = server
    zip_dirname = 'bundlehash'
    zip_path, _ = _bundle_paths(export_path, zip_dirname)
    attempts = []

    def flaky_build(book_list, build_loc, file_locations):
        attempts.append(build_loc)
        if len(attempts) == 1:
            raise OSError('disk full')
        os.mkdir(build_loc)
        with open(os.path.join(build_loc, '3.zip'), 'w') as fp:
            fp.write('text')

    monkeypatch.setattr(DownloadServer, 'build_split_archive', flaky_build)
    with pytest.raises(OSError):
        DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)

    DownloadServer.create_zip_bundle(['Genesis'], zip_path, zip_dirname, export_path)
    assert len(attempts) == 2
    assert os.path.isfile(os.path.join(zip_path, '3.zip'))
