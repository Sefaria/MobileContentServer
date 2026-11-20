import os
import sys
import types

import pytest

# DownloadServer imports local_settings and JsonExporterForIOS at module import, and JsonExporterForIOS
# imports django and the Sefaria library. These route tests need neither, so stub both modules.
local_settings = types.ModuleType('local_settings')
local_settings.SEFARIA_EXPORT_PATH = '/exports'
local_settings.DEBUG_MODE = False
sys.modules['local_settings'] = local_settings

exporter = types.ModuleType('JsonExporterForIOS')
exporter.build_split_archive = lambda *args, **kwargs: None
exporter.SEFARIA_EXPORT_PATH = '/exports'
exporter.get_export_path = lambda schema_version: f'/exports/{schema_version}'
exporter.updated_books_list = lambda: []
exporter.new_books_since_last_update = lambda: []
exporter.clear_old_bundles = lambda *args, **kwargs: None
exporter.SCHEMA_VERSION = '7'
exporter.PREV_SCHEMA_VERSION = '6'
sys.modules['JsonExporterForIOS'] = exporter

import DownloadServer


@pytest.fixture()
def client(tmp_path, monkeypatch):
    bundles = tmp_path / '7' / 'bundles'
    package = bundles / 'COMPLETE LIBRARY'
    package.mkdir(parents=True)
    (package / 'Genesis.zip').write_text('')
    (package / 'Exodus.zip').write_text('')
    # A directory outside the bundle directory, and a regular file inside it.
    (tmp_path / 'secret').mkdir()
    (tmp_path / 'secret' / 'passwd').write_text('')
    (bundles / 'plain-file').write_text('')
    monkeypatch.setattr(DownloadServer, 'SEFARIA_EXPORT_PATH', str(tmp_path))
    return DownloadServer.app.test_client()


def test_package_data_lists_bundle_directory(client):
    response = client.get('/packageData?package=COMPLETE%20LIBRARY&schema_version=7')
    assert response.status_code == 200
    assert response.get_json() == [
        'static/ios-export/7/bundles/COMPLETE LIBRARY/Exodus.zip',
        'static/ios-export/7/bundles/COMPLETE LIBRARY/Genesis.zip',
    ]


@pytest.mark.parametrize('package', ['../../secret', '../secret', '/etc', 'COMPLETE LIBRARY/../../secret', '..', '.'])
def test_package_data_rejects_path_outside_bundles(client, package):
    response = client.get('/packageData', query_string={'package': package, 'schema_version': '7'})
    assert response.status_code == 200
    assert response.get_json() == []


def test_package_data_answers_empty_list_for_a_regular_file(client):
    response = client.get('/packageData?package=plain-file&schema_version=7')
    assert response.status_code == 200
    assert response.get_json() == []


def test_package_data_without_package_answers_empty_list(client):
    response = client.get('/packageData')
    assert response.status_code == 200
    assert response.get_json() == []
