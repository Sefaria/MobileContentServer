import pytest
from JsonExporterForIOS import *
from sefaria.model.text import library

model.library.rebuild_toc()

@pytest.fixture()
def simple_index_exporter():
    i = library.get_index("Job")
    return IndexExporter(i)


@pytest.fixture()
def all_version_index_exporter():
    i = library.get_index("Job")
    return IndexExporter(i, include_all_versions=True)


def test_num_chunks(simple_index_exporter, all_version_index_exporter):
    assert len(simple_index_exporter._text_map['Job']['chunks']) == 2
    assert len(all_version_index_exporter._text_map['Job']['chunks']) > 2
    

@pytest.mark.parametrize(('title', 'tref', 'include_all_versions', 'expected_num_versions'), [
    ['Job', 'Job 17', False, 2],
    ['Job', 'Job 17', True, 15],
    ['Derashat Shabbat HaGadol', 'Derashat Shabbat HaGadol 1', False, 1],
])
def test_section_data(title, tref, include_all_versions, expected_num_versions):
    index = library.get_index(title)
    exporter = IndexExporter(index, include_all_versions)
    oref = model.Ref(tref)
    num_versions = len(index.versionSet()) if expected_num_versions is None else expected_num_versions
    num_segments = len(oref.all_segment_refs())
    text_by_version, metadata = exporter.section_data(oref)
    assert len(text_by_version) == num_versions
    for key, value in text_by_version.items():
        assert len(value) <= num_segments
        vtitle, lang = key
        assert vtitle is not None
        assert lang is not None


def test_merged_chunk():
    """
    Test that top priority version title is chosen for a merged chunk
    """
    index = library.get_index("Berakhot")
    exporter = IndexExporter(index, False)
    text_by_version, metadata = exporter.section_data(model.Ref("Berakhot 2a"))
    assert ('William Davidson Edition - English', 'en') in text_by_version


def test_remove_empty_versions():
    """
    Test that we remove empty versions from a section
    """
    index = library.get_index("Exodus")
    exporter = IndexExporter(index, True)
    text_by_version, metadata = exporter.section_data(model.Ref("Exodus 10"))
    test_vtitle, test_lang = 'Sefaria Community Translation', 'en'
    assert (test_vtitle, test_lang) not in text_by_version
    assert next((v for v in metadata['versions'] if v['versionTitle'] == test_vtitle and v['language'] == test_lang), None) is None

