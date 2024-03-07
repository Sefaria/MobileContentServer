import pytest
import JsonExporterForIOS as jefi
from sefaria.model.text import library, Ref

library.rebuild_toc()

@pytest.fixture()
def simple_index_exporter():
    i = library.get_index("Job")
    return jefi.IndexExporter(i)


@pytest.fixture()
def all_version_index_exporter():
    i = library.get_index("Job")
    return jefi.IndexExporter(i, include_all_versions=True)


def test_num_chunks(simple_index_exporter, all_version_index_exporter):
    assert len(simple_index_exporter._text_map['Job']['chunks']) == 2
    assert len(all_version_index_exporter._text_map['Job']['chunks']) > 2
    

@pytest.mark.parametrize(('tref', 'include_all_versions', 'expected_num_versions'), [
    ['Job 17', False, 2],
    ['Job 17', True, 15],
    ['Derashat Shabbat HaGadol 1', False, 1],
    ['Malbim Beur Hamilot on Nahum 1:1', False, 1],
])
def test_section_data(tref, include_all_versions, expected_num_versions):
    oref = Ref(tref)
    index = oref.index
    exporter = jefi.IndexExporter(index, include_all_versions)
    num_segments = len(oref.all_segment_refs())
    text_by_version, metadata = exporter.section_data(oref)
    assert len(text_by_version) == expected_num_versions
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
    exporter = jefi.IndexExporter(index, False)
    text_by_version, metadata = exporter.section_data(Ref("Berakhot 2a"))
    assert ('William Davidson Edition - English', 'en') in text_by_version


def test_remove_empty_versions():
    """
    Test that we remove empty versions from a section
    """
    index = library.get_index("Exodus")
    exporter = jefi.IndexExporter(index, True)
    text_by_version, metadata = exporter.section_data(Ref("Exodus 10"))
    test_vtitle, test_lang = 'Sefaria Community Translation', 'en'
    assert (test_vtitle, test_lang) not in text_by_version
    assert next((v for v in metadata['versions'] if v['versionTitle'] == test_vtitle and v['language'] == test_lang), None) is None


# Currently just for enabling easy debugging of export_text_json
@pytest.mark.parametrize(('title',), [
    ['Malbim Beur Hamilot on Nahum'],
])
def test_export_text_json(title):
    index = library.get_index(title)
    exported = jefi.export_text_json(index)
