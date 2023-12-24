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
    

@pytest.mark.parametrize(('title', 'tref', 'include_all_versions'), [
    ['Job', 'Job 17', False],
    ['Job', 'Job 17', True],
    ['Derashat Shabbat HaGadol', 'Derashat Shabbat HaGadol 1', False],
])
def test_section_data(title, tref, include_all_versions):
    index = library.get_index(title)
    exporter = IndexExporter(index, include_all_versions)
    oref = model.Ref(tref)
    num_versions = len(index.versionSet()) if include_all_versions else 2
    num_segments = len(oref.all_segment_refs())
    text_by_version, metadata = exporter.section_data(oref)
    assert len(text_by_version) == num_versions
    for key, value in text_by_version.items():
        assert len(value) <= num_segments
        print(key)
