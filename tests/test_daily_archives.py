import pytest
from boatrace_ai.ingestion.daily_archives import build_archive_spec

def test_program_archive_url():
    spec=build_archive_spec("2026-06-30","program")
    assert spec["filename"]=="b260630.lzh"
    assert spec["url"]=="https://www1.mbrace.or.jp/od2/B/202606/b260630.lzh"

def test_result_archive_url():
    spec=build_archive_spec("2026-06-30","result")
    assert spec["filename"]=="k260630.lzh"
    assert spec["url"]=="https://www1.mbrace.or.jp/od2/K/202606/k260630.lzh"

def test_invalid_archive_type():
    with pytest.raises(ValueError):
        build_archive_spec("2026-06-30","invalid")
