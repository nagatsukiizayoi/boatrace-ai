from datetime import date, datetime, timezone

import pytest

from boatrace_ai.ingestion.post_race_label_sources import (
    PostRaceLabelContractError,
    PostRaceLabelIntegrityError,
    build_source_paths,
    collect_post_race_label_source,
)


UTC = timezone.utc
RACE_DATE = date(2026, 8, 10)
FIXED_TIME = datetime(
    2026,
    8,
    10,
    12,
    0,
    tzinfo=UTC,
)
ARCHIVE_BYTES = b"\x00-lh5-post-race-label"


class FakeResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        yield ARCHIVE_BYTES

    def close(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return FakeResponse()


class FailingSession:
    def get(self, *args, **kwargs):
        raise AssertionError("HTTP must not be called")


def clock():
    return FIXED_TIME


def validator(path):
    assert path.is_file()


def test_build_source_paths(tmp_path):
    paths = build_source_paths(
        RACE_DATE,
        tmp_path,
    )

    assert paths["archive"].name == "k260810.lzh"
    assert paths["metadata"].name == "k260810.lzh.json"
    assert "post_race_label_v1" in str(
        paths["directory"]
    )


def test_collects_post_race_source_with_metadata(tmp_path):
    session = FakeSession()

    result = collect_post_race_label_source(
        RACE_DATE,
        tmp_path,
        session=session,
        now_fn=clock,
        validator=validator,
    )

    metadata = result["metadata"]

    assert result["status"] == "CACHED"
    assert session.calls == 1
    assert metadata["contract_version"] == (
        "post_race_label_source_v1"
    )
    assert metadata["source_type"] == "result"
    assert metadata["label_eligible"] is True
    assert metadata["provenance_status"] == "VERIFIED"
    assert metadata["fetched_at"] == (
        FIXED_TIME.isoformat()
    )


def test_reuses_valid_source_without_http(tmp_path):
    collect_post_race_label_source(
        RACE_DATE,
        tmp_path,
        session=FakeSession(),
        now_fn=clock,
        validator=validator,
    )

    result = collect_post_race_label_source(
        RACE_DATE,
        tmp_path,
        session=FailingSession(),
        now_fn=clock,
        validator=validator,
    )

    assert result["status"] == "CACHED"


def test_rejects_tampered_source_archive(tmp_path):
    collect_post_race_label_source(
        RACE_DATE,
        tmp_path,
        session=FakeSession(),
        now_fn=clock,
        validator=validator,
    )

    paths = build_source_paths(
        RACE_DATE,
        tmp_path,
    )
    paths["archive"].write_bytes(
        b"tampered-lh5-data"
    )

    with pytest.raises(
        PostRaceLabelIntegrityError
    ):
        collect_post_race_label_source(
            RACE_DATE,
            tmp_path,
            session=FailingSession(),
            now_fn=clock,
            validator=validator,
        )


def test_rejects_naive_clock_without_http(tmp_path):
    naive = datetime(2026, 8, 10, 12, 0)
    session = FakeSession()

    with pytest.raises(
        PostRaceLabelContractError
    ):
        collect_post_race_label_source(
            RACE_DATE,
            tmp_path,
            session=session,
            now_fn=lambda: naive,
            validator=validator,
        )

    assert session.calls == 0
