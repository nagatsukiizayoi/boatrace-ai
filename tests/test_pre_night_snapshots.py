import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from boatrace_ai.ingestion import (
    pre_night_snapshots as snapshots,
)


JST = ZoneInfo("Asia/Tokyo")
UTC = dt.timezone.utc


class FakeResponse:
    def __init__(
        self,
        content=b"fake-lzh-content",
        status_code=200,
        headers=None,
    ):
        self.content = content
        self.status_code = status_code
        self.headers = headers or {
            "Content-Type": (
                "application/octet-stream"
            ),
            "ETag": '"test-etag"',
        }
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(
                f"HTTP {self.status_code}"
            )

    def iter_content(
        self,
        chunk_size,
    ):
        midpoint = max(
            1,
            len(self.content) // 2,
        )

        yield self.content[:midpoint]
        yield self.content[midpoint:]

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(
        self,
        url,
        headers,
        timeout,
        stream,
    ):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
                "stream": stream,
            }
        )
        return self.response


class FailingSession:
    def get(self, *args, **kwargs):
        raise AssertionError(
            "HTTP request must not occur "
            "when valid cache exists"
        )


def make_clock(*values):
    iterator = iter(values)

    def now_fn():
        return next(iterator)

    return now_fn


@pytest.fixture
def disable_real_lzh_validation(
    monkeypatch,
):
    monkeypatch.setattr(
        snapshots,
        "validate_lzh_file",
        lambda path: {
            "path": str(path),
            "size": Path(path).stat().st_size,
        },
    )


def test_build_pre_night_as_of():
    actual = snapshots.build_pre_night_as_of(
        "2026-08-10"
    )

    assert actual == dt.datetime(
        2026,
        8,
        9,
        21,
        30,
        tzinfo=JST,
    )


def test_collects_eligible_program_snapshot(
    tmp_path,
    disable_real_lzh_validation,
):
    response = FakeResponse(
        content=b"eligible-program-archive"
    )

    session = FakeSession(response)

    clock = make_clock(
        dt.datetime(
            2026,
            8,
            9,
            12,
            0,
            tzinfo=UTC,
        ),
        dt.datetime(
            2026,
            8,
            9,
            12,
            1,
            tzinfo=UTC,
        ),
    )

    outcome = (
        snapshots
        .collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=session,
            now_fn=clock,
        )
    )

    assert outcome["cached"] is False
    assert (
        outcome["eligible_for_pre_night"]
        is True
    )

    archive = outcome["paths"]["archive"]
    metadata_path = (
        outcome["paths"]["metadata"]
    )

    assert archive.read_bytes() == (
        b"eligible-program-archive"
    )

    metadata = json.loads(
        metadata_path.read_text(
            encoding="utf-8"
        )
    )

    assert metadata["source_type"] == (
        "program"
    )

    assert metadata["snapshot_type"] == (
        "PRE_NIGHT"
    )

    assert metadata[
        "eligible_for_pre_night"
    ] is True

    assert metadata[
        "eligibility_reason"
    ] == "FETCHED_BY_AS_OF"

    assert metadata[
        "source_response_sha256"
    ] == metadata["archive_sha256"]

    assert len(session.calls) == 1
    assert response.closed is True


def test_reuses_valid_cache_without_http(
    tmp_path,
    disable_real_lzh_validation,
):
    response = FakeResponse(
        content=b"cached-program-archive"
    )

    first = (
        snapshots
        .collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FakeSession(response),
            now_fn=make_clock(
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    0,
                    tzinfo=UTC,
                ),
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    1,
                    tzinfo=UTC,
                ),
            ),
        )
    )

    second = (
        snapshots
        .collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FailingSession(),
        )
    )

    assert first["cached"] is False
    assert second["cached"] is True
    assert (
        second["eligible_for_pre_night"]
        is True
    )


def test_rejects_archive_without_metadata(
    tmp_path,
    disable_real_lzh_validation,
):
    paths = snapshots.build_snapshot_paths(
        "2026-08-10",
        tmp_path,
    )

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    paths["archive"].write_bytes(
        b"orphan-archive"
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="must exist together",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FailingSession(),
        )


def test_rejects_metadata_without_archive(
    tmp_path,
    disable_real_lzh_validation,
):
    paths = snapshots.build_snapshot_paths(
        "2026-08-10",
        tmp_path,
    )

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    paths["metadata"].write_text(
        "{}",
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="must exist together",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FailingSession(),
        )


def test_rejects_hash_mismatch(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = (
        snapshots
        .collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FakeSession(
                FakeResponse(
                    content=b"original-content"
                )
            ),
            now_fn=make_clock(
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    0,
                    tzinfo=UTC,
                ),
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    1,
                    tzinfo=UTC,
                ),
            ),
        )
    )

    outcome["paths"]["archive"].write_bytes(
        b"tampered-content"
    )

    with pytest.raises(
        snapshots.PreNightIntegrityError,
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FailingSession(),
        )


def test_records_but_rejects_late_snapshot(
    tmp_path,
    disable_real_lzh_validation,
):
    response = FakeResponse(
        content=b"late-program-archive"
    )

    paths = snapshots.build_snapshot_paths(
        "2026-08-10",
        tmp_path,
    )

    with pytest.raises(
        snapshots.PreNightEligibilityError,
        match="after PRE_NIGHT",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FakeSession(response),
            now_fn=make_clock(
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    31,
                    tzinfo=UTC,
                ),
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    32,
                    tzinfo=UTC,
                ),
            ),
        )

    assert paths["archive"].is_file()
    assert paths["metadata"].is_file()

    metadata = json.loads(
        paths["metadata"].read_text(
            encoding="utf-8"
        )
    )

    assert metadata[
        "eligible_for_pre_night"
    ] is False

    assert metadata[
        "eligibility_reason"
    ] == "FETCHED_AFTER_AS_OF"


def test_rejects_naive_clock(
    tmp_path,
    disable_real_lzh_validation,
):
    response = FakeResponse()

    naive_time = dt.datetime(
        2026,
        8,
        9,
        12,
        0,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="timezone",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FakeSession(response),
            now_fn=make_clock(
                naive_time,
            ),
        )


def test_rejects_unsupported_contract_version(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = (
        snapshots
        .collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FakeSession(
                FakeResponse(
                    content=b"contract-test"
                )
            ),
            now_fn=make_clock(
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    0,
                    tzinfo=UTC,
                ),
                dt.datetime(
                    2026,
                    8,
                    9,
                    12,
                    1,
                    tzinfo=UTC,
                ),
            ),
        )
    )

    metadata_path = (
        outcome["paths"]["metadata"]
    )

    metadata = json.loads(
        metadata_path.read_text(
            encoding="utf-8"
        )
    )

    metadata["contract_version"] = (
        "unsupported-version"
    )

    metadata_path.write_text(
        json.dumps(metadata),
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="unsupported contract_version",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-08-10",
            tmp_path,
            session=FailingSession(),
        )
