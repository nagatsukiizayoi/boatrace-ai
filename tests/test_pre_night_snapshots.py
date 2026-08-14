import hashlib
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


# BEGIN PRE_NIGHT_PROGRAM_CACHE_PROVENANCE_V1_TESTS


def _collect_cache_provenance_fixture(tmp_path):
    return snapshots.collect_pre_night_program_snapshot(
        "2026-08-10",
        tmp_path,
        session=FakeSession(
            FakeResponse(
                content=b"cache-provenance-test"
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


def _read_cache_provenance_metadata(paths):
    return json.loads(
        paths["metadata"].read_text(
            encoding="utf-8"
        )
    )


def _write_cache_provenance_metadata(
    paths,
    metadata,
):
    paths["metadata"].write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _validate_cache_without_http(tmp_path):
    return snapshots.collect_pre_night_program_snapshot(
        "2026-08-10",
        tmp_path,
        session=FailingSession(),
    )


def test_cache_rejects_malformed_metadata_json(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    outcome["paths"]["metadata"].write_text(
        "{not-json",
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="not valid JSON",
    ):
        _validate_cache_without_http(tmp_path)


def test_cache_rejects_non_boolean_eligibility(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata["eligible_for_pre_night"] = "false"
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="must be bool",
    ):
        _validate_cache_without_http(tmp_path)


@pytest.mark.parametrize(
    "invalid_size",
    [True, "12", 0, -1],
)
def test_cache_rejects_invalid_response_size(
    tmp_path,
    disable_real_lzh_validation,
    invalid_size,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata["response_size"] = invalid_size
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="positive int",
    ):
        _validate_cache_without_http(tmp_path)


@pytest.mark.parametrize(
    ("field_name", "invalid_hash"),
    [
        ("source_response_sha256", ""),
        ("source_response_sha256", "abc"),
        ("source_response_sha256", "g" * 64),
        ("source_response_sha256", 123),
        ("archive_sha256", ""),
        ("archive_sha256", "abc"),
        ("archive_sha256", "g" * 64),
        ("archive_sha256", 123),
    ],
)
def test_cache_rejects_malformed_sha256(
    tmp_path,
    disable_real_lzh_validation,
    field_name,
    invalid_hash,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata[field_name] = invalid_hash
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="SHA-256|64 hex",
    ):
        _validate_cache_without_http(tmp_path)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        (
            "source_url",
            "https://example.invalid/wrong.lzh",
        ),
        (
            "archive_path",
            "/tmp/different-program.lzh",
        ),
    ],
)
def test_cache_rejects_provenance_binding_mismatch(
    tmp_path,
    disable_real_lzh_validation,
    field_name,
    replacement,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata[field_name] = replacement
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match=f"{field_name} mismatch",
    ):
        _validate_cache_without_http(tmp_path)


@pytest.mark.parametrize(
    "invalid_status",
    [True, "200", 199, 300],
)
def test_cache_rejects_invalid_http_status(
    tmp_path,
    disable_real_lzh_validation,
    invalid_status,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata["http_status"] = invalid_status
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="http_status",
    ):
        _validate_cache_without_http(tmp_path)


def test_cache_rejects_non_object_http_headers(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata["http_headers"] = []
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="http_headers must be object",
    ):
        _validate_cache_without_http(tmp_path)


def test_cache_rejects_request_started_after_fetch(
    tmp_path,
    disable_real_lzh_validation,
):
    outcome = _collect_cache_provenance_fixture(
        tmp_path
    )
    paths = outcome["paths"]
    metadata = _read_cache_provenance_metadata(
        paths
    )
    metadata["request_started_at"] = (
        "2026-08-09T12:02:00+00:00"
    )
    _write_cache_provenance_metadata(
        paths,
        metadata,
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="must not be after fetched_at",
    ):
        _validate_cache_without_http(tmp_path)


def test_pair_commit_failure_is_fail_closed(
    tmp_path,
    disable_real_lzh_validation,
    monkeypatch,
):
    paths = snapshots.build_snapshot_paths(
        "2026-08-10",
        tmp_path,
    )
    path_type = type(paths["archive"])
    original_replace = path_type.replace

    def fail_metadata_commit(self, target):
        if path_type(target) == paths["metadata"]:
            raise OSError(
                "injected metadata commit failure"
            )

        return original_replace(self, target)

    monkeypatch.setattr(
        path_type,
        "replace",
        fail_metadata_commit,
    )

    with pytest.raises(
        OSError,
        match="injected metadata commit failure",
    ):
        _collect_cache_provenance_fixture(
            tmp_path
        )

    assert paths["archive"].is_file()
    assert not paths["metadata"].exists()
    assert not (
        paths["directory"] / ".collection.lock"
    ).exists()
    assert list(
        paths["directory"].glob("*.part")
    ) == []

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="must exist together",
    ):
        _validate_cache_without_http(tmp_path)


# BEGIN PHASE1_D1B1_TESTS
# D1-B1 Snapshot v2 deadline-evidence metadata binding tests.


def _d1b1_snapshots():
    import boatrace_ai.ingestion.pre_night_snapshots as snapshots
    return snapshots


def _d1b1_validated_evidence():
    return {
        "schema_version": "test-deadline-evidence-v1",
        "request_started_at": "2026-07-28T10:00:00+00:00",
        "fetched_at": "2026-07-28T10:00:01+00:00",
        "source_name": "official-test-source",
        "source_locator": "https://example.invalid/deadlines",
        "source_authority": "official-test-authority",
        "source_timezone": "Asia/Tokyo",
        "race_date": "2026-07-29",
        "venue_code": "01",
        "http_status": 200,
        "response_headers": {},
        "raw_source_sha256": "0" * 64,
        "race_deadlines": [],
    }


def test_d1b1_t01_legacy_binding_is_empty():
    snapshots = _d1b1_snapshots()
    assert snapshots._build_deadline_evidence_metadata(None) == {}


def test_d1b1_t02_validated_evidence_is_bound(monkeypatch):
    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: b"canonical",
    )

    metadata = snapshots._build_deadline_evidence_metadata(
        {"unvalidated": True}
    )

    assert metadata["deadline_evidence"] is validated
    assert set(metadata) == {
        "deadline_evidence",
        "deadline_evidence_sha256",
    }


def test_d1b1_t03_canonicalizer_receives_validated_value(
    monkeypatch,
):
    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()
    received = []

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )

    def canonicalize(evidence):
        received.append(evidence)
        return b"canonical"

    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        canonicalize,
    )

    snapshots._build_deadline_evidence_metadata(
        {"unvalidated": True}
    )
    assert received == [validated]


def test_d1b1_t04_sha256_binds_canonical_bytes(monkeypatch):
    import hashlib

    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()
    canonical = b"deterministic canonical evidence"

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: canonical,
    )

    metadata = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )

    assert metadata["deadline_evidence_sha256"] == (
        hashlib.sha256(canonical).hexdigest()
    )


def test_d1b1_t05_timestamps_are_not_regenerated(monkeypatch):
    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: b"canonical",
    )

    metadata = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )
    bound = metadata["deadline_evidence"]

    assert bound["request_started_at"] == (
        validated["request_started_at"]
    )
    assert bound["fetched_at"] == validated["fetched_at"]


def test_d1b1_t06_invalid_evidence_fails_closed(monkeypatch):
    import pytest

    snapshots = _d1b1_snapshots()

    def reject(evidence):
        raise ValueError("invalid deadline evidence")

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        reject,
    )

    with pytest.raises(
        ValueError,
        match="invalid deadline evidence",
    ):
        snapshots._build_deadline_evidence_metadata(
            {"invalid": True}
        )


def test_d1b1_t07_canonicalization_failure_propagates(
    monkeypatch,
):
    import pytest

    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )

    def fail_canonicalization(evidence):
        raise TypeError("not canonicalizable")

    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        fail_canonicalization,
    )

    with pytest.raises(TypeError, match="not canonicalizable"):
        snapshots._build_deadline_evidence_metadata(
            {"input": True}
        )


def test_d1b1_t08_digest_uses_exact_canonical_output(
    monkeypatch,
):
    import hashlib

    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )

    first = b'{"a":1}'
    second = b'{"a":1}\n'

    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: first,
    )
    first_result = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )

    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: second,
    )
    second_result = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )

    assert first_result["deadline_evidence_sha256"] == (
        hashlib.sha256(first).hexdigest()
    )
    assert second_result["deadline_evidence_sha256"] == (
        hashlib.sha256(second).hexdigest()
    )
    assert (
        first_result["deadline_evidence_sha256"]
        != second_result["deadline_evidence_sha256"]
    )


def test_d1b1_t09_validation_precedes_directory_and_network(
    monkeypatch,
    tmp_path,
):
    import pytest

    snapshots = _d1b1_snapshots()
    events = []

    class ForbiddenSession:
        def get(self, *args, **kwargs):
            events.append("network")
            raise AssertionError("network must not be reached")

    def reject(evidence):
        events.append("validate")
        raise ValueError("reject before publication")

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        reject,
    )

    with pytest.raises(
        ValueError,
        match="reject before publication",
    ):
        snapshots.collect_pre_night_program_snapshot(
            "2026-07-29",
            tmp_path,
            session=ForbiddenSession(),
            deadline_evidence={"invalid": True},
        )

    assert events == ["validate"]
    assert not (tmp_path / "snapshots").exists()
    assert list(tmp_path.rglob("*")) == []


def test_d1b1_t10_validation_is_before_first_publication_call():
    import inspect

    snapshots = _d1b1_snapshots()
    source = inspect.getsource(
        snapshots.collect_pre_night_program_snapshot
    )

    validation_index = source.index(
        "_build_deadline_evidence_metadata("
    )
    directory_index = source.index("directory.mkdir(")
    network_index = source.index("client.get(")
    temporary_archive_index = source.index(
        'temporary_archive.open("xb")'
    )
    temporary_metadata_index = source.index(
        "temporary_metadata.open("
    )

    assert validation_index < directory_index
    assert validation_index < network_index
    assert validation_index < temporary_archive_index
    assert validation_index < temporary_metadata_index


def test_d1b1_t11_raw_bytes_are_not_added_to_binding(
    monkeypatch,
):
    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: b"canonical",
    )

    metadata = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )

    assert set(metadata) == {
        "deadline_evidence",
        "deadline_evidence_sha256",
    }
    assert "raw_source_bytes" not in metadata
    assert "raw_html" not in metadata
    assert "canonical_bytes" not in metadata


def test_d1b1_t12_no_cutoff_or_safety_margin_is_generated(
    monkeypatch,
):
    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: b"canonical",
    )

    metadata = snapshots._build_deadline_evidence_metadata(
        {"input": True}
    )
    serialized = repr(metadata)

    assert "eligibility_cutoff_at" not in serialized
    assert "safety_margin_seconds" not in serialized


def test_d1b1_t13_valid_evidence_is_atomically_published_and_cached(
    monkeypatch,
    tmp_path,
):
    import datetime as dt
    import hashlib
    import json

    snapshots = _d1b1_snapshots()
    validated = _d1b1_validated_evidence()
    canonical = b"canonical-deadline-evidence"
    archive_bytes = b"test-program-archive"
    network_calls = []

    class Response:
        status_code = 200
        headers = {
            "Content-Type": "application/octet-stream",
        }

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            assert chunk_size > 0
            yield archive_bytes

        def close(self):
            return None

    class Session:
        def get(self, url, **kwargs):
            network_calls.append(url)
            return Response()

    class ForbiddenCachedSession:
        def get(self, *args, **kwargs):
            raise AssertionError(
                "cached snapshot must not perform another request"
            )

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        lambda evidence: validated,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        lambda evidence: canonical,
    )
    monkeypatch.setattr(
        snapshots,
        "validate_lzh_file",
        lambda path: None,
    )

    times = iter([
        dt.datetime(
            2026, 7, 28, 12, 0, 0,
            tzinfo=dt.timezone.utc,
        ),
        dt.datetime(
            2026, 7, 28, 12, 1, 0,
            tzinfo=dt.timezone.utc,
        ),
    ])

    first = snapshots.collect_pre_night_program_snapshot(
        "2026-07-29",
        tmp_path,
        session=Session(),
        now_fn=lambda: next(times),
        deadline_evidence={"input": True},
    )

    paths = snapshots.build_snapshot_paths(
        "2026-07-29",
        tmp_path,
    )

    assert paths["archive"].is_file()
    assert paths["metadata"].is_file()
    assert not list(paths["directory"].glob("*.part"))
    assert not (paths["directory"] / ".collection.lock").exists()

    metadata = json.loads(
        paths["metadata"].read_text(encoding="utf-8")
    )

    expected_digest = hashlib.sha256(
        canonical
    ).hexdigest()

    assert metadata["deadline_evidence"] == validated
    assert metadata["deadline_evidence_sha256"] == expected_digest
    assert first["metadata"]["deadline_evidence"] == validated
    assert (
        first["metadata"]["deadline_evidence_sha256"]
        == expected_digest
    )
    assert first["cached"] is False
    assert len(network_calls) == 1

    second = snapshots.collect_pre_night_program_snapshot(
        "2026-07-29",
        tmp_path,
        session=ForbiddenCachedSession(),
        deadline_evidence={"input": True},
    )

    assert second["cached"] is True
    assert second["metadata"]["deadline_evidence"] == validated
    assert (
        second["metadata"]["deadline_evidence_sha256"]
        == expected_digest
    )
    assert len(network_calls) == 1


def test_d1b1_t14_deadline_evidence_is_keyword_only():
    import inspect

    snapshots = _d1b1_snapshots()
    signature = inspect.signature(
        snapshots.collect_pre_night_program_snapshot
    )
    parameter = signature.parameters["deadline_evidence"]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None
# END PHASE1_D1B1_TESTS


# D1B5-STAGE1-PUBLICATION-TESTS-BEGIN


def _d1b5_publication_contract(
    monkeypatch,
    *,
    race_date="2026-07-30",
    venue_code="01",
    extra=None,
):
    payload = {
        "contract_version": "test-deadline-evidence-v1",
        "race_date": race_date,
        "venue_code": venue_code,
        "race_deadlines": [],
    }

    if extra:
        payload.update(extra)

    def validate(value):
        if not isinstance(value, dict):
            raise ValueError("invalid deadline evidence")
        return dict(value)

    def canonicalize(value):
        normalized = validate(value)
        text = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return (text + "\n").encode("utf-8")

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        validate,
    )
    monkeypatch.setattr(
        snapshots,
        "canonical_deadline_evidence_bytes",
        canonicalize,
    )

    return payload, canonicalize(payload)


def _d1b5_destination(tmp_path, race_date, venue_code):
    year, month, day = race_date.split("-")
    return (
        tmp_path
        / "prospective"
        / "pre_night"
        / "deadline_evidence"
        / year
        / month
        / day
        / venue_code
        / "deadline_evidence.json"
    )


def test_d1b5_s1_r1_t01_publishes_exact_per_venue_artifact(
    tmp_path,
    monkeypatch,
):
    payload, canonical = _d1b5_publication_contract(
        monkeypatch
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )

    assert result["publication_status"] == "CREATED"
    assert result["cached"] is False
    assert result["race_date"] == "2026-07-30"
    assert result["venue_code"] == "01"
    assert destination.read_bytes() == canonical


def test_d1b5_s1_r1_t02_uses_exact_canonical_bytes(
    tmp_path,
    monkeypatch,
):
    payload, canonical = _d1b5_publication_contract(
        monkeypatch
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert (
        result["paths"]["deadline_evidence"].read_bytes()
        == canonical
    )
    assert canonical.endswith(b"\n")
    assert not canonical.endswith(b"\n\n")


def test_d1b5_s1_r1_t03_returns_exact_byte_digest(
    tmp_path,
    monkeypatch,
):
    payload, canonical = _d1b5_publication_contract(
        monkeypatch
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert result["deadline_evidence_sha256"] == (
        hashlib.sha256(canonical).hexdigest()
    )
    assert result["byte_length"] == len(canonical)


def test_d1b5_s1_r1_t04_creates_parent_directories(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert result["paths"]["directory"].is_dir()
    assert result["paths"]["deadline_evidence"].is_file()


def test_d1b5_s1_r1_t05_reuses_identical_cache(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    first = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )
    before = first["paths"]["deadline_evidence"].stat().st_mtime_ns

    second = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )
    after = second["paths"]["deadline_evidence"].stat().st_mtime_ns

    assert second["cached"] is True
    assert second["publication_status"] == "VALIDATED_REUSE"
    assert before == after


def test_d1b5_s1_r1_t06_rejects_different_existing_bytes(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)
    destination.write_text(
        json.dumps(
            {
                **payload,
                "contract_version": "different",
            },
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshots.PreNightCacheError):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t07_rejects_malformed_cached_json(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)
    destination.write_text("{not-json", encoding="utf-8")

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="valid UTF-8 JSON",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t08_rejects_noncanonical_cached_bytes(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)
    destination.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="non-canonical",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t09_rejects_cached_race_date_mismatch(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    conflicting = {
        **payload,
        "race_date": "2026-07-31",
    }

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)
    destination.write_text(
        json.dumps(
            conflicting,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="race_date mismatch",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t10_rejects_cached_venue_mismatch(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    conflicting = {
        **payload,
        "venue_code": "02",
    }

    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)
    destination.write_text(
        json.dumps(
            conflicting,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        snapshots.PreNightCacheError,
        match="venue_code mismatch",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


@pytest.mark.parametrize(
    ("race_date", "venue_code"),
    [
        ("not-a-date", "01"),
        ("2026-07-30", "../x"),
        ("2026-07-30", "00"),
        ("2026-07-30", "25"),
        ("2026-07-30", "1"),
    ],
)
def test_d1b5_s1_r1_t11_rejects_invalid_binding_keys(
    tmp_path,
    monkeypatch,
    race_date,
    venue_code,
):
    payload, _ = _d1b5_publication_contract(
        monkeypatch,
        race_date=race_date,
        venue_code=venue_code,
    )

    with pytest.raises(snapshots.PreNightContractError):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t12_propagates_duplicate_race_rejection(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    def reject(_):
        raise ValueError("duplicate race_no")

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        reject,
    )

    with pytest.raises(ValueError, match="duplicate race_no"):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    assert not (tmp_path / "prospective").exists()


def test_d1b5_s1_r1_t13_propagates_child_identity_rejection(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    def reject(_):
        raise ValueError("child race identity mismatch")

    monkeypatch.setattr(
        snapshots,
        "validate_deadline_evidence",
        reject,
    )

    with pytest.raises(
        ValueError,
        match="child race identity mismatch",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t14_rejects_forbidden_raw_payload(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(
        monkeypatch,
        extra={"raw_html": "<html></html>"},
    )

    with pytest.raises(
        snapshots.PreNightContractError,
        match="forbidden raw payload",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t15_rejects_symlink_data_root(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    actual_root = tmp_path / "actual"
    actual_root.mkdir()
    linked_root = tmp_path / "linked"

    try:
        linked_root.symlink_to(
            actual_root,
            target_is_directory=True,
        )
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        snapshots.PreNightContractError,
        match="symlink",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            linked_root,
            deadline_evidence=payload,
        )


def test_d1b5_s1_r1_t16_success_leaves_no_temp_or_lock(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    directory = result["paths"]["directory"]
    assert not (
        directory / ".deadline_evidence.json.tmp"
    ).exists()
    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()


def test_d1b5_s1_r1_t17_failure_cleans_current_temp_and_lock(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    def fail_link(*_args, **_kwargs):
        raise OSError("injected publication failure")

    monkeypatch.setattr(snapshots.os, "link", fail_link)

    with pytest.raises(
        snapshots.PreNightIntegrityError,
        match="atomic publication failed",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    directory = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    ).parent

    assert not (
        directory / ".deadline_evidence.json.tmp"
    ).exists()
    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()


def test_d1b5_s1_r1_t18_existing_lock_fails_closed(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    directory = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    ).parent
    directory.mkdir(parents=True)

    lock = directory / ".deadline_evidence.lock"
    lock.write_text("unknown owner", encoding="utf-8")

    with pytest.raises(
        snapshots.PreNightSnapshotError,
        match="lock exists",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    assert lock.read_text(encoding="utf-8") == "unknown owner"


def test_d1b5_s1_r1_t19_performs_post_publish_validation(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    original = (
        snapshots
        ._validate_cached_deadline_evidence_artifact
    )
    calls = {"count": 0}

    def counted(**kwargs):
        calls["count"] += 1
        return original(**kwargs)

    monkeypatch.setattr(
        snapshots,
        "_validate_cached_deadline_evidence_artifact",
        counted,
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert result["publication_status"] == "CREATED"
    assert calls["count"] == 1


def test_d1b5_s1_r1_t20_does_not_publish_stage2_artifacts(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    files = [
        path.name
        for path in tmp_path.rglob("*")
        if path.is_file()
    ]

    assert files == ["deadline_evidence.json"]
    assert "deadline_evidence_collection.json" not in files
    assert "execution_manifest.json" not in files
    assert "prospective_manifest.json" not in files
    assert result["relative_path"] == (
        "prospective/pre_night/deadline_evidence/"
        "2026/07/30/01/deadline_evidence.json"
    )


def test_d1b5_stage1_public_api_is_keyword_only():
    import inspect

    signature = inspect.signature(
        snapshots.publish_pre_night_deadline_evidence
    )

    assert list(signature.parameters) == [
        "data_root",
        "deadline_evidence",
    ]
    assert signature.parameters[
        "deadline_evidence"
    ].kind is inspect.Parameter.KEYWORD_ONLY


# D1B5-S1-I2-FOCUSED-TESTS-BEGIN


def test_d1b5_s1_i2_unique_temporary_paths_are_generated(
    tmp_path,
):
    first = snapshots._d1b5_deadline_evidence_paths(
        tmp_path,
        "2026-07-30",
        "01",
    )
    second = snapshots._d1b5_deadline_evidence_paths(
        tmp_path,
        "2026-07-30",
        "01",
    )

    assert first["temporary"] != second["temporary"]
    assert first["temporary"].parent == first["directory"]
    assert second["temporary"].parent == second["directory"]
    assert first["temporary"].name.startswith(
        ".deadline_evidence.json."
    )
    assert first["temporary"].name.endswith(".tmp")


def test_d1b5_s1_i2_preserves_unrelated_temporary_file(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)
    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)

    unrelated = (
        destination.parent
        / ".deadline_evidence.json.unrelated.tmp"
    )
    unrelated.write_bytes(b"unrelated")

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert result["publication_status"] == "CREATED"
    assert unrelated.read_bytes() == b"unrelated"
    assert list(
        destination.parent.glob(
            ".deadline_evidence.json.*.tmp"
        )
    ) == [unrelated]


def test_d1b5_s1_i2_parent_directory_fsync_is_attempted(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)
    calls = []

    original = snapshots._d1b5_fsync_directory

    def counted(directory):
        calls.append(directory)
        return original(directory)

    monkeypatch.setattr(
        snapshots,
        "_d1b5_fsync_directory",
        counted,
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert calls == [result["paths"]["directory"]]


def test_d1b5_s1_i2_directory_fsync_failure_is_fail_closed(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    def fail_directory_fsync(_directory):
        raise snapshots.PreNightIntegrityError(
            "injected directory fsync failure"
        )

    monkeypatch.setattr(
        snapshots,
        "_d1b5_fsync_directory",
        fail_directory_fsync,
    )

    with pytest.raises(
        snapshots.PreNightIntegrityError,
        match="injected directory fsync failure",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    directory = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    ).parent

    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()
    assert not list(
        directory.glob(
            ".deadline_evidence.json.*.tmp"
        )
    )


def test_d1b5_s1_i2_rejects_lock_path_symlink(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)
    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )
    destination.parent.mkdir(parents=True)

    target = tmp_path / "foreign-lock"
    target.write_text("foreign", encoding="utf-8")
    lock = destination.parent / ".deadline_evidence.lock"

    try:
        lock.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        snapshots.PreNightContractError,
        match="symlink",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    assert target.read_text(encoding="utf-8") == "foreign"
    assert lock.is_symlink()


def test_d1b5_s1_i2_rejects_current_temporary_symlink(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    fixed_random = b"\x01" * 16
    monkeypatch.setattr(
        snapshots.os,
        "urandom",
        lambda _count: fixed_random,
    )

    paths = snapshots._d1b5_deadline_evidence_paths(
        tmp_path,
        "2026-07-30",
        "01",
    )
    paths["directory"].mkdir(parents=True)

    target = tmp_path / "foreign-temporary"
    target.write_text("foreign", encoding="utf-8")

    try:
        paths["temporary"].symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlink unavailable")

    with pytest.raises(
        snapshots.PreNightContractError,
        match="symlink",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    assert target.read_text(encoding="utf-8") == "foreign"
    assert paths["temporary"].is_symlink()


def test_d1b5_s1_i2_lock_close_failure_is_fail_closed(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    real_close = snapshots.os.close
    calls = {"count": 0}

    def fail_first_close(descriptor):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("injected lock close failure")
        return real_close(descriptor)

    monkeypatch.setattr(
        snapshots.os,
        "close",
        fail_first_close,
    )

    with pytest.raises(
        snapshots.PreNightIntegrityError,
        match="lock descriptor close failed",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=payload,
        )

    directory = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    ).parent

    assert calls["count"] >= 2
    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()


def test_d1b5_s1_i2_atomic_conflict_does_not_overwrite(
    tmp_path,
    monkeypatch,
):
    payload, canonical = _d1b5_publication_contract(
        monkeypatch
    )
    destination = _d1b5_destination(
        tmp_path,
        "2026-07-30",
        "01",
    )

    def competing_link(source, target):
        target.write_bytes(source.read_bytes())
        raise FileExistsError(
            "injected competing publication"
        )

    monkeypatch.setattr(
        snapshots.os,
        "link",
        competing_link,
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert destination.read_bytes() == canonical
    assert result["cached"] is True
    assert (
        result["publication_status"]
        == "VALIDATED_REUSE"
    )


def test_d1b5_s1_i2_durability_order_after_publication(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)
    events = []

    real_link = snapshots.os.link
    real_fsync_file = snapshots.fsync_file
    real_directory_fsync = (
        snapshots._d1b5_fsync_directory
    )

    def tracked_link(source, destination):
        events.append("link")
        return real_link(source, destination)

    def tracked_file_fsync(path):
        events.append("destination_fsync")
        return real_fsync_file(path)

    def tracked_directory_fsync(path):
        events.append("directory_fsync")
        return real_directory_fsync(path)

    monkeypatch.setattr(
        snapshots.os,
        "link",
        tracked_link,
    )
    monkeypatch.setattr(
        snapshots,
        "fsync_file",
        tracked_file_fsync,
    )
    monkeypatch.setattr(
        snapshots,
        "_d1b5_fsync_directory",
        tracked_directory_fsync,
    )

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    assert result["publication_status"] == "CREATED"
    assert events == [
        "link",
        "destination_fsync",
        "directory_fsync",
    ]


def test_d1b5_s1_i2_success_removes_owned_unique_temp(
    tmp_path,
    monkeypatch,
):
    payload, _ = _d1b5_publication_contract(monkeypatch)

    result = snapshots.publish_pre_night_deadline_evidence(
        tmp_path,
        deadline_evidence=payload,
    )

    directory = result["paths"]["directory"]

    assert not list(
        directory.glob(
            ".deadline_evidence.json.*.tmp"
        )
    )
    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()


# D1B5-S1-I2-FOCUSED-TESTS-END


# D1B5-STAGE1-PUBLICATION-TESTS-END
