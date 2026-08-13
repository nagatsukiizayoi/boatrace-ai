"""Stage 1 R2 Deadline Evidence publication tests."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from boatrace_ai.ingestion import pre_night_deadlines as deadlines
from boatrace_ai.ingestion import pre_night_snapshots as snapshots
from boatrace_ai.pipelines import pre_night_daily as daily

# Approved Stage 1 R2 authorization traceability.
APPROVED_CONTRACT_ID = "D1B5-STAGE1-DEADLINE-EVIDENCE-PUBLICATION-V2-R2-APPROVED"


RAW_SOURCE = b"<html>official deadline source</html>"
JST = timezone(timedelta(hours=9))


def valid_evidence(
    *,
    source_name="Official race page",
):
    fetched = datetime(
        2026,
        8,
        20,
        8,
        0,
        tzinfo=JST,
    )

    return deadlines.build_deadline_evidence(
        raw_source_bytes=RAW_SOURCE,
        source_locator=(
            "https://www.boatrace.jp/official/"
            "deadline?jcd=01&hd=20260820"
        ),
        source_name=source_name,
        source_authority="BOAT RACE振興会",
        request_started_at=(
            fetched - timedelta(seconds=2)
        ),
        fetched_at=fetched,
        http_status=200,
        response_headers={
            "Content-Type": (
                "text/html; charset=UTF-8"
            ),
        },
        race_date="2026-08-20",
        venue_code="01",
        source_timezone="Asia/Tokyo",
        race_deadlines=[
            {
                "race_no": race_no,
                "deadline_kind": (
                    deadlines.DEADLINE_KIND
                ),
                "scheduled_deadline_at": (
                    fetched
                    + timedelta(
                        hours=1,
                        minutes=race_no,
                    )
                ),
            }
            for race_no in range(1, 13)
        ],
    )


def publication_directory(tmp_path):
    return (
        tmp_path
        / "prospective"
        / "pre_night"
        / "deadline_evidence"
        / "2026"
        / "08"
        / "20"
        / "01"
    )


# T-M06-ATOMIC-PUBLISH-001
def test_m06_atomic_publish(tmp_path):
    evidence = valid_evidence()

    canonical = (
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
    )

    result = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
    )

    destination = (
        result["paths"]["deadline_evidence"]
    )

    assert result["publication_status"] == "CREATED"
    assert result["cached"] is False
    assert destination.read_bytes() == canonical


# T-M06-INCOMPLETE-ISOLATION-001
def test_m06_incomplete_isolation(
    tmp_path,
    monkeypatch,
):
    evidence = valid_evidence()

    def fail_link(*_args, **_kwargs):
        raise OSError(
            "injected publication failure"
        )

    monkeypatch.setattr(
        snapshots.os,
        "link",
        fail_link,
    )

    with pytest.raises(
        snapshots.PreNightIntegrityError,
        match="atomic publication failed",
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )

    directory = publication_directory(tmp_path)

    assert not (
        directory / "deadline_evidence.json"
    ).exists()

    assert not (
        directory / ".deadline_evidence.lock"
    ).exists()

    assert list(
        directory.glob(
            ".deadline_evidence.json.*.tmp"
        )
    ) == []


# T-M06-RERUN-CLEANUP-001
def test_m06_rerun_preserves_unowned_temp(
    tmp_path,
):
    evidence = valid_evidence()
    directory = publication_directory(tmp_path)

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    unowned = (
        directory
        / ".deadline_evidence.json.unowned.tmp"
    )

    unowned.write_bytes(b"unowned")

    result = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
    )

    assert result["publication_status"] == "CREATED"
    assert unowned.read_bytes() == b"unowned"


# T-M08-VALIDATED-REUSE-001
def test_m08_validated_reuse(tmp_path):
    evidence = valid_evidence()

    first = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
    )

    destination = (
        first["paths"]["deadline_evidence"]
    )

    before = destination.stat().st_mtime_ns

    second = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
    )

    assert second["publication_status"] == (
        "VALIDATED_REUSE"
    )

    assert second["cached"] is True
    assert destination.stat().st_mtime_ns == before


# T-M08-CONFLICT-FAIL-CLOSED-001
def test_m08_conflict_fails_without_overwrite(
    tmp_path,
):
    requested = valid_evidence()

    conflicting = valid_evidence(
        source_name="Different source"
    )

    created = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=conflicting,
        )
    )

    destination = (
        created["paths"]["deadline_evidence"]
    )

    before = destination.read_bytes()

    with pytest.raises(
        snapshots.PreNightCacheError
    ):
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=requested,
        )

    assert destination.read_bytes() == before


# T-M08-IDEMPOTENT-REUSE-001
def test_m08_repeated_reuse_is_idempotent(
    tmp_path,
):
    evidence = valid_evidence()

    results = [
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
        for _ in range(3)
    ]

    assert (
        results[0]["publication_status"]
        == "CREATED"
    )

    assert [
        result["publication_status"]
        for result in results[1:]
    ] == [
        "VALIDATED_REUSE",
        "VALIDATED_REUSE",
    ]

    assert len({
        result["deadline_evidence_sha256"]
        for result in results
    }) == 1


# T-M09-CANONICAL-JSON-001
def test_m09_canonical_json():
    evidence = valid_evidence()

    reordered = dict(
        reversed(list(evidence.items()))
    )

    first = (
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
    )

    second = (
        deadlines.canonical_deadline_evidence_bytes(
            reordered
        )
    )

    assert first == second
    assert first.endswith(b"\n")
    assert not first.endswith(b"\n\n")
    assert not first.startswith(b"\xef\xbb\xbf")

    assert (
        json.loads(first.decode("utf-8"))
        == evidence
    )


# T-M09-UNSUPPORTED-VALUE-001
def test_m09_unsupported_value_fails_closed():
    evidence = valid_evidence()

    evidence["unsupported"] = float("nan")

    with pytest.raises(
        deadlines.PreNightDeadlineEvidenceError
    ):
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )


# T-M09-DETERMINISTIC-BYTES-001
def test_m09_deterministic_bytes():
    evidence = valid_evidence()

    outputs = {
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
        for _ in range(10)
    }

    assert len(outputs) == 1


# T-M10-SHA256-PUBLICATION-001
def test_m10_sha256_of_published_bytes(
    tmp_path,
):
    evidence = valid_evidence()

    canonical = (
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
    )

    expected = hashlib.sha256(
        canonical
    ).hexdigest()

    result = (
        snapshots.publish_pre_night_deadline_evidence(
            tmp_path,
            deadline_evidence=evidence,
        )
    )

    destination = (
        result["paths"]["deadline_evidence"]
    )

    assert (
        result["deadline_evidence_sha256"]
        == expected
    )

    assert hashlib.sha256(
        destination.read_bytes()
    ).hexdigest() == expected


# T-M10-PUBLICATION-ORDER-001
def test_publication_precedes_collector(
    tmp_path,
    monkeypatch,
):
    evidence = valid_evidence()
    events = []

    original_publisher = (
        daily.publish_pre_night_deadline_evidence
    )

    def recording_publisher(*args, **kwargs):
        result = original_publisher(
            *args,
            **kwargs,
        )
        events.append("publication_completed")
        return result

    class CollectorReached(RuntimeError):
        pass

    def recording_collector(*_args, **_kwargs):
        events.append("collector_called")
        raise CollectorReached(
            "collector reached after publication"
        )

    monkeypatch.setattr(
        daily,
        "publish_pre_night_deadline_evidence",
        recording_publisher,
    )

    before_cutoff = datetime(
        2026,
        8,
        19,
        0,
        0,
        tzinfo=JST,
    )

    with pytest.raises(
        CollectorReached,
        match="collector reached after publication",
    ):
        daily.run_pre_night_daily(
            "2026-08-20",
            tmp_path,
            dry_run=False,
            collector=recording_collector,
            now_fn=lambda: before_cutoff,
            deadline_evidence=evidence,
        )

    assert events == [
        "publication_completed",
        "collector_called",
    ]


# T-M10-DIGEST-MISMATCH-GUARD-001
def test_digest_mismatch_blocks_collector(
    tmp_path,
    monkeypatch,
):
    evidence = valid_evidence()
    collector_called = False

    def mismatched_publisher(*_args, **_kwargs):
        return {
            "publication_status": "CREATED",
            "cached": False,
            "deadline_evidence_sha256": "0" * 64,
            "paths": {
                "deadline_evidence": (
                    tmp_path / "not-used.json"
                ),
            },
        }

    def recording_collector(*_args, **_kwargs):
        nonlocal collector_called
        collector_called = True
        raise AssertionError(
            "collector must not run after digest mismatch"
        )

    monkeypatch.setattr(
        daily,
        "publish_pre_night_deadline_evidence",
        mismatched_publisher,
    )

    before_cutoff = datetime(
        2026,
        8,
        19,
        0,
        0,
        tzinfo=JST,
    )

    with pytest.raises(
        daily.PreNightDailyIntegrityError,
        match=(
            "Published deadline evidence "
            "digest mismatch"
        ),
    ):
        daily.run_pre_night_daily(
            "2026-08-20",
            tmp_path,
            dry_run=False,
            collector=recording_collector,
            now_fn=lambda: before_cutoff,
            deadline_evidence=evidence,
        )

    assert collector_called is False


# T-M10-STABLE-DIGEST-001
def test_m10_stable_digest():
    evidence = valid_evidence()

    reordered = dict(
        reversed(list(evidence.items()))
    )

    first = hashlib.sha256(
        deadlines.canonical_deadline_evidence_bytes(
            evidence
        )
    ).hexdigest()

    second = hashlib.sha256(
        deadlines.canonical_deadline_evidence_bytes(
            reordered
        )
    ).hexdigest()

    assert first == second
    assert len(first) == 64
