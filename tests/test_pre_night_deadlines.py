from __future__ import annotations

import ast
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from boatrace_ai.ingestion import pre_night_deadlines as deadlines


RAW = b"<html><body>deadline schedule</body></html>"
JST = timezone(timedelta(hours=9))


def valid_kwargs():
    fetched = datetime(2026, 7, 28, 8, 0, tzinfo=JST)

    return {
        "raw_source_bytes": RAW,
        "source_locator": (
            "https://www.boatrace.jp/owpc/pc/race/racelist"
            "?rno=1&jcd=01&hd=20260728"
        ),
        "source_name": "BOAT RACE official race page",
        "source_authority": "BOAT RACE振興会",
        "request_started_at": (
            datetime(2026, 7, 28, 7, 59, 58, tzinfo=JST)
        ),
        "fetched_at": fetched,
        "http_status": 200,
        "response_headers": {
            "Content-Type": "text/html; charset=UTF-8",
            "ETag": '"example"',
        },
        "race_date": "2026-07-28",
        "venue_code": "01",
        "source_timezone": "explicit-source-timezone-evidence",
        "race_deadlines": [
            {
                "race_no": race_no,
                "deadline_kind": deadlines.DEADLINE_KIND,
                "scheduled_deadline_at": (
                    fetched + timedelta(hours=1, minutes=race_no)
                ),
            }
            for race_no in range(1, 13)
        ],
    }


def build_valid():
    return deadlines.build_deadline_evidence(**valid_kwargs())


def test_accepts_complete_valid_12_race_input():
    evidence = build_valid()
    assert [x["race_no"] for x in evidence["race_deadlines"]] == list(
        range(1, 13)
    )


def test_output_is_deterministic():
    first = valid_kwargs()
    second = valid_kwargs()
    second["race_deadlines"] = list(
        reversed(second["race_deadlines"])
    )
    second["response_headers"] = {
        "ETag": '"example"',
        "Content-Type": "text/html; charset=UTF-8",
    }

    assert (
        deadlines.build_deadline_evidence(**first)
        == deadlines.build_deadline_evidence(**second)
    )


def test_canonical_bytes_are_deterministic():
    first = build_valid()
    second = dict(reversed(list(first.items())))
    assert (
        deadlines.canonical_deadline_evidence_bytes(first)
        == deadlines.canonical_deadline_evidence_bytes(second)
    )


def test_canonical_bytes_have_exactly_one_trailing_lf():
    output = deadlines.canonical_deadline_evidence_bytes(build_valid())
    assert output.endswith(b"\n")
    assert not output.endswith(b"\n\n")


def test_canonical_bytes_use_utf8_without_bom():
    output = deadlines.canonical_deadline_evidence_bytes(build_valid())
    assert not output.startswith(b"\xef\xbb\xbf")
    assert "振興会" in output.decode("utf-8")


def test_canonical_bytes_reject_nan():
    evidence = build_valid()
    evidence["http_status"] = float("nan")

    with pytest.raises(deadlines.PreNightDeadlineEvidenceError):
        deadlines.canonical_deadline_evidence_bytes(evidence)


def test_rejects_empty_raw_source_bytes():
    kwargs = valid_kwargs()
    kwargs["raw_source_bytes"] = b""

    with pytest.raises(deadlines.PreNightDeadlineIntegrityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_verifies_raw_source_sha256():
    evidence = build_valid()
    validated = deadlines.validate_deadline_evidence(
        evidence,
        raw_source_bytes=RAW,
    )
    assert validated == evidence


def test_rejects_raw_source_sha256_mismatch():
    evidence = build_valid()

    with pytest.raises(deadlines.PreNightDeadlineIntegrityError):
        deadlines.validate_deadline_evidence(
            evidence,
            raw_source_bytes=b"different",
        )


def test_rejects_empty_source_locator():
    kwargs = valid_kwargs()
    kwargs["source_locator"] = ""

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.build_deadline_evidence(**kwargs)


@pytest.mark.parametrize(
    "field",
    ["source_name", "source_authority"],
)
def test_rejects_empty_source_identity(field):
    kwargs = valid_kwargs()
    kwargs[field] = ""

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_non_success_http_status():
    kwargs = valid_kwargs()
    kwargs["http_status"] = 404

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_naive_request_started_at():
    kwargs = valid_kwargs()
    kwargs["request_started_at"] = datetime(2026, 7, 28, 7, 59)

    with pytest.raises(deadlines.PreNightDeadlineTimestampError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_naive_fetched_at():
    kwargs = valid_kwargs()
    kwargs["fetched_at"] = datetime(2026, 7, 28, 8, 0)

    with pytest.raises(deadlines.PreNightDeadlineTimestampError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_request_after_fetch():
    kwargs = valid_kwargs()
    kwargs["request_started_at"] = (
        kwargs["fetched_at"] + timedelta(seconds=1)
    )

    with pytest.raises(deadlines.PreNightDeadlineTimestampError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_invalid_race_date():
    kwargs = valid_kwargs()
    kwargs["race_date"] = "2026-02-30"

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_invalid_venue_code():
    kwargs = valid_kwargs()
    kwargs["venue_code"] = "1"

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_boolean_race_number():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][0]["race_no"] = True

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_race_number_outside_1_to_12():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][0]["race_no"] = 13

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_duplicate_race_number():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][1]["race_no"] = 1

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_missing_race_number():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"] = kwargs["race_deadlines"][:-1]

    with pytest.raises(deadlines.PreNightDeadlineIdentityError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_unknown_top_level_field():
    evidence = build_valid()
    evidence["unknown"] = "forbidden"

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.validate_deadline_evidence(evidence)


def test_rejects_unknown_per_race_field():
    evidence = build_valid()
    evidence["race_deadlines"][0]["unknown"] = "forbidden"

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.validate_deadline_evidence(evidence)


def test_rejects_naive_scheduled_deadline():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][0]["scheduled_deadline_at"] = (
        datetime(2026, 7, 28, 10, 0)
    )

    with pytest.raises(deadlines.PreNightDeadlineTimestampError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_deadline_not_after_fetch():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][0]["scheduled_deadline_at"] = (
        kwargs["fetched_at"]
    )

    with pytest.raises(deadlines.PreNightDeadlineTimestampError):
        deadlines.build_deadline_evidence(**kwargs)


def test_rejects_wrong_deadline_kind():
    kwargs = valid_kwargs()
    kwargs["race_deadlines"][0]["deadline_kind"] = "RACE_START"

    with pytest.raises(deadlines.PreNightDeadlineSchemaError):
        deadlines.build_deadline_evidence(**kwargs)


def test_does_not_infer_official_timezone():
    kwargs = valid_kwargs()
    marker = "explicit-unverified-source-timezone"
    kwargs["source_timezone"] = marker

    evidence = deadlines.build_deadline_evidence(**kwargs)

    assert evidence["source_timezone"] == marker
    assert evidence["source_timezone"] != "Asia/Tokyo"


def test_does_not_activate_safety_margin():
    evidence = build_valid()

    assert "safety_margin_seconds" not in evidence
    assert "eligibility_cutoff_at" not in evidence

    for race in evidence["race_deadlines"]:
        assert "safety_margin_seconds" not in race
        assert "eligibility_cutoff_at" not in race


def _module_tree():
    return ast.parse(
        Path(deadlines.__file__).read_text(encoding="utf-8")
    )


def test_module_performs_no_network_access():
    tree = _module_tree()
    forbidden_modules = {
        "requests",
        "httpx",
        "urllib",
        "socket",
        "aiohttp",
    }

    imported = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(
                alias.name.split(".")[0]
                for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    assert imported.isdisjoint(forbidden_modules)


def test_module_performs_no_filesystem_publication():
    tree = _module_tree()
    forbidden_calls = {
        "open",
        "write",
        "write_text",
        "write_bytes",
        "mkdir",
        "replace",
        "rename",
        "unlink",
        "remove",
        "dump",
    }

    called = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if isinstance(node.func, ast.Name):
            called.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            called.add(node.func.attr)

    # json.dumps is permitted; json.dump is not.
    assert called.isdisjoint(forbidden_calls)
