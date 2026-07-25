from __future__ import annotations

import builtins
import copy
import json
import socket

import pytest

from boatrace_ai.ingestion.historical_provenance_exceptions import (
    HISTORICAL_PROVENANCE_EXCEPTION_CONTRACT_VERSION,
    PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY,
    build_historical_provenance_exception_manifest,
    historical_provenance_exceptions,
    serialize_historical_provenance_exception_manifest,
    validate_historical_provenance_exception_manifest,
)


EXPECTED_PATHS = [
    "raw/programs/b260630.lzh",
    "raw/racer_stats/fan2604.lzh",
    "raw/results/k260630.lzh",
]

EXPECTED_INTEGRITY = {
    "raw/programs/b260630.lzh": (
        32860,
        "2c759c141bc4733b10a652f603927d87"
        "b2ec3e1025b8da7a10e81d60921c3085",
    ),
    "raw/results/k260630.lzh": (
        40017,
        "3e5247ce2b3f8ce0957bf3fce6bc3c6e"
        "b8ae1bbe09c4d1f814afa7d3087715c2",
    ),
    "raw/racer_stats/fan2604.lzh": (
        183497,
        "5c1e21cfe5a4dd53ea26eb07aca4905c"
        "4c20c3160b4a206ba96e92702eb4b46d",
    ),
}


def test_contract_version_is_fixed() -> None:
    assert (
        HISTORICAL_PROVENANCE_EXCEPTION_CONTRACT_VERSION
        == "historical_provenance_exception_v1"
    )


def test_exactly_three_entries_exist() -> None:
    assert len(historical_provenance_exceptions()) == 3


def test_paths_and_source_types_are_exact() -> None:
    entries = historical_provenance_exceptions()

    assert sorted(entry.relative_path for entry in entries) == EXPECTED_PATHS
    assert {
        entry.relative_path: entry.source_type
        for entry in entries
    } == {
        "raw/programs/b260630.lzh": "program",
        "raw/results/k260630.lzh": "result",
        "raw/racer_stats/fan2604.lzh": "racer_stats",
    }


def test_integrity_values_are_exact() -> None:
    entries = historical_provenance_exceptions()

    assert {
        entry.relative_path: (
            entry.size_bytes,
            entry.sha256,
        )
        for entry in entries
    } == EXPECTED_INTEGRITY


def test_every_entry_is_historical_research_only() -> None:
    for entry in historical_provenance_exceptions():
        assert (
            entry.provenance_status
            == PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY
        )
        assert entry.eligibility_reason == "PROVENANCE_MISSING"


def test_every_entry_is_ineligible_for_pre_night() -> None:
    assert all(
        entry.eligible_for_pre_night is False
        for entry in historical_provenance_exceptions()
    )


def test_acquisition_provenance_fields_remain_none() -> None:
    for entry in historical_provenance_exceptions():
        assert entry.fetched_at is None
        assert entry.source_url is None
        assert entry.collector_version is None
        assert entry.acquisition_method is None


def test_archive_contract_contains_no_modified_timestamp() -> None:
    for entry in historical_provenance_exceptions():
        data = entry.to_dict()
        lowered_keys = {key.lower() for key in data}

        assert "modified" not in lowered_keys
        assert "modified_at" not in lowered_keys
        assert "mtime" not in lowered_keys
        assert "archive_member_modified_at" not in lowered_keys


def test_archive_fields_are_exact() -> None:
    entries = historical_provenance_exceptions()

    assert all(entry.archive_format == "LZH" for entry in entries)
    assert all(entry.compression_method == "-lh5-" for entry in entries)
    assert {
        entry.archive_member_name
        for entry in entries
    } == {
        "B260630.TXT",
        "K260630.TXT",
        "fan2604.txt",
    }


def test_manifest_schema_and_counts_are_exact() -> None:
    manifest = build_historical_provenance_exception_manifest()

    assert manifest["production_eligible_entry_count"] == 0
    assert manifest["historical_research_only_entry_count"] == 3
    assert manifest["fetched_at_reconstruction_performed"] is False
    assert manifest["filesystem_mtime_used"] is False
    assert (
        manifest["archive_member_timestamp_used_as_fetched_at"]
        is False
    )


def test_serialization_is_deterministic() -> None:
    first = serialize_historical_provenance_exception_manifest()
    second = serialize_historical_provenance_exception_manifest()

    assert first == second
    assert json.loads(first) == (
        build_historical_provenance_exception_manifest()
    )


def test_validator_accepts_exact_manifest() -> None:
    validate_historical_provenance_exception_manifest(
        build_historical_provenance_exception_manifest()
    )


def test_validator_rejects_duplicate_and_unknown_paths() -> None:
    duplicate = build_historical_provenance_exception_manifest()
    duplicate["entries"].append(
        copy.deepcopy(duplicate["entries"][0])
    )

    with pytest.raises(ValueError):
        validate_historical_provenance_exception_manifest(duplicate)

    unknown = build_historical_provenance_exception_manifest()
    unknown["entries"][0]["relative_path"] = "raw/unknown/archive.lzh"

    with pytest.raises(ValueError):
        validate_historical_provenance_exception_manifest(unknown)


def test_validator_rejects_changed_hash_or_size() -> None:
    for field, value in (
        ("sha256", "0" * 64),
        ("size_bytes", 1),
    ):
        manifest = build_historical_provenance_exception_manifest()
        manifest["entries"][0][field] = value

        with pytest.raises(ValueError):
            validate_historical_provenance_exception_manifest(manifest)


def test_validator_rejects_inferred_provenance() -> None:
    for field, value in (
        ("fetched_at", "2026-06-29T21:30:00+09:00"),
        ("source_url", "https://example.invalid/archive.lzh"),
        ("collector_version", "guessed"),
        ("acquisition_method", "guessed"),
    ):
        manifest = build_historical_provenance_exception_manifest()
        manifest["entries"][0][field] = value

        with pytest.raises(ValueError):
            validate_historical_provenance_exception_manifest(manifest)


def test_contract_requires_no_file_or_network_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("external access is prohibited")

    monkeypatch.setattr(builtins, "open", fail)
    monkeypatch.setattr(socket, "create_connection", fail)

    manifest = build_historical_provenance_exception_manifest()
    validate_historical_provenance_exception_manifest(manifest)

    assert len(manifest["entries"]) == 3
