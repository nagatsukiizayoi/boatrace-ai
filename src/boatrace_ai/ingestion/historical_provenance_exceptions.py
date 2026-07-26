"""Contracts for historical archives with missing acquisition provenance.

The entries in this module are historical research exceptions only.

They do not reconstruct or infer:

* fetched_at
* source_url
* collector_version
* acquisition method
* PRE_NIGHT production eligibility

Filesystem mtimes and archive-member timestamps are deliberately absent
from this contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping


HISTORICAL_PROVENANCE_EXCEPTION_CONTRACT_VERSION = (
    "historical_provenance_exception_v1"
)

PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY = (
    "PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY"
)

ELIGIBILITY_REASON_PROVENANCE_MISSING = "PROVENANCE_MISSING"

_ALLOWED_USES = (
    "SHA256_AND_SIZE_VERIFICATION",
    "ARCHIVE_FORMAT_INSPECTION",
    "HISTORICAL_RESEARCH",
    "EXCEPTION_MANIFEST_RECORDING",
)

_PROHIBITED_USES = (
    "PRE_NIGHT_PRODUCTION_FEATURE_SOURCE",
    "PRODUCTION_ELIGIBILITY_EVIDENCE",
    "FETCHED_AT_RECONSTRUCTION",
    "SOURCE_URL_RECONSTRUCTION",
    "ACQUISITION_METHOD_RECONSTRUCTION",
)


@dataclass(frozen=True)
class HistoricalProvenanceException:
    """Immutable historical provenance exception entry."""

    relative_path: str
    source_type: str
    size_bytes: int
    sha256: str
    archive_format: str
    archive_member_name: str
    compression_method: str
    provenance_status: str = (
        PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY
    )
    eligible_for_pre_night: bool = False
    eligibility_reason: str = (
        ELIGIBILITY_REASON_PROVENANCE_MISSING
    )
    fetched_at: None = None
    source_url: None = None
    collector_version: None = None
    acquisition_method: None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-serializable representation."""

        return {
            "relative_path": self.relative_path,
            "source_type": self.source_type,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "archive_format": self.archive_format,
            "archive_member_name": self.archive_member_name,
            "compression_method": self.compression_method,
            "provenance_status": self.provenance_status,
            "eligible_for_pre_night": (
                self.eligible_for_pre_night
            ),
            "eligibility_reason": self.eligibility_reason,
            "fetched_at": self.fetched_at,
            "source_url": self.source_url,
            "collector_version": self.collector_version,
            "acquisition_method": self.acquisition_method,
            "allowed_uses": list(_ALLOWED_USES),
            "prohibited_uses": list(_PROHIBITED_USES),
        }


_EXCEPTIONS = (
    HistoricalProvenanceException(
        relative_path="raw/programs/b260630.lzh",
        source_type="program",
        size_bytes=32860,
        sha256=(
            "2c759c141bc4733b10a652f603927d87"
            "b2ec3e1025b8da7a10e81d60921c3085"
        ),
        archive_format="LZH",
        archive_member_name="B260630.TXT",
        compression_method="-lh5-",
    ),
    HistoricalProvenanceException(
        relative_path="raw/results/k260630.lzh",
        source_type="result",
        size_bytes=40017,
        sha256=(
            "3e5247ce2b3f8ce0957bf3fce6bc3c6e"
            "b8ae1bbe09c4d1f814afa7d3087715c2"
        ),
        archive_format="LZH",
        archive_member_name="K260630.TXT",
        compression_method="-lh5-",
    ),
    HistoricalProvenanceException(
        relative_path="raw/racer_stats/fan2604.lzh",
        source_type="racer_stats",
        size_bytes=183497,
        sha256=(
            "5c1e21cfe5a4dd53ea26eb07aca4905c"
            "4c20c3160b4a206ba96e92702eb4b46d"
        ),
        archive_format="LZH",
        archive_member_name="fan2604.txt",
        compression_method="-lh5-",
    ),
)


def historical_provenance_exceptions(
) -> tuple[HistoricalProvenanceException, ...]:
    """Return the fixed immutable historical exception entries."""

    return _EXCEPTIONS


def build_historical_provenance_exception_manifest(
) -> dict[str, Any]:
    """Build a deterministic in-memory manifest.

    This function performs no file, network, clock or environment access.
    """

    return {
        "contract_version": (
            HISTORICAL_PROVENANCE_EXCEPTION_CONTRACT_VERSION
        ),
        "policy_status": (
            PROVENANCE_MISSING_HISTORICAL_RESEARCH_ONLY
        ),
        "production_eligible_entry_count": 0,
        "historical_research_only_entry_count": len(_EXCEPTIONS),
        "fetched_at_reconstruction_performed": False,
        "filesystem_mtime_used": False,
        "archive_member_timestamp_used_as_fetched_at": False,
        "source_url_inference_performed": False,
        "acquisition_method_inference_performed": False,
        "entries": [
            entry.to_dict()
            for entry in _EXCEPTIONS
        ],
    }


def validate_historical_provenance_exception_manifest(
    manifest: Mapping[str, Any],
) -> None:
    """Validate an exact historical exception manifest.

    Raises:
        TypeError: If the manifest is not mapping-like.
        ValueError: If any contract field or entry differs.
    """

    if not isinstance(manifest, Mapping):
        raise TypeError("manifest must be a mapping")

    expected = build_historical_provenance_exception_manifest()

    if manifest.get("contract_version") != expected["contract_version"]:
        raise ValueError("unsupported contract_version")

    if manifest.get("policy_status") != expected["policy_status"]:
        raise ValueError("invalid policy_status")

    entries = manifest.get("entries")

    if not isinstance(entries, list):
        raise ValueError("entries must be a list")

    paths = [
        entry.get("relative_path")
        for entry in entries
        if isinstance(entry, Mapping)
    ]

    if len(paths) != len(entries):
        raise ValueError("every entry must be a mapping")

    if len(paths) != len(set(paths)):
        raise ValueError("duplicate relative_path")

    expected_entries = {
        entry["relative_path"]: entry
        for entry in expected["entries"]
    }

    actual_paths = set(paths)
    expected_paths = set(expected_entries)

    if actual_paths != expected_paths:
        raise ValueError("unknown or missing relative_path")

    for entry in entries:
        path = entry["relative_path"]

        if dict(entry) != expected_entries[path]:
            raise ValueError(
                f"entry does not match fixed contract: {path}"
            )

    if dict(manifest) != expected:
        raise ValueError("manifest does not match fixed contract")


def serialize_historical_provenance_exception_manifest() -> str:
    """Return stable canonical JSON without generation timestamps."""

    manifest = build_historical_provenance_exception_manifest()
    validate_historical_provenance_exception_manifest(manifest)

    return json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
