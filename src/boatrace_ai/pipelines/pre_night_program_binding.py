"""Stage 3 immutable Program Entries PIT binding publication."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import numbers
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from boatrace_ai.pipelines.pre_night_deadline_collection import (
    COLLECTION_CONTRACT_VERSION,
)


PROGRAM_BINDING_CONTRACT_VERSION = (
    "pre_night_program_entries_binding_v1"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
)

_COLLECTION_KEYS = {
    "contract_version",
    "race_date",
    "expected_venue_codes",
    "entry_count",
    "entries",
}

_COLLECTION_ENTRY_KEYS = {
    "race_date",
    "venue_code",
    "relative_path",
    "deadline_evidence_sha256",
    "byte_length",
    "contract_version",
}

_BINDING_KEYS = {
    "contract_version",
    "race_date",
    "deadline_evidence_collection_sha256",
    "venue_bindings",
}

_VENUE_BINDING_KEYS = {
    "deadline_evidence_sha256",
    "program_source_sha256",
    "races",
    "binding_sha256",
}

_REQUIRED_PROGRAM_IDENTITY_FIELDS = {
    "race_date",
    "venue_code",
    "race_no",
    "boat_no",
}


class PreNightProgramBindingError(Exception):
    """Base Stage 3 Program Entries binding error."""


class PreNightProgramBindingContractError(
    PreNightProgramBindingError
):
    """Invalid caller input, identity or unsafe path."""


class PreNightProgramBindingCacheError(
    PreNightProgramBindingError
):
    """Existing Stage 2 or Stage 3 artifact is invalid."""


class PreNightProgramBindingIntegrityError(
    PreNightProgramBindingError
):
    """Publication or durability operation failed."""


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise PreNightProgramBindingContractError(
            "binding value is not canonical JSON compatible"
        ) from error

    return (text + "\n").encode("utf-8")


def _require_race_date(value: Any) -> str:
    if not isinstance(value, str):
        raise PreNightProgramBindingContractError(
            "race_date must be string"
        )

    try:
        normalized = dt.date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as error:
        raise PreNightProgramBindingContractError(
            "race_date is invalid"
        ) from error

    if normalized != value:
        raise PreNightProgramBindingContractError(
            "race_date must be canonical ISO date"
        )

    return value


def _require_venue_code(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 2
        or not value.isascii()
        or not value.isdigit()
        or not 1 <= int(value) <= 24
    ):
        raise PreNightProgramBindingContractError(
            "venue_code must be 01 through 24"
        )

    return value


def _require_sha256(
    value: Any,
    field_name: str,
) -> str:
    if (
        not isinstance(value, str)
        or _SHA256_RE.fullmatch(value) is None
    ):
        raise PreNightProgramBindingContractError(
            f"{field_name} must be 64 lowercase hexadecimal characters"
        )

    return value


def _require_run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value in {".", ".."}
        or _RUN_ID_RE.fullmatch(value) is None
    ):
        raise PreNightProgramBindingContractError(
            "run_id contains forbidden characters"
        )

    return value


def _require_integer(
    value: Any,
    *,
    field_name: str,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, numbers.Integral)
    ):
        raise PreNightProgramBindingContractError(
            f"{field_name} must be integer"
        )

    normalized = int(value)

    if not minimum <= normalized <= maximum:
        raise PreNightProgramBindingContractError(
            f"{field_name} must be between {minimum} and {maximum}"
        )

    return normalized


def _binding_paths(
    data_root,
    *,
    race_date: str,
    run_id: str,
) -> dict[str, Path]:
    root = Path(data_root)
    date_value = dt.date.fromisoformat(race_date)

    collection = (
        root
        / "prospective"
        / "pre_night"
        / "deadline_evidence_collections"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
        / "deadline_evidence_collection.json"
    )

    directory = (
        root
        / "prospective"
        / "pre_night"
        / "runs"
        / f"{date_value.year:04d}"
        / f"{date_value.month:02d}"
        / f"{date_value.day:02d}"
        / run_id
    )

    destination = directory / "program_entries_binding.json"
    uid = os.urandom(16).hex()

    return {
        "root": root,
        "collection": collection,
        "directory": directory,
        "destination": destination,
        "lock": directory / ".program_entries_binding.lock",
        "temporary": (
            directory
            / f".program_entries_binding.json.{uid}.tmp"
        ),
    }


def _assert_safe_path(
    *,
    root: Path,
    target: Path,
    label: str,
) -> None:
    if root.exists() and root.is_symlink():
        raise PreNightProgramBindingContractError(
            "data_root must not be symlink"
        )

    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise PreNightProgramBindingContractError(
            f"{label} path escapes data_root"
        ) from error

    root_resolved = root.resolve(strict=False)
    target_resolved = target.resolve(strict=False)

    try:
        target_resolved.relative_to(root_resolved)
    except ValueError as error:
        raise PreNightProgramBindingContractError(
            f"{label} resolved path escapes data_root"
        ) from error

    current = root

    for component in relative.parts:
        current = current / component

        if current.exists() and current.is_symlink():
            raise PreNightProgramBindingContractError(
                f"{label} path contains symlink: {current}"
            )


def _assert_safe_paths(paths: Mapping[str, Path]) -> None:
    root = paths["root"]
    directory = paths["directory"]

    for label in (
        "collection",
        "directory",
        "destination",
        "lock",
        "temporary",
    ):
        _assert_safe_path(
            root=root,
            target=paths[label],
            label=label,
        )

    if paths["destination"].parent != directory:
        raise PreNightProgramBindingContractError(
            "destination parent mismatch"
        )

    if paths["lock"].parent != directory:
        raise PreNightProgramBindingContractError(
            "lock parent mismatch"
        )

    if paths["temporary"].parent != directory:
        raise PreNightProgramBindingContractError(
            "temporary parent mismatch"
        )


def _load_collection(
    *,
    paths: Mapping[str, Path],
    race_date: str,
    expected_sha256: str,
) -> dict[str, Any]:
    collection_path = paths["collection"]

    _assert_safe_path(
        root=paths["root"],
        target=collection_path,
        label="deadline collection",
    )

    if (
        not collection_path.exists()
        or collection_path.is_symlink()
        or not collection_path.is_file()
    ):
        raise PreNightProgramBindingCacheError(
            "deadline collection must be regular file"
        )

    stored_bytes = collection_path.read_bytes()

    actual_sha256 = hashlib.sha256(
        stored_bytes
    ).hexdigest()

    if actual_sha256 != expected_sha256:
        raise PreNightProgramBindingCacheError(
            "deadline collection SHA-256 mismatch"
        )

    try:
        payload = json.loads(
            stored_bytes.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreNightProgramBindingCacheError(
            "deadline collection is not valid UTF-8 JSON"
        ) from error

    if not isinstance(payload, dict):
        raise PreNightProgramBindingCacheError(
            "deadline collection must be JSON object"
        )

    if set(payload) != _COLLECTION_KEYS:
        raise PreNightProgramBindingCacheError(
            "deadline collection fields mismatch"
        )

    if _canonical_json_bytes(payload) != stored_bytes:
        raise PreNightProgramBindingCacheError(
            "deadline collection is non-canonical"
        )

    if (
        payload["contract_version"]
        != COLLECTION_CONTRACT_VERSION
    ):
        raise PreNightProgramBindingCacheError(
            "deadline collection contract_version mismatch"
        )

    if payload["race_date"] != race_date:
        raise PreNightProgramBindingCacheError(
            "deadline collection race_date mismatch"
        )

    venue_codes = payload["expected_venue_codes"]

    if (
        not isinstance(venue_codes, list)
        or not venue_codes
    ):
        raise PreNightProgramBindingCacheError(
            "deadline collection venue list is invalid"
        )

    try:
        normalized_venues = [
            _require_venue_code(value)
            for value in venue_codes
        ]
    except PreNightProgramBindingContractError as error:
        raise PreNightProgramBindingCacheError(
            "deadline collection venue code is invalid"
        ) from error

    if (
        normalized_venues
        != sorted(set(normalized_venues), key=int)
    ):
        raise PreNightProgramBindingCacheError(
            "deadline collection venue order is invalid"
        )

    entries = payload["entries"]

    if not isinstance(entries, list):
        raise PreNightProgramBindingCacheError(
            "deadline collection entries must be list"
        )

    if (
        isinstance(payload["entry_count"], bool)
        or payload["entry_count"] != len(entries)
        or len(entries) != len(normalized_venues)
    ):
        raise PreNightProgramBindingCacheError(
            "deadline collection entry_count mismatch"
        )

    deadline_by_venue: dict[str, str] = {}

    for index, entry in enumerate(entries):
        if (
            not isinstance(entry, dict)
            or set(entry) != _COLLECTION_ENTRY_KEYS
        ):
            raise PreNightProgramBindingCacheError(
                f"deadline collection entry {index} fields mismatch"
            )

        venue_code = entry["venue_code"]

        if venue_code != normalized_venues[index]:
            raise PreNightProgramBindingCacheError(
                "deadline collection entry order mismatch"
            )

        if entry["race_date"] != race_date:
            raise PreNightProgramBindingCacheError(
                "deadline collection entry race_date mismatch"
            )

        try:
            digest = _require_sha256(
                entry["deadline_evidence_sha256"],
                "deadline_evidence_sha256",
            )
        except PreNightProgramBindingContractError as error:
            raise PreNightProgramBindingCacheError(
                "deadline collection entry digest is invalid"
            ) from error

        if (
            not isinstance(entry["relative_path"], str)
            or not entry["relative_path"]
        ):
            raise PreNightProgramBindingCacheError(
                "deadline collection relative_path is invalid"
            )

        if (
            isinstance(entry["byte_length"], bool)
            or not isinstance(entry["byte_length"], int)
            or entry["byte_length"] <= 0
        ):
            raise PreNightProgramBindingCacheError(
                "deadline collection byte_length is invalid"
            )

        if (
            not isinstance(entry["contract_version"], str)
            or not entry["contract_version"]
        ):
            raise PreNightProgramBindingCacheError(
                "deadline collection entry contract_version is invalid"
            )

        deadline_by_venue[venue_code] = digest

    if set(deadline_by_venue) != set(normalized_venues):
        raise PreNightProgramBindingCacheError(
            "deadline collection venue coverage mismatch"
        )

    return {
        "payload": payload,
        "deadline_by_venue": deadline_by_venue,
        "venue_codes": normalized_venues,
        "sha256": actual_sha256,
        "byte_length": len(stored_bytes),
    }


def _normalize_program_source_digests(
    value: Any,
    *,
    venue_codes: Sequence[str],
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise PreNightProgramBindingContractError(
            "program_source_sha256_by_venue must be mapping"
        )

    normalized: dict[str, str] = {}

    for raw_venue, raw_digest in value.items():
        venue_code = _require_venue_code(raw_venue)

        if venue_code in normalized:
            raise PreNightProgramBindingContractError(
                "duplicate program source venue"
            )

        normalized[venue_code] = _require_sha256(
            raw_digest,
            f"program_source_sha256_by_venue[{venue_code}]",
        )

    expected = set(venue_codes)
    actual = set(normalized)

    if actual != expected:
        raise PreNightProgramBindingContractError(
            "program source venue coverage mismatch: "
            f"missing={sorted(expected - actual, key=int)}, "
            f"extra={sorted(actual - expected, key=int)}"
        )

    return {
        venue: normalized[venue]
        for venue in sorted(normalized, key=int)
    }


def _program_records(value: Any) -> list[Mapping[str, Any]]:
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            records = value.to_dict(orient="records")
        except TypeError as error:
            raise PreNightProgramBindingContractError(
                "program_entries DataFrame conversion failed"
            ) from error

    elif (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes, bytearray))
    ):
        records = list(value)

    else:
        raise PreNightProgramBindingContractError(
            "program_entries must be DataFrame or sequence"
        )

    if not records:
        raise PreNightProgramBindingContractError(
            "program_entries must not be empty"
        )

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise PreNightProgramBindingContractError(
                f"program_entries[{index}] must be mapping"
            )

    return records


def _normalize_program_identities(
    program_entries: Any,
    *,
    race_date: str,
    venue_codes: Sequence[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    records = _program_records(program_entries)
    expected_venues = set(venue_codes)

    identities: dict[str, dict[int, set[int]]] = {
        venue: {}
        for venue in venue_codes
    }

    seen: set[tuple[str, str, int, int]] = set()

    for index, record in enumerate(records):
        missing = (
            _REQUIRED_PROGRAM_IDENTITY_FIELDS
            - set(record)
        )

        if missing:
            raise PreNightProgramBindingContractError(
                f"program_entries[{index}] fields missing: "
                f"{sorted(missing)}"
            )

        row_race_date = _require_race_date(
            record["race_date"]
        )
        venue_code = _require_venue_code(
            record["venue_code"]
        )
        race_no = _require_integer(
            record["race_no"],
            field_name="race_no",
            minimum=1,
            maximum=12,
        )
        boat_no = _require_integer(
            record["boat_no"],
            field_name="boat_no",
            minimum=1,
            maximum=6,
        )

        if row_race_date != race_date:
            raise PreNightProgramBindingContractError(
                "program entry race_date mismatch"
            )

        if venue_code not in expected_venues:
            raise PreNightProgramBindingContractError(
                "program entry venue is not in deadline collection"
            )

        identity = (
            row_race_date,
            venue_code,
            race_no,
            boat_no,
        )

        if identity in seen:
            raise PreNightProgramBindingContractError(
                "duplicate program boat identity"
            )

        seen.add(identity)

        boats = identities[venue_code].setdefault(
            race_no,
            set(),
        )
        boats.add(boat_no)

    normalized: dict[
        str,
        dict[str, dict[str, Any]],
    ] = {}

    expected_races = set(range(1, 13))
    expected_boats = set(range(1, 7))

    for venue_code in venue_codes:
        races = identities[venue_code]

        if set(races) != expected_races:
            raise PreNightProgramBindingContractError(
                f"venue {venue_code} must contain races 1 through 12"
            )

        normalized_races: dict[
            str,
            dict[str, Any],
        ] = {}

        for race_no in range(1, 13):
            boats = races[race_no]

            if boats != expected_boats:
                raise PreNightProgramBindingContractError(
                    f"venue {venue_code} race {race_no} "
                    "must contain boats 1 through 6"
                )

            normalized_races[str(race_no)] = {
                "boats": {
                    str(boat_no): {}
                    for boat_no in sorted(boats)
                },
            }

        normalized[venue_code] = normalized_races

    return normalized


def _validate_key_addressed_races(
    value: Any,
    *,
    venue_code: str,
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise PreNightProgramBindingContractError(
            f"venue {venue_code} races must be key-addressed mapping"
        )

    expected_race_keys = {
        str(race_no)
        for race_no in range(1, 13)
    }
    actual_race_keys = set(value)

    if actual_race_keys != expected_race_keys:
        raise PreNightProgramBindingContractError(
            f"venue {venue_code} race keys mismatch"
        )

    normalized: dict[str, dict[str, Any]] = {}

    for race_no in range(1, 13):
        race_key = str(race_no)
        race_binding = value[race_key]

        if (
            not isinstance(race_binding, Mapping)
            or set(race_binding) != {"boats"}
        ):
            raise PreNightProgramBindingContractError(
                f"venue {venue_code} race {race_no} "
                "binding fields mismatch"
            )

        boats = race_binding["boats"]

        if not isinstance(boats, Mapping):
            raise PreNightProgramBindingContractError(
                f"venue {venue_code} race {race_no} "
                "boats must be key-addressed mapping"
            )

        expected_boat_keys = {
            str(boat_no)
            for boat_no in range(1, 7)
        }
        actual_boat_keys = set(boats)

        if actual_boat_keys != expected_boat_keys:
            raise PreNightProgramBindingContractError(
                f"venue {venue_code} race {race_no} "
                "boat keys mismatch"
            )

        normalized_boats: dict[str, dict[str, Any]] = {}

        for boat_no in range(1, 7):
            boat_key = str(boat_no)
            boat_binding = boats[boat_key]

            if (
                not isinstance(boat_binding, Mapping)
                or set(boat_binding)
            ):
                raise PreNightProgramBindingContractError(
                    f"venue {venue_code} race {race_no} "
                    f"boat {boat_no} binding must be empty object"
                )

            normalized_boats[boat_key] = {}

        normalized[race_key] = {
            "boats": normalized_boats,
        }

    return normalized


def build_pre_night_program_entries_binding(
    *,
    race_date,
    deadline_evidence_collection_sha256,
    deadline_evidence_sha256_by_venue,
    program_source_sha256_by_venue,
    program_entries,
) -> dict[str, Any]:
    """Build the deterministic Stage 3 canonical payload."""

    race_date = _require_race_date(race_date)

    collection_digest = _require_sha256(
        deadline_evidence_collection_sha256,
        "deadline_evidence_collection_sha256",
    )

    if not isinstance(
        deadline_evidence_sha256_by_venue,
        Mapping,
    ):
        raise PreNightProgramBindingContractError(
            "deadline_evidence_sha256_by_venue must be mapping"
        )

    deadline_digests: dict[str, str] = {}

    for raw_venue, raw_digest in (
        deadline_evidence_sha256_by_venue.items()
    ):
        venue_code = _require_venue_code(raw_venue)

        if venue_code in deadline_digests:
            raise PreNightProgramBindingContractError(
                "duplicate deadline venue"
            )

        deadline_digests[venue_code] = _require_sha256(
            raw_digest,
            f"deadline_evidence_sha256_by_venue[{venue_code}]",
        )

    if not deadline_digests:
        raise PreNightProgramBindingContractError(
            "deadline venue mapping must not be empty"
        )

    venue_codes = sorted(deadline_digests, key=int)

    program_digests = _normalize_program_source_digests(
        program_source_sha256_by_venue,
        venue_codes=venue_codes,
    )

    races_by_venue = _normalize_program_identities(
        program_entries,
        race_date=race_date,
        venue_codes=venue_codes,
    )

    venue_bindings: dict[str, dict[str, Any]] = {}

    for venue_code in venue_codes:
        digest_material = {
            "race_date": race_date,
            "venue_code": venue_code,
            "deadline_evidence_sha256": (
                deadline_digests[venue_code]
            ),
            "program_source_sha256": (
                program_digests[venue_code]
            ),
            "races": races_by_venue[venue_code],
        }

        binding_sha256 = hashlib.sha256(
            _canonical_json_bytes(digest_material)
        ).hexdigest()

        venue_bindings[venue_code] = {
            "deadline_evidence_sha256": (
                deadline_digests[venue_code]
            ),
            "program_source_sha256": (
                program_digests[venue_code]
            ),
            "races": races_by_venue[venue_code],
            "binding_sha256": binding_sha256,
        }

    return {
        "contract_version": (
            PROGRAM_BINDING_CONTRACT_VERSION
        ),
        "race_date": race_date,
        "deadline_evidence_collection_sha256": (
            collection_digest
        ),
        "venue_bindings": venue_bindings,
    }


def canonical_program_entries_binding_bytes(
    binding: Mapping[str, Any],
) -> bytes:
    """Return exact canonical Stage 3 bytes."""

    if not isinstance(binding, Mapping):
        raise PreNightProgramBindingContractError(
            "binding must be mapping"
        )

    if set(binding) != _BINDING_KEYS:
        raise PreNightProgramBindingContractError(
            "binding fields mismatch"
        )

    if (
        binding["contract_version"]
        != PROGRAM_BINDING_CONTRACT_VERSION
    ):
        raise PreNightProgramBindingContractError(
            "binding contract_version mismatch"
        )

    _require_race_date(binding["race_date"])
    _require_sha256(
        binding["deadline_evidence_collection_sha256"],
        "deadline_evidence_collection_sha256",
    )

    venue_bindings = binding["venue_bindings"]

    if (
        not isinstance(venue_bindings, Mapping)
        or not venue_bindings
    ):
        raise PreNightProgramBindingContractError(
            "venue_bindings must be non-empty mapping"
        )

    venue_codes = [
        _require_venue_code(value)
        for value in venue_bindings
    ]

    if venue_codes != sorted(set(venue_codes), key=int):
        raise PreNightProgramBindingContractError(
            "venue_bindings must use deterministic order"
        )

    for venue_code in venue_codes:
        venue_binding = venue_bindings[venue_code]

        if (
            not isinstance(venue_binding, Mapping)
            or set(venue_binding) != _VENUE_BINDING_KEYS
        ):
            raise PreNightProgramBindingContractError(
                f"venue binding {venue_code} fields mismatch"
            )

        deadline_digest = _require_sha256(
            venue_binding["deadline_evidence_sha256"],
            "deadline_evidence_sha256",
        )
        program_digest = _require_sha256(
            venue_binding["program_source_sha256"],
            "program_source_sha256",
        )
        recorded_binding_digest = _require_sha256(
            venue_binding["binding_sha256"],
            "binding_sha256",
        )

        normalized_races = _validate_key_addressed_races(
            venue_binding["races"],
            venue_code=venue_code,
        )

        material = {
            "race_date": binding["race_date"],
            "venue_code": venue_code,
            "deadline_evidence_sha256": deadline_digest,
            "program_source_sha256": program_digest,
            "races": normalized_races,
        }

        expected_binding_digest = hashlib.sha256(
            _canonical_json_bytes(material)
        ).hexdigest()

        if recorded_binding_digest != expected_binding_digest:
            raise PreNightProgramBindingContractError(
                f"venue binding SHA-256 mismatch: {venue_code}"
            )

    return _canonical_json_bytes(dict(binding))


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError as error:
        raise PreNightProgramBindingIntegrityError(
            "binding destination fsync failed"
        ) from error


def _fsync_directory(directory: Path) -> None:
    descriptor = None
    close_error = None

    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
        )
        descriptor = os.open(directory, flags)
        os.fsync(descriptor)
    except OSError as error:
        raise PreNightProgramBindingIntegrityError(
            "binding parent-directory fsync failed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                close_error = error

        if close_error is not None:
            raise PreNightProgramBindingIntegrityError(
                "binding directory descriptor close failed"
            ) from close_error


def _receipt(
    *,
    paths: Mapping[str, Path],
    race_date: str,
    run_id: str,
    digest: str,
    byte_length: int,
    venue_count: int,
    cached: bool,
) -> dict[str, Any]:
    return {
        "race_date": race_date,
        "run_id": run_id,
        "relative_path": (
            paths["destination"]
            .relative_to(paths["root"])
            .as_posix()
        ),
        "program_entries_binding_sha256": digest,
        "overall_binding_sha256": digest,
        "byte_length": byte_length,
        "venue_count": venue_count,
        "paths": {
            "directory": paths["directory"],
            "program_entries_binding": (
                paths["destination"]
            ),
        },
        "cached": cached,
        "publication_status": (
            "VALIDATED_REUSE" if cached else "CREATED"
        ),
    }


def _validate_cached_binding(
    *,
    paths: Mapping[str, Path],
    expected_payload: Mapping[str, Any],
    expected_bytes: bytes,
    expected_sha256: str,
    run_id: str,
) -> dict[str, Any]:
    destination = paths["destination"]

    _assert_safe_paths(paths)

    if (
        not destination.exists()
        or destination.is_symlink()
        or not destination.is_file()
    ):
        raise PreNightProgramBindingCacheError(
            "binding cache must be regular file"
        )

    stored_bytes = destination.read_bytes()

    try:
        stored_payload = json.loads(
            stored_bytes.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreNightProgramBindingCacheError(
            "binding cache is not valid UTF-8 JSON"
        ) from error

    if not isinstance(stored_payload, dict):
        raise PreNightProgramBindingCacheError(
            "binding cache must be JSON object"
        )

    try:
        stored_canonical = (
            canonical_program_entries_binding_bytes(
                stored_payload
            )
        )
    except PreNightProgramBindingContractError as error:
        raise PreNightProgramBindingCacheError(
            "binding cache validation failed"
        ) from error

    if stored_canonical != stored_bytes:
        raise PreNightProgramBindingCacheError(
            "binding cache is non-canonical"
        )

    if stored_payload != expected_payload:
        raise PreNightProgramBindingCacheError(
            "binding cache payload conflict"
        )

    if stored_bytes != expected_bytes:
        raise PreNightProgramBindingCacheError(
            "binding cache byte conflict"
        )

    stored_sha256 = hashlib.sha256(
        stored_bytes
    ).hexdigest()

    if stored_sha256 != expected_sha256:
        raise PreNightProgramBindingCacheError(
            "binding cache digest conflict"
        )

    return _receipt(
        paths=paths,
        race_date=stored_payload["race_date"],
        run_id=run_id,
        digest=stored_sha256,
        byte_length=len(stored_bytes),
        venue_count=len(
            stored_payload["venue_bindings"]
        ),
        cached=True,
    )


def publish_pre_night_program_entries_binding(
    data_root,
    *,
    run_id,
    race_date,
    deadline_evidence_collection_sha256,
    program_source_sha256_by_venue,
    program_entries,
) -> dict[str, Any]:
    """Publish one immutable Stage 3 binding in a run directory."""

    race_date = _require_race_date(race_date)
    run_id = _require_run_id(run_id)
    expected_collection_digest = _require_sha256(
        deadline_evidence_collection_sha256,
        "deadline_evidence_collection_sha256",
    )

    paths = _binding_paths(
        data_root,
        race_date=race_date,
        run_id=run_id,
    )
    _assert_safe_paths(paths)

    collection = _load_collection(
        paths=paths,
        race_date=race_date,
        expected_sha256=expected_collection_digest,
    )

    payload = build_pre_night_program_entries_binding(
        race_date=race_date,
        deadline_evidence_collection_sha256=(
            collection["sha256"]
        ),
        deadline_evidence_sha256_by_venue=(
            collection["deadline_by_venue"]
        ),
        program_source_sha256_by_venue=(
            program_source_sha256_by_venue
        ),
        program_entries=program_entries,
    )

    canonical_bytes = (
        canonical_program_entries_binding_bytes(
            payload
        )
    )
    digest = hashlib.sha256(
        canonical_bytes
    ).hexdigest()

    destination = paths["destination"]

    if destination.exists():
        return _validate_cached_binding(
            paths=paths,
            expected_payload=payload,
            expected_bytes=canonical_bytes,
            expected_sha256=digest,
            run_id=run_id,
        )

    directory = paths["directory"]
    directory.mkdir(parents=True, exist_ok=True)
    _assert_safe_paths(paths)

    lock_path = paths["lock"]
    temporary = paths["temporary"]

    lock_fd = None
    lock_acquired = False
    lock_identity = None
    temporary_created = False

    try:
        try:
            lock_fd = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            lock_acquired = True
            lock_stat = os.fstat(lock_fd)
            lock_identity = (
                lock_stat.st_dev,
                lock_stat.st_ino,
            )
        except FileExistsError as error:
            raise PreNightProgramBindingError(
                "binding publication lock exists"
            ) from error
        except OSError as error:
            raise PreNightProgramBindingError(
                "binding lock acquisition failed"
            ) from error

        try:
            os.close(lock_fd)
        except OSError as error:
            raise PreNightProgramBindingIntegrityError(
                "binding lock descriptor close failed"
            ) from error
        else:
            lock_fd = None

        _assert_safe_paths(paths)

        if destination.exists():
            return _validate_cached_binding(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
                run_id=run_id,
            )

        try:
            with temporary.open("xb") as handle:
                temporary_created = True
                handle.write(canonical_bytes)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception as error:
            raise PreNightProgramBindingIntegrityError(
                "binding temporary write failed"
            ) from error

        temporary_bytes = temporary.read_bytes()

        if temporary_bytes != canonical_bytes:
            raise PreNightProgramBindingIntegrityError(
                "binding temporary byte mismatch"
            )

        if (
            hashlib.sha256(temporary_bytes).hexdigest()
            != digest
        ):
            raise PreNightProgramBindingIntegrityError(
                "binding temporary digest mismatch"
            )

        _assert_safe_paths(paths)

        if destination.exists():
            return _validate_cached_binding(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
                run_id=run_id,
            )

        try:
            os.link(temporary, destination)
        except FileExistsError:
            return _validate_cached_binding(
                paths=paths,
                expected_payload=payload,
                expected_bytes=canonical_bytes,
                expected_sha256=digest,
                run_id=run_id,
            )
        except OSError as error:
            raise PreNightProgramBindingIntegrityError(
                "binding atomic publication failed"
            ) from error

        _fsync_file(destination)
        _fsync_directory(directory)

        verified = _validate_cached_binding(
            paths=paths,
            expected_payload=payload,
            expected_bytes=canonical_bytes,
            expected_sha256=digest,
            run_id=run_id,
        )

        return {
            **verified,
            "cached": False,
            "publication_status": "CREATED",
        }

    finally:
        cleanup_error = None

        if temporary_created:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                cleanup_error = error

        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                if cleanup_error is None:
                    cleanup_error = error
            else:
                lock_fd = None

        if lock_acquired:
            try:
                current_stat = lock_path.lstat()
            except FileNotFoundError:
                current_stat = None
            except OSError as error:
                current_stat = None
                if cleanup_error is None:
                    cleanup_error = error

            if current_stat is not None:
                current_identity = (
                    current_stat.st_dev,
                    current_stat.st_ino,
                )

                if current_identity != lock_identity:
                    if cleanup_error is None:
                        cleanup_error = RuntimeError(
                            "binding lock ownership identity changed"
                        )
                else:
                    try:
                        lock_path.unlink()
                    except OSError as error:
                        if cleanup_error is None:
                            cleanup_error = error

        if cleanup_error is not None:
            raise PreNightProgramBindingIntegrityError(
                "binding owned-resource cleanup failed"
            ) from cleanup_error


__all__ = [
    "PROGRAM_BINDING_CONTRACT_VERSION",
    "PreNightProgramBindingCacheError",
    "PreNightProgramBindingContractError",
    "PreNightProgramBindingError",
    "PreNightProgramBindingIntegrityError",
    "build_pre_night_program_entries_binding",
    "canonical_program_entries_binding_bytes",
    "publish_pre_night_program_entries_binding",
]
