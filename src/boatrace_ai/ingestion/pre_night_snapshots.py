from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from boatrace_ai.ingestion.daily_archives import (
    USER_AGENT,
    build_archive_spec,
    validate_lzh_file,
)
from boatrace_ai.ingestion.pre_night_deadlines import (
    canonical_deadline_evidence_bytes,
    validate_deadline_evidence,
)


JST = ZoneInfo("Asia/Tokyo")
UTC = dt.timezone.utc

CONTRACT_VERSION = "pre_night_program_snapshot_v1"
COLLECTOR_VERSION = "pre_night_program_collector_v1"
AS_OF_RULE = "PREVIOUS_DAY_21_30_JST"

REQUIRED_METADATA_FIELDS = {
    "contract_version",
    "collector_version",
    "source_type",
    "archive_type",
    "snapshot_type",
    "race_date",
    "as_of_rule",
    "as_of_time",
    "snapshot_at",
    "request_started_at",
    "fetched_at",
    "source_url",
    "http_status",
    "response_size",
    "source_response_sha256",
    "archive_sha256",
    "archive_path",
    "eligible_for_pre_night",
    "eligibility_reason",
}


class PreNightSnapshotError(RuntimeError):
    pass


class PreNightContractError(PreNightSnapshotError):
    pass


class PreNightCacheError(PreNightSnapshotError):
    pass


class PreNightIntegrityError(PreNightSnapshotError):
    pass


class PreNightEligibilityError(PreNightSnapshotError):
    pass


def normalize_race_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()

    if isinstance(value, dt.date):
        return value

    return dt.date.fromisoformat(str(value))


def require_aware_datetime(
    value: dt.datetime,
    field_name: str,
) -> dt.datetime:
    if not isinstance(value, dt.datetime):
        raise PreNightContractError(
            f"{field_name} must be datetime"
        )

    if (
        value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise PreNightContractError(
            f"{field_name} must include timezone"
        )

    return value


def parse_timestamp(
    value,
    field_name: str,
) -> dt.datetime:
    if value is None:
        raise PreNightContractError(
            f"{field_name} is missing"
        )

    text = str(value).strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as error:
        raise PreNightContractError(
            f"{field_name} is invalid: {value}"
        ) from error

    return require_aware_datetime(
        parsed,
        field_name,
    )


def build_pre_night_as_of(
    race_date,
) -> dt.datetime:
    date_value = normalize_race_date(race_date)
    previous_day = date_value - dt.timedelta(days=1)

    return dt.datetime.combine(
        previous_day,
        dt.time(21, 30),
        tzinfo=JST,
    )


def build_snapshot_paths(
    race_date,
    data_root,
) -> dict[str, Path]:
    date_value = normalize_race_date(race_date)
    spec = build_archive_spec(
        date_value,
        "program",
    )

    directory = (
        Path(data_root)
        / "snapshots"
        / "pre_night_v2"
        / date_value.isoformat()
        / "program"
    )

    archive_path = directory / spec["filename"]

    metadata_path = archive_path.with_suffix(
        archive_path.suffix + ".json"
    )

    return {
        "directory": directory,
        "archive": archive_path,
        "metadata": metadata_path,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def atomic_write_json(
    payload: dict,
    destination: Path,
) -> None:
    temporary = destination.with_name(
        destination.name
        + "."
        + uuid.uuid4().hex
        + ".part"
    )

    try:
        with temporary.open(
            "x",
            encoding="utf-8",
        ) as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        temporary.replace(destination)

    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def load_metadata(path: Path) -> dict:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except Exception as error:
        raise PreNightCacheError(
            f"metadata is not valid JSON: {path}"
        ) from error

    if not isinstance(payload, dict):
        raise PreNightCacheError(
            f"metadata must be object: {path}"
        )

    return payload



# BEGIN PRE_NIGHT_PROGRAM_CACHE_PROVENANCE_V1


def require_exact_bool(
    value,
    field_name: str,
) -> bool:
    if not isinstance(value, bool):
        raise PreNightContractError(
            f"{field_name} must be bool"
        )

    return value


def require_positive_int(
    value,
    field_name: str,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
    ):
        raise PreNightContractError(
            f"{field_name} must be positive int"
        )

    return value


def require_sha256(
    value,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise PreNightContractError(
            f"{field_name} must be SHA-256 string"
        )

    normalized = value.lower()

    if len(normalized) != 64:
        raise PreNightContractError(
            f"{field_name} must contain "
            "64 hex characters"
        )

    try:
        int(normalized, 16)
    except ValueError as error:
        raise PreNightContractError(
            f"{field_name} must contain "
            "64 hex characters"
        ) from error

    return normalized


def validate_cached_snapshot(
    race_date,
    data_root,
) -> dict:
    date_value = normalize_race_date(race_date)
    paths = build_snapshot_paths(
        date_value,
        data_root,
    )

    archive_path = paths["archive"]
    metadata_path = paths["metadata"]

    archive_exists = archive_path.is_file()
    metadata_exists = metadata_path.is_file()

    if archive_exists != metadata_exists:
        raise PreNightCacheError(
            "archive and metadata must exist together: "
            f"archive={archive_exists}, "
            f"metadata={metadata_exists}"
        )

    if not archive_exists:
        raise PreNightCacheError(
            f"cached snapshot does not exist: "
            f"{archive_path}"
        )

    metadata = load_metadata(metadata_path)

    missing_fields = sorted(
        field
        for field in REQUIRED_METADATA_FIELDS
        if metadata.get(field) is None
    )

    if missing_fields:
        raise PreNightContractError(
            "metadata required fields missing: "
            f"{missing_fields}"
        )

    expected_spec = build_archive_spec(
        date_value,
        "program",
    )

    if metadata.get("source_url") != expected_spec["url"]:
        raise PreNightContractError(
            "metadata source_url mismatch"
        )

    if metadata.get("archive_path") != str(archive_path):
        raise PreNightContractError(
            "metadata archive_path mismatch"
        )

    http_status = require_positive_int(
        metadata.get("http_status"),
        "http_status",
    )

    if not 200 <= http_status < 300:
        raise PreNightContractError(
            "metadata http_status must be successful"
        )

    if not isinstance(
        metadata.get("http_headers"),
        dict,
    ):
        raise PreNightContractError(
            "metadata http_headers must be object"
        )

    if (
        metadata["contract_version"]
        != CONTRACT_VERSION
    ):
        raise PreNightContractError(
            "unsupported contract_version: "
            f"{metadata['contract_version']}"
        )

    if (
        metadata["collector_version"]
        != COLLECTOR_VERSION
    ):
        raise PreNightContractError(
            "unsupported collector_version: "
            f"{metadata['collector_version']}"
        )

    if metadata["source_type"] != "program":
        raise PreNightContractError(
            "PRE_NIGHT source_type must be program"
        )

    if metadata["archive_type"] != "program":
        raise PreNightContractError(
            "PRE_NIGHT archive_type must be program"
        )

    if metadata["snapshot_type"] != "PRE_NIGHT":
        raise PreNightContractError(
            "snapshot_type must be PRE_NIGHT"
        )

    if metadata["as_of_rule"] != AS_OF_RULE:
        raise PreNightContractError(
            "unexpected as_of_rule: "
            f"{metadata['as_of_rule']}"
        )

    if (
        metadata["race_date"]
        != date_value.isoformat()
    ):
        raise PreNightContractError(
            "metadata race_date mismatch"
        )

    as_of_time = parse_timestamp(
        metadata["as_of_time"],
        "as_of_time",
    )

    expected_as_of = build_pre_night_as_of(
        date_value
    )

    if as_of_time != expected_as_of:
        raise PreNightContractError(
            "metadata as_of_time mismatch: "
            f"expected={expected_as_of.isoformat()}, "
            f"actual={as_of_time.isoformat()}"
        )

    snapshot_at = parse_timestamp(
        metadata["snapshot_at"],
        "snapshot_at",
    )

    if snapshot_at != expected_as_of:
        raise PreNightContractError(
            "snapshot_at must equal as_of_time"
        )

    request_started_at = parse_timestamp(
        metadata["request_started_at"],
        "request_started_at",
    )

    fetched_at = parse_timestamp(
        metadata["fetched_at"],
        "fetched_at",
    )

    if request_started_at > fetched_at:
        raise PreNightContractError(
            "request_started_at must not be "
            "after fetched_at"
        )

    actual_size = int(
        archive_path.stat().st_size
    )

    actual_sha256 = sha256_file(
        archive_path
    )

    expected_size = require_positive_int(
        metadata["response_size"],
        "response_size",
    )

    if actual_size != expected_size:
        raise PreNightIntegrityError(
            "archive size mismatch: "
            f"expected={expected_size}, "
            f"actual={actual_size}"
        )

    expected_response_sha256 = require_sha256(
        metadata["source_response_sha256"],
        "source_response_sha256",
    )

    expected_archive_sha256 = require_sha256(
        metadata["archive_sha256"],
        "archive_sha256",
    )

    if actual_sha256.lower() != (
        expected_response_sha256
    ):
        raise PreNightIntegrityError(
            "source response SHA-256 mismatch"
        )

    if actual_sha256.lower() != (
        expected_archive_sha256
    ):
        raise PreNightIntegrityError(
            "archive SHA-256 mismatch"
        )

    validate_lzh_file(archive_path)

    eligible = fetched_at <= expected_as_of

    metadata_eligible = require_exact_bool(
        metadata["eligible_for_pre_night"],
        "eligible_for_pre_night",
    )

    if metadata_eligible != eligible:
        raise PreNightContractError(
            "eligible_for_pre_night mismatch"
        )

    expected_reason = (
        "FETCHED_BY_AS_OF"
        if eligible
        else "FETCHED_AFTER_AS_OF"
    )

    if (
        metadata["eligibility_reason"]
        != expected_reason
    ):
        raise PreNightContractError(
            "eligibility_reason mismatch"
        )

    if not eligible:
        raise PreNightEligibilityError(
            "cached program snapshot was fetched "
            "after PRE_NIGHT as_of_time"
        )

    return {
        "paths": paths,
        "metadata": metadata,
        "cached": True,
        "eligible_for_pre_night": True,
    }



def _build_deadline_evidence_metadata(
    deadline_evidence,
) -> dict:
    """Validate and deterministically bind optional D1-A evidence."""
    if deadline_evidence is None:
        return {}

    validated_evidence = validate_deadline_evidence(
        deadline_evidence
    )
    canonical_bytes = canonical_deadline_evidence_bytes(
        validated_evidence
    )

    return {
        "deadline_evidence": validated_evidence,
        "deadline_evidence_sha256": hashlib.sha256(
            canonical_bytes
        ).hexdigest(),
    }


def _require_cached_deadline_binding(
    outcome: dict,
    deadline_metadata: dict,
) -> dict:
    """Require a cached Snapshot v2 to match requested evidence."""
    if not deadline_metadata:
        return outcome

    metadata = outcome.get("metadata")

    if not isinstance(metadata, dict):
        raise PreNightIntegrityError(
            "cached snapshot metadata is unavailable"
        )

    for field_name in (
        "deadline_evidence",
        "deadline_evidence_sha256",
    ):
        if metadata.get(field_name) != deadline_metadata[field_name]:
            raise PreNightIntegrityError(
                "cached snapshot deadline evidence mismatch: "
                f"{field_name}"
            )

    return outcome


def collect_pre_night_program_snapshot(
    race_date,
    data_root,
    timeout=60,
    session=None,
    now_fn=None,
    *,
    deadline_evidence=None,
) -> dict:
    date_value = normalize_race_date(race_date)
    paths = build_snapshot_paths(
        date_value,
        data_root,
    )

    archive_path = paths["archive"]
    metadata_path = paths["metadata"]

    deadline_metadata = (
        _build_deadline_evidence_metadata(
            deadline_evidence
        )
    )

    archive_exists = archive_path.exists()
    metadata_exists = metadata_path.exists()

    if archive_exists or metadata_exists:
        outcome = validate_cached_snapshot(
            date_value,
            data_root,
        )
        return _require_cached_deadline_binding(
            outcome,
            deadline_metadata,
        )

    directory = paths["directory"]
    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    lock_path = directory / ".collection.lock"

    try:
        lock_descriptor = os.open(
            lock_path,
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY,
        )
    except FileExistsError as error:
        raise PreNightSnapshotError(
            f"collection lock already exists: "
            f"{lock_path}"
        ) from error

    os.close(lock_descriptor)

    temporary_archive = archive_path.with_name(
        archive_path.name
        + "."
        + uuid.uuid4().hex
        + ".part"
    )

    temporary_metadata = metadata_path.with_name(
        metadata_path.name
        + "."
        + uuid.uuid4().hex
        + ".part"
    )

    client = (
        session
        if session is not None
        else requests
    )

    clock = (
        now_fn
        if now_fn is not None
        else lambda: dt.datetime.now(UTC)
    )

    response = None

    try:
        if archive_path.exists() or metadata_path.exists():
            outcome = validate_cached_snapshot(
                date_value,
                data_root,
            )
            return _require_cached_deadline_binding(
                outcome,
                deadline_metadata,
            )

        spec = build_archive_spec(
            date_value,
            "program",
        )

        request_started_at = (
            require_aware_datetime(
                clock(),
                "request_started_at",
            )
        )

        response = client.get(
            spec["url"],
            headers={
                "User-Agent": USER_AGENT,
            },
            timeout=timeout,
            stream=True,
        )

        response.raise_for_status()

        digest = hashlib.sha256()
        response_size = 0

        with temporary_archive.open("xb") as handle:
            for chunk in response.iter_content(
                chunk_size=1024 * 128,
            ):
                if not chunk:
                    continue

                handle.write(chunk)
                digest.update(chunk)
                response_size += len(chunk)

            handle.flush()
            os.fsync(handle.fileno())

        fetched_at = require_aware_datetime(
            clock(),
            "fetched_at",
        )

        validate_lzh_file(
            temporary_archive
        )

        response_sha256 = (
            digest.hexdigest()
        )

        actual_sha256 = sha256_file(
            temporary_archive
        )

        if response_sha256 != actual_sha256:
            raise PreNightIntegrityError(
                "stream and archive SHA-256 differ"
            )

        actual_size = int(
            temporary_archive.stat().st_size
        )

        if response_size != actual_size:
            raise PreNightIntegrityError(
                "stream and archive size differ"
            )

        as_of_time = build_pre_night_as_of(
            date_value
        )

        eligible = (
            fetched_at.astimezone(UTC)
            <= as_of_time.astimezone(UTC)
        )

        eligibility_reason = (
            "FETCHED_BY_AS_OF"
            if eligible
            else "FETCHED_AFTER_AS_OF"
        )

        status_code = int(
            getattr(
                response,
                "status_code",
                200,
            )
        )

        response_headers = dict(
            getattr(
                response,
                "headers",
                {},
            )
            or {}
        )

        metadata = {
            "contract_version": (
                CONTRACT_VERSION
            ),
            "collector_version": (
                COLLECTOR_VERSION
            ),
            "source_type": "program",
            "archive_type": "program",
            "snapshot_type": "PRE_NIGHT",
            "race_date": (
                date_value.isoformat()
            ),
            "as_of_rule": AS_OF_RULE,
            "as_of_time": (
                as_of_time.isoformat()
            ),
            "snapshot_at": (
                as_of_time.isoformat()
            ),
            "request_started_at": (
                request_started_at.isoformat()
            ),
            "fetched_at": (
                fetched_at.isoformat()
            ),
            "source_url": spec["url"],
            "http_status": status_code,
            "response_size": response_size,
            "source_response_sha256": (
                response_sha256
            ),
            "archive_sha256": (
                actual_sha256
            ),
            "archive_path": str(
                archive_path
            ),
            "eligible_for_pre_night": (
                eligible
            ),
            "eligibility_reason": (
                eligibility_reason
            ),
            "http_headers": {
                str(key): str(value)
                for key, value
                in response_headers.items()
            },
        }
        metadata.update(deadline_metadata)

        with temporary_metadata.open(
            "x",
            encoding="utf-8",
        ) as handle:
            json.dump(
                metadata,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        fsync_file(temporary_archive)

        # archiveを先に確定し、metadataを最後に確定する。
        # metadataがcommit markerとして機能する。
        temporary_archive.replace(
            archive_path
        )

        temporary_metadata.replace(
            metadata_path
        )

        outcome = validate_cached_snapshot(
            date_value,
            data_root,
        )
        outcome = _require_cached_deadline_binding(
            outcome,
            deadline_metadata,
        )

        outcome["cached"] = False
        return outcome

    except PreNightEligibilityError:
        raise

    except Exception:
        temporary_archive.unlink(
            missing_ok=True
        )
        temporary_metadata.unlink(
            missing_ok=True
        )
        raise

    finally:
        temporary_archive.unlink(
            missing_ok=True
        )
        temporary_metadata.unlink(
            missing_ok=True
        )
        lock_path.unlink(
            missing_ok=True
        )

        if response is not None:
            close = getattr(
                response,
                "close",
                None,
            )

            if callable(close):
                close()
