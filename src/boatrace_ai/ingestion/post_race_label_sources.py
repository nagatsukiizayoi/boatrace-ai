from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import requests

from boatrace_ai.ingestion.daily_archives import (
    build_archive_spec,
    validate_lzh_file,
)


UTC = timezone.utc

CONTRACT_VERSION = "post_race_label_source_v1"
COLLECTOR_VERSION = "post_race_label_collector_v1"

REQUIRED_METADATA_FIELDS = {
    "contract_version",
    "collector_version",
    "source_type",
    "archive_type",
    "snapshot_type",
    "race_date",
    "request_started_at",
    "fetched_at",
    "source_url",
    "http_status",
    "response_size",
    "source_response_sha256",
    "archive_sha256",
    "archive_path",
    "label_eligible",
    "provenance_status",
}


class PostRaceLabelSourceError(RuntimeError):
    pass


class PostRaceLabelContractError(PostRaceLabelSourceError):
    pass


class PostRaceLabelCacheError(PostRaceLabelSourceError):
    pass


class PostRaceLabelIntegrityError(PostRaceLabelSourceError):
    pass


def normalize_race_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise PostRaceLabelContractError(
            f"Invalid race_date: {value!r}"
        ) from exc


def require_aware_datetime(
    value: datetime,
    field_name: str,
) -> datetime:
    if not isinstance(value, datetime):
        raise PostRaceLabelContractError(
            f"{field_name} must be a datetime"
        )

    if value.tzinfo is None or value.utcoffset() is None:
        raise PostRaceLabelContractError(
            f"{field_name} must be timezone-aware"
        )

    return value


def parse_timestamp(
    value: str,
    field_name: str,
) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise PostRaceLabelContractError(
            f"{field_name} is not a valid ISO-8601 timestamp"
        ) from exc

    return require_aware_datetime(parsed, field_name)


def build_source_paths(
    race_date: date | datetime | str,
    data_root: str | Path,
) -> dict[str, Path]:
    normalized = normalize_race_date(race_date)
    root = Path(data_root)

    directory = (
        root
        / "snapshots"
        / "post_race_label_v1"
        / normalized.isoformat()
        / "result"
    )

    archive = directory / f"k{normalized:%y%m%d}.lzh"

    return {
        "directory": directory,
        "archive": archive,
        "metadata": Path(str(archive) + ".json"),
    }


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def fsync_file(path: str | Path) -> None:
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def atomic_write_json(
    value: dict,
    path: str | Path,
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".part",
        dir=target.parent,
    )
    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, target)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def load_metadata(path: str | Path) -> dict:
    metadata_path = Path(path)

    try:
        with metadata_path.open(
            "r",
            encoding="utf-8",
        ) as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise PostRaceLabelCacheError(
            f"Unable to load metadata: {metadata_path}"
        ) from exc

    if not isinstance(value, dict):
        raise PostRaceLabelCacheError(
            "Source metadata must be a JSON object"
        )

    return value


def validate_cached_source(
    race_date: date | datetime | str,
    data_root: str | Path,
    validator: Callable[[Path], None] | None = None,
) -> dict:
    normalized = normalize_race_date(race_date)
    paths = build_source_paths(normalized, data_root)

    archive_exists = paths["archive"].is_file()
    metadata_exists = paths["metadata"].is_file()

    if archive_exists != metadata_exists:
        raise PostRaceLabelCacheError(
            "Archive and metadata must either both exist "
            "or both be absent"
        )

    if not archive_exists:
        raise PostRaceLabelCacheError(
            "Post-race label source is not cached"
        )

    metadata = load_metadata(paths["metadata"])
    missing = sorted(
        REQUIRED_METADATA_FIELDS - set(metadata)
    )

    if missing:
        raise PostRaceLabelContractError(
            f"Missing source metadata fields: {missing}"
        )

    if metadata["contract_version"] != CONTRACT_VERSION:
        raise PostRaceLabelContractError(
            "Unsupported source contract version"
        )

    if metadata["source_type"] != "result":
        raise PostRaceLabelContractError(
            "source_type must be result"
        )

    if metadata["archive_type"] != "result":
        raise PostRaceLabelContractError(
            "archive_type must be result"
        )

    if metadata["race_date"] != normalized.isoformat():
        raise PostRaceLabelContractError(
            "race_date does not match the requested date"
        )

    parse_timestamp(
        metadata["request_started_at"],
        "request_started_at",
    )
    parse_timestamp(
        metadata["fetched_at"],
        "fetched_at",
    )

    expected_path = str(paths["archive"])

    if metadata["archive_path"] != expected_path:
        raise PostRaceLabelContractError(
            "archive_path does not match the source path"
        )

    if metadata["label_eligible"] is not True:
        raise PostRaceLabelContractError(
            "Source is not label-eligible"
        )

    if metadata["provenance_status"] != "VERIFIED":
        raise PostRaceLabelContractError(
            "Source provenance is not verified"
        )

    actual_size = paths["archive"].stat().st_size

    if metadata["response_size"] != actual_size:
        raise PostRaceLabelIntegrityError(
            "Source archive size mismatch"
        )

    actual_sha256 = sha256_file(paths["archive"])

    if metadata["archive_sha256"] != actual_sha256:
        raise PostRaceLabelIntegrityError(
            "Source archive SHA-256 mismatch"
        )

    if (
        metadata["source_response_sha256"]
        != actual_sha256
    ):
        raise PostRaceLabelIntegrityError(
            "Source response SHA-256 mismatch"
        )

    validation_function = validator or validate_lzh_file

    try:
        validation_function(paths["archive"])
    except Exception as exc:
        raise PostRaceLabelIntegrityError(
            "Source archive format validation failed"
        ) from exc

    return {
        "status": "CACHED",
        "paths": paths,
        "metadata": metadata,
    }


def collect_post_race_label_source(
    race_date: date | datetime | str,
    data_root: str | Path,
    timeout: int = 60,
    session=None,
    now_fn=None,
    validator: Callable[[Path], None] | None = None,
) -> dict:
    normalized = normalize_race_date(race_date)
    paths = build_source_paths(normalized, data_root)

    archive_exists = paths["archive"].exists()
    metadata_exists = paths["metadata"].exists()

    if archive_exists or metadata_exists:
        return validate_cached_source(
            normalized,
            data_root,
            validator=validator,
        )

    clock = now_fn or (lambda: datetime.now(UTC))
    request_started_at = require_aware_datetime(
        clock(),
        "request_started_at",
    )

    spec = build_archive_spec(
        normalized,
        "result",
    )

    own_session = session is None
    http_session = session or requests.Session()
    response = None

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{paths['archive'].name}.",
        suffix=".part",
        dir=paths["directory"],
    )
    temporary_path = Path(temporary_name)

    digest = hashlib.sha256()
    response_size = 0

    try:
        with os.fdopen(fd, "wb") as handle:
            response = http_session.get(
                spec["url"],
                timeout=timeout,
                stream=True,
                headers={
                    "User-Agent": (
                        "BOATRACE-AI post-race-label/1.0"
                    )
                },
            )
            response.raise_for_status()

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if not chunk:
                    continue

                handle.write(chunk)
                digest.update(chunk)
                response_size += len(chunk)

            handle.flush()
            os.fsync(handle.fileno())

        validation_function = (
            validator or validate_lzh_file
        )

        try:
            validation_function(temporary_path)
        except Exception as exc:
            raise PostRaceLabelIntegrityError(
                "Downloaded archive failed format validation"
            ) from exc

        fetched_at = require_aware_datetime(
            clock(),
            "fetched_at",
        )

        archive_sha256 = digest.hexdigest()

        metadata = {
            "contract_version": CONTRACT_VERSION,
            "collector_version": COLLECTOR_VERSION,
            "source_type": "result",
            "archive_type": "result",
            "snapshot_type": "post_race_label",
            "race_date": normalized.isoformat(),
            "request_started_at":
                request_started_at.isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "source_url": spec["url"],
            "http_status": getattr(
                response,
                "status_code",
                None,
            ),
            "response_size": response_size,
            "source_response_sha256":
                archive_sha256,
            "archive_sha256": archive_sha256,
            "archive_path": str(paths["archive"]),
            "label_eligible": True,
            "provenance_status": "VERIFIED",
        }

        os.replace(
            temporary_path,
            paths["archive"],
        )
        fsync_file(paths["archive"])

        atomic_write_json(
            metadata,
            paths["metadata"],
        )

        return validate_cached_source(
            normalized,
            data_root,
            validator=validator,
        )

    finally:
        if temporary_path.exists():
            temporary_path.unlink()

        if response is not None:
            close = getattr(response, "close", None)
            if callable(close):
                close()

        if own_session:
            close = getattr(http_session, "close", None)
            if callable(close):
                close()
