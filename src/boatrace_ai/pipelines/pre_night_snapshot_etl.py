from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from pathlib import Path

import pandas as pd

from boatrace_ai.ingestion.daily_archives import (
    extract_archive,
)
from boatrace_ai.ingestion.pre_night_snapshots import (
    AS_OF_RULE,
    CONTRACT_VERSION,
    COLLECTOR_VERSION,
    build_pre_night_as_of,
    normalize_race_date,
    validate_cached_snapshot,
)
from boatrace_ai.parsers.program import (
    parse_program_file,
)


FEATURE_VERSION = "pre_night_program_snapshot_v1"
OUTPUT_CONTRACT_VERSION = (
    "pre_night_program_parquet_v1"
)

RACE_KEYS = [
    "race_date",
    "venue_code",
    "race_no",
]

ROW_KEYS = [
    "race_date",
    "venue_code",
    "race_no",
    "boat_no",
]

REQUIRED_PROGRAM_COLUMNS = set(ROW_KEYS)

PROVENANCE_COLUMNS = {
    "as_of_time",
    "snapshot_at",
    "feature_version",
    "feature_contract_version",
    "feature_source_type",
    "feature_source_url",
    "feature_source_sha256",
    "feature_source_fetched_at",
    "feature_source_max_time",
    "source_max_time",
    "feature_collector_version",
    "provenance_status",
}

PROHIBITED_COLUMNS = {
    "finish_position",
    "finish_raw",
    "race_time_raw",
    "result_available",
    "race_cancelled",
    "payout_yen",
    "payout_status",
    "popularity",
    "bet_type",
    "combination",
}


class PreNightSnapshotETLError(RuntimeError):
    pass


class PreNightFeatureContractError(
    PreNightSnapshotETLError
):
    pass


class PreNightPointInTimeError(
    PreNightSnapshotETLError
):
    pass


class PreNightOutputIntegrityError(
    PreNightSnapshotETLError
):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def build_output_paths(
    race_date,
    data_root,
) -> dict[str, Path]:
    date_value = normalize_race_date(race_date)

    directory = (
        Path(data_root)
        / "snapshots"
        / "pre_night_v2"
        / date_value.isoformat()
        / "features"
    )

    parquet_path = (
        directory
        / "program_snapshot.parquet"
    )

    manifest_path = parquet_path.with_suffix(
        parquet_path.suffix + ".json"
    )

    return {
        "directory": directory,
        "parquet": parquet_path,
        "manifest": manifest_path,
    }


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


def atomic_write_parquet(
    frame: pd.DataFrame,
    destination: Path,
) -> None:
    temporary = destination.with_name(
        destination.name
        + "."
        + uuid.uuid4().hex
        + ".part"
    )

    try:
        frame.to_parquet(
            temporary,
            index=False,
            engine="pyarrow",
        )

        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())

        temporary.replace(destination)

    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def select_program_text_file(
    files,
    race_date,
) -> Path:
    date_value = normalize_race_date(race_date)
    expected_prefix = (
        "B" + date_value.strftime("%y%m%d")
    ).upper()

    candidates = sorted(
        Path(path)
        for path in files
        if (
            Path(path).is_file()
            and Path(path).stem.upper()
            == expected_prefix
        )
    )

    if len(candidates) != 1:
        raise PreNightFeatureContractError(
            "Expected exactly one program text "
            f"file for {date_value.isoformat()}: "
            f"{candidates}"
        )

    return candidates[0]


def normalize_timestamp(
    value,
    field_name: str,
) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except Exception as error:
        raise PreNightFeatureContractError(
            f"{field_name} is invalid: {value}"
        ) from error

    if timestamp.tzinfo is None:
        raise PreNightFeatureContractError(
            f"{field_name} must include timezone"
        )

    return timestamp


def validate_program_structure(
    frame: pd.DataFrame,
    race_date,
) -> dict:
    date_value = normalize_race_date(race_date)

    missing_columns = sorted(
        REQUIRED_PROGRAM_COLUMNS
        - set(frame.columns)
    )

    if missing_columns:
        raise PreNightFeatureContractError(
            "Required program columns missing: "
            f"{missing_columns}"
        )

    prohibited = sorted(
        PROHIBITED_COLUMNS
        & set(frame.columns)
    )

    if prohibited:
        raise PreNightFeatureContractError(
            "Prohibited post-race columns found: "
            f"{prohibited}"
        )

    if frame.empty:
        raise PreNightFeatureContractError(
            "Program frame is empty"
        )

    race_dates = pd.to_datetime(
        frame["race_date"],
        errors="coerce",
    ).dt.date

    if race_dates.isna().any():
        raise PreNightFeatureContractError(
            "race_date contains invalid values"
        )

    mismatches = int(
        (race_dates != date_value).sum()
    )

    if mismatches:
        raise PreNightFeatureContractError(
            "Program race_date mismatch rows: "
            f"{mismatches}"
        )

    duplicate_rows = int(
        frame.duplicated(ROW_KEYS).sum()
    )

    if duplicate_rows:
        raise PreNightFeatureContractError(
            "Duplicate program row keys: "
            f"{duplicate_rows}"
        )

    race_sizes = frame.groupby(
        RACE_KEYS,
        dropna=False,
    ).size()

    invalid_race_sizes = int(
        (race_sizes != 6).sum()
    )

    if invalid_race_sizes:
        raise PreNightFeatureContractError(
            "Invalid six-boat race count: "
            f"{invalid_race_sizes}"
        )

    invalid_boat_sets = int(
        frame.groupby(
            RACE_KEYS,
            dropna=False,
        )["boat_no"].apply(
            lambda values: (
                set(map(int, values))
                != set(range(1, 7))
            )
        ).sum()
    )

    if invalid_boat_sets:
        raise PreNightFeatureContractError(
            "Invalid boat set count: "
            f"{invalid_boat_sets}"
        )

    race_count = int(
        frame[RACE_KEYS]
        .drop_duplicates()
        .shape[0]
    )

    return {
        "row_count": int(len(frame)),
        "race_count": race_count,
        "duplicate_rows": duplicate_rows,
        "invalid_race_sizes": (
            invalid_race_sizes
        ),
        "invalid_boat_sets": (
            invalid_boat_sets
        ),
        "prohibited_columns": prohibited,
    }


def attach_feature_provenance(
    frame: pd.DataFrame,
    metadata: dict,
    race_date,
) -> pd.DataFrame:
    date_value = normalize_race_date(race_date)

    as_of_time = normalize_timestamp(
        metadata["as_of_time"],
        "as_of_time",
    )

    snapshot_at = normalize_timestamp(
        metadata["snapshot_at"],
        "snapshot_at",
    )

    fetched_at = normalize_timestamp(
        metadata["fetched_at"],
        "fetched_at",
    )

    expected_as_of = pd.Timestamp(
        build_pre_night_as_of(
            date_value
        )
    )

    if as_of_time != expected_as_of:
        raise PreNightPointInTimeError(
            "as_of_time does not match "
            "PREVIOUS_DAY_21_30_JST"
        )

    if snapshot_at != as_of_time:
        raise PreNightPointInTimeError(
            "snapshot_at must equal as_of_time"
        )

    if fetched_at > as_of_time:
        raise PreNightPointInTimeError(
            "feature source was fetched after "
            "as_of_time"
        )

    if (
        metadata.get(
            "eligible_for_pre_night"
        )
        is not True
    ):
        raise PreNightPointInTimeError(
            "source metadata is not eligible "
            "for PRE_NIGHT"
        )

    if (
        metadata.get(
            "eligibility_reason"
        )
        != "FETCHED_BY_AS_OF"
    ):
        raise PreNightPointInTimeError(
            "source eligibility reason is not "
            "FETCHED_BY_AS_OF"
        )

    if metadata.get("source_type") != "program":
        raise PreNightFeatureContractError(
            "feature source_type must be program"
        )

    if metadata.get("archive_type") != "program":
        raise PreNightFeatureContractError(
            "archive_type must be program"
        )

    if metadata.get("snapshot_type") != "PRE_NIGHT":
        raise PreNightFeatureContractError(
            "snapshot_type must be PRE_NIGHT"
        )

    if metadata.get("as_of_rule") != AS_OF_RULE:
        raise PreNightFeatureContractError(
            "as_of_rule mismatch"
        )

    if (
        metadata.get("contract_version")
        != CONTRACT_VERSION
    ):
        raise PreNightFeatureContractError(
            "source contract_version mismatch"
        )

    if (
        metadata.get("collector_version")
        != COLLECTOR_VERSION
    ):
        raise PreNightFeatureContractError(
            "collector_version mismatch"
        )

    source_sha256 = str(
        metadata.get(
            "archive_sha256",
            "",
        )
    ).strip().lower()

    if not source_sha256:
        raise PreNightFeatureContractError(
            "archive_sha256 is missing"
        )

    output = frame.copy()

    output["as_of_time"] = as_of_time
    output["snapshot_at"] = snapshot_at
    output["feature_version"] = (
        FEATURE_VERSION
    )
    output["feature_contract_version"] = (
        OUTPUT_CONTRACT_VERSION
    )
    output["feature_source_type"] = (
        "program"
    )
    output["feature_source_url"] = (
        metadata["source_url"]
    )
    output["feature_source_sha256"] = (
        source_sha256
    )
    output[
        "feature_source_fetched_at"
    ] = fetched_at
    output[
        "feature_source_max_time"
    ] = fetched_at
    output["source_max_time"] = fetched_at
    output[
        "feature_collector_version"
    ] = metadata["collector_version"]
    output["provenance_status"] = (
        "ELIGIBLE"
    )

    return output


def validate_point_in_time_frame(
    frame: pd.DataFrame,
) -> dict:
    missing_columns = sorted(
        PROVENANCE_COLUMNS
        - set(frame.columns)
    )

    if missing_columns:
        raise PreNightPointInTimeError(
            "Provenance columns missing: "
            f"{missing_columns}"
        )

    null_counts = {
        column: int(
            frame[column].isna().sum()
        )
        for column in sorted(
            PROVENANCE_COLUMNS
        )
    }

    null_rows = sum(null_counts.values())

    if null_rows:
        raise PreNightPointInTimeError(
            "Provenance null values found: "
            f"{null_counts}"
        )

    as_of_values = pd.to_datetime(
        frame["as_of_time"],
        utc=True,
        errors="coerce",
    )

    source_values = pd.to_datetime(
        frame["feature_source_max_time"],
        utc=True,
        errors="coerce",
    )

    legacy_source_values = pd.to_datetime(
        frame["source_max_time"],
        utc=True,
        errors="coerce",
    )

    invalid_timestamps = int(
        as_of_values.isna().sum()
        + source_values.isna().sum()
        + legacy_source_values.isna().sum()
    )

    if invalid_timestamps:
        raise PreNightPointInTimeError(
            "Invalid PIT timestamp values: "
            f"{invalid_timestamps}"
        )

    future_source_rows = int(
        (source_values > as_of_values).sum()
    )

    if future_source_rows:
        raise PreNightPointInTimeError(
            "Future feature source rows: "
            f"{future_source_rows}"
        )

    source_alias_mismatches = int(
        (
            source_values
            != legacy_source_values
        ).sum()
    )

    if source_alias_mismatches:
        raise PreNightPointInTimeError(
            "source_max_time alias mismatch rows: "
            f"{source_alias_mismatches}"
        )

    ineligible_status_rows = int(
        frame["provenance_status"]
        .ne("ELIGIBLE")
        .sum()
    )

    if ineligible_status_rows:
        raise PreNightPointInTimeError(
            "Ineligible provenance rows: "
            f"{ineligible_status_rows}"
        )

    source_sha_counts = int(
        frame[
            "feature_source_sha256"
        ].nunique(dropna=False)
    )

    if source_sha_counts != 1:
        raise PreNightPointInTimeError(
            "Expected exactly one program "
            "source SHA-256"
        )

    return {
        "future_source_rows": (
            future_source_rows
        ),
        "source_alias_mismatches": (
            source_alias_mismatches
        ),
        "ineligible_status_rows": (
            ineligible_status_rows
        ),
        "provenance_null_count": (
            null_rows
        ),
        "source_sha256_count": (
            source_sha_counts
        ),
    }


def validate_existing_output(
    paths: dict[str, Path],
    source_metadata: dict,
) -> dict:
    parquet_path = paths["parquet"]
    manifest_path = paths["manifest"]

    parquet_exists = parquet_path.is_file()
    manifest_exists = manifest_path.is_file()

    if parquet_exists != manifest_exists:
        raise PreNightOutputIntegrityError(
            "Parquet and manifest must exist "
            "together"
        )

    if not parquet_exists:
        raise PreNightOutputIntegrityError(
            "Existing output does not exist"
        )

    try:
        manifest = json.loads(
            manifest_path.read_text(
                encoding="utf-8"
            )
        )
    except Exception as error:
        raise PreNightOutputIntegrityError(
            "Output manifest is invalid"
        ) from error

    if (
        manifest.get("contract_version")
        != OUTPUT_CONTRACT_VERSION
    ):
        raise PreNightOutputIntegrityError(
            "Output contract version mismatch"
        )

    actual_sha256 = sha256_file(
        parquet_path
    )

    if (
        actual_sha256
        != manifest.get("parquet_sha256")
    ):
        raise PreNightOutputIntegrityError(
            "Parquet SHA-256 mismatch"
        )

    if (
        manifest.get("feature_source_sha256")
        != source_metadata.get(
            "archive_sha256"
        )
    ):
        raise PreNightOutputIntegrityError(
            "Source SHA-256 lineage mismatch"
        )

    frame = pd.read_parquet(
        parquet_path
    )

    structure = validate_program_structure(
        frame,
        source_metadata["race_date"],
    )

    pit = validate_point_in_time_frame(
        frame
    )

    if int(manifest["row_count"]) != len(frame):
        raise PreNightOutputIntegrityError(
            "Manifest row_count mismatch"
        )

    if (
        int(manifest["race_count"])
        != structure["race_count"]
    ):
        raise PreNightOutputIntegrityError(
            "Manifest race_count mismatch"
        )

    return {
        "paths": paths,
        "manifest": manifest,
        "structure": structure,
        "pit": pit,
        "skipped": True,
    }


def build_pre_night_program_parquet(
    race_date,
    data_root,
    overwrite=False,
    parser=None,
    extractor=None,
    snapshot_validator=None,
    now_fn=None,
) -> dict:
    date_value = normalize_race_date(race_date)

    parser = (
        parse_program_file
        if parser is None
        else parser
    )

    extractor = (
        extract_archive
        if extractor is None
        else extractor
    )

    snapshot_validator = (
        validate_cached_snapshot
        if snapshot_validator is None
        else snapshot_validator
    )

    clock = (
        now_fn
        if now_fn is not None
        else lambda: dt.datetime.now(
            dt.timezone.utc
        )
    )

    snapshot = snapshot_validator(
        date_value,
        data_root,
    )

    metadata = snapshot["metadata"]
    source_paths = snapshot["paths"]

    if (
        snapshot.get(
            "eligible_for_pre_night"
        )
        is not True
    ):
        raise PreNightPointInTimeError(
            "Program snapshot is not eligible"
        )

    paths = build_output_paths(
        date_value,
        data_root,
    )

    parquet_exists = (
        paths["parquet"].exists()
    )

    manifest_exists = (
        paths["manifest"].exists()
    )

    if (
        not overwrite
        and (
            parquet_exists
            or manifest_exists
        )
    ):
        cached_output = validate_existing_output(
                    paths,
                    metadata,
                )
        _validate_pipeline_pit_eligibility(
            cached_output["manifest"],
            race_date=date_value.isoformat(),
            as_of_time=metadata["as_of_time"],
        )
        return cached_output

    paths["directory"].mkdir(
        parents=True,
        exist_ok=True,
    )

    extracted_files = extractor(
        source_paths["archive"],
        data_root,
        "program",
        overwrite=False,
    )

    program_file = select_program_text_file(
        extracted_files,
        date_value,
    )

    frame = parser(
        program_file,
        race_date=date_value.isoformat(),
    )

    structure = validate_program_structure(
        frame,
        date_value,
    )

    output = attach_feature_provenance(
        frame,
        metadata,
        date_value,
    )

    pit = validate_point_in_time_frame(
        output
    )
    eligibility_decision = evaluate_pre_night_pit_eligibility(
        race_date=date_value.isoformat(),
        as_of_time=metadata["as_of_time"],
        details={
            "future_source_rows": pit["future_source_rows"],
            "provenance_null_count": pit["provenance_null_count"],
        },
    )

    atomic_write_parquet(
        output,
        paths["parquet"],
    )

    parquet_sha256 = sha256_file(
        paths["parquet"]
    )

    generated_at = clock()

    if (
        generated_at.tzinfo is None
        or generated_at.utcoffset() is None
    ):
        raise PreNightFeatureContractError(
            "generated_at clock must include "
            "timezone"
        )

    manifest = {
        "contract_version": (
            OUTPUT_CONTRACT_VERSION
        ),
        "feature_version": (
            FEATURE_VERSION
        ),
        "race_date": (
            date_value.isoformat()
        ),
        "as_of_rule": AS_OF_RULE,
        "as_of_time": (
            metadata["as_of_time"]
        ),
        "snapshot_at": (
            metadata["snapshot_at"]
        ),
        "feature_source_type": (
            "program"
        ),
        "feature_source_url": (
            metadata["source_url"]
        ),
        "feature_source_sha256": (
            metadata["archive_sha256"]
        ),
        "feature_source_fetched_at": (
            metadata["fetched_at"]
        ),
        "feature_source_max_time": (
            metadata["fetched_at"]
        ),
        "source_contract_version": (
            metadata["contract_version"]
        ),
        "collector_version": (
            metadata["collector_version"]
        ),
        "source_archive_path": str(
            source_paths["archive"]
        ),
        "source_metadata_path": str(
            source_paths["metadata"]
        ),
        "program_text_path": str(
            program_file
        ),
        "parquet_path": str(
            paths["parquet"]
        ),
        "parquet_sha256": (
            parquet_sha256
        ),
        "row_count": (
            structure["row_count"]
        ),
        "race_count": (
            structure["race_count"]
        ),
        "future_source_rows": (
            pit["future_source_rows"]
        ),
        "provenance_null_count": (
            pit["provenance_null_count"]
        ),
        "provenance_status": (
            "ELIGIBLE"
        ),
        "generated_at": (
            generated_at.isoformat()
        ),
    }
    manifest.update(
        _pit_manifest_fields(eligibility_decision)
    )
    _validate_pipeline_pit_eligibility(
        manifest,
        race_date=date_value.isoformat(),
        as_of_time=metadata["as_of_time"],
    )

    atomic_write_json(
        manifest,
        paths["manifest"],
    )

    return {
        "paths": paths,
        "manifest": manifest,
        "structure": structure,
        "pit": pit,
        "skipped": False,
    }

# BEGIN PRE_NIGHT_PIT_SAFETY_GATE_V1_INTEGRATION
from boatrace_ai.pipelines.pre_night_eligibility import (
    PreNightEligibilityDecision,
    decision_from_exception,
    eligible_decision,
)


def evaluate_pre_night_pit_eligibility(
    *,
    race_date: str,
    as_of_time: str,
    validation_error: BaseException | None = None,
    details: dict[str, object] | None = None,
) -> PreNightEligibilityDecision:
    """Expose snapshot validation as an explicit PIT decision.

    Existing validators remain authoritative. Call this helper with no
    error only after those validators have completed successfully.
    """

    if validation_error is None:
        return eligible_decision(
            race_date=race_date,
            as_of_time=as_of_time,
            details={} if details is None else details,
        )

    return decision_from_exception(
        validation_error,
        race_date=race_date,
        as_of_time=as_of_time,
        details={} if details is None else details,
    )
# END PRE_NIGHT_PIT_SAFETY_GATE_V1_INTEGRATION


# PRE_NIGHT_PIT_SAFETY_GATE_AST_RETRY_SNAPSHOT_V1
from boatrace_ai.pipelines.pre_night_eligibility import (
    PreNightEligibilityDecision as _PitDecision,
    manifest_eligibility_fields as _pit_manifest_fields,
)


def _validate_pipeline_pit_eligibility(
    manifest,
    *,
    race_date,
    as_of_time,
):
    """Strictly validate the four serialized PIT eligibility fields."""

    if not isinstance(manifest, dict):
        raise PreNightOutputIntegrityError(
            "Pipeline manifest must be a dict"
        )

    required = {
        "eligibility_status",
        "eligible_for_pre_night",
        "eligibility_reason",
        "pit_eligibility",
    }

    missing = required - set(manifest)
    if missing:
        raise PreNightOutputIntegrityError(
            "Pipeline manifest eligibility fields missing: "
            f"{sorted(missing)}"
        )

    nested = manifest.get("pit_eligibility")
    if not isinstance(nested, dict):
        raise PreNightOutputIntegrityError(
            "Pipeline pit_eligibility must be a dict"
        )

    nested_required = {
        "status",
        "eligible",
        "reason",
        "race_date",
        "as_of_time",
    }

    nested_missing = nested_required - set(nested)
    if nested_missing:
        raise PreNightOutputIntegrityError(
            "Pipeline nested eligibility fields missing: "
            f"{sorted(nested_missing)}"
        )

    if not isinstance(
        manifest.get("eligible_for_pre_night"),
        bool,
    ):
        raise PreNightOutputIntegrityError(
            "Pipeline eligible_for_pre_night must be bool"
        )

    if not isinstance(nested.get("eligible"), bool):
        raise PreNightOutputIntegrityError(
            "Pipeline nested eligible must be bool"
        )

    try:
        decision = _PitDecision(
            status=nested["status"],
            eligible=nested["eligible"],
            reason=nested["reason"],
            race_date=nested["race_date"],
            as_of_time=nested["as_of_time"],
            details=nested.get("details", {}),
        )
        expected = _pit_manifest_fields(decision)
    except Exception as exc:
        raise PreNightOutputIntegrityError(
            "Pipeline PIT eligibility decision is invalid"
        ) from exc

    for field_name in sorted(required):
        if manifest.get(field_name) != expected[field_name]:
            raise PreNightOutputIntegrityError(
                "Pipeline PIT eligibility mismatch: "
                f"{field_name}"
            )

    if str(nested["race_date"]) != str(race_date):
        raise PreNightOutputIntegrityError(
            "Pipeline PIT eligibility race_date mismatch"
        )

    if str(nested["as_of_time"]) != str(as_of_time):
        raise PreNightOutputIntegrityError(
            "Pipeline PIT eligibility as_of_time mismatch"
        )

    return expected
