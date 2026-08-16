"""Fail-closed CLI for the pre-night daily pipeline."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
import sys
from typing import Any, Sequence

from boatrace_ai.pipelines.pre_night_bound_daily import (
    run_pre_night_bound_daily,
)
from boatrace_ai.pipelines.pre_night_daily import (
    run_pre_night_daily,
)


CUTOFF_DATE = date(2026, 6, 30)

APPROVED_DATA_ROOTS = (
    Path(
        "/content/drive/MyDrive/"
        "boatrace-ai-data/raw/pre_night"
    ),
    Path(
        "/content/drive/MyDrive/"
        "boatrace-ai-data/incoming/pre_night"
    ),
    Path(
        "/content/drive/MyDrive/"
        "boatrace-ai-data/future/pre_night"
    ),
    Path(
        "/content/drive/MyDrive/boatrace-ai-data/"
        "fresh_holdout_sources/pre_night"
    ),
)

FORBIDDEN_BASENAMES = {
    "train.parquet",
    "validation.parquet",
    "test.parquet",
    "holdout.parquet",
    "excluded_races.parquet",
}

_VENUE_CODE_RE = re.compile(
    r"^(0[1-9]|1[0-9]|2[0-4])$"
)
_RUN_ID_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
)


def _parse_race_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "race-date must be a valid YYYY-MM-DD date"
        ) from exc

    if parsed <= CUTOFF_DATE:
        raise argparse.ArgumentTypeError(
            "race-date must be 2026-07-01 or later"
        )

    return parsed


def _parse_venue_code(value: str) -> str:
    if _VENUE_CODE_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(
            "venue-code must be a two-digit value "
            "from 01 through 24"
        )

    return value


def _parse_run_id(value: str) -> str:
    if _RUN_ID_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(
            "run-id must start with an alphanumeric "
            "character and contain only letters, digits, "
            "periods, underscores, or hyphens; "
            "maximum length is 128"
        )

    return value


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validate_data_root(value: str) -> Path:
    raw_path = Path(value).expanduser()

    if any(
        part.lower() in FORBIDDEN_BASENAMES
        for part in raw_path.parts
    ):
        raise argparse.ArgumentTypeError(
            "data-root must not reference "
            "train/test/validation/holdout data"
        )

    resolved = raw_path.resolve(strict=False)
    approved = tuple(
        root.expanduser().resolve(strict=False)
        for root in APPROVED_DATA_ROOTS
    )

    if not any(
        resolved == root
        or _is_relative_to(resolved, root)
        for root in approved
    ):
        allowed_text = ", ".join(
            str(root) for root in approved
        )
        raise argparse.ArgumentTypeError(
            "data-root must be inside one of: "
            + allowed_text
        )

    return resolved


def _load_json_object(
    path_value: str | None,
    *,
    label: str,
    required: bool = False,
) -> dict[str, Any] | None:
    if path_value is None:
        if required:
            raise ValueError(
                f"{label} JSON file is required"
            )
        return None

    path = Path(path_value).expanduser()

    if path.is_symlink():
        raise ValueError(
            f"{label} JSON file must not be a symlink"
        )

    if not path.is_file():
        raise ValueError(
            f"{label} JSON file does not exist: {path}"
        )

    try:
        text = path.read_text(encoding="utf-8")
        value = json.loads(text)
    except (OSError, UnicodeError) as exc:
        raise ValueError(
            f"{label} JSON file could not be read"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{label} must contain valid JSON"
        ) from exc

    if not isinstance(value, dict):
        raise ValueError(
            f"{label} JSON root must be an object"
        )

    if not value:
        raise ValueError(
            f"{label} JSON object must not be empty"
        )

    return value


def _load_deadline_evidence(
    path_value: str | None,
) -> dict[str, Any] | None:
    return _load_json_object(
        path_value,
        label="deadline evidence",
        required=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="boatrace-pre-night",
        description=(
            "Run the pre-night daily pipeline. "
            "The default mode is dry-run. "
            "Specify both --venue-code and --run-id "
            "to enable venue-bound runtime wiring."
        ),
    )

    parser.add_argument(
        "--race-date",
        required=True,
        type=_parse_race_date,
        help="race date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--data-root",
        required=True,
        type=_validate_data_root,
        help="approved pre-night data root",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="plan the run without live publication",
    )
    mode_group.add_argument(
        "--live",
        action="store_true",
        help="perform the live pipeline run",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="allow supported live artifacts to be replaced",
    )
    parser.add_argument(
        "--deadline-evidence",
        default=None,
        help="path to deadline evidence JSON",
    )
    parser.add_argument(
        "--venue-code",
        default=None,
        type=_parse_venue_code,
        help=(
            "two-digit venue code from 01 through 24; "
            "must be used with --run-id"
        ),
    )
    parser.add_argument(
        "--run-id",
        default=None,
        type=_parse_run_id,
        help=(
            "immutable venue-bound run identifier; "
            "must be used with --venue-code"
        ),
    )
    parser.add_argument(
        "--authorization-state",
        default=None,
        help=(
            "path to authorization-state JSON; required "
            "for venue-bound live runs"
        ),
    )
    parser.add_argument(
        "--test-state",
        default=None,
        help=(
            "path to test-state JSON; required "
            "for venue-bound live runs"
        ),
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    dry_run = not args.live

    if dry_run and args.overwrite:
        parser.error(
            "--overwrite may only be used with --live"
        )

    if args.live and not args.deadline_evidence:
        parser.error(
            "--live requires --deadline-evidence"
        )

    venue_supplied = args.venue_code is not None
    run_id_supplied = args.run_id is not None

    if venue_supplied != run_id_supplied:
        parser.error(
            "--venue-code and --run-id must be "
            "specified together"
        )

    venue_bound = venue_supplied and run_id_supplied

    state_options_supplied = (
        args.authorization_state is not None
        or args.test_state is not None
    )

    if state_options_supplied and not venue_bound:
        parser.error(
            "--authorization-state and --test-state "
            "may only be used with --venue-code "
            "and --run-id"
        )

    if venue_bound and args.live:
        if not args.authorization_state:
            parser.error(
                "venue-bound --live requires "
                "--authorization-state"
            )

        if not args.test_state:
            parser.error(
                "venue-bound --live requires --test-state"
            )

    try:
        deadline_evidence = _load_deadline_evidence(
            args.deadline_evidence
        )

        authorization_state = _load_json_object(
            args.authorization_state,
            label="authorization state",
            required=venue_bound and args.live,
        )

        test_state = _load_json_object(
            args.test_state,
            label="test state",
            required=venue_bound and args.live,
        )

        if venue_bound:
            result = run_pre_night_bound_daily(
                args.race_date.isoformat(),
                args.data_root,
                venue_code=args.venue_code,
                run_id=args.run_id,
                dry_run=dry_run,
                overwrite=args.overwrite,
                deadline_evidence=deadline_evidence,
                authorization_state=authorization_state,
                test_state=test_state,
            )
            runtime_mode = "VENUE_BOUND"
        else:
            result = run_pre_night_daily(
                args.race_date.isoformat(),
                args.data_root,
                dry_run=dry_run,
                overwrite=args.overwrite,
                deadline_evidence=deadline_evidence,
            )
            runtime_mode = "LEGACY"
    except Exception as exc:
        error_output = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "dry_run": dry_run,
            "runtime_mode": (
                "VENUE_BOUND"
                if venue_bound
                else "LEGACY"
            ),
        }
        print(
            json.dumps(
                error_output,
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1

    output = {
        "ok": True,
        "race_date": args.race_date.isoformat(),
        "data_root": str(args.data_root),
        "dry_run": dry_run,
        "overwrite": args.overwrite,
        "runtime_mode": runtime_mode,
        "result": result,
    }

    if venue_bound:
        output["venue_code"] = args.venue_code
        output["run_id"] = args.run_id

    print(
        json.dumps(
            output,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
