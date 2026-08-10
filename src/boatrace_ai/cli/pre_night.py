"""Fail-closed CLI for the pre-night daily pipeline."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys
from typing import Any, Sequence

from boatrace_ai.pipelines.pre_night_daily import run_pre_night_daily


CUTOFF_DATE = date(2026, 6, 30)

APPROVED_DATA_ROOTS = (
    Path("/content/drive/MyDrive/boatrace-ai-data/raw/pre_night"),
    Path("/content/drive/MyDrive/boatrace-ai-data/incoming/pre_night"),
    Path("/content/drive/MyDrive/boatrace-ai-data/future/pre_night"),
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


def _parse_race_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "race-dateはYYYY-MM-DD形式で指定してください"
        ) from exc

    if parsed <= CUTOFF_DATE:
        raise argparse.ArgumentTypeError(
            "race-dateは2026-07-01以降でなければなりません"
        )

    return parsed


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
            "train/test/holdout等の禁止ファイルは指定できません"
        )

    resolved = raw_path.resolve(strict=False)
    approved = tuple(
        root.expanduser().resolve(strict=False)
        for root in APPROVED_DATA_ROOTS
    )

    if not any(
        resolved == root or _is_relative_to(resolved, root)
        for root in approved
    ):
        allowed_text = ", ".join(str(root) for root in approved)
        raise argparse.ArgumentTypeError(
            "data-rootは承認済みルート配下に限定されます: "
            + allowed_text
        )

    return resolved


def _load_deadline_evidence(path_value: str | None) -> dict[str, Any] | None:
    if path_value is None:
        return None

    path = Path(path_value).expanduser()

    if path.is_symlink():
        raise ValueError(
            "deadline evidenceにシンボリックリンクは使用できません"
        )

    if not path.is_file():
        raise ValueError(
            f"deadline evidenceが存在しません: {path}"
        )

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            "deadline evidenceは有効なJSONでなければなりません"
        ) from exc

    if not isinstance(value, dict) or not value:
        raise ValueError(
            "deadline evidenceは空でないJSONオブジェクトが必要です"
        )

    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="boatrace-pre-night",
        description=(
            "Run the pre-night daily pipeline. "
            "The default mode is dry-run."
        ),
    )

    parser.add_argument(
        "--race-date",
        required=True,
        type=_parse_race_date,
        help="対象レース日。YYYY-MM-DD、2026-07-01以降",
    )
    parser.add_argument(
        "--data-root",
        required=True,
        type=_validate_data_root,
        help="承認済みpre-night出力ルート",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="書き込みを伴わない検証モード。省略時もdry-run",
    )
    mode_group.add_argument(
        "--live",
        action="store_true",
        help="実収集モード。deadline evidenceが必須",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="既存成果物の上書きを要求する",
    )
    parser.add_argument(
        "--deadline-evidence",
        default=None,
        help="deadline evidence JSONのパス",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    dry_run = not args.live

    if dry_run and args.overwrite:
        parser.error(
            "--overwriteは--liveと同時に指定する必要があります"
        )

    if args.live and not args.deadline_evidence:
        parser.error(
            "--liveには--deadline-evidenceが必要です"
        )

    try:
        deadline_evidence = _load_deadline_evidence(
            args.deadline_evidence
        )

        result = run_pre_night_daily(
            args.race_date.isoformat(),
            args.data_root,
            dry_run=dry_run,
            overwrite=args.overwrite,
            deadline_evidence=deadline_evidence,
        )
    except Exception as exc:
        error_output = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "dry_run": dry_run,
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
        "result": result,
    }

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
