# Pre-night collector CLI

## Purpose

This CLI invokes the existing pre-night daily pipeline
through a fail-closed command-line interface.

## Dry-run

The default mode is dry-run.

```bash
boatrace-pre-night \
  --race-date 2026-07-01 \
  --data-root /content/drive/MyDrive/boatrace-ai-data/incoming/pre_night
```

The Python module form is also available.

```bash
python -m boatrace_ai.cli.pre_night \
  --race-date 2026-07-01 \
  --data-root /content/drive/MyDrive/boatrace-ai-data/incoming/pre_night
```

## Live mode

Do not run live mode before human review.

```bash
boatrace-pre-night \
  --race-date 2026-07-01 \
  --data-root /content/drive/MyDrive/boatrace-ai-data/incoming/pre_night \
  --live \
  --deadline-evidence /path/to/deadline-evidence.json
```

## Safety constraints

- Race date must be 2026-07-01 or later.
- Only approved output roots are accepted.
- Train, validation, test and holdout paths are rejected.
- Live mode requires deadline evidence.
- Live execution requires human review.
- Fresh Holdout evaluation is a separate process.
