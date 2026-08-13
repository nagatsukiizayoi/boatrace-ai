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

<!-- D1B5-STAGE1-R2-DEADLINE-EVIDENCE-BEGIN -->
## Deadline Evidence publication

Contract: `D1B5-STAGE1-DEADLINE-EVIDENCE-PUBLICATION-V2-R2-APPROVED`

In live mode, validated Deadline Evidence is published before the
program collector starts.

The final artifact is stored under the following path pattern:

    prospective/pre_night/deadline_evidence/YYYY/MM/DD/VV/deadline_evidence.json

`VV` is the two-digit venue code.

Successful publication states:

- `CREATED`: a new canonical artifact was published and verified.
- `VALIDATED_REUSE`: an identical artifact was verified and reused.

Malformed, non-canonical, digest-mismatched or conflicting artifacts
stop execution before downstream processing.
<!-- D1B5-STAGE1-R2-DEADLINE-EVIDENCE-END -->
