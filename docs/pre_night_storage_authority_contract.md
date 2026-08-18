# Pre-Night Storage Authority Contract

Status: Proposed normative contract for Option A.

## 1. Purpose

This contract separates pre-night staging artifacts from
authoritative prospective artifacts.

The existence of an artifact in staging does not make that
artifact prospective, authoritative, model-approved, or
production-approved.

## 2. Normative terms

The words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, and MAY
are normative requirements in this contract.

## 3. Storage roles

### 3.1 Staging root

The staging root is:

```text
/content/drive/MyDrive/boatrace-ai-data/raw/pre_night
```

Artifacts under this root MUST be classified as:

```text
NON_AUTHORITATIVE_STAGING
```

The staging root MAY contain:

- downloaded source archives;
- source metadata;
- extracted source files;
- parser intermediate results;
- cache entries;
- snapshot candidates;
- feature-matrix candidates;
- manifest candidates;
- rejected or unverified artifacts.

A staging artifact MUST NOT be treated as:

- `PROSPECTIVE_PIT_CERTIFIED`;
- an authoritative model input;
- an approved prediction input;
- a production-approved artifact.

### 3.2 Authority root

The authority root is:

```text
/content/drive/MyDrive/boatrace-ai-data/prospective/pre_night
```

Only artifacts that pass the complete promotion contract MAY
be published under this root.

An authoritative prospective artifact MUST be classified as:

```text
PROSPECTIVE_PIT_CERTIFIED
```

The authority root MUST NOT be used as an unverified download,
extraction, parser-work, or cache directory.

## 4. Existing CLI compatibility

The existing pre-night `--data-root` option MUST retain its
current staging role until an explicit authority-root interface
is implemented, tested, and reviewed.

For the existing CLI contract:

```text
--data-root = staging root
```

The implementation MUST NOT silently reinterpret `--data-root`
as an authority root.

The authority root MUST be supplied through an explicit and
independently validated contract.

It MUST NOT be inferred merely by replacing `raw/pre_night` with
`prospective/pre_night` in a path string.

This document alone does not define a new CLI option.
A new CLI option requires source changes, tests, help-text changes,
and human review.

## 5. Binding keys

Every promoted artifact MUST be bound to:

```text
race_date + venue_code
```

When deeper granularity is required, the keys are:

```text
race_date + venue_code + race_no
race_date + venue_code + race_no + boat_no
```

`venue_code` MUST remain a two-character string in the supported
range.

Date, venue, race, or boat mismatches MUST fail closed.
They MUST NOT be silently normalized during promotion.

## 6. Promotion boundary

Copying or moving a file from staging to the authority root is
not, by itself, promotion.

Promotion MUST be performed through one reviewed entry point.

The promotion entry point MUST verify, as applicable:

1. approved source identity;
2. source archive exact-byte SHA-256;
3. source metadata exact-byte SHA-256;
4. Deadline Evidence;
5. Deadline Evidence Collection;
6. Program Entries Binding;
7. venue-scoped Snapshot;
8. Pipeline Manifest;
9. Execution Manifest;
10. Prospective Manifest v3;
11. race-date and venue-code consistency;
12. point-in-time eligibility;
13. absence of post-race fields;
14. Git branch and commit binding;
15. authorization binding;
16. target-path containment;
17. immutable publication requirements;
18. post-write exact-byte verification.

If a required check is missing, false, malformed, or inconsistent,
promotion MUST fail closed.

## 7. Point-in-time requirements

Promotion MUST NOT occur after the approved `as_of_time`.

A run blocked before acquisition because the as-of time passed
MUST use a classification equivalent to:

```text
NOT_STARTED_BLOCKED_AFTER_AS_OF
```

An acquired or generated artifact that violates PIT requirements
MUST use a classification equivalent to:

```text
REJECTED_PIT_VIOLATION
```

Neither classification is eligible for authority publication.

The implementation MUST NOT spoof, override, or backdate time
evidence.

## 8. Manifest chain

The authoritative chain is:

```text
Source Archive
-> Source Metadata
-> Deadline Evidence
-> Deadline Evidence Collection
-> Program Entries Binding
-> Snapshot
-> Pipeline Manifest
-> Execution Manifest
-> Prospective Manifest v3
-> Feature Matrix
-> Approved Model
-> Prediction Artifact
```

Each downstream manifest MUST bind the exact-byte SHA-256 of its
required upstream artifacts.

A missing or mismatched digest MUST fail closed.

## 9. Canonical JSON

Contract JSON MUST use a documented canonical representation.

Unless a schema defines a stricter representation, canonical JSON
uses:

- UTF-8;
- `ensure_ascii=False`;
- sorted keys;
- compact separators;
- a documented final-newline policy.

SHA-256 MUST be calculated over the exact canonical bytes required
by the relevant schema.

## 10. Manifest digest rule

A manifest MUST NOT calculate a payload digest over a field that
contains that same payload digest.

The common procedure is:

1. exclude digest fields defined by the schema;
2. canonicalize the remaining payload;
3. calculate `manifest_payload_sha256`;
4. construct the completed manifest;
5. write the completed canonical manifest;
6. calculate the completed file SHA-256 externally or in an
   upstream receipt.

Schemas SHOULD distinguish:

```text
manifest_payload_sha256
manifest_file_sha256
```

## 11. Immutable publication

Authority publication MUST be immutable.

When an authority destination already exists:

- identical canonical bytes MAY produce `VALIDATED_REUSE`;
- different bytes MUST produce
  `IMMUTABLE_PUBLICATION_CONFLICT`.

An authority conflict MUST NOT be resolved through:

- overwrite;
- automatic deletion;
- backup followed by replacement;
- latest-file selection;
- warning-only continuation.

## 12. Drive publication

Google Drive mounted storage MUST NOT automatically be assumed to
have the same atomicity and durability semantics as a local POSIX
file system.

Drive publication MUST use a capability-appropriate contract such
as:

```text
CONTROLLED_DRIVE_PUBLICATION_WITH_POST_WRITE_VERIFICATION
```

Publication MUST reopen the final destination and recalculate its
exact-byte SHA-256.

If required publication capabilities cannot be demonstrated,
publication MUST stop without creating an authoritative result.

## 13. Model-input restriction

Model inference MUST NOT consume an artifact merely because it
exists under the staging root.

An approved model input MUST be bound to:

- a valid Prospective Manifest v3;
- a valid Feature Matrix Manifest;
- a matching feature contract;
- an approved model digest;
- a complete authoritative digest chain.

A direct staging-to-model path MUST fail closed.

## 14. Existing artifacts

Existing artifacts under `raw/pre_night` MUST NOT be bulk moved
or automatically promoted.

Existing artifacts MUST be classified as one of:

```text
NON_AUTHORITATIVE_STAGING
HISTORICAL_NON_PROSPECTIVE
NON_AUTHORITATIVE_UNVERIFIABLE
```

An existing artifact MAY be promoted only through an independently
reviewed migration-verification process that satisfies the same
authority requirements as a new artifact.

## 15. Required implementation sequence

Implementation MUST proceed in this order:

1. approve this storage-authority contract;
2. add failing contract tests;
3. define explicit staging-root and authority-root configuration;
4. implement one promotion entry point;
5. implement immutable publication and post-write verification;
6. run targeted tests;
7. run the full test suite;
8. review the complete Git diff;
9. run a future-date dry-run before the approved as-of time;
10. require separate authorization before live execution.

## 16. Non-goals

This contract does not:

- authorize live execution;
- authorize network collection;
- authorize betting;
- approve an existing research model;
- make existing staging artifacts prospective;
- permit reuse of an expired authorization or run ID;
- permit execution after the approved as-of time.

## Option A CLI root contract

The pre-night CLI separates the staging root from the prospective
authority root.

- `--data-root` identifies the non-authoritative staging root.
- The staging layout is `raw/pre_night`.
- Artifacts held under the staging root are classified as
  `NON_AUTHORITATIVE_STAGING`.
- `--authority-root` identifies the explicit prospective authority root.
- The authority layout is `prospective/pre_night`.
- Only artifacts that pass the complete promotion contract may be
  classified as `PROSPECTIVE_PIT_CERTIFIED`.

`--data-root` and `--authority-root` have distinct argparse destination
values: `data_root` and `authority_root`. The authority root must not be
derived implicitly from the staging root.

The public CLI parser requires `--authority-root` and forwards it
explicitly through the venue-bound runtime wiring. Legacy programmatic
invocations are handled only by the compatibility adapter in `main()`.
This storage-authority wiring does not authorize collection, publication,
model inference, betting, or live execution.

<!-- authority-root-backward-compatibility -->

## Backward compatibility

`--authority-root` is available as an explicit storage-authority
option. It remains optional at the global CLI parsing boundary so
existing staging-only invocations retain their established behavior.

When omitted, the CLI preserves the legacy staging contract. When
supplied for venue-bound processing, the validated authority root is
propagated through `run_pre_night_bound_daily()` to the prospective
publication stages. The authority path must end with
`prospective/pre_night`.

Supplying `--authority-root` does not authorize live execution.
The existing `--live`, deadline-evidence, authorization-state, and
test-state controls remain independent requirements.
