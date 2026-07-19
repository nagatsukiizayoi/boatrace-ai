# Payout Schema v1 Draft

## 1. Document information

- Schema name: Payout Schema
- Version: v1 Draft
- Target system: boatrace-ai
- Implementation branch: `feat/payout-etl-v1`
- Parser checkpoint: `b3a1df6863b6d2e05dd9a6d2d28f011810d0f044`
- Daily ETL checkpoint: `213c0cee1e3da1811d338f9aaed64d52c0debaa2`
- Reference data: June 2026, all 30 days
- Validation status: parser, daily ETL, 28 tests and full-month validation succeeded

## 2. Storage path

- Daily payout data: `curated/payouts/YYYY/MM/DD/race_payouts.parquet`
- Daily quality data: `curated/races/YYYY/MM/DD/quality.json`
- Monthly execution summary: `system/monthly_runs/YYYYMM/monthly_etl_summary.json`

Payout data is stored separately from entry, result and merged race data.

## 3. Row definition

One record represents one race, one bet type and one winning selection or payout status.

Multiple winning selections are stored as multiple records. No fixed limit is imposed on the number of selections for any bet type.

Examples:

- A QUINELLA_PLACE race with three winning combinations creates three records.
- An EXACTA race with two winning combinations creates two records.
- A cancelled race creates seven records, one for each bet type.
- A bet type marked NOT_ESTABLISHED creates one record without a combination.

## 4. Primary key

The logical primary key is:

- `race_date`
- `venue_code`
- `race_no`
- `bet_type`
- `selection_no`
- `payout_status`

Duplicate primary keys are rejected by the parser and daily quality checks.

## 5. Bet types

| Raw value | bet_type | Ordered | Combination length |
|---|---|---:|---:|
| 単勝 | WIN | false | 1 |
| 複勝 | PLACE | false | 1 |
| 2連単 | EXACTA | true | 2 |
| 2連複 | QUINELLA | false | 2 |
| 拡連複 | QUINELLA_PLACE | false | 2 |
| 3連単 | TRIFECTA | true | 3 |
| 3連複 | TRIO | false | 3 |

Every race is expected to contain all seven bet-type groups, including cancelled and not-established groups.

## 6. Column definitions

| Column | Type | Null allowed | Description |
|---|---|---:|---|
| race_date | string | no | Race date in YYYY-MM-DD format |
| venue_code | string | no | Two-digit venue code |
| race_no | int64 | no | Race number from 1 to 12 |
| bet_type | string | no | Normalized bet type |
| bet_type_raw | string | no | Original Japanese bet type |
| selection_no | int64 | no | Sequence within race, bet type and status |
| combination | string | yes | Normalized winning combination |
| combination_raw | string | yes | Original combination representation |
| payout_yen | Int64 | yes | Payout amount in yen |
| popularity | Int64 | yes | Popularity rank |
| payout_status | string | no | Payout status |
| is_ordered | boolean | no | Whether combination order matters |
| source_kind | string | no | DETAIL or SUMMARY |
| source_line_no | int64 | no | Original TXT line number |
| source_file | string | no | Original result TXT filename |

## 7. Payout status

### NORMAL

- A valid winning combination and positive payout are required.
- Popularity is required for EXACTA, QUINELLA, QUINELLA_PLACE, TRIFECTA and TRIO.
- Popularity is not required for WIN and PLACE.

### SPECIAL_PAYOUT

- Used for 特払い.
- A positive payout is required.
- Combination and popularity may be null.

### NOT_ESTABLISHED

- Used for 不成立.
- Combination, payout and popularity may be null.
- A full race marked レース不成立 creates all seven bet-type records.

### CANCELLED

- Used for races shown as 中止 in the payout summary.
- A cancelled race creates all seven bet-type records.
- Combination, payout and popularity are null.

### REFUND

- Reserved for explicit 返還 records.
- Combination and payout rules depend on the original source record.

### MISSING

- Reserved for future use.
- Missing groups are currently treated as daily quality errors rather than written as MISSING rows.

## 8. Source priority

The race-detail payout block is the authoritative source for normal, special and not-established payouts.

The top payout summary is not used for normal payouts because it duplicates detailed records and contains fewer bet types.

The summary is used only to identify fully cancelled races that have no race-detail block.

Source priority:

1. DETAIL for NORMAL, SPECIAL_PAYOUT and NOT_ESTABLISHED
2. SUMMARY for CANCELLED

## 9. Validation rules

- All boat numbers must be between 1 and 6.
- A combination must not contain duplicate boat numbers.
- Combination length must match the bet type.
- NORMAL and SPECIAL_PAYOUT amounts must be positive.
- Required popularity values must be positive.
- Every entry race must have seven payout groups.
- Unexpected payout groups are rejected.
- Duplicate primary keys are rejected.
- Result-side cancelled races must match payout-side CANCELLED races.
- Partial result absence remains a quality error.

## 10. Daily quality fields

- `payout_record_count`
- `payout_group_count`
- `payout_expected_group_count`
- `payout_missing_group_count`
- `payout_unexpected_group_count`
- `payout_duplicate_keys`
- `payout_invalid_combinations`
- `payout_invalid_amounts`
- `payout_invalid_popularity`
- `payout_summary_mismatch`
- `payout_cancelled_group_count`
- `payout_not_established_count`
- `payout_special_count`
- `payout_status_counts`
- `payout_bet_type_counts`

The following payout fields must be zero for daily SUCCESS:

- `payout_missing_group_count`
- `payout_unexpected_group_count`
- `payout_duplicate_keys`
- `payout_invalid_combinations`
- `payout_invalid_amounts`
- `payout_invalid_popularity`
- `payout_summary_mismatch`

## 11. June 2026 validation baseline

| Metric | Value |
|---|---:|
| Days | 30 |
| Entry records | 28,152 |
| Result records | 27,726 |
| Merged records | 28,152 |
| Races | 4,692 |
| Venue-days | 391 |
| Payout records | 46,687 |
| Payout groups | 32,844 |
| NORMAL | 46,165 |
| CANCELLED | 497 |
| NOT_ESTABLISHED | 19 |
| SPECIAL_PAYOUT | 6 |
| Cancelled races | 71 |
| Cancelled entry rows | 426 |
| Duplicate payout keys | 0 |
| Missing payout groups | 0 |
| Unexpected payout groups | 0 |
| Invalid combinations | 0 |
| Cancellation mismatches | 0 |

## 12. Release criteria

Payout Schema v1 can be promoted from Draft after:

1. All automated tests pass.
2. GitHub Actions succeeds.
3. All 30 June 2026 daily files produce SUCCESS.
4. Payout record count is 46,687.
5. Payout group count is 32,844.
6. All required-zero payout quality fields are zero.
7. The 71 cancelled races match entry and payout records.
8. The full-month production artifacts are regenerated.
9. April and May 2026 are validated with the same schema.
10. The schema document is updated from Draft to Final.
