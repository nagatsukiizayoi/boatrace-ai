# Historical ETL Schema v1

## 1. 文書情報

- スキーマ名: Historical ETL Schema
- スキーマバージョン: 1
- 対象システム: boatrace-ai
- 基準コミット: c90ac2368d3d6a887aac0eba7b26fbb77220fdae
- 基準データ: 2026年6月
- 基準月次状態: SUCCESS
- 自動テスト: 21件合格
- GitHub Actions: 成功

この文書は、公式番組表・成績データから生成する履歴ETL成果物の初期安定スキーマを定義する。

## 2. 保存構成

- `raw/programs/bYYMMDD.lzh`: 公式番組表圧縮ファイル
- `raw/results/kYYMMDD.lzh`: 公式成績圧縮ファイル
- `temporary/programs/bYYMMDD/BYYMMDD.TXT`: 展開後番組表
- `temporary/results/kYYMMDD/KYYMMDD.TXT`: 展開後成績
- `curated/entries/YYYY/MM/DD/program_entries.parquet`: 番組表
- `curated/results/YYYY/MM/DD/race_results.parquet`: 成績
- `curated/races/YYYY/MM/DD/program_result_merged.parquet`: 統合データ
- `curated/races/YYYY/MM/DD/quality.json`: 日次品質情報
- `system/monthly_runs/YYYYMM/monthly_etl_summary.json`: 月次処理情報

## 3. 共通キー

選手行の結合キーは次の5列とする。

- `race_date`
- `venue_code`
- `race_no`
- `boat_no`
- `racer_id`

レースキーは次の3列とする。

- `race_date`
- `venue_code`
- `race_no`

結合キーは日別ファイル内で一意でなければならない。

値の基本表現は次のとおり。

- `race_date`: YYYY-MM-DD形式
- `venue_code`: 2桁文字列
- `race_no`: 1～12の整数
- `boat_no`: 1～6の整数
- `racer_id`: 4桁文字列
- 割合: パーセント単位
- 欠損可能な整数: 論理的にnullable integerとして扱う

## 4. program_entries.parquet

番組表の選手エントリーを1選手1行で保存する。

2026年6月27日の確認形状は1080行×20列。

| 列名 | pandas型 | 必須 | 説明 |
|---|---|---:|---|
| race_date | object | Yes | レース日 |
| venue_code | object | Yes | 会場コード |
| race_no | int64 | Yes | レース番号 |
| boat_no | int64 | Yes | 枠・艇番号 |
| racer_id | string | Yes | 選手ID |
| racer_name | object | Yes | 番組表の選手名 |
| age | int64 | Yes | 年齢 |
| branch | object | Yes | 支部 |
| weight_kg | int64 | Yes | 体重 |
| class | string | Yes | 級別 |
| national_win_rate | float64 | Yes | 全国勝率 |
| national_place2_rate_pct | float64 | Yes | 全国2連率 |
| local_win_rate | float64 | Yes | 当地勝率 |
| local_place2_rate_pct | float64 | Yes | 当地2連率 |
| motor_no | int64 | Yes | モーター番号 |
| motor_place2_rate_pct | float64 | Yes | モーター2連率 |
| boat_no_equipment | int64 | Yes | ボート機材番号 |
| boat_place2_rate_pct | float64 | Yes | ボート2連率 |
| series_results_raw | string | No | 節内成績の原文 |
| source_file | object | Yes | 番組表TXTファイル名 |

### 4.1 機材番号境界

ボート機材番号は、100番台または1～2桁として解析する。

使用する境界パターンは `(?:1\d{2}|\d{1,2})`。

確認例:

- `62100.00` は機材番号62、2連率100.00
- `47100.00` は機材番号47、2連率100.00
- `167 28.89` は機材番号167、2連率28.89

## 5. race_results.parquet

成績データを1選手1行で保存する。

2026年6月27日の確認形状は1008行×15列。

中止によって成績が存在しない選手行は、このファイルへ追加しない。

| 列名 | pandas型 | 必須 | 説明 |
|---|---|---:|---|
| race_date | object | Yes | レース日 |
| venue_code | object | Yes | 会場コード |
| race_no | int64 | Yes | レース番号 |
| finish_raw | string | Yes | 着順または特殊着順 |
| boat_no | int64 | Yes | 艇番号 |
| racer_id | string | Yes | 選手ID |
| racer_name_result | object | Yes | 成績表の選手名 |
| motor_no_result | int64 | Yes | モーター番号 |
| boat_no_equipment_result | int64 | Yes | ボート機材番号 |
| exhibition_time | Float64 | No | 展示タイム |
| course | Int64 | No | 進入コース |
| start_timing_raw | object | No | ST原文 |
| race_time_raw | object | No | レースタイム原文 |
| finish_position | Int64 | No | 通常着順1～6 |
| source_file_result | object | Yes | 成績TXTファイル名 |

### 5.1 レース距離

レース見出しは1800mに限定しない。

対応パターンは `H\d{4}m`。

確認済み距離:

- 1800m
- 1200m

### 5.2 特殊着順

通常着順以外は `finish_raw` に保持し、`finish_position` を欠損とする。

確認対象例:

- 00
- F
- L0
- K0
- K1
- S0
- S1
- S2

特殊行で数値情報が存在しない場合は、`exhibition_time`、`course`、`finish_position` の欠損を許容する。

## 6. program_result_merged.parquet

番組表を基準に成績を外部結合した選手単位の統合データ。

2026年6月27日の確認形状は1080行×34列。

番組表20列と成績由来列に加えて、次の列を保存する。

| 列名 | pandas型 | 必須 | 説明 |
|---|---|---:|---|
| result_available | bool | Yes | 成績行の有無 |
| race_cancelled | bool | Yes | 全6艇の成績が欠損したレースか |
| racer_name_program | object | Yes | 番組表の選手名 |
| racer_name_canonical | object | Yes | 統合後の代表選手名 |

外部結合による欠損があるため、統合ファイルの `motor_no_result` と `boat_no_equipment_result` は、pandasで読み込むとfloat64になる場合がある。

## 7. 中止・欠損の扱い

### 7.1 全6艇欠損

同一レースの番組表6艇すべてについて成績行がない場合、次の値を設定する。

- `result_available = False`
- `race_cancelled = True`

これは説明可能な中止として扱い、品質異常にはしない。

`quality.json` には次を記録する。

- `cancelled_race_count`
- `cancelled_entry_count`
- `cancelled_races`
- `finish_status_counts.CANCELLED`

### 7.2 部分欠損

同一レースで1～5艇だけ成績が欠損している場合、`partial_result_missing` を1以上にする。

部分欠損は品質エラーとして日次ETLを失敗させる。

### 7.3 成績側のみの行

番組表に存在せず成績側だけに存在する行は `right_only` として記録し、品質エラーとする。

### 7.4 不成立と中止の区別

成績行が存在する不成立レースは、全艇欠損の中止レースと区別する。

例:

- `finish_raw = "00"`
- `result_available = True`
- `race_cancelled = False`
- `finish_position = null`

## 8. 選手名照合

結合は選手名ではなく、`racer_id` を含む共通キーで行う。

選手名は次の順序で検査する。

1. 完全一致
2. 一方が他方の前方一致
3. 短い名前が長い名前の文字順部分列

確認例:

- 番組表: 大豆生蒼
- 成績表: 大豆生田蒼

同一 `racer_id` で文字順部分列の場合は、説明可能な略称として扱う。

文字順が異なる場合は `name_unexplained_mismatch` として品質エラーにする。

## 9. quality.json

基本項目:

- `race_date`
- `record_count`
- `program_record_count`
- `result_record_count`
- `venue_count`
- `race_count`
- `created_at`
- `status`

重複・結合検査:

- `duplicate_keys`
- `program_duplicate_keys`
- `result_duplicate_keys`
- `merge_counts`
- `left_only`
- `right_only`
- `partial_result_missing`

中止情報:

- `cancelled_entry_count`
- `cancelled_race_count`
- `cancelled_races`

構造検査:

- `invalid_race_size`
- `invalid_boat_sets`

内容照合:

- `name_exact_mismatch`
- `name_expected_abbreviation`
- `name_unexplained_mismatch`
- `motor_mismatch`
- `boat_equipment_mismatch`

着順情報:

- `special_finish_count`
- `finish_status_counts`

### 9.1 必須ゼロ項目

日次処理を成功させるには、次の項目がすべて0でなければならない。

- `duplicate_keys`
- `program_duplicate_keys`
- `result_duplicate_keys`
- `right_only`
- `partial_result_missing`
- `invalid_race_size`
- `invalid_boat_sets`
- `name_unexplained_mismatch`
- `motor_mismatch`
- `boat_equipment_mismatch`

`left_only` は、6艇単位の完全な中止レースとして説明できる場合だけ許容する。

## 10. monthly_etl_summary.json

保存項目:

- `target_month`
- `status`
- `days_total`
- `days_processed`
- `days_success`
- `days_skipped`
- `days_failed`
- `record_count`
- `race_count`
- `venue_day_count`
- `start_day`
- `end_day`
- `wait_seconds`
- `overwrite_outputs`
- `overwrite_archives`
- `started_at`
- `finished_at`
- `details`

月次成功条件:

- `status = SUCCESS`
- `days_failed = 0`

## 11. 2026年6月の基準値

- 対象日数: 30
- 成功日数: 30
- 失敗日数: 0
- 番組表行数: 28152
- 成績行数: 27726
- 統合行数: 28152
- レース数: 4692
- 会場日数: 391
- 特殊着順数: 354
- 中止レース数: 71
- 中止選手行数: 426

中止内訳:

| 日付 | 中止レース数 | 中止選手行数 |
|---|---:|---:|
| 2026-06-02 | 10 | 60 |
| 2026-06-03 | 48 | 288 |
| 2026-06-16 | 1 | 6 |
| 2026-06-27 | 12 | 72 |
| 合計 | 71 | 426 |

## 12. スキーマ変更方針

次の変更を行う場合は、スキーマバージョンを更新する。

- 列の削除
- 列名の変更
- 主キーの変更
- データ型の非互換変更
- 中止判定ルールの変更
- 品質必須ゼロ項目の変更
- 特殊着順の意味変更

列追加の場合も、利用側への影響を確認して文書の改訂履歴へ記録する。

## 13. 今後の拡張

次の成果物として払戻データを追加する。

- `curated/payouts/YYYY/MM/DD/race_payouts.parquet`

払戻データは別成果物として追加し、既存3つのParquetの意味を変更しない。
