# boatrace-ai

BOAT RACE prediction and paper trading system.

公開コードと非公開データを分離し、公式公開データの取得、履歴ETL、特徴量生成、確率予測、評価、ペーパートレードを段階的に構築するプロジェクトです。

## Current status

Historical ETL v1 is complete for June 2026.

- 30日すべての月次ETLに成功
- 番組表28,152行
- 成績27,726行
- 4,692レース
- 391会場日
- 71中止レースを理由付きで保持
- 354特殊着順を保持
- 日次品質検査の必須ゼロ項目はすべて合格
- 21件の自動テストに合格
- GitHub Actionsに合格

Current stable commit:

- `c90ac2368d3d6a887aac0eba7b26fbb77220fdae`

## Documentation

- [Historical ETL Schema v1](docs/historical_etl_schema_v1.md)

## Repository scope

この公開リポジトリには、次のものを保存します。

- Pythonソースコード
- GitHub Actions
- 自動テスト
- データスキーマ
- 設定テンプレート
- セットアップ手順
- ダミーデータ

次の情報は公開リポジトリへ保存しません。

- 公式サイトから取得した実データ
- Parquet成果物
- 実際の予測結果
- EV推奨候補
- 学習済みモデル
- 個人のペーパートレード結果
- Google認証情報
- Google Sheets ID
- Google Drive ID
- Discord Webhook URL
- GitHubトークン

## Historical ETL

現在の履歴ETLは、公式番組表と成績ファイルから次の成果物を生成します。

- `program_entries.parquet`
- `race_results.parquet`
- `program_result_merged.parquet`
- `quality.json`
- `monthly_etl_summary.json`

対応済みの主な形式:

- 1800mおよび1200m
- 1～3桁の有効なボート機材番号
- 2連率100.00との番号境界
- K0、K1、S0、S1、S2、F、L0、00などの特殊着順
- 選手名の公式データ上の略称
- レース中止および会場単位の中止
- 全6艇欠損と部分欠損の区別

## Development checks

ローカルテスト:

- `python -m pytest -q`

構文検査:

- `python -m compileall -q src scripts tests`

月次ETL:

- `python scripts/process_month.py --month YYYY-MM --data-root DATA_ROOT --start-day 1 --end-day 31 --wait-seconds 2`

実データの保存先には、公開リポジトリ外のGoogle Driveを使用します。

## Roadmap

1. Historical ETL schema v1の固定
2. 払戻データパーサー
3. 直近3か月の形式検証
4. DuckDB参照層
5. 直近3年の履歴取得
6. PRE_NIGHT特徴量
7. 時系列バックテスト
8. Logistic RegressionおよびLightGBM基準モデル
9. 予測・評価・ペーパートレード
10. Google SheetsおよびDiscord連携

## Safety and usage policy

- 自動投票は実装しません。
- 初期運用はペーパートレードに限定します。
- データ取得元の規約とアクセス頻度を尊重します。
- CAPTCHA、ログイン制限、有料情報などの回避は行いません。
- 予測精度や将来の利益は保証されません。
- データ形式変更や品質異常を検出した場合は、安全側に停止します。
