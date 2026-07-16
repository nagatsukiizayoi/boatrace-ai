import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

service_account_info=json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
spreadsheet_id=os.environ["GOOGLE_SPREADSHEET_ID"].strip()
drive_folder_id=os.environ["GOOGLE_DRIVE_FOLDER_ID"].strip()
discord_webhook_url=os.environ["DISCORD_WEBHOOK_URL"].strip()
scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
credentials=Credentials.from_service_account_info(service_account_info,scopes=scopes)
sheets_client=gspread.authorize(credentials)
drive_client=build("drive","v3",credentials=credentials,cache_discovery=False)
spreadsheet=sheets_client.open_by_key(spreadsheet_id)

definitions={"DASHBOARD":["項目","値","更新時刻"],"TODAY_RACES":["race_id","日付","場","R","締切","現在ステージ","状態","更新時刻"],"PREDICTIONS":["race_id","stage","bet_type","買い目","予測順位","予測確率","オッズ","期待倍率","モデル","データ品質","信頼度","推奨","作成時刻"],"FINAL_CANDIDATES":["race_id","場","R","締切","券種","買い目","予測確率","オッズ","期待倍率","推奨額","信頼度","理由","更新時刻"],"STAGE_COMPARISON":["race_id","bet_type","買い目","PRE_NIGHT","MORNING","PRE_EXHIBITION","POST_EXHIBITION","FINAL","変化理由"],"RESULTS":["日付","race_id","場","R","結果","的中","投資","払戻","利益","更新時刻"],"METRICS":["評価期間","stage","bet_type","model_version","レース数","Top1的中率","Top3的中率","Top5的中率","Top10的中率","LogLoss","BrierScore","CalibrationError","回収率"],"DATA_IMPORT_STATUS":["データ種別","対象期間","取得元","状態","件数","最終取得時刻","エラー"],"SYSTEM_CONFIG":["設定キー","設定値","説明","更新時刻"],"MANUAL_INPUT":["race_id","stage","項目","値","入力時刻","入力者","is_manual","再予測要求"],"MODEL_APPROVAL":["model_version","stage","bet_type","model_scope","評価状態","昇格候補","approve","承認時刻"],"SOURCE_REGISTRY":["source_id","ソース名","URL","対象データ","利用条件確認日","自動取得可否","最終正常取得時刻","取得成功率","公式一致率"],"RUN_LOG":["実行時刻","処理","ステータス","件数","エラーコード","メッセージ"]}

worksheets={}
for title,headers in definitions.items():
    titles=[item.title for item in spreadsheet.worksheets()]
    worksheet=spreadsheet.worksheet(title) if title in titles else spreadsheet.add_worksheet(title=title,rows=1000,cols=max(20,len(headers)+2))
    worksheet.update(values=[headers],range_name="A1") if not worksheet.row_values(1) else None
    worksheet.freeze(rows=1)
    worksheets[title]=worksheet

now=datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")
worksheets["DASHBOARD"].update(values=[["システム名","BOATRACE AI",now],["構築レベル","Level 1",now],["接続状態","正常",now],["自動投票","無効",now],["標準EV","EV_110",now]],range_name="A2")
default_config=[["SYSTEM_ENABLED","TRUE","システム全体の有効・無効",now],["DATA_FETCH_ENABLED","TRUE","データ取得の有効・無効",now],["PREDICTION_ENABLED","TRUE","予測処理の有効・無効",now],["EV_RECOMMENDATION_ENABLED","TRUE","EV推奨の有効・無効",now],["NOTIFICATION_ENABLED","TRUE","Discord通知の有効・無効",now],["STANDARD_EV","1.10","標準表示する期待倍率",now],["DAILY_BUDGET","10000","1日の標準予算",now],["RACE_BUDGET","500","1レースの標準上限",now],["UNIT_BET","100","1点あたりの金額",now],["MIN_DATA_QUALITY","80","推奨対象の最低品質",now],["MAX_ODDS_AGE_MINUTES","15","オッズの最大経過時間",now],["SAFE_STOP","FALSE","異常時の安全停止状態",now],["RESUME_APPROVED","FALSE","安全停止後の手動復帰承認",now]]
if len(worksheets["SYSTEM_CONFIG"].get_all_values())<=1:
    worksheets["SYSTEM_CONFIG"].update(values=default_config,range_name="A2")

folder=drive_client.files().get(fileId=drive_folder_id,fields="id,name,mimeType",supportsAllDrives=True).execute()
assert folder.get("mimeType")=="application/vnd.google-apps.folder","GOOGLE_DRIVE_FOLDER_ID is not a folder"
worksheets["RUN_LOG"].append_row([now,"LEVEL1_SETUP_CHECK","SUCCESS",str(len(worksheets)),"","Sheets・Drive・Discord接続確認完了"],value_input_option="USER_ENTERED")
discord_response=requests.post(discord_webhook_url,json={"username":"BOATRACE AI","content":"✅ BOATRACE AI Level 1 接続テスト成功\nGoogle Sheets・Google Drive・Discordの接続を確認しました。","allowed_mentions":{"parse":[]}},timeout=20)
assert discord_response.status_code in (200,204),"Discord returned HTTP "+str(discord_response.status_code)
print("LEVEL1_SETUP_CHECK: SUCCESS")
print("Worksheets:",len(worksheets))
print("Google Drive folder access: OK")
print("Discord notification: OK")
