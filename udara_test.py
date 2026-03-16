import os
import logging
import base64
import requests
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def patch_udara_traffic():
    load_dotenv(override=True)
    domain = "udara-bali.com"
    
    login_seo = os.getenv("DATAFORSEO_LOGIN")
    password_seo = os.getenv("DATAFORSEO_PASSWORD")
    creds_seo = f"{login_seo}:{password_seo}"
    base64_seo = base64.b64encode(creds_seo.encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {base64_seo}", "Content-Type": "application/json"}
    
    # 1. Ping DataForSEO Traffic (Indonesia)
    logging.info(f"Pinging DataForSEO (Indonesia 2360) strictly for {domain}...")
    url_tr = "https://api.dataforseo.com/v3/dataforseo_labs/google/ranked_keywords/live"
    post_data_tr = [{"target": domain, "location_code": 2360, "language_code": "en", "limit": 1}]
    
    traffic_str = "No Data"
    try:
        req_tr = requests.post(url_tr, json=post_data_tr, headers=headers)
        data_tr = req_tr.json()
        if data_tr.get("tasks") and len(data_tr["tasks"]) > 0:
            task = data_tr["tasks"][0]
            if task.get("result") and len(task["result"]) > 0:
                 metrics_obj = task["result"][0].get("metrics", {})
                 if "organic" in metrics_obj:
                     traffic_str = str(metrics_obj["organic"].get("etv", "No Data"))
                 else:
                     traffic_str = "No Data"
            else:
                 traffic_str = f"API Message: {task.get('status_message', 'No Data')}"
    except Exception as e:
        logging.error(f"DataForSEO API failed: {e}")
        return

    logging.info(f"Traffic Result for {domain} in Indonesia: {traffic_str}")

    # 2. Patch Google Sheet
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Find which row Udara is on
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:I").execute()
    rows = result.get('values', [])
    
    target_row_index = -1
    for i, row in enumerate(rows):
        if len(row) > 0 and domain in row[0].strip().lower():
            target_row_index = i + 1
            break

    if target_row_index == -1:
        logging.error(f"Could not find {domain} inside the Google Sheet. Please make sure it exists.")
        return

    # Update Column G (Traffic) natively
    body_traffic = {"values": [[traffic_str]]}
    sheet.values().update(
        spreadsheetId=sheet_id,
        range=f"Sheet1!G{target_row_index}",
        valueInputOption="USER_ENTERED",
        body=body_traffic
    ).execute()
    
    logging.info(f"Successfully injected traffic '{traffic_str}' into Row {target_row_index} in Google Sheets!")

if __name__ == "__main__":
    patch_udara_traffic()
