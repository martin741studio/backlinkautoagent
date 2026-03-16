import os
import json
import base64
import requests
import urllib.parse
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

def main():
    load_dotenv(override=True)
    
    # 1. Set up Google Sheets connection
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    # Select the NEXT domain in the prospect list (row 3 of the Google Sheet)
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A3").execute()
    rows = result.get('values', [])
    if not rows or not rows[0]:
        print("No domain found in A3.")
        return
        
    raw_domain = rows[0][0].strip()
    
    # Clean the domain string for API processing
    domain = raw_domain
    if domain.startswith("http"):
        domain = urllib.parse.urlparse(domain).netloc
    domain = domain.replace("www.", "")
        
    print(f"Domain tested: {domain}")
    
    # 6. Call DataForSEO traffic endpoint for that domain
    login_seo = os.getenv("DATAFORSEO_LOGIN")
    password_seo = os.getenv("DATAFORSEO_PASSWORD")
    creds_seo = f"{login_seo}:{password_seo}"
    base64_seo = base64.b64encode(creds_seo.encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {base64_seo}", "Content-Type": "application/json"}
    
    url_tr = "https://api.dataforseo.com/v3/dataforseo_labs/google/domain_rank_overview/live"
    # Testing US traffic (2840) per requirements
    post_data_tr = [{"target": domain, "location_code": 2840, "language_code": "en"}]
    
    res = requests.post(url_tr, json=post_data_tr, headers=headers)
    data_tr = res.json()
    
    # 7. Print the full API response for debugging
    print("\nRaw API response:")
    print(json.dumps(data_tr, indent=2))
    
    # 8. Parse the organic monthly traffic value
    traffic_str = "No Data"
    if data_tr.get("tasks") and len(data_tr["tasks"]) > 0:
        task = data_tr["tasks"][0]
        if task.get("result") and len(task["result"]) > 0 and task["result"][0].get("items") and len(task["result"][0]["items"]) > 0:
            metrics_obj = task["result"][0]["items"][0].get("metrics", {})
            if "organic" in metrics_obj:
                raw_traffic = metrics_obj["organic"].get("etv", 0)
                traffic_str = str(int(round(float(raw_traffic))))
            else:
                traffic_str = "No Data"
        else:
            traffic_str = f"API Message: {task.get('status_message', 'No Data')}"
            
    print(f"\nParsed traffic value: {traffic_str}")
    
    # 9. Write ONLY the parsed traffic number into cell G3
    body_traffic = {"values": [[traffic_str]]}
    sheet.values().update(
        spreadsheetId=sheet_id,
        range="Sheet1!G3",
        valueInputOption="USER_ENTERED",
        body=body_traffic
    ).execute()
    
    print("Written to sheet cell: G3")

if __name__ == "__main__":
    main()
