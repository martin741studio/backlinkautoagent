import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_headers():
    load_dotenv(override=True)
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    
    # Get sheet ID for "Sheet1"
    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_id_num = None
    for s in sheets:
        if s.get("properties", {}).get("title", "") == "Sheet1":
            sheet_id_num = s.get("properties", {}).get("sheetId", 0)
            break
            
    if sheet_id_num is None:
        sheet_id_num = 0

    notes = [
        "The root domain and the specific URL where the prospect was found organically.",
        "AI Analysis: Paid Footprints\n🟢 Green: No mentions of paid posts or advertising. Natural site.\n🔴 Red: Site actively begs for 'Guest Posts' or 'Write for Us'. High risk of being a penalized link farm.",
        "AI Analysis: Niche Relevance\n🟢 Green: Heavily focused on wellness, spa, recovery, or yoga matching our core client.\n🟡 Yellow: Tangentially related (e.g., general travel blog).\n🔴 Red: Completely unrelated topic.",
        "AI Analysis: Site Legitimacy\n🟢 Green: AI confirms it is a real business with physical services, prices, and professional writing.\n🔴 Red: Reads like a Private Blog Network (PBN) with spun, auto-generated, or incredibly thin content.",
        "Emails automatically scraped via BeautifulSoup from the homepage. If 'None Found', manual outreach hunting is required.",
        "Top Inbound Link Countries\n🟢 Green: Matches our desired demographic (e.g., Australia, UK, USA, WW).\n🔴 Red: Massive spikes from unrelated/spammy geo-locations.",
        "DataForSEO: Estimated Monthly Organic Traffic\n🟢 Green: High active monthly traffic (Site is trusted by Google).\n🔴 Red: '0' or 'No Data' (Site is either hyper-local, brand new, or penalized by Google).",
        "Referring Domains (RD) vs Total Backlinks (BL)\n🟢 Green: Healthy natural ratio (usually 1:1 to 1:3). e.g., 500 RD / 1500 BL.\n🔴 Red: Highly unnatural ratio. e.g., 10 RD / 50,000 BL. (Signals spammy, automated site-wide footer links).",
        "DataForSEO: Backlink Toxicity (0 - 100)\n🟢 Green (0 - 30): Healthy, normal website.\n🟡 Yellow (31 - 60): Proceed with caution. Manually review their outbound links.\n🔴 Red (61 - 100): Highly toxic. Do not build a link here; it could damage your client's ranking.",
        "Operational metric: Processing duration for this specific domain.",
        "Operational metric: Exact API cost spent executing the SEO & AI queries for this row.",
        "Operational metric: Line-item receipts for Backlinks, Traffic Volume, and Gemini API spend."
    ]

    requests = []
    
    # 1. Freeze the first row
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id_num,
                "gridProperties": {
                    "frozenRowCount": 1
                }
            },
            "fields": "gridProperties.frozenRowCount"
        }
    })
    
    # 2. Add notes to the cells in the first row
    for col_index, note in enumerate(notes):
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id_num,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1
                },
                "rows": [
                    {
                        "values": [
                            {
                                "note": note
                            }
                        ]
                    }
                ],
                "fields": "note"
            }
        })
        
    body = {
        'requests': requests
    }
    
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body=body
    ).execute()
    logging.info("Successfully updated headers with notes and frozen row.")

if __name__ == "__main__":
    update_headers()
