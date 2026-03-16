import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    load_dotenv(override=True)
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Get the existing data from the sheet (Columns A through I)
    # We mainly need Row 1 for headers and G (Traffic) & I (Spam) for data.
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:M").execute()
    rows = result.get('values', [])
    
    if not rows:
        logging.info("Sheet is empty.")
        return

    update_data = []
    
    # 1. Update Header M1
    update_data.append({
        "range": "Sheet1!M1",
        "values": [["Lead Qualification (Verdict)"]]
    })

    # 2. Re-calculate logic for every existing row
    for i in range(1, len(rows)):
        row = rows[i]
        
        # Col G (index 6): Traffic
        p2_traffic = row[6] if len(row) > 6 else 0
        
        # Col I (index 8): Spam Score
        p3_spam = row[8] if len(row) > 8 else 100
        
        verdict = "🟡 MANUAL REVIEW"
        try:
            # Parse traffic
            traffic_raw = str(p2_traffic).replace(',', '').strip()
            has_traffic = False
            if traffic_raw.isdigit():
                traffic_int = int(traffic_raw)
                has_traffic = True
            else:
                traffic_int = 0
            
            # Parse Spam
            spam_raw = str(p3_spam).strip()
            if spam_raw.isdigit():
                spam_int = int(spam_raw)
            elif spam_raw.replace('.','',1).isdigit():
                spam_int = int(float(spam_raw))
            else:
                spam_int = 100
            
            # Application Logic
            verdict_note = "\n(Note: No Traffic Data)" if not has_traffic else ""
            
            if spam_int > 60:
                verdict = f"🔴 REJECTED (High Spam){verdict_note}"
            elif has_traffic and traffic_int < 100:
                verdict = f"🔴 REJECTED (Low Traffic){verdict_note}"
            elif spam_int > 30:
                verdict = f"🟡 MANUAL REVIEW (Moderate Spam){verdict_note}"
            else:
                verdict = f"🟢 APPROVED{verdict_note}"
        except Exception:
            verdict = "🟡 MANUAL REVIEW (Parse Error)"
            
        # Add to batch update (M corresponds to cell Mi+1)
        row_number = i + 1
        update_data.append({
            "range": f"Sheet1!M{row_number}",
            "values": [[verdict]]
        })

    # Execute Batch Update
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": update_data
    }
    
    sheet.values().batchUpdate(
        spreadsheetId=sheet_id, 
        body=body
    ).execute()
    
    logging.info(f"Successfully retroactively updated Column M for {len(rows)-1} historical rows!")

if __name__ == "__main__":
    main()
