import os
import json
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

from modules.module_6_apollo import run_apollo_enrichment

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_existing_sheet_data(sheet, sheet_id):
    # Read A:P to get Domain (A), Contact (E), and Verdict (M) 
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A2:P1000").execute()
    rows = result.get('values', [])
    
    prospects = []
    for i, row in enumerate(rows):
        # row index corresponding to the actual sheet row (A2 -> row 2)
        row_num = i + 2
        
        domain = row[0].strip() if len(row) > 0 else ""
        contact = row[4].strip() if len(row) > 4 else ""
        verdict = row[12].strip() if len(row) > 12 else ""
        
        if domain:
            prospects.append({
                "_row_num": row_num,
                "Domain": domain,
                "Contact": contact,
                "verdict": verdict
            })
            
    return prospects

def save_back_to_sheet(sheet, sheet_id, prospects):
    # We only update the Contact field (Column E)
    update_data = []
    for p in prospects:
        row_num = p.get("_row_num")
        contact = p.get("Contact", "")
        
        if row_num:
            update_data.append({
                "range": f"Sheet1!E{row_num}",
                "values": [[contact]]
            })
            
    if update_data:
        sheet.values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": update_data}
        ).execute()
        logging.info("Successfully updated Contact fields in Google Sheet.")

def main():
    load_dotenv(override=True)
    
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    logging.info("Loading existing sheet data...")
    prospects = load_existing_sheet_data(sheet, sheet_id)
    
    logging.info(f"Loaded {len(prospects)} prospects. Running Apollo Enrichment...")
    enriched_prospects = run_apollo_enrichment(prospects)
    
    logging.info("Saving results back to Google Sheet...")
    save_back_to_sheet(sheet, sheet_id, enriched_prospects)
    logging.info("Module 6 Standalone execution complete.")

if __name__ == "__main__":
    main()
