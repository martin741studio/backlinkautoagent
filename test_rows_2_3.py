import os
import json
import logging
import urllib.parse
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

from modules.module_2_research import run_traffic, run_backlinks, run_analysis

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_float(val):
    if val == "No Data" or val == "TBD" or "API" in str(val) or val == "N/A" or val == "":
        return 0.0
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def main():
    load_dotenv(override=True)
    
    # 1. Google Sheets Setup
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    # 2. Read exactly A2 and A3
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A2:A3").execute()
    rows = result.get('values', [])
    if not rows:
        logging.info("No domains found in A2:A3.")
        return
        
    prospects = []
    for row in rows:
        if row:
            raw_url = row[0].strip()
            domain = raw_url
            if domain.startswith("http"):
                domain = urllib.parse.urlparse(domain).netloc
            domain = domain.replace("www.", "")
            prospects.append({"Domain": domain, "URL (Domain)": raw_url})

    logging.info(f"Testing these exact domains: {[p['Domain'] for p in prospects]}")
    
    # Force delete cache for these 2 domains to guarantee fresh accurate data pull for the test
    cache_file = 'data/module_2_cache.json'
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            try:
                cache = json.load(f)
            except:
                cache = {}
        for p in prospects:
            if p["Domain"] in cache:
                del cache[p["Domain"]]
                logging.info(f"Deleted {p['Domain']} from cache for a true fresh ping.")
        with open(cache_file, 'w') as f:
            json.dump(cache, f, indent=4)
            
    # Run the pipeline module by module
    # We bypass the traffic > 500 filter in this isolated test script 
    # so we guarantee ALL columns are populated on the sheet for review purposes.
    logging.info("Executing M2 - Traffic...")
    prospects = run_traffic(prospects)
    
    logging.info("Executing M2 - Backlinks...")
    prospects = run_backlinks(prospects)
    
    logging.info("Executing M2 - Analysis (Gemini)...")
    # For this test, we override 'TBD' checks to ensure Gemini runs on both
    prospects = run_analysis(prospects)

    # 3. Write securely back to Row 2 and Row 3 (A:M)
    for i, p in enumerate(prospects):
        row_num = i + 2
        
        # Calculate Verdict & Spam Interpretation
        tf = parse_float(p.get("Phase 2 - Traffic Volume", 0))
        spam_raw = str(p.get("Phase 3 - Spam Score", "0")).replace("N/A", "0").replace("TBD", "0")
        try:
            spam = float(spam_raw)
        except:
            spam = 0
            
        red_flag = str(p.get("Phase 1 - Write for Us Red Flags", "")).upper()
        topic = str(p.get("Phase 1 - Topical Match", "")).upper()
        
        # Spam Score string:
        if spam <= 30:
            spam_val_str = "Low Risk"
        elif spam <= 60:
            spam_val_str = "Manual Review"
        else:
            spam_val_str = "High Risk"

        # Lead Qualification Verdict
        if tf < 100 or spam > 60 or "YES" in red_flag or "LOW" in topic:
            qt = "🔴 FAIL"
        elif spam > 30 or "MEDIUM" in topic:
            qt = "🟡 REVIEW"
        else:
            qt = "🟢 PASS"

        values = [
            p.get("Phase 1 - Write for Us Red Flags", ""), # B
            p.get("Phase 1 - Topical Match", ""), # C
            p.get("Quality Score (Phase 1 & 2)", ""), # D
            p.get("Contact", ""), # E
            p.get("Phase 2 - Geography", ""), # F
            p.get("Phase 2 - Traffic Volume", ""), # G
            p.get("Phase 3 - Inbound Ratios", ""), # H
            spam_val_str, # I
            p.get("Time Taken (Seconds)", 0), # J
            p.get("Total Cost (USD)", "$0.00"), # K
            p.get("Cost Breakdown", ""), # L
            qt # M
        ]

        logging.info(f"Row {row_num} Payload: {values}")

        sheet.values().update(
            spreadsheetId=sheet_id,
            range=f"Sheet1!B{row_num}:M{row_num}",
            valueInputOption="USER_ENTERED",
            body={"values": [values]}
        ).execute()
        logging.info(f"Successfully injected fully-populated columns into Row {row_num}")
        
if __name__ == "__main__":
    main()
