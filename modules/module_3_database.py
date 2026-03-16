import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_existing_domains_from_sheet():
    """Helper snippet to safely read existing domains from Google Sheets to avoid API token burns."""
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not os.path.exists(creds_file) or not sheet_id:
        return []
        
    import urllib.parse
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    try:
        creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        existing_data = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:A").execute()
        
        extracted_domains = []
        for row in existing_data.get('values', []):
            if row:
                raw_url = row[0].strip().lower()
                if not raw_url.startswith("http"):
                    raw_url = "https://" + raw_url
                try:
                    domain = urllib.parse.urlparse(raw_url).netloc
                    domain = domain.replace("www.", "")
                    if domain:
                        extracted_domains.append(domain)
                except Exception:
                    extracted_domains.append(raw_url)
        return extracted_domains
    except Exception as e:
        logging.error(f"Failed to fetch existing domains: {e}")
        return []

def run_module_3(researched_data):
    """
    Module 3: Database Entry
    Takes enriched (or raw) prospects and pushes them cleanly into Google Sheets.
    """
    logging.info("Starting Module 3: Database Entry (Google Sheets)")
    
    # Setup credentials
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    
    if not os.path.exists(creds_file):
        logging.error(f"Google credentials file not found at {creds_file}")
        return
        
    if not sheet_id:
        logging.error("GOOGLE_SHEET_ID not set in .env")
        return

    # Authenticate and build service
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        creds_file, scopes=SCOPES)
        
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        
        # 1. First, check if headers exist. If the sheet is completely empty, add them.
        result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A1:G1").execute()
        headers = result.get('values', [])
        
        expected_headers = [
            "URL (Domain)",
            "Phase 1 - Write for Us Red Flags",
            "Phase 1 - Topical Match",
            "Quality Score (Phase 1 & 2)",
            "Contact",
            "Phase 2 - Geography",
            "Phase 2 - Traffic Volume",
            "Phase 3 - Inbound Ratios",
            "Phase 3 - Spam Score",
            "Time Taken (Seconds)",
            "Total Cost (USD)",
            "Cost Breakdown",
            "Lead Qualification (Verdict)"
        ]
        
        if not headers:
            logging.info("Sheet is empty, writing headers...")
            
            # Fetch sheet ID specifically
            sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            sheets = sheet_metadata.get('sheets', '')
            sheet_id_num = 0
            for s in sheets:
                if s.get("properties", {}).get("title", "") == "Sheet1":
                    sheet_id_num = s.get("properties", {}).get("sheetId", 0)
                    break
                    
            body = {'values': [expected_headers]}
            sheet.values().update(
                spreadsheetId=sheet_id, range="Sheet1!A1",
                valueInputOption="RAW", body=body).execute()
                
            # Add Sticky Header (Freeze Row 1) and Notes
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
                "Operational metric: Line-item receipts for Backlinks, Traffic Volume, and Gemini API spend.",
                "Automated Verdict:\n🟢 APPROVED (Traffic > 100, Spam < 30)\n🔴 REJECTED (Spam > 30 OR Traffic under 100)"
            ]
            
            requests_list = []
            requests_list.append({
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
            for col_index, note in enumerate(notes):
                requests_list.append({
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id_num,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": col_index,
                            "endColumnIndex": col_index + 1
                        },
                        "rows": [{"values": [{"note": note}]}],
                        "fields": "note"
                    }
                })
            
            try:
                service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={'requests': requests_list}
                ).execute()
                logging.info("Applied frozen row and header tooltips.")
            except Exception as e:
                logging.error(f"Failed to apply sticky row / tooltips: {e}")

        # 2. Get existing domains to avoid duplicates
        existing_data = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:A").execute()
        existing_domain_urls = [row[0] for row in existing_data.get('values', []) if row]
        
        # 3. Prepare rows to append
        rows_to_append = []
        for item in researched_data:
            url_domain = item.get('URL (Domain)', 'TBD')
            if str(url_domain) in existing_domain_urls:
                continue # Skip existing domain
            p1_flags = item.get('Phase 1 - Write for Us Red Flags', 'TBD')
            p1_match = item.get('Phase 1 - Topical Match', 'TBD')
            quality = item.get('Quality Score (Phase 1 & 2)', 'TBD')
            contact = item.get('Contact', 'TBD')
            p2_geography = item.get('Phase 2 - Geography', 'TBD')
            p2_traffic = item.get('Phase 2 - Traffic Volume', 'TBD')
            p3_ratios = item.get('Phase 3 - Inbound Ratios', 'TBD')
            p3_spam = item.get('Phase 3 - Spam Score', 'TBD')
            time_taken = item.get('Time Taken (Seconds)', 'TBD')
            total_cost = item.get('Total Cost (USD)', 'N/A')
            cost_break = item.get('Cost Breakdown', 'N/A')
            
            # --- Native Python Traffic Light Calculation (No Tokens Used) ---
            verdict = "🟡 MANUAL REVIEW"
            try:
                # Parse traffic (Strip commas, check if it's a valid digit)
                traffic_raw = str(p2_traffic).replace(',', '').strip()
                has_traffic = False
                if traffic_raw.isdigit():
                    traffic_int = int(traffic_raw)
                    has_traffic = True
                else:
                    traffic_int = 0
                    
                # Parse Spam Score (Default to 100 if missing so it fails securely)
                spam_raw = str(p3_spam).strip()
                if spam_raw.isdigit():
                    spam_int = int(spam_raw)
                elif spam_raw.replace('.','',1).isdigit(): # Handle float just in case
                    spam_int = int(float(spam_raw))
                else:
                    spam_int = 100
                    
                # The Golden Rules
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
            
            row = [
                str(url_domain),
                str(p1_flags),
                str(p1_match),
                str(quality),
                str(contact),
                str(p2_geography),
                str(p2_traffic),
                str(p3_ratios),
                str(p3_spam),
                str(time_taken),
                str(total_cost),
                str(cost_break),
                str(verdict)
            ]
            rows_to_append.append(row)
            
        # 4. Append to sheet
        if rows_to_append:
            body = {'values': rows_to_append}
            sheet.values().append(
                spreadsheetId=sheet_id,
                range="Sheet1!A:M",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()
            logging.info(f"Successfully pushed {len(rows_to_append)} rows to Google Sheet.")
        else:
            logging.info("No new rows to append (all domains already exist in Google Sheet).")
            
    except Exception as e:
        logging.error(f"Failed connecting to Google Sheets: {e}")

# Local testing
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
    mock_data = [{"domain": "testlaw.com", "url": "https://testlaw.com/contact", "source_query": "Test"}]
    run_module_3(mock_data)
