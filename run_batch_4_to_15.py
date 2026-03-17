import os
import json
import logging
import time
import urllib.parse
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

from modules.url_sanitizer import normalize_domain_url
from modules.module_1_prospecting import run_module_1
from modules.module_2_research import run_traffic, run_backlinks, run_analysis, load_json, save_json, CACHE_FILE
from modules.module_4_outreach import run_outreach

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    load_dotenv(override=True)
    
    # 1. Google Sheets Setup
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # 2. Read Column A:M to capture verdict (Column M is index 12)
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:M").execute()
    rows = result.get('values', [])
    
    headers = rows[0] if rows else ["URL (Domain)"]
    existing_data = [] # tuple (url, verdict)
    for r in rows[1:]:
        if r and r[0].strip():
            url = r[0].strip()
            v = r[12].strip() if len(r) > 12 else None
            existing_data.append((url, v))
            
    # Normalize existing URLs
    normalized_list = []
    seen = set()
    updates = []
    
    # Clean up Column A
    for i, (raw_url, v) in enumerate(existing_data):
        row_num = i + 2
        norm = normalize_domain_url(raw_url)
        
        if norm != "INVALID_URL" and norm not in seen:
            seen.add(norm)
            normalized_list.append((row_num, norm, v))
            if norm != raw_url:
                updates.append({"range": f"Sheet1!A{row_num}", "values": [[norm]]})
                
    # If any need normalization in sheet, update them
    if updates:
        body = {"valueInputOption": "USER_ENTERED", "data": updates}
        sheet.values().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
        logging.info(f"Normalized {len(updates)} existing rows in Google Sheet towards root domains.")

    # Goal: 14 total domains to reach row 15
    current_domain_count = len(normalized_list)
    desired_domain_count = 14
    
    prospects_to_add = []
    
    if current_domain_count < desired_domain_count:
        logging.info(f"Currently have {current_domain_count} domains. Prospecting for more...")
        with open('config/client_profile_template.json', 'r') as f:
            config = json.load(f)
        
        raw_prospects = run_module_1(config['search_parameters'], config['client_details']['website_url'])
        
        for p in raw_prospects:
            norm = normalize_domain_url(p.get("url", "")) # get actual URL reported by DataForSEO
            if norm == "INVALID_URL":
                norm = normalize_domain_url(p.get("domain", ""))
                
            if norm != "INVALID_URL" and norm not in seen:
                seen.add(norm)
                prospects_to_add.append(norm)
                if len(normalized_list) + len(prospects_to_add) >= desired_domain_count:
                    break

        if prospects_to_add:
            append_body = {"values": [[p] for p in prospects_to_add]}
            sheet.values().append(
                spreadsheetId=sheet_id,
                range="Sheet1!A:A",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=append_body
            ).execute()
            
            # Since rows could be padded, let's just re-fetch to safely grab the newly appended row numbers
            result_new = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1!A:M").execute()
            rows_new = result_new.get('values', [])
            
            normalized_list = []
            seen = set()
            for i, r in enumerate(rows_new[1:]):
                if r and r[0].strip():
                    url = r[0].strip()
                    v = r[12].strip() if len(r) > 12 else None
                    seen.add(url)
                    normalized_list.append((i + 2, url, v))
            
            logging.info(f"Successfully populated prospects up to Row 15.")
            
    # Process rows 4 to 15 strictly
    targets = []
    for r_num, url, v in normalized_list:
        if 4 <= r_num <= 15:
            domain_only = urllib.parse.urlparse(url).netloc
            targets.append({"Domain": domain_only, "URL (Domain)": url, "_row_num": r_num, "sheet_verdict": v})
            
    if not targets:
        logging.info("No targets found between row 4 and 15.")
        return
        
    logging.info(f"Executing Batch Processing on {len(targets)} domains (Rows 4-15).")
    
    def normalize_output_format(p):
        # --- Geography Traffic Light ---
        geo = p.get("Phase 2 - Geography")
        if geo and isinstance(geo, str):
            if not geo.startswith("🟢") and not geo.startswith("🔴") and geo != "TBD":
                target_geos = ["US", "UK", "AU", "WW", "CA"]
                p["Phase 2 - Geography"] = f"🟢 {geo}" if any(g in geo for g in target_geos) else f"🔴 {geo}"

        # --- Spam Score Formatting ---
        spam = p.get("Phase 3 - Spam Score")
        if spam is not None and str(spam) not in ["TBD", "N/A", "None", ""]:
            try:
                p["Phase 3 - Spam Score"] = int(float(str(spam)))
            except:
                p["Phase 3 - Spam Score"] = None
        elif str(spam) in ["TBD", "N/A", "None", ""]:
            p["Phase 3 - Spam Score"] = None

        # --- Quality Score Filtering (Remove old cached strings like 'Rank: 697 | TBD') ---
        qs = p.get("Quality Score (Phase 1 & 2)")
        if qs is not None:
            if isinstance(qs, str) and "Rank:" in qs:
                p["Quality Score (Phase 1 & 2)"] = None
            else:
                try:
                    p["Quality Score (Phase 1 & 2)"] = int(qs)
                except:
                    p["Quality Score (Phase 1 & 2)"] = None
                    
        # --- Update Costs ---
        total_seo = p.get("_cost_bl", 0.0) + p.get("_cost_tr", 0.0)
        p["Total Cost (USD)"] = f"${total_seo:.5f}"
        p["Cost Breakdown"] = f"DataForSEO Backlinks: ${p.get('_cost_bl', 0.0):.5f} | DataForSEO Traffic: ${p.get('_cost_tr', 0.0):.5f} | Gemini: Unknown/Free"
        
        # --- Compute Verdict and Score ---
        signals_dict = {}
        red_flag_text = p.get("Phase 1 - Write for Us Red Flags")
        topic_text = p.get("Phase 1 - Topical Match")
        inbound_text = p.get("Phase 3 - Inbound Ratios")
        
        if red_flag_text:
            if "🔴" in red_flag_text: signals_dict["red_flags_status"] = "RED"
            elif "🟢" in red_flag_text: signals_dict["red_flags_status"] = "GREEN"
            
        if topic_text:
            if "🔴" in topic_text: signals_dict["topical_status"] = "RED"
            elif "🟢" in topic_text: signals_dict["topical_status"] = "GREEN"

        geo_val = p.get("Phase 2 - Geography")
        if geo_val:
            if "🔴" in geo_val: signals_dict["geo_status"] = "RED"
            elif "🟢" in geo_val: signals_dict["geo_status"] = "GREEN"

        if inbound_text:
            if "🔴" in inbound_text: signals_dict["inbound_status"] = "RED"
            elif "🟢" in inbound_text: signals_dict["inbound_status"] = "GREEN"
            
        if p.get("Phase 3 - Spam Score") is not None:
            sp = p["Phase 3 - Spam Score"]
            if sp <= 30: signals_dict["spam_status"] = "GREEN"
            elif sp <= 60: signals_dict["spam_status"] = "YELLOW"
            else: signals_dict["spam_status"] = "RED"
            
        signals_full = [
            signals_dict.get("red_flags_status"),
            signals_dict.get("topical_status"),
            signals_dict.get("geo_status"),
            signals_dict.get("inbound_status"),
            signals_dict.get("spam_status")
        ]
        
        score = 30
        for s in signals_full:
            if s == "GREEN": score += 8
            elif s == "YELLOW": score += 3
            elif s == "RED": score -= 25
            
        present_signals = [s for s in signals_full if s is not None]
        total_signals = 5
        comp_ratio = len(present_signals) / total_signals if total_signals > 0 else 0
        
        if comp_ratio == 1.0: score += 5
        elif comp_ratio >= 0.6: score += 3
        elif comp_ratio < 0.4: score -= 10
        
        if "RED" in present_signals:
            score = min(score, 60)
            
        score = max(0, min(100, int(score)))
        p["score"] = score
        
        # Fetch highest precedence priority: The actual physical label in the Google Sheet right now
        sv = p.get("sheet_verdict")
        if sv in ["🟢 APPROVED", "🟡 REVIEW", "🔴 REJECTED"]:
            p["verdict"] = sv
        elif "verdict" not in p or p["verdict"] not in ["🟢 APPROVED", "🟡 REVIEW", "🔴 REJECTED"]:
            if "RED" in present_signals:
                p["verdict"] = "🔴 REJECTED"
            elif "YELLOW" in present_signals:
                p["verdict"] = "🟡 REVIEW"
            else:
                p["verdict"] = "🟢 APPROVED"
            
        return p
        
    processed_targets = []
    
    cache_data = load_json(CACHE_FILE)
    
    FORCE_REFRESH = False
    
    for p in targets:
        if "_row_num" not in p:
            raise ValueError(f"Missing _row_num in object: {p}")
            
        d_name = p['Domain']
        cached_p = cache_data.get(d_name, {})
        
        # Merge cached into p safely without overwriting row_num (ensures _traffic_done etc are restored)
        p.update({k: v for k, v in cached_p.items() if k not in p or p[k] is None})
            
        if p.get("_fully_processed") and not FORCE_REFRESH:
            # We still normalize to ensure cost formats, etc., are up to date
            p = normalize_output_format(p)
            logging.info(f"⏭️ HARD SKIP Row {p['_row_num']} ({p['Domain']}) → Fully processed ({p.get('verdict')})")
            processed_targets.append(p)
            continue
            
        logging.info(f"Processing object for Row {p['_row_num']}: {p['Domain']}")
        
        row_start = time.time()
        
        # --- Research Processing ---
        if not p.get("_traffic_done"):
            p = run_traffic([p])[0]
        else:
            logging.info(f"Skipping Traffic → already completed")

        if not p.get("_backlinks_done"):
            p = run_backlinks([p])[0]
        else:
            logging.info(f"Skipping Backlinks → already completed")

        if p.get("_gemini_done"):
            logging.info(f"Skipping Gemini → already completed")
        else:
            # Failsafe applied natively since we skip anyway, but double enforcing
            p = run_analysis([p])[0]
        
        row_end = time.time()
        
        # Accumulate time rather than replacing, so we don't wipe historical metrics when modules skip
        p["time_taken"] = round(p.get("time_taken", 0) + (row_end - row_start), 2)
        
        p["_fully_processed"] = True
        
        # Manually lock final processing logic back into Cache directly
        cache_data[d_name].update(p)
        save_json(cache_data, CACHE_FILE)
        
        # Apply strict normalization to fix broken cached formats
        p = normalize_output_format(p)
        
        processed_targets.append(p)
        
    targets = processed_targets
    
    # Load client profile for Outreach
    client_profile_path = "config/client_profile_template.json"
    client_profile = {}
    if os.path.exists(client_profile_path):
        with open(client_profile_path, 'r') as f:
            client_profile = json.load(f)
            
    # Execute Outreach Module 4
    targets = run_outreach(targets, client_profile)
    
    # Final Database Write
    update_data = []
    for p in targets:
        row_num = p["_row_num"]
        
        tf = p.get("Phase 2 - Traffic Volume")
        spam = p.get("Phase 3 - Spam Score")
        red_flag_text = p.get("Phase 1 - Write for Us Red Flags")
        topic_text = p.get("Phase 1 - Topical Match")
        geo_text = p.get("Phase 2 - Geography")
        inbound_text = p.get("Phase 3 - Inbound Ratios")
        spam_val_str = None
        if spam is not None:
            if spam <= 30: spam_val_str = "🟢"
            elif spam <= 60: spam_val_str = "🟡"
            else: spam_val_str = "🔴"

        def safe_val(v):
            if v is None: return None
            if isinstance(v, str) and any(err in v for err in ["API Message", "TBD", "Error"]): return None
            return v
            
        values = [
            safe_val(red_flag_text), # B
            safe_val(topic_text), # C
            safe_val(p.get("Quality Score (Phase 1 & 2)")), # D
            safe_val(p.get("Contact")), # E
            safe_val(geo_text), # F
            safe_val(tf), # G
            safe_val(inbound_text), # H
            safe_val(spam_val_str), # I
            p.get("time_taken", 0), # J
            safe_val(p.get("Total Cost (USD)")), # K
            safe_val(p.get("Cost Breakdown")), # L
            p.get("verdict"), # M
            p.get("score"), # N
            safe_val(p.get("Outreach Subject")), # O
            safe_val(p.get("Outreach Body")) # P
        ]
        
        update_data.append({
            "range": f"Sheet1!B{row_num}:P{row_num}",
            "values": [values]
        })

    if update_data:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": update_data
        }
        sheet.values().batchUpdate(
            spreadsheetId=sheet_id,
            body=body
        ).execute()
        logging.info(f"Success! Exported {len(update_data)} parsed profiles to Google Sheets.")

if __name__ == "__main__":
    main()
