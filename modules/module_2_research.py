import os
import json
import logging
import base64
import requests
import re
import time
from bs4 import BeautifulSoup
from google import genai

CACHE_FILE = 'data/module_2_cache.json'

def load_json(filepath):
    if os.path.exists(filepath):
        # Gracefully handle empty or malformed cache
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                if data.get("_version") != "v2":
                    logging.info("Old cache version detected. Resetting to v2.")
                    return {"_version": "v2"}
                return data
        except Exception:
            return {"_version": "v2"}
    return {"_version": "v2"}

def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def initialize_empty(domain_name, url=""):
    if not url: url = f"https://{domain_name}"
    return {
        "Domain": domain_name,
        "URL (Domain)": url,
        "Phase 1 - Real Website vs PBN": None,
        "Phase 1 - Topical Match": None,
        "Phase 1 - Content Quality": None,
        "Phase 1 - Write for Us Red Flags": None,
        "Contact": None,
        "Quality Score (Phase 1 & 2)": None,
        "Phase 2 - Geography": None,
        "Phase 2 - Traffic Volume": None,
        "Phase 3 - Inbound Ratios": None,
        "Phase 3 - Spam Score": None,
        "time_taken": 0,
        "Total Cost (USD)": "$0.00000",
        "Cost Breakdown": "DataForSEO Backlinks: $0.00 | DataForSEO Traffic: $0.00 | Gemini: Free",
        "_p2_rank": None,
        "_cost_bl": 0.0,
        "_cost_tr": 0.0,
        "_traffic_done": False,
        "_backlinks_done": False,
        "_gemini_done": False,
        "_fully_processed": False
    }

def get_seo_headers():
    login_seo = os.getenv("DATAFORSEO_LOGIN")
    password_seo = os.getenv("DATAFORSEO_PASSWORD")
    creds_seo = f"{login_seo}:{password_seo}"
    base64_seo = base64.b64encode(creds_seo.encode("ascii")).decode("ascii")
    return {"Authorization": f"Basic {base64_seo}", "Content-Type": "application/json"}

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def _update_costs(obj):
    total_seo = obj.get("_cost_bl", 0.0) + obj.get("_cost_tr", 0.0)
    obj["Total Cost (USD)"] = f"${total_seo:.5f}"
    obj["Cost Breakdown"] = f"DataForSEO Backlinks: ${obj.get('_cost_bl', 0.0):.5f} | DataForSEO Traffic: ${obj.get('_cost_tr', 0.0):.5f} | Gemini: Unknown/Free"

def run_traffic(domains):
    logging.info("--- RUNNING TRAFFIC MODULE ---")
    cache = load_json(CACHE_FILE)
    headers = get_seo_headers()

    to_query = []
    for dom in domains:
        d_name = dom if isinstance(dom, str) else dom.get("Domain", dom.get("domain", dom.get("url", "")))
        url = dom if isinstance(dom, str) else dom.get("URL (Domain)", dom.get("url", d_name))
        row = cache.get(d_name)
        if not row:
            cache[d_name] = initialize_empty(d_name, url)
            row = cache[d_name]
            
        # Retry logic for failed pings based strictly on flags
        if not row.get("_traffic_done"):
             to_query.append(d_name)
             
    if to_query:
        logging.info(f"Pinging {len(to_query)} domains for Traffic via DataForSEO individually...")
        # DataForSEO `domain_rank_overview` does NOT support batching (1 target per request)
        for d_name in to_query:
            post_data = [{"target": d_name, "location_code": 2840, "language_code": "en"}]
            try:
                res = requests.post("https://api.dataforseo.com/v3/dataforseo_labs/google/domain_rank_overview/live", json=post_data, headers=headers)
                data = res.json()
                cost = data.get("cost", 0)
                cache[d_name]["_cost_tr"] += cost
                
                tasks = data.get("tasks", [])
                if tasks and tasks[0].get("result") and len(tasks[0]["result"]) > 0 and tasks[0]["result"][0].get("items") and len(tasks[0]["result"][0]["items"]) > 0:
                    metrics_obj = tasks[0]["result"][0]["items"][0].get("metrics", {})
                    if "organic" in metrics_obj:
                        raw_traffic = metrics_obj["organic"].get("etv")
                        if raw_traffic is not None:
                            cache[d_name]["Phase 2 - Traffic Volume"] = int(round(float(raw_traffic)))
                        else:
                            cache[d_name]["Phase 2 - Traffic Volume"] = None
                    else:
                        cache[d_name]["Phase 2 - Traffic Volume"] = None
                else:
                    cache[d_name]["Phase 2 - Traffic Volume"] = None
            except Exception as e:
                logging.error(f"Traffic failed for {d_name}: {e}")
                cache[d_name]["Phase 2 - Traffic Volume"] = None
            finally:
                cache[d_name]["_traffic_done"] = True
                
        save_json(cache, CACHE_FILE)
        
    out = []
    for d in domains:
        d_name = d if isinstance(d, str) else d.get("Domain", d.get("domain", d.get("url", "")))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
            # HARD RULE: Preserve incoming metadata (like _row_num)
            if isinstance(d, dict):
                obj.update({k: v for k, v in d.items() if k not in obj or obj[k] is None})
            out.append(obj)
    return out


def run_backlinks(domains):
    logging.info("--- RUNNING BACKLINKS MODULE ---")
    cache = load_json(CACHE_FILE)
    headers = get_seo_headers()

    to_query = []
    for dom in domains:
        # dom is already an enriched object dict from run_traffic if running full pipeline
        d_name = dom.get("Domain", dom.get("domain", ""))
        row = cache.get(d_name)
        if row and not row.get("_backlinks_done"):
             to_query.append(d_name)
             
    if to_query:
        logging.info(f"Pinging {len(to_query)} domains for Backlinks via DataForSEO individually...")
        for d_name in to_query:
            post_data = [{"target": d_name}]
            try:
                res = requests.post("https://api.dataforseo.com/v3/backlinks/summary/live", json=post_data, headers=headers)
                data = res.json()
                cost = data.get("cost", 0)
                cache[d_name]["_cost_bl"] += cost
                
                tasks = data.get("tasks", [])
                if tasks and tasks[0].get("result") and len(tasks[0]["result"]) > 0:
                    res_item = tasks[0]["result"][0]
                    cache[d_name]["_p2_rank"] = res_item.get("rank")
                    
                    countries = res_item.get("referring_links_countries", {})
                    top_countries = sorted(countries.items(), key=lambda x: x[1], reverse=True)[:3]
                    geo_str = ", ".join([f"{c[0]} ({c[1]})" for c in top_countries if c[0]])
                    
                    if geo_str:
                        target_geos = ["US", "UK", "AU", "WW", "CA"]
                        has_target = any(g in geo_str for g in target_geos)
                        cache[d_name]["Phase 2 - Geography"] = f"🟢 {geo_str}" if has_target else f"🔴 {geo_str}"
                    else:
                        cache[d_name]["Phase 2 - Geography"] = None
                    
                    score = res_item.get("backlinks_spam_score")
                    cache[d_name]["Phase 3 - Spam Score"] = int(score) if score is not None else None
                    
                    rd = res_item.get("referring_domains", 0)
                    bl = res_item.get("backlinks", 1)
                    if bl == 0: bl = 1
                    ratio = rd / bl
                    cache[d_name]["Phase 3 - Inbound Ratios"] = f"🟢 {rd} RD / {bl} BL" if ratio > 0.05 else f"🔴 {rd} RD / {bl} BL"
                else:
                    cache[d_name]["Phase 3 - Spam Score"] = None
                    cache[d_name]["Phase 2 - Geography"] = None
                    cache[d_name]["Phase 3 - Inbound Ratios"] = None
            except Exception as e:
                logging.error(f"Backlinks failed for {d_name}: {e}")
                cache[d_name]["Phase 3 - Spam Score"] = None
                cache[d_name]["Phase 2 - Geography"] = None
                cache[d_name]["Phase 3 - Inbound Ratios"] = None
            finally:
                cache[d_name]["_backlinks_done"] = True
                
        save_json(cache, CACHE_FILE)

    out = []
    for d in domains:
        d_name = d.get("Domain", d.get("domain", ""))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
            # HARD RULE: Preserve incoming metadata (like _row_num)
            obj.update({k: v for k, v in d.items() if k not in obj or obj[k] is None})
            out.append(obj)
    return out


def run_analysis(domains):
    logging.info("--- RUNNING GEMINI ANALYSIS MODULE ---")
    cache = load_json(CACHE_FILE)
    api_key_gemini = os.getenv("GEMINI_API_KEY")
    
    if not api_key_gemini:
        logging.error("No Gemini API key. Skipping.")
        return domains
        
    client = genai.Client(api_key=api_key_gemini)

    for i, dom in enumerate(domains):
        d_name = dom.get("Domain", dom.get("domain", ""))
        url = dom.get("URL (Domain)", dom.get("url", ""))
        row = cache.get(d_name)
        if not row: continue # Should theoretically never happen due to pipeline flow
        
        if row.get("_gemini_done"):
            continue
            
        # Scrape and Analysis
        clean_text = ""
        if row.get("Contact") is None:
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                req_url = url if str(url).startswith('http') else 'https://' + url
                res = requests.get(req_url, headers=headers, timeout=10)
                soup = BeautifulSoup(res.text, 'html.parser')
                clean_text = ' '.join(soup.get_text(separator=' ', strip=True).split())[:3000]
                emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', res.text))
                valid_emails = [e for e in emails if not e.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'))]
                if valid_emails:
                    row["Contact"] = ", ".join(valid_emails)
            except Exception:
                pass
                
        if clean_text and row.get("Phase 1 - Write for Us Red Flags") is None:
            try:
                logging.info(f"   -> Pinging Gemini specifically for {d_name}")
                prompt = f"""
                Analyze this website text from a wellness center in Canggu Bali.
                Text: {clean_text}
                
                Answer these 3 questions based on the text to evaluate the website's quality.
                1. "Write for Us" Red Flags: Detect guest post / link farm signals ("guest post", "write for us", "submit article", paid guest posting).
                2. Topical match: Is the content aligned with the wellness/spa/yoga/recovery niche?
                3. Quality Score: Score overall site quality 1-10 (Content depth, Audience targeting, Brand legitimacy, Writing quality).
                
                Format EXACTLY as this JSON structure:
                {{
                    "red_flags": {{
                        "status": "GREEN" or "RED",
                        "notes": ["list", "of", "trigger", "keywords", "found"] // Empty if GREEN
                    }},
                    "topical_match": {{
                        "status": "GREEN" or "RED",
                        "notes": ["list", "of", "topical", "keywords", "found"]
                    }},
                    "quality_score": 8
                }}
                """
                resp = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                resp_text = resp.text
                if "```json" in resp_text:
                     resp_text = resp_text.split("```json")[1].split("```")[0].strip()
                ans = json.loads(resp_text)
                
                # Format to user specification
                rf_data = ans.get("red_flags", {})
                if rf_data.get("status") == "RED":
                    row["Phase 1 - Write for Us Red Flags"] = "🔴 " + ", ".join(rf_data.get("notes", []))
                else:
                    row["Phase 1 - Write for Us Red Flags"] = "🟢"

                tm_data = ans.get("topical_match", {})
                if tm_data.get("status") == "RED":
                    row["Phase 1 - Topical Match"] = "🔴 " + ", ".join(tm_data.get("notes", []))
                else:
                    row["Phase 1 - Topical Match"] = "🟢 " + ", ".join(tm_data.get("notes", []))

                row["Quality Score (Phase 1 & 2)"] = int(ans.get("quality_score")) if ans.get("quality_score") is not None else None
            except Exception as e:
                logging.error(f"Gemini failed for {d_name}: {e}")
                row["Phase 1 - Write for Us Red Flags"] = None
                row["Phase 1 - Topical Match"] = None
                row["Quality Score (Phase 1 & 2)"] = None
                
        row["_gemini_done"] = True
        
        # No longer concatenating PBN/Content Quality into Quality Score since Gemini generates it natively
        
        save_json(cache, CACHE_FILE)

    out = []
    for d in domains:
        d_name = d.get("Domain", d.get("domain", ""))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
            # HARD RULE: Preserve incoming metadata (like _row_num)
            obj.update({k: v for k, v in d.items() if k not in obj or obj[k] is None})
            out.append(obj)
    return out
