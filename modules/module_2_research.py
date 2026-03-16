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
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def initialize_empty(domain_name, url=""):
    if not url: url = f"https://{domain_name}"
    return {
        "Domain": domain_name,
        "URL (Domain)": url,
        "Phase 1 - Real Website vs PBN": "TBD",
        "Phase 1 - Topical Match": "TBD",
        "Phase 1 - Content Quality": "TBD",
        "Phase 1 - Write for Us Red Flags": "TBD",
        "Contact": "None Found",
        "Quality Score (Phase 1 & 2)": "TBD",
        "Phase 2 - Geography": "TBD",
        "Phase 2 - Traffic Volume": "TBD",
        "Phase 3 - Inbound Ratios": "TBD",
        "Phase 3 - Spam Score": "TBD",
        "Time Taken (Seconds)": 0,
        "Total Cost (USD)": "$0.00000",
        "Cost Breakdown": "DataForSEO Backlinks: $0.00 | DataForSEO Traffic: $0.00 | Gemini: Free",
        "_p2_rank": "N/A",
        "_cost_bl": 0.0,
        "_cost_tr": 0.0
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
            
        traffic_status = row.get("Phase 2 - Traffic Volume", "TBD")
        # Retry logic for failed pings
        if traffic_status == "TBD" or "API Message" in str(traffic_status):
             to_query.append(d_name)
             
    if to_query:
        logging.info(f"Batch pinging {len(to_query)} domains for Traffic via DataForSEO...")
        # DataForSEO allows up to 100 targets per request
        for chunk in chunks(to_query, 100):
            post_data = []
            targets = []
             
            for d_name in chunk:
                targets.append(d_name)
                post_data.append({"target": d_name, "location_code": 2840, "language_code": "en"})
            
            try:
                res = requests.post("https://api.dataforseo.com/v3/dataforseo_labs/google/domain_rank_overview/live", json=post_data, headers=headers)
                data = res.json()
                cost = data.get("cost", 0)
                cost_each = cost / len(targets) if targets else 0
                
                tasks = data.get("tasks", [])
                for task in tasks:
                    t_target = task.get("data", {}).get("target", "")
                    if not t_target: continue
                        
                    cache[t_target]["_cost_tr"] += cost_each
                    
                    if task.get("result") and len(task["result"]) > 0 and task["result"][0].get("items") and len(task["result"][0]["items"]) > 0:
                        metrics_obj = task["result"][0]["items"][0].get("metrics", {})
                        if "organic" in metrics_obj:
                            raw_traffic = metrics_obj["organic"].get("etv", 0)
                            cache[t_target]["Phase 2 - Traffic Volume"] = str(int(round(float(raw_traffic))))
                        else:
                            cache[t_target]["Phase 2 - Traffic Volume"] = "No Data"
                    else:
                        cache[t_target]["Phase 2 - Traffic Volume"] = f"API Message: {task.get('status_message', 'No Data')}"
            except Exception as e:
                logging.error(f"Traffic batch failed: {e}")
                
        save_json(cache, CACHE_FILE)
        
    out = []
    for d in domains:
        d_name = d if isinstance(d, str) else d.get("Domain", d.get("domain", d.get("url", "")))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
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
        if row and row.get("_p2_rank") == "N/A":
             to_query.append(d_name)
             
    if to_query:
        logging.info(f"Batch pinging {len(to_query)} domains for Backlinks via DataForSEO...")
        for chunk in chunks(to_query, 100):
            post_data = [{"target": t} for t in chunk]
            try:
                res = requests.post("https://api.dataforseo.com/v3/backlinks/summary/live", json=post_data, headers=headers)
                data = res.json()
                cost = data.get("cost", 0)
                cost_each = cost / len(chunk) if chunk else 0
                
                tasks = data.get("tasks", [])
                for task in tasks:
                    t_target = task.get("data", {}).get("target", "")
                    if not t_target: continue
                        
                    cache[t_target]["_cost_bl"] += cost_each
                    
                    if task.get("result") and len(task["result"]) > 0:
                        res_item = task["result"][0]
                        cache[t_target]["_p2_rank"] = res_item.get("rank", "N/A")
                        
                        countries = res_item.get("referring_links_countries", {})
                        top_countries = sorted(countries.items(), key=lambda x: x[1], reverse=True)[:3]
                        cache[t_target]["Phase 2 - Geography"] = ", ".join([f"{c[0]} ({c[1]})" for c in top_countries if c[0]])
                        
                        cache[t_target]["Phase 3 - Spam Score"] = res_item.get("backlinks_spam_score", "N/A")
                        rd = res_item.get("referring_domains", 0)
                        bl = res_item.get("backlinks", 1)
                        cache[t_target]["Phase 3 - Inbound Ratios"] = f"{rd} RD / {bl} BL"
            except Exception as e:
                logging.error(f"Backlinks batch failed: {e}")
                
        save_json(cache, CACHE_FILE)

    out = []
    for d in domains:
        d_name = d.get("Domain", d.get("domain", ""))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
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
        
        # Scrape and Analysis
        clean_text = ""
        if row.get("Phase 1 - Write for Us Red Flags", "TBD") == "TBD" or row.get("Contact") == "None Found":
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
                
        if clean_text and row.get("Phase 1 - Write for Us Red Flags", "TBD") == "TBD":
            try:
                logging.info(f"   -> Pinging Gemini specifically for {d_name}")
                prompt = f"""
                Analyze this website text from a wellness center in Canggu Bali.
                Text: {clean_text}
                
                Answer these 3 questions based on the text:
                1. "Write for Us" Red Flags? Does it mention "write for us", "guest post", "submit article", contributor submission pages, or paid guest posting? Output exact format: "🔴 YES - [short reason]" or "🟢 NO - [short reason]".
                2. Topical match? Does it match the wellness/spa/yoga/recovery niche? Output exactly one word: High, Medium, or Low.
                3. Quality Score? Score the overall site quality from 1-10 based on editorial depth, legitimacy, and non-spam content. Output exactly the number (e.g., 8).
                
                Format as JSON with exact keys: 'red_flags', 'topical_match', 'quality_score'.
                """
                resp = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                resp_text = resp.text
                if "```json" in resp_text:
                     resp_text = resp_text.split("```json")[1].split("```")[0].strip()
                ans = json.loads(resp_text)
                row["Phase 1 - Write for Us Red Flags"] = ans.get("red_flags", "")
                row["Phase 1 - Topical Match"] = ans.get("topical_match", "")
                row["Quality Score (Phase 1 & 2)"] = str(ans.get("quality_score", ""))
            except Exception as e:
                logging.error(f"Gemini failed for {d_name}: {e}")
                
        # No longer concatenating PBN/Content Quality into Quality Score since Gemini generates it natively
        
        save_json(cache, CACHE_FILE)

    out = []
    for d in domains:
        d_name = d.get("Domain", d.get("domain", ""))
        obj = cache.get(d_name)
        if obj:
            _update_costs(obj)
            out.append(obj)
    return out
