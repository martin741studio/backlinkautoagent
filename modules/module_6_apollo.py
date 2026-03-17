import os
import json
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

CACHE_FILE = "data/module_6_apollo_cache.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(data):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def run_apollo_enrichment(targets):
    """
    Module 6 (Apollo Enrichment Phase)
    Enriches ONLY high-quality, pre-qualified leads with email contacts.
    """
    logging.info("--- RUNNING APOLLO ENRICHMENT ---")
    
    api_key = os.getenv("APOLLO_API_KEY")
    if not api_key:
        logging.warning("APOLLO_API_KEY not found in environment. Skipping Apollo enrichment.")
        return targets

    cache = load_cache()
    
    # Fail-safe aggregate domains
    blocked_domains = ["tripadvisor.com", "yelp.com", "booking.com", "klook.com", "agoda.com", "airbnb.com", "facebook.com", "instagram.com", "youtube.com", "medium.com"]
    valid_roles = ["founder", "owner", "marketing", "manager", "ceo", "director", "head"]

    for p in targets:
        domain = p.get("Domain", "")
        verdict = p.get("Verdict", p.get("verdict", ""))
        
        if p.get("Contact") is None:
            p["Contact"] = ""

        # Maps pipeline 'Contact' mapping which aligns to Column E
        existing_email = (p.get("Contact") or "").strip() 
        if not existing_email and p.get("Email"):
            existing_email = (p.get("Email") or "").strip()

        # Resets Tracking
        p["_apollo_attempted"] = False
        p["_apollo_enriched"] = False
        
        # 1. Skip Conditions (Strictly Enforced)
        if "APPROVED" not in verdict:
            logging.info(f"Skipping Apollo for {domain} -> Verdict is not APPROVED ({verdict})")
            continue
            
        if any(b in domain.lower() for b in blocked_domains):
            logging.info(f"Skipping Apollo for {domain} -> Blocked aggregator domain")
            continue
            
        if existing_email:
            logging.info(f"Skipping Apollo for {domain} -> Email already exists in Column E ({existing_email})")
            continue
            
        # 2. Cache Verification (Zero Waste rule)
        if domain in cache:
            logging.info(f"   -> Loaded Apollo data from cache for {domain}")
            cached_data = cache[domain]
            if cached_data.get("email"):
                found_email = cached_data["email"]
                # Even if we skipped hitting Apollo, we can safely append if valid
                if existing_email:
                    if found_email not in existing_email:
                        p["Contact"] = f"{existing_email}, {found_email}"
                else:
                    p["Contact"] = found_email
                    
                p["Email"] = p["Contact"] # mirror
                p["_apollo_enriched"] = True
            continue

        # 3. Apollo REST Execution
        logging.info(f"   -> Pinging Apollo API for highly qualified target: {domain}...")
        p["_apollo_attempted"] = True
        
        url = "https://api.apollo.io/api/v1/mixed_people/search"
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": api_key
        }
        
        payload = {
            "q_organization_domains_list": [domain],
            "person_seniorities": ["owner", "founder", "c_suite"],
            "contact_email_status": ["verified"],
            "per_page": 3
        }
        
        logging.info(f"      -> Apollo Payload: {payload}")
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=20)
            logging.info(f"      -> Apollo Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                people = data.get("people", [])
                found_email = None
                
                if people:
                    for person in people:
                        # Extract email ONLY if has_email is True
                        if person.get("has_email") == True and person.get("email"):
                            found_email = person.get("email")
                            title = person.get("title", "Unknown")
                            logging.info(f"      -> Success! Found validated decision-maker email: {found_email} ({title})")
                            break
                        
                if found_email:
                    if existing_email:
                        if found_email not in existing_email:
                            p["Contact"] = f"{existing_email}, {found_email}"
                    else:
                        p["Contact"] = found_email
                        
                    p["Email"] = p["Contact"]
                    p["_apollo_enriched"] = True
                    cache[domain] = {"email": found_email, "status": "success"}
                else:
                    p["_apollo_enriched"] = False
                    cache[domain] = {"email": None, "status": "no_email_found"}
                    logging.info(f"      -> Apollo returned no valid decision-makers/emails for {domain}")
            
            else:
                p["_apollo_enriched"] = False
                logging.error(f"      -> Apollo API returned error status: {response.status_code} - {response.text}")
                cache[domain] = {"email": None, "status": f"error_{response.status_code}"}
                
            save_cache(cache)
            time.sleep(1.5) # Soft delay for rate limits
            
        except Exception as e:
            logging.error(f"      -> Error calling Apollo API for {domain}: {e}")
            cache[domain] = {"email": None, "status": "error"}
            save_cache(cache)
            time.sleep(1.5)
            continue
            
    return targets
