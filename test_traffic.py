import os
import logging
import base64
import requests
import json
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_traffic(domain):
    load_dotenv(override=True)
    login_seo = os.getenv("DATAFORSEO_LOGIN")
    password_seo = os.getenv("DATAFORSEO_PASSWORD")
    
    if not login_seo or not password_seo:
        logging.error("DataForSEO credentials missing from .env")
        return

    creds_seo = f"{login_seo}:{password_seo}"
    base64_seo = base64.b64encode(creds_seo.encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {base64_seo}", "Content-Type": "application/json"}
    
    logging.info(f"Testing different DataForSEO Traffic endpoints for: {domain}")
    
    # Endpoint 1: Domain Rank Overview (Current approach)
    try:
        logging.info("\n--- Approach 1: Google Domain Rank Overview ---")
        url_1 = "https://api.dataforseo.com/v3/dataforseo_labs/google/domain_rank_overview/live"
        post_data_1 = [{"target": domain, "location_code": 2840, "language_code": "en"}] # Try US instead of Global
        req_1 = requests.post(url_1, json=post_data_1, headers=headers)
        data_1 = req_1.json()
        print(json.dumps(data_1, indent=2)[:1000] + "\n...[truncated]")
    except Exception as e:
        logging.error(f"Endpoint 1 failed: {e}")

    # Endpoint 2: Ranked Keywords Live
    try:
        logging.info("\n--- Approach 2: Google Ranked Keywords (Often better for traffic estimates) ---")
        url_2 = "https://api.dataforseo.com/v3/dataforseo_labs/google/ranked_keywords/live"
        post_data_2 = [{"target": domain, "location_code": 2840, "language_code": "en", "limit": 1}]
        req_2 = requests.post(url_2, json=post_data_2, headers=headers)
        data_2 = req_2.json()
        print(json.dumps(data_2, indent=2)[:1000] + "\n...[truncated]")
    except Exception as e:
        logging.error(f"Endpoint 2 failed: {e}")

if __name__ == "__main__":
    test_traffic("udara-bali.com")
