import json
import logging
import argparse
import os
from dotenv import load_dotenv
from modules.module_1_prospecting import run_module_1
from modules.module_3_database import run_module_3, get_existing_domains_from_sheet
from modules.module_2_research import run_traffic, run_backlinks, run_analysis

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    with open('config/client_profile_template.json', 'r') as file:
        return json.load(file)

def parse_float(val):
    if val == "No Data" or val == "TBD" or "API" in str(val) or val == "N/A":
        return 0.0
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def main():
    parser = argparse.ArgumentParser(description="Backlink Automation Pipeline")
    parser.add_argument("--module", type=str, choices=["all", "traffic", "backlinks", "analysis"], default="all", help="Specific sub-module to run")
    parser.add_argument("--test", action="store_true", help="Run in test mode (1 domain, no GS writes, disabled Gemini)")
    args = parser.parse_args()

    load_dotenv(override=True)
    
    logging.info("Starting Master Automation Blueprint: Link Building & Outreach")
    config = load_config()
    client_site = config['client_details']['website_url']
    
    # 1. Load Domains
    os.makedirs('data', exist_ok=True)
    prospects_file = 'data/module_1_prospects.json'
    
    if os.path.exists(prospects_file):
        with open(prospects_file, 'r') as f:
             master_domain_list = json.load(f)
    else:
        master_domain_list = run_module_1(config['search_parameters'], client_site)
        with open(prospects_file, 'w') as f:
             json.dump(master_domain_list, f, indent=4)
             
    # 2. Filter existing Google Sheet domains
    existing_sheet_urls = [str(url).strip().lower() for url in get_existing_domains_from_sheet()]
    new_domains = []
    for d in master_domain_list:
        d_str = (d if isinstance(d, str) else d.get("domain", "")).strip().lower()
        if d_str not in existing_sheet_urls:
            new_domains.append(d)
            
    if args.test:
        new_domains = new_domains[:1]
        
    prospects = new_domains
    
    if not prospects:
         logging.info("No new domains to process!")
         return
         
    # 3. Traffic Module
    if args.module in ['all', 'traffic']:
        prospects = run_traffic(prospects)
        
        # 4. Filter by traffic (>500)
        if args.module == 'all':
            original_count = len(prospects)
            prospects = [p for p in prospects if parse_float(p.get("Phase 2 - Traffic Volume", 0)) > 500.0]
            logging.info(f"Traffic Filter (>500): {len(prospects)}/{original_count} qualified domains remaining.")
            
    # 5. Backlinks Module
    if args.module in ['all', 'backlinks']:
        if prospects:
             prospects = run_backlinks(prospects)
        
        # 6. Filter by Authority (Spam < 30)
        if args.module == 'all' and prospects:
            original_count = len(prospects)
            prospects = [p for p in prospects if parse_float(p.get("Phase 3 - Spam Score", "0")) < 30.0]
            logging.info(f"Authority Filter (Spam <30): {len(prospects)}/{original_count} qualified domains remaining.")
            
    # 7. Gemini Analysis Module
    if args.module in ['all', 'analysis']:
        if args.test:
             logging.info("Skipping Gemini in test mode.")
        elif prospects:
             prospects = run_analysis(prospects)
             
    # 8. Google Sheets Write
    if args.test:
        logging.info("--- TEST RESULTS ---")
        for p in prospects:
            print(json.dumps(p, indent=2))
        logging.info("Skipping Google Sheets writing in test mode.")
    else:
        if args.module == 'all' and prospects:
            run_module_3(prospects)
            
    logging.info("Pipeline Complete.")

if __name__ == "__main__":
    main()
