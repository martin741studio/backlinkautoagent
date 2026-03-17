import os
import json
import logging
import time
import requests
from bs4 import BeautifulSoup
from google import genai
from pydantic import BaseModel, Field

# Load cache
CACHE_FILE = "data/module_4_cache.json"

def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                if data.get("_version") != "v1":
                    return {"_version": "v1"}
                return data
        except Exception:
            return {"_version": "v1"}
    return {"_version": "v1"}

def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

class OutreachResponse(BaseModel):
    subject: str = Field(description="The catchy, non-clickbaity email subject line")
    body: str = Field(description="The full email body pitching a guest post or partnership based on context")

def scrape_context(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        req_url = url if str(url).startswith('http') else 'https://' + url
        res = requests.get(req_url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Extract headings for context
        headings = []
        for tag in ['h1', 'h2', 'h3']:
            for h in soup.find_all(tag):
                text = h.get_text(strip=True)
                if len(text) > 10 and text not in headings:
                    headings.append(text)
                    
        # Grab first 2000 chars of paragraph text for context
        paragraphs = [p.get_text(strip=True) for p in soup.find_all('p') if len(p.get_text(strip=True)) > 20]
        p_text = " ".join(paragraphs)[:1500]
        
        return {
            "headings": headings[:15],
            "text": p_text
        }
    except Exception as e:
        logging.warning(f"Failed to scrape {url} for M4 context: {e}")
        return None

def run_outreach(targets, client_profile):
    logging.info("--- RUNNING MODULE 4 (OUTREACH ASSEMBLY) ---")
    cache = load_json(CACHE_FILE)
    api_key_gemini = os.getenv("GEMINI_API_KEY")
    
    if not api_key_gemini:
        logging.error("No Gemini API key found in .env. Skipping Outreach Module.")
        return targets
        
    client = genai.Client(api_key=api_key_gemini)
    
    business_desc = client_profile.get("client_details", {}).get("business_description", "A local business")
    business_name = client_profile.get("client_details", {}).get("business_name", "Our Company")
    website_url = client_profile.get("client_details", {}).get("website_url", "")
    
    out = []
    
    for p in targets:
        d_name = p.get("Domain", "")
        url = p.get("URL (Domain)", p.get("url", d_name))
        
        # We check if the orchestrator marked it as APPROVED.
        # If it's not approved, or if we explicitly don't want to email, skip.
        verdict = p.get("verdict", "")
        if "APPROVED" not in verdict:
            logging.info(f"Skipping Outreach for {d_name} -> Verdict is not APPROVED ({verdict})")
            p["Outreach Subject"] = None
            p["Outreach Body"] = None
            out.append(p)
            continue
            
        cached_m4 = cache.get(d_name, {})
        
        # Hydrate p from M4 cache if it already executed successfully
        if cached_m4.get("_outreach_done"):
            logging.info(f"Skipping Outreach -> already completed for {d_name}")
            p["Outreach Subject"] = cached_m4.get("subject")
            p["Outreach Body"] = cached_m4.get("body")
            p["_outreach_done"] = True
            out.append(p)
            continue
            
        logging.info(f"   -> Scraping Context & Generating Outreach Pitch for {d_name}")
        
        # 1. Scrape Context
        site_context = scrape_context(url)
        context_str = "No specific contextual headings extracted."
        if site_context:
            context_str = f"Site Headings: {site_context['headings']}\nSite Intro Text: {site_context['text']}"
            
        # 2. Prompt Gemini
        sys_instructions = (
            "You are an elite B2B outreach specialist building link-building and guest posting partnerships. "
            "Write highly personalized, non-spammy, direct cold emails. "
            "Never use generic greetings like 'Dear Webmaster'. Keep it under 150 words. "
            "Always reference their site context specifically to prove you actually read their website. "
            "Tone: Professional, highly relevant, and conversational."
        )
        
        prompt = f"""
        We are reaching out from:
        Business: {business_name}
        Description: {business_desc}
        URL: {website_url}
        
        We are pitching a high-quality guest post or link partnership to the website: {d_name}.
        Here is recent context scraped directly from their site (use this to personalize the email):
        {context_str}
        
        Task: Write a personalized cold email to the editor. 
        1. Create a catchy, non-clickbaity subject line.
        2. Write the body text. Propose a specific, highly relevant article topic we can write for them based on their recent headings or text.
        Make sure the content aligns with BOTH their context and our business seamlessly. No placeholders for names, just say 'Hi team' if no name is available.
        """
        
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config={
                    'response_mime_type': 'application/json',
                    'response_schema': OutreachResponse,
                    'system_instruction': sys_instructions,
                    'temperature': 0.7
                }
            )
            data = json.loads(response.text)
            
            p["Outreach Subject"] = data.get("subject")
            p["Outreach Body"] = data.get("body")
            p["_outreach_done"] = True
            
            # Save strictly to local M4 Cache
            cached_m4["subject"] = data.get("subject")
            cached_m4["body"] = data.get("body")
            cached_m4["_outreach_done"] = True
            cache[d_name] = cached_m4
            save_json(cache, CACHE_FILE)
            
            time.sleep(2) # rate limit safety
            
        except Exception as e:
            logging.error(f"Failed to generate outreach for {d_name}: {e}")
            p["Outreach Subject"] = None
            p["Outreach Body"] = None
            p["_outreach_done"] = False
            
        out.append(p)
        
    return out
