import urllib.parse
import re
import logging

def normalize_domain_url(raw_url: str) -> str:
    """
    STEP 1 & STEP 2: NORMALIZE EVERY URL & ENFORCE HOMEPAGE RULE
    - Remove www.
    - Force https://
    - Remove trailing /
    - Remove URL parameters (?ref=...)
    - Remove anchors (#section)
    - Strip everything after the domain (Homepage rule)
    """
    if not raw_url or not isinstance(raw_url, str):
        return "INVALID_URL"
        
    raw_url = raw_url.strip().lower()
    
    if not raw_url.startswith("http"):
        raw_url = "https://" + raw_url
        
    try:
        parsed = urllib.parse.urlparse(raw_url)
        netloc = parsed.netloc
        
        # Remove www.
        if netloc.startswith("www."):
            netloc = netloc[4:]
            
        if not netloc:
             return "INVALID_URL"
             
        # Enforce highly strict root domain Homepage rule (STEP 2)
        # Reconstruct as cleanly as possible strictly enforcing https and no trailing slash
        clean_url = f"https://{netloc}"
        
        # Basic validation to ensure it looks like a domain (has a dot, no weird characters)
        if "." not in netloc or re.search(r'[^a-z0-9.-]', netloc):
             return "INVALID_URL"
             
        return clean_url
        
    except Exception as e:
        logging.error(f"URL Normalization Error on {raw_url}: {e}")
        return "INVALID_URL"

def test_normalization():
    # Test cases representing user specification
    test_urls = [
        "http://www.example.com/about?ref=ads",
        "https://example.com/services/webdesign",
        "www.test.co.uk/",
        "dirty-domain.com#footer",
        "invalid_string_no_domain"
    ]
    
    for u in test_urls:
        print(f"Original: {u}  =>  Normalized: {normalize_domain_url(u)}")

if __name__ == "__main__":
    test_normalization()
