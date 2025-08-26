import pandas as pd
from datetime import datetime, timedelta
import re
import logging
import random
import time

from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import TimeFilters, RelevanceFilters

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# --- GLOBAL VARIABLES & HELPER DATA ---
linkedin_jobs_data = []
linkedin_scraped_count = 0

STARTUP_INDICATORS = [
    'startup', 'stealth', 'early stage', 'seed stage', 'series a', 'series b', 'series c',
    'pre-series', 'venture backed', 'vc funded', 'angel funded', 'founded 20',
    'fast-growing', 'scaling', 'disruptive', 'innovative', 'emerging',
    'fintech startup', 'edtech startup', 'healthtech startup', 'agritech startup',
    'saas startup', 'ai startup', 'unicorn', 'deep tech', 'growth stage', 'scale-up'
]

EXCLUDE_LARGE_COMPANIES = [
    'google', 'microsoft', 'amazon', 'meta', 'facebook', 'apple', 'netflix', 'tesla', 'uber',
    'tcs', 'infosys', 'wipro', 'cognizant', 'hcl', 'accenture', 'capgemini', 'deloitte',
    'pwc', 'kpmg', 'ey', 'mckinsey', 'bain', 'bcg', 'reliance', 'tata'
]

INDIAN_CITIES = [
    'Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Pune', 'Chennai', 'Kolkata', 'Ahmedabad',
    'Jaipur', 'Kochi', 'Goa', 'Chandigarh'
]

# --- HELPER FUNCTIONS ---
def convert_date(text):
    if text is None: return None
    text = str(text).lower().strip()
    today = datetime.today()

    if re.search(r'^\d{4}-\d{2}-\d{2}', text):
        try: return datetime.strptime(text.split('t')[0], '%Y-%m-%d').strftime('%d-%m-%Y')
        except ValueError: pass
    
    if (m := re.search(r'^(\d+)h$', text)): return today.strftime('%d-%m-%Y')
    if (m := re.search(r'^(\d+)d$', text)): return (today - timedelta(days=int(m.group(1)))).strftime('%d-%m-%Y')
    if (m := re.search(r'^(\d+)w$', text)): return (today - timedelta(weeks=int(m.group(1)))).strftime('%d-%m-%Y')
    if (m := re.search(r'^(\d+)mo$', text)): return (today - timedelta(days=int(m.group(1)) * 30)).strftime('%d-%m-%Y')
    
    if "yesterday" in text: return (today - timedelta(days=1)).strftime('%d-%m-%Y')
    if (m := re.search(r'(\d+)\s+days?', text)): return (today - timedelta(days=int(m.group(1)))).strftime('%d-%m-%Y')
    if (m := re.search(r'(\d+)\s+weeks?', text)): return (today - timedelta(weeks=int(m.group(1)))).strftime('%d-%m-%Y')
    if (m := re.search(r'(\d+)\s+months?', text)): return (today - timedelta(days=int(m.group(1)) * 30)).strftime('%d-%m-%Y')
    
    return today.strftime('%d-%m-%Y') # Default for "today", "just now", etc.

def is_startup_company(company_name, description=""):
    if not company_name: return False
    company_lower = company_name.lower()
    desc_lower = description.lower() if description else ""
    for large_company in EXCLUDE_LARGE_COMPANIES:
        if large_company in company_lower:
            return False
    for indicator in STARTUP_INDICATORS:
        if indicator in company_lower or indicator in desc_lower:
            return True
    return False

def extract_experience_from_description(description):
    if not description: return ""
    desc_lower = description.lower()
    patterns = [
        r'(\d+)\+?\s*to\s*(\d+)\s*years', r'(\d+)\s*-\s*(\d+)\s*years', # e.g., 3 to 5 years, 3 - 5 years
        r'(\d+)\+?\s*years?' # e.g., 3+ years, 5 years
    ]
    for pattern in patterns:
        match = re.search(pattern, desc_lower)
        if match:
            if len(match.groups()) == 2 and match.group(2):
                return f"{match.group(1)}-{match.group(2)} years"
            return f"{match.group(1)}+ years"
    return ""

def extract_detailed_location(description, location):
    if not description: description = ""
    text = (description + " " + location).lower()
    
    if any(k in text for k in ['remote', 'work from home', 'wfh']):
        return "Remote"
    if 'hybrid' in text:
        return f"Hybrid ({location})"
    return location

# --- LINKEDIN SCRAPER ---
def create_linkedin_queries():
    queries = []
    startup_keywords = ['startup', 'seed stage', 'series a', 'venture backed', 'fintech', 'saas']
    roles = ['Product Manager', 'Software Engineer', 'Founder\'s Office', 'Growth Hacker']
    
    for role in roles:
        for keyword in startup_keywords:
            for city in INDIAN_CITIES:
                queries.append(
                    Query(
                        query=f'{role} {keyword}',
                        options=QueryOptions(
                            locations=[city],
                            limit=25,
                            filters=QueryFilters(
                                time=TimeFilters.MONTH,
                                relevance=RelevanceFilters.RELEVANT
                            )
                        )
                    )
                )
    random.shuffle(queries)
    return queries

def on_linkedin_data(data: EventData):
    global linkedin_jobs_data, linkedin_scraped_count
    if not is_startup_company(data.company, data.description):
        return
    
    linkedin_jobs_data.append({
        'Company': data.company,
        'Role': data.title,
        'Location': extract_detailed_location(data.description, data.location),
        'Experience': extract_experience_from_description(data.description),
        'Posted Date': convert_date(data.date),
        'Source Portal': 'LinkedIn'
    })
    linkedin_scraped_count += 1
    print(f"[LinkedIn] Scraped startup job #{linkedin_scraped_count}: {data.title} at {data.company}")

def run_linkedin_scraper(limit=50):
    global linkedin_jobs_data, linkedin_scraped_count
    linkedin_jobs_data = []; linkedin_scraped_count = 0
    
    scraper = LinkedinScraper(headless=True, max_workers=1, slow_mo=1)
    scraper.on(Events.DATA, on_linkedin_data)
    
    queries = create_linkedin_queries()
    # Scraper has an internal limit, we break manually when we hit our target
    for query in queries:
        if linkedin_scraped_count >= limit:
            break
        scraper.run([query])
        
    return pd.DataFrame(linkedin_jobs_data)

# --- IIMJOBS SCRAPER ---
def scrape_iimjobs_page(url, scroll_times=2):
    jobs = []
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            for _ in range(scroll_times):
                page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
                time.sleep(2)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            for card in soup.select('div.MuiPaper-root:has(p[data-testid="job_title"])'):
                title_tag = card.select_one('p[data-testid="job_title"]')
                full_title = title_tag.get_text(strip=True) if title_tag else ''
                company, role = (parts[0].strip(), parts[1].strip()) if " - " in full_title and len(parts := full_title.split(" - ", 1)) >= 2 else (None, full_title)
                
                jobs.append({
                    'Company': company, 'Role': role,
                    'Location': card.select_one('p[data-testid="job_location"]').get_text(strip=True) if card.select_one('p[data-testid="job_location"]') else '',
                    'Experience': card.select_one('span[data-testid="job_experience"]').get_text(strip=True) if card.select_one('span[data-testid="job_experience"]') else '',
                    'Posted Date': convert_date(card.select_one('span[data-testid="date_posted"]').get_text(strip=True) if card.select_one('span[data-testid="date_posted"]') else ''),
                    'Source Portal': 'IIMJobs'
                })
            browser.close()
        except PlaywrightTimeoutError: print(f"Timeout error on {url}")
        except Exception as e: print(f"An error occurred during IIMJobs scraping: {e}")
    return jobs

def run_iimjobs_scraper(scroll_limit=2):
    iim_categories = [
        ("https://www.iimjobs.com/c/banking-finance-jobs", "Banking & Finance"),
        ("https://www.iimjobs.com/c/sales-marketing-jobs", "Sales & Marketing"),
        ("https://www.iimjobs.com/c/consulting-general-mgmt-jobs", "Consulting"),
        ("https://www.iimjobs.com/c/hr-ir-jobs", "HR"),
        ("https://www.iimjobs.com/c/it-systems-jobs", "IT & Systems"),
        ("https://www.iimjobs.com/c/scm-operations-jobs", "SCM & Operations"),
        ("https://www.iimjobs.com/c/legal-jobs", "Legal"),
        ("https://www.iimjobs.com/c/bpo-jobs", "BPO")
    ]
    all_jobs = []
    for url, category in iim_categories:
        print(f"[IIMJobs] Scraping category: {category}")
        all_jobs.extend(scrape_iimjobs_page(url, scroll_times=scroll_limit))
    return pd.DataFrame(all_jobs)

# --- MAIN ORCHESTRATOR ---
def run_full_scrape(linkedin_limit, iimjobs_limit):
    df_linkedin = run_linkedin_scraper(limit=linkedin_limit)
    df_iim = run_iimjobs_scraper(scroll_limit=iimjobs_limit)
    if df_linkedin.empty and df_iim.empty: return pd.DataFrame()
    
    combined_df = pd.concat([df_linkedin, df_iim], ignore_index=True).fillna('')
    combined_df.drop_duplicates(subset=['Company', 'Role', 'Location'], inplace=True, keep='first')
    return combined_df
