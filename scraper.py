import pandas as pd
from datetime import datetime, timedelta
import re
import logging
import time

from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import TimeFilters

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

linkedin_jobs_data = []

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
    
    return today.strftime('%d-%m-%Y')

def on_linkedin_data(data: EventData):
    global linkedin_jobs_data
    linkedin_jobs_data.append({
        'Company': data.company, 'Role': data.title, 'Location': data.location,
        'Experience': '', 'Posted Date': convert_date(data.date), 'Source Portal': 'LinkedIn'
    })

def run_linkedin_scraper(limit=50):
    global linkedin_jobs_data
    linkedin_jobs_data = []
    scraper = LinkedinScraper(headless=True, max_workers=1, slow_mo=1)
    scraper.on(Events.DATA, on_linkedin_data)
    queries = [Query(query="startup", options=QueryOptions(limit=limit, filters=QueryFilters(time=TimeFilters.WEEK)))]
    scraper.run(queries)
    return pd.DataFrame(linkedin_jobs_data)

def run_iimjobs_scraper(scroll_limit=2):
    url = "https://www.iimjobs.com/c/it-systems-jobs"
    jobs = []
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            for _ in range(scroll_limit):
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
    return pd.DataFrame(jobs)

def run_full_scrape(linkedin_limit, iimjobs_limit):
    df_linkedin = run_linkedin_scraper(limit=linkedin_limit)
    df_iim = run_iimjobs_scraper(scroll_limit=iimjobs_limit)
    if df_linkedin.empty and df_iim.empty: return pd.DataFrame()
    return pd.concat([df_linkedin, df_iim], ignore_index=True).fillna('').drop_duplicates(subset=['Company', 'Role', 'Location'], keep='first')
