import os
import time
import pandas as pd
from datetime import datetime, timedelta
import re
import logging
import random

# --- Google Sheets Specific Imports ---
# import gspread
# from gspread import exceptions as gspread_exceptions
# from oauth2client.service_account import ServiceAccountCredentials

# --- LinkedIn Scraper Specific Imports ---
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, OnSiteOrRemoteFilters

# --- Playwright for IIMJobs ---
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Configure logging for the entire script
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# --- GLOBAL CONFIGURATION ---
# The name of your Google Spreadsheet
# GOOGLE_SHEET_NAME = "Job Scraping" # <<< VERIFY THIS: Must match your Google Sheet name exactly
# # The path to your service account credentials JSON file
# SERVICE_ACCOUNT_FILE = 'merito-463909-3b7151d0f605.json' # <<< VERIFY THIS PATH (relative to script location)

# # Name for the combined sheet where all data will be saved
# FINAL_UNIFIED_SHEET = "temporary"

# --- SCRAPING LIMIT CONFIGURATION ---
# Adjust these values to control the depth of scraping for each platform
LINKEDIN_DEFAULT_TARGET_JOBS = 20 # Aim for this many unique startup jobs from LinkedIn
IIMJOBS_DEFAULT_SCROLL_TIMES = 5   # Number of times to scroll down on IIMJobs pages

# --- GLOBAL VARIABLES FOR LINKEDIN SCRAPER (to collect data during callbacks) ---
linkedin_jobs_data = [] # Global list to store scraped job data
linkedin_scraped_count = 0 # Global counter for processed jobs

# --- COMMON HELPER DATA ---
# Startup indicators - keywords that suggest a company is a startup
STARTUP_INDICATORS = [
    'startup', 'stealth', 'early stage', 'seed stage', 'series a', 'series b', 'series c',
    'pre-series', 'venture backed', 'vc funded', 'angel funded', 'founded 20', # 'founded 20' covers 20xx
    'fast-growing', 'scaling', 'disruptive', 'innovative', 'emerging',
    'fintech startup', 'edtech startup', 'healthtech startup', 'agritech startup',
    'proptech startup', 'foodtech startup', 'mobility startup', 'cleantech startup',
    'saas startup', 'ai startup', 'blockchain startup', 'crypto startup',
    'unicorn', 'decacorn', 'zebra company', 'b2b startup', 'b2c startup',
    'marketplace startup', 'platform startup', 'tech startup', 'deep tech',
    'growth stage', 'scale-up', 'hyper-growth'
]

# Large companies to exclude (case-insensitive for comparison)
EXCLUDE_LARGE_COMPANIES = [
    'google', 'microsoft', 'amazon', 'meta', 'facebook', 'apple', 'netflix',
    'tesla', 'uber', 'airbnb', 'spotify', 'twitter', 'linkedin', 'salesforce',
    'oracle', 'ibm', 'intel', 'nvidia', 'adobe', 'paypal', 'ebay', 'yahoo',
    'tcs', 'infosys', 'wipro', 'cognizant', 'hcl', 'accenture', 'capgemini',
    'deloitte', 'pwc', 'kpmg', 'ey', 'mckinsey', 'bain', 'bcg',
    'reliance', 'tata', 'mahindra', 'bajaj', 'godrej', 'birla', 'adani',
    'flipkart', 'paytm', 'ola', 'swiggy', 'zomato', 'byju', 'unacademy'
]

# Indian cities for location extraction and LinkedIn queries
INDIAN_CITIES = [
    'Bangalore', 'Mumbai', 'Delhi', 'Hyderabad', 'Pune', 'Chennai', 'Kolkata',
    'Ahmedabad', 'Surat', 'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore',
    'Kochi', 'Coimbatore', 'Goa', 'Chandigarh', 'Dehradun', 'Mysore', 'Guwahati',
    'Thane', 'Bhopal', 'Visakhapatnam', 'Patna', 'Vadodara', 'Ludhiana', 'Agra',
    'Nashik', 'Thiruvananthapuram', 'Gandhinagar', 'Kozhikode', 'Mangalore',
    'Bhubaneswar', 'Ranchi', 'Jamshedpur', 'Udaipur', 'Varanasi'
]

# --- GOOGLE SHEETS FUNCTIONS ---

# def get_google_sheet_client():
#     """Authenticates with Google Sheets using a service account and returns a client."""
#     try:
#         scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
#         creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
#         client = gspread.authorize(creds)
#         print("\n[Google Sheets] Successfully authenticated.")
#         return client
#     except Exception as e:
#         print(f"\n[Google Sheets] ERROR: Failed to authenticate. Please check 'SERVICE_ACCOUNT_FILE' path and ensure Google Sheets/Drive APIs are enabled in your Google Cloud project.")
#         print(f"        Details: {e}")
#         return None

# def write_unified_data_to_sheet(client, spreadsheet_name, worksheet_name, new_dataframe):
#     """
#     Writes a pandas DataFrame to a specified worksheet in a Google Sheet.
#     It reads existing data, combines it with new data, deduplicates,
#     and then writes the unique combined data back, preserving order.
#     """
#     if new_dataframe.empty:
#         print(f"[Google Sheets] No new data to write to unified worksheet '{worksheet_name}'. Skipping.")
#         return

#     try:
#         spreadsheet = client.open(spreadsheet_name)
#         try:
#             worksheet = spreadsheet.worksheet(worksheet_name)
#             print(f"[Google Sheets] Found existing unified worksheet: '{worksheet_name}'")

#             # Read existing data from the sheet
#             existing_data = worksheet.get_all_values()
#             if existing_data:
#                 # Assuming the first row is headers
#                 existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
#                 print(f"[Google Sheets] Read {len(existing_df)} existing records from '{worksheet_name}'.")
#             else:
#                 existing_df = pd.DataFrame()
#                 print(f"[Google Sheets] Existing unified worksheet '{worksheet_name}' is empty.")

#         except gspread_exceptions.WorksheetNotFound:
#             print(f"[Google Sheets] Unified worksheet '{worksheet_name}' not found. Creating a new one...")
#             worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols="1")
#             print(f"[Google Sheets] Created new unified worksheet: '{worksheet_name}'")
#             existing_df = pd.DataFrame() # No existing data if worksheet was just created

#         # Combine existing and new data
#         if not existing_df.empty:
#             for col in existing_df.columns:
#                 if col not in new_dataframe.columns:
#                     new_dataframe[col] = ''
#             new_dataframe = new_dataframe[existing_df.columns]
#         else:
#             pass 

#         # Concatenate and deduplicate
#         combined_df = pd.concat([existing_df, new_dataframe], ignore_index=True)
#         initial_combined_count = len(combined_df)

#         combined_df['Posted Date Temp'] = pd.to_datetime(combined_df['Posted Date'], dayfirst=True, errors='coerce')
#         combined_df.drop_duplicates(subset=['Company', 'Role', 'Location', 'Experience', 'Posted Date Temp'], inplace=True, keep='first')
#         final_combined_count = len(combined_df)
#         print(f"[Google Sheets] Removed {initial_combined_count - final_combined_count} duplicates (including existing ones).")
#         combined_df.drop(columns=['Posted Date Temp'], inplace=True)

#         # Re-sort the entire dataset after deduplication to maintain consistent order
#         if 'Source Portal' in combined_df.columns:
#             source_order = ['LinkedIn', 'IIMJobs']
#             combined_df['Source Portal'] = pd.Categorical(combined_df['Source Portal'], categories=source_order, ordered=True)
#             combined_df['Posted Date Temp Sort'] = pd.to_datetime(combined_df['Posted Date'], dayfirst=True, errors='coerce') # Temp for sorting
#             combined_df.sort_values(by=['Source Portal', 'Posted Date Temp Sort'], ascending=[True, False], inplace=True)
#             combined_df.drop(columns=['Posted Date Temp Sort'], inplace=True)
#         else:
#             combined_df['Posted Date Temp Sort'] = pd.to_datetime(combined_df['Posted Date'], dayfirst=True, errors='coerce') # Temp for sorting
#             combined_df.sort_values(by=['Posted Date Temp Sort'], ascending=[False], inplace=True)
#             combined_df.drop(columns=['Posted Date Temp Sort'], inplace=True)

#         # Convert DataFrame to a list of lists (including headers)
#         data_to_write = [combined_df.columns.values.tolist()] + combined_df.values.tolist()

#         # Clear existing content and write new data
#         worksheet.clear()
#         worksheet.update(data_to_write)
#         print(f"[Google Sheets] Successfully updated unified Google Sheet '{spreadsheet_name}', worksheet '{worksheet_name}' with {len(combined_df)} unique jobs.")
#     except gspread_exceptions.SpreadsheetNotFound:
#         print(f"[Google Sheets] ERROR: Google Spreadsheet '{spreadsheet_name}' not found.")
#         print("        Please check the spreadsheet name and ensure your service account has editor access.")
#     except Exception as e:
#         print(f"[Google Sheets] An error occurred while writing to unified Google Sheet: {e}")
#         return

# --- HELPER FUNCTIONS FOR DATE CONVERSION AND EXTRACTION ---

def convert_human_readable_date_to_dd_mm_yyyy(text):
    """
    Converts a human-readable date string (e.g., '2025-08-24', '2d', '3 weeks ago')
    to a DD-MM-YYYY string. This function is designed to be robust for various formats.
    """
    text = str(text).lower().strip()
    today = datetime.today()

    if not text:
        return None

    # *** NEW: Handle YYYY-MM-DD format from the datetime attribute ***
    match_yyyy_mm_dd = re.search(r'^(\d{4}-\d{2}-\d{2})', text)
    if match_yyyy_mm_dd:
        try:
            date_obj = datetime.strptime(match_yyyy_mm_dd.group(1), '%Y-%m-%d')
            return date_obj.strftime('%d-%m-%Y')
        except ValueError:
            pass # Continue to other checks

    # Handle compact formats like "5h", "2d", "3w", "1mo"
    match_hours = re.search(r'^(\d+)h$', text)
    if match_hours:
        return today.strftime('%d-%m-%Y')

    match_days_compact = re.search(r'^(\d+)d$', text)
    if match_days_compact:
        days = int(match_days_compact.group(1))
        return (today - timedelta(days=days)).strftime('%d-%m-%Y')

    match_weeks_compact = re.search(r'^(\d+)w$', text)
    if match_weeks_compact:
        weeks = int(match_weeks_compact.group(1))
        return (today - timedelta(days=weeks*7)).strftime('%d-%m-%Y')

    match_months_compact = re.search(r'^(\d+)mo$', text)
    if match_months_compact:
        months = int(match_months_compact.group(1))
        return (today - timedelta(days=months*30)).strftime('%d-%m-%Y')

    # Handle longer formats
    if "just now" in text or "few seconds ago" in text or "an hour ago" in text or "hours ago" in text:
        return today.strftime('%d-%m-%Y')
    elif "today" in text:
        return today.strftime('%d-%m-%Y')
    elif "yesterday" in text:
        return (today - timedelta(days=1)).strftime('%d-%m-%Y')
    else:
        match_days = re.search(r'(\d+)\s+days?', text)
        if match_days:
            days = int(match_days.group(1))
            return (today - timedelta(days=days)).strftime('%d-%m-%Y')

        match_weeks = re.search(r'(\d+)\s+weeks?', text)
        if match_weeks:
            weeks = int(match_weeks.group(1))
            return (today - timedelta(days=weeks*7)).strftime('%d-%m-%Y')

        match_months = re.search(r'(\d+)\s+months?', text)
        if match_months:
            months = int(match_months.group(1))
            return (today - timedelta(days=months*30)).strftime('%d-%m-%Y')

        try:
            # Handle explicit written dates like "15 June 2025"
            for fmt in ['%d %b %Y', '%d %B %Y', '%b %d, %Y', '%B %d, %Y']:
                try:
                    return datetime.strptime(text, fmt).strftime('%d-%m-%Y')
                except ValueError:
                    pass
        except ValueError:
            pass

        return None

def is_startup_company(company_name, description="", company_link=""):
    """
    Checks if a company is likely a startup based on name, description, and predefined indicators.
    Excludes known large companies.
    """
    if not company_name: return False

    company_lower = company_name.lower()
    desc_lower = description.lower() if description else ""

    for large_company in EXCLUDE_LARGE_COMPANIES:
        if large_company in company_lower:
            return False

    for indicator in STARTUP_INDICATORS:
        if indicator in company_lower or indicator in desc_lower:
            return True

    startup_patterns = [
        r'\b(?:founded|established|started)\s+(?:in\s+)?20[1-2][0-9]\b',
        r'\b(?:series\s+[a-z]|seed|pre-seed|angel)\s+funding\b',
        r'\b(?:\d+)\s*(?:m|million|k|thousand)\s+raised\b',
        r'\bemployees?\s*:\s*(?:1-10|11-50|51-200|201-500|under\s+\d+)\b',
        r'\b(?:venture|vc|angel)\s+backed\b',
        r'\b(?:rapid|fast)\s+growth\b',
        r'\b(?:disrupt|revolutioniz|transform)ing\b',
        r'\bearly\s+stage\b',
        r'\bground\s+floor\b',
        r'\bwear\s+many\s+hats\b',
        r'\bfast-paced\s+environment\b'
    ]

    combined_text = f"{company_lower} {desc_lower}"
    for pattern in startup_patterns:
        if re.search(pattern, combined_text):
            return True

    size_indicators = ['small team', 'tight-knit', 'close-knit', 'growing team', 'small company']
    for indicator in size_indicators:
        if indicator in combined_text:
            return True

    return False

def extract_experience_from_description(description):
    """
    Extracts experience requirements from a job description string.
    """
    if not description: return ""

    experience_patterns = [
        r'(\d+)\+?\s*years?\s*(?:of\s*)?experience',
        r'(\d+)\+?\s*years?\s*in',
        r'minimum\s*(\d+)\s*years?',
        r'at\s*least\s*(\d+)\s*years?',
        r'(\d+)\+?\s*years?\s*relevant',
    ]

    desc_lower = description.lower()
    for pattern in experience_patterns:
        match = re.search(pattern, desc_lower)
        if match: return f"{match.group(1)}+ years"

    entry_level_keywords = ['entry level', 'fresher', 'graduate', 'junior', '0-1 years', 'no experience']
    for keyword in entry_level_keywords:
        if keyword in desc_lower: return "Entry level"

    return ""

def extract_detailed_location(description, location):
    """
    Extracts and formats detailed location information, including work type (Remote, Hybrid, On-site).
    Prioritizes Indian cities and uses 'Remote' or 'Hybrid' if keywords are found.
    """
    if not description: description = ""

    indian_cities_map = {
        'bangalore': ['bangalore', 'bengaluru', 'blr'], 'mumbai': ['mumbai', 'bombay', 'bom'],
        'delhi': ['delhi', 'new delhi', 'ncr', 'gurgaon', 'gurugram', 'noida', 'faridabad', 'ghaziabad'],
        'hyderabad': ['hyderabad', 'hyd', 'secunderabad'], 'pune': ['pune', 'pimpri-chinchwad'],
        'chennai': ['chennai', 'madras', 'maa'], 'kolkata': ['kolkata', 'calcutta', 'cal'],
        'ahmedabad': ['ahmedabad', 'amdavad'], 'surat': ['surat'], 'jaipur': ['jaipur'],
        'lucknow': ['lucknow'], 'kanpur': ['kanpur'], 'nagpur': ['nagpur'], 'indore': ['indore'],
        'thane': ['thane'], 'bhopal': ['bhopal'], 'visakhapatnam': ['visakhapatnam', 'vizag'],
        'pimpri': ['pimpri'], 'patna': ['patna'], 'vadodara': ['vadodara', 'baroda'],
        'ludhiana': ['ludhiana'], 'agra': ['agra'], 'nashik': ['nashik'], 'kochi': ['kochi', 'cochin'],
        'coimbatore': ['coimbatore'], 'thiruvananthapuram': ['thiruvananthapuram', 'trivandrum'],
        'goa': ['goa', 'panaji'], 'chandigarh': ['chandigarh'], 'dehradun': ['dehradun'],
        'mysore': ['mysore', 'mysuru'], 'guwahati': ['guwahati'], 'bhopal': ['bhopal'],
        'gandhinagar': ['gandhinagar'], 'kozhikode': ['kozhikode'], 'mangalore': ['mangalore'],
        'bhubaneswar': ['bhubaneswar'], 'ranchi': ['ranchi'], 'jamshedpur': ['jamshedpur'],
        'udaipur': ['udaipur'], 'varanasi': ['varanasi']
    }

    remote_keywords = ['remote', 'work from home', 'wfh', 'telecommute', 'distributed', 'anywhere', 'location independent']
    hybrid_keywords = ['hybrid', 'flexible', 'mix of remote', 'remote + office', 'work from office', 'wfo']

    desc_lower = description.lower()
    loc_lower = location.lower() if location else ""
    combined_text = f"{desc_lower} {loc_lower}"

    cities_found = []
    for city, variations in indian_cities_map.items():
        for variation in variations:
            if variation in combined_text:
                cities_found.append(city.title())
                break

    cities_found = list(dict.fromkeys(cities_found))

    work_type = ""
    if any(keyword in combined_text for keyword in remote_keywords):
        work_type = "Remote"
    elif any(keyword in combined_text for keyword in hybrid_keywords):
        work_type = "Hybrid"
    else:
        work_type = "On-site"

    if cities_found:
        city_str = ", ".join(cities_found[:3])
        if work_type == "Remote":
            return f"Remote (Based: {city_str})"
        elif work_type == "Hybrid":
            return f"Hybrid - {city_str}"
        else:
            return f"{city_str} (On-site)"
    else:
        if location:
            if work_type == "Remote":
                return f"Remote ({location})"
            elif work_type == "Hybrid":
                return f"Hybrid - {location}"
            else:
                return f"{location} (On-site)"
        else:
            return work_type if work_type else ""


# --- LINKEDIN SCRAPER CALL-BACKS AND FUNCTIONS ---

def on_linkedin_data(data: EventData):
    """Event handler for 'data' event from LinkedinScraper. Processes and filters job data."""
    global linkedin_jobs_data, linkedin_scraped_count

    try:
        if not is_startup_company(data.company, data.description, data.company_link):
            return

        experience = extract_experience_from_description(data.description)
        detailed_location = extract_detailed_location(data.description, data.location)
        posted_date_str = convert_human_readable_date_to_dd_mm_yyyy(data.date)

        job_data = {
            'Company': data.company or '',
            'Role': data.title or '',
            'Location': detailed_location,
            'Experience': experience,
            'Posted Date': posted_date_str,
        }

        linkedin_jobs_data.append(job_data)
        linkedin_scraped_count += 1

        print(f'[LinkedIn] [STARTUP {linkedin_scraped_count}] {data.title} at {data.company}')

    except Exception as e:
        print(f'[LinkedIn] [ERROR] Processing job data: {e}')

def on_linkedin_metrics(metrics: EventMetrics):
    print(f'[LinkedIn] [METRICS] Processed {metrics.processed} jobs, Failed: {metrics.failed}')

def on_linkedin_error(error):
    print(f'[LinkedIn] [ERROR] Scraper reported an error: {error}')

def on_linkedin_end():
    print(f'[LinkedIn] [END] Scraping completed for this query batch.')

def create_linkedin_queries(indian_cities_list):
    """
    Generates LinkedIn search queries focused on startup roles within India and Remote.
    """
    startup_search_terms = [
        'startup', 'early stage company', 'seed stage', 'series a', 'series b', 'series c',
        'pre-series', 'venture backed', 'tech startup', 'fintech startup', 'edtech startup', 'healthtech startup',
        'agritech startup', 'saas startup', 'ai startup', 'blockchain startup', 'crypto startup',
        'deep tech startup', 'growth stage company', 'scale-up company', 'hyper-growth startup',
        'pre-seed startup', 'angel funded', 'fast-growing tech', 'innovative startup',
        'disruptive technology', 'emerging tech company'
    ]

    startup_roles = [
        'Full Stack Developer', 'Frontend Developer', 'Backend Developer', 'Product Manager',
        'Growth Hacker', 'Marketing Manager', 'Business Development', 'Data Scientist',
        'DevOps Engineer', 'UI/UX Designer', 'Sales Executive', 'Customer Success',
        'Content Writer', 'Social Media Manager', 'Founder Associate', 'Software Engineer',
        'Mobile Developer', 'Machine Learning Engineer', 'AI Engineer', 'Blockchain Developer',
        'Cybersecurity Specialist', 'Cloud Architect', 'Technical Writer', 'Scrum Master',
        'Growth Marketer', 'Operations Manager', 'Finance Analyst', 'HR Manager',
        'Data Analyst', 'Business Analyst', 'Operations Executive', 'Product Designer',
        'Digital Marketing Specialist', 'HR Business Partner', 'Financial Controller'
    ]

    general_terms = ['small company', 'growing company', 'early stage', 'venture capital startup', 'tech scale-up', 'innovative small business', 'fast growth tech']

    queries = []

    # Queries combining startup terms with Indian cities and Remote options
    for city in indian_cities_list:
        for term in startup_search_terms[:15]:
            queries.append(Query(
                query=f'{term} {city}',
                options=QueryOptions(
                    locations=[city, 'India', 'Remote'],
                    apply_link=True,
                    limit=50,
                    skip_promoted_jobs=True,
                    filters=QueryFilters(
                        relevance=RelevanceFilters.RECENT,
                        time=TimeFilters.MONTH,
                        type=[TypeFilters.FULL_TIME, TypeFilters.CONTRACT, TypeFilters.PART_TIME],
                        on_site_or_remote=[OnSiteOrRemoteFilters.REMOTE, OnSiteOrRemoteFilters.HYBRID, OnSiteOrRemoteFilters.ON_SITE]
                    )
                )
            ))

    # Queries combining startup roles with general Indian locations
    for role in startup_roles:
        queries.append(Query(
            query=f'{role} startup',
            options=QueryOptions(
                locations=['India', 'Remote'] + indian_cities_list[:10],
                apply_link=True,
                limit=50,
                skip_promoted_jobs=True,
                filters=QueryFilters(
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.CONTRACT]
                )
            )
        ))

    # General startup-related terms for India and Remote
    for term in general_terms:
        queries.append(Query(
            query=term,
            options=QueryOptions(
                locations=['India', 'Remote'],
                apply_link=True,
                limit=75,
                skip_promoted_jobs=True,
                filters=QueryFilters(
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.PART_TIME, TypeFilters.CONTRACT]
                )
            )
        ))

    random.shuffle(queries)
    return queries[:150]

def run_linkedin_query_with_retry(scraper, query, linkedin_max_retries, retry_count=0):
    """Runs a single LinkedIn query with retry logic."""
    try:
        scraper.run([query])
        time.sleep(random.uniform(2, 5))
        return True
    except Exception as e:
        print(f'[LinkedIn] [RETRY {retry_count + 1}/{linkedin_max_retries}] Error running query "{query.query[:50]}...": {e}')
        if retry_count < linkedin_max_retries - 1:
            time.sleep(random.uniform(5, 10))
            return run_linkedin_query_with_retry(scraper, query, linkedin_max_retries, retry_count + 1)
        else:
            print(f'[LinkedIn] [FAILED] Max retries reached for query "{query.query[:50]}..."')
            return False

def run_linkedin_scraper_main(linkedin_target_jobs, linkedin_max_workers, linkedin_slow_mo, linkedin_headless):
    """
    Orchestrates the LinkedIn job scraping process.
    Returns a DataFrame of collected jobs, does NOT write to Google Sheet directly.
    """
    global linkedin_jobs_data, linkedin_scraped_count

    print("\n" + "=" * 60)
    print("             STARTING LINKEDIN STARTUP JOBS SCRAPER")
    print("=" * 60)
    print(f"Target: Minimum {linkedin_target_jobs} startup jobs.")
    print("Filtering for startup companies in India/Remote only.")

    linkedin_jobs_data = [] # Reset global list for each run
    linkedin_scraped_count = 0

    try:
        scraper = LinkedinScraper(
            chrome_executable_path=None,
            chrome_binary_location=None,
            chrome_options=None,
            headless=linkedin_headless,
            max_workers=linkedin_max_workers,
            slow_mo=linkedin_slow_mo,
            page_load_timeout=90
        )

        scraper.on(Events.DATA, on_linkedin_data)
        scraper.on(Events.ERROR, on_linkedin_error)
        scraper.on(Events.END, on_linkedin_end)
        scraper.on(Events.METRICS, on_linkedin_metrics)

        queries = create_linkedin_queries(INDIAN_CITIES)
        print(f"[LinkedIn] Created {len(queries)} startup-focused queries for India/Remote.")

        linkedin_max_retries = 3
        for i, query in enumerate(queries, 1):
            if linkedin_scraped_count >= linkedin_target_jobs:
                print(f"[LinkedIn] Reached minimum target of {linkedin_target_jobs} startup jobs. Stopping further queries.")
                break

            print(f"[LinkedIn] Processing query {i}/{len(queries)}: {query.query[:50]}...")
            run_linkedin_query_with_retry(scraper, query, linkedin_max_retries)

        print("\n[LinkedIn] Scraping complete. Preparing data for normalization.")

        if linkedin_jobs_data:
            df = pd.DataFrame(linkedin_jobs_data)
            print(f"[LinkedIn] Total raw jobs collected: {len(df)}")
            return df
        else:
            print("[LinkedIn] No startup jobs found.")
            return pd.DataFrame()

    except Exception as e:
        print(f"[LinkedIn] Major error during LinkedIn job scraping: {e}")
        if linkedin_jobs_data:
            print("[LinkedIn] Attempting to return partial LinkedIn results due to error...")
            return pd.DataFrame(linkedin_jobs_data)
        return pd.DataFrame()


# --- IIMJOBS SCRAPER FUNCTIONS ---

def scrape_iimjobs_infinite_scroll(url, category_name, scroll_times=IIMJOBS_DEFAULT_SCROLL_TIMES, scroll_pause=2):
    jobs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        print(f"[IIMJobs] Opened: {url}")

        try:
            page.wait_for_selector('p[data-testid="job_title"]', timeout=15000)
            print("[IIMJobs] Initial job titles loaded.")
        except Exception as e:
            print(f"[IIMJobs] Timeout waiting for initial job titles on {url}. Error: {e}")
            browser.close()
            return []

        for i in range(scroll_times):
            print(f"[IIMJobs] Scrolling {i+1}/{scroll_times} for {category_name}...")
            page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
            time.sleep(scroll_pause)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        job_cards = soup.select('div.MuiPaper-root:has(p[data-testid="job_title"])')
        print(f"[IIMJobs] Total job cards found for {category_name} after scrolling: {len(job_cards)}")

        for card in job_cards:
            title_tag = card.select_one('p[data-testid="job_title"]')
            full_title = title_tag.get_text(strip=True) if title_tag else ''

            company, role = "", ""
            if " - " in full_title:
                parts = full_title.split(" - ", 1)
                if len(parts) >= 2:
                    company = parts[0].strip()
                    role = parts[1].strip()
                else:
                    role = full_title.strip()
            else:
                role = full_title.strip()

            exp_tag = card.select_one('span[data-testid="job_experience"]')
            experience = exp_tag.get_text(strip=True) if exp_tag else ''

            loc_tag = card.select_one('p[data-testid="job_location"]')
            location = loc_tag.get_text(strip=True) if loc_tag else ''
            
            posted_date_tag = card.select_one('span[data-testid="date_posted"]')
            posted_date_text = posted_date_tag.get_text(strip=True) if posted_date_tag else ''
            posted_date = convert_human_readable_date_to_dd_mm_yyyy(posted_date_text)

            jobs.append({
                'Company': company,
                'Role': role,
                'Experience': experience,
                'Location': location,
                'Posted Date': posted_date,
                'Category': category_name
            })
        browser.close()
    return jobs

# --- DATA NORMALIZATION FUNCTIONS ---

def normalize_linkedin_data(linkedin_df_raw):
    """Normalizes raw LinkedIn data to the unified schema."""
    if linkedin_df_raw.empty:
        return pd.DataFrame(columns=['Company', 'Role', 'Location', 'Experience', 'Posted Date', 'Source Portal'])

    df = linkedin_df_raw.copy()
    
    df = df.fillna('')

    required_cols = ['Company', 'Role', 'Location', 'Experience', 'Posted Date']
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''

    df['Posted Date'] = pd.to_datetime(df['Posted Date'], dayfirst=True, errors='coerce').dt.strftime('%d-%m-%Y').fillna('')
    df['Source Portal'] = 'LinkedIn'

    initial_count = len(df)
    df.drop_duplicates(subset=['Company', 'Role', 'Location', 'Experience'], inplace=True)
    print(f"[LinkedIn Normalization] Removed {initial_count - len(df)} duplicates.")

    return df[['Company', 'Role', 'Location', 'Experience', 'Posted Date', 'Source Portal']]

def normalize_iimjobs_data(scroll_times=5):
    """Scrapes and normalizes IIMJobs data to the unified schema."""
    iim_categories = [
        ("https://www.iimjobs.com/c/banking-finance-jobs", "Banking"),
        ("https://www.iimjobs.com/c/sales-marketing-jobs", "Sales"),
        ("https://www.iimjobs.com/c/consulting-general-mgmt-jobs", "Consulting"),
        ("https://www.iimjobs.com/c/hr-ir-jobs", "HR"),
        ("https://www.iimjobs.com/c/it-systems-jobs", "IT"),
        ("https://www.iimjobs.com/c/scm-operations-jobs", "SCM Operations"),
        ("https://www.iimjobs.com/c/legal-jobs", "Legal"),
        ("https://www.iimjobs.com/c/bpo-jobs", "BPO")
    ]
    all_iim_jobs_raw = []
    for url, category in iim_categories:
        print(f"[IIMJobs Normalization] Scraping category: {category}")
        jobs = scrape_iimjobs_infinite_scroll(url, category, scroll_times=scroll_times, scroll_pause=1)
        all_iim_jobs_raw.extend(jobs)

    df = pd.DataFrame(all_iim_jobs_raw)
    if df.empty:
        return pd.DataFrame(columns=['Company', 'Role', 'Location', 'Experience', 'Posted Date', 'Source Portal'])
    
    df = df.fillna('')

    df['Posted Date'] = pd.to_datetime(df['Posted Date'], dayfirst=True, errors='coerce').dt.strftime('%d-%m-%Y').fillna('')
    df['Source Portal'] = 'IIMJobs'
    
    initial_count = len(df)
    df.drop_duplicates(subset=['Company', 'Role', 'Location', 'Experience'], inplace=True)
    print(f"[IIMJobs Normalization] Removed {initial_count - len(df)} duplicates.")

    return df[['Company', 'Role', 'Location', 'Experience', 'Posted Date', 'Source Portal']]


# --- MAIN ORCHESTRATION BLOCK ---

# def main():
#     gc = get_google_sheet_client()
#     if not gc:
#         print("Google Sheets authentication failed. Exiting.")
#         return

#     print("\n" + "#" * 70)
#     print("                 STARTING COMPREHENSIVE JOB SCRAPER")
#     print("                 LinkedIn, IIMJobs Integration")
#     print("#" * 70)

#     all_scraped_dataframes = []

#     # 1. Scrape LinkedIn Jobs
#     print("\n[STEP 1/2] Initiating LinkedIn job scraping and normalization...")
#     linkedin_df_raw = run_linkedin_scraper_main(
#         linkedin_target_jobs=LINKEDIN_DEFAULT_TARGET_JOBS,
#         linkedin_max_workers=2,
#         linkedin_slow_mo=1.5,
#         linkedin_headless=True
#     )
#     df_linkedin_normalized = normalize_linkedin_data(linkedin_df_raw)
#     if not df_linkedin_normalized.empty:
#         all_scraped_dataframes.append(df_linkedin_normalized)
#         print(f"[Main] LinkedIn: Collected {len(df_linkedin_normalized)} unique startup jobs.")
#     else:
#         print("[Main] LinkedIn: No data collected.")


#     # 2. Scrape IIMJobs
#     print("\n[STEP 2/2] Initiating IIMJobs scraping and normalization...")
#     df_iim_normalized = normalize_iimjobs_data()
#     if not df_iim_normalized.empty:
#         all_scraped_dataframes.append(df_iim_normalized)
#         print(f"[Main] IIMJobs: Collected {len(df_iim_normalized)} unique jobs.")
#     else:
#         print("[Main] IIMJobs: No data collected.")


#     # 3. Combine all data and save to a unified sheet
#     print("\n[FINAL STEP] Consolidating and saving all jobs to unified sheet...")
#     if all_scraped_dataframes:
#         combined_df = pd.concat(all_scraped_dataframes, ignore_index=True)
#         print(f"Total raw combined records (before final deduplication): {len(combined_df)}")

#         initial_combined_count = len(combined_df)
#         combined_df.drop_duplicates(subset=['Company', 'Role', 'Location', 'Experience'], inplace=True)
#         final_combined_count = len(combined_df)
#         print(f"Removed {initial_combined_count - final_combined_count} cross-source duplicates.")

#         # Ensure all necessary columns are defined and ordered correctly
#         final_columns = ['Company', 'Role', 'Location', 'Experience', 'Posted Date', 'Source Portal']
#         combined_df = combined_df[final_columns]

#         # Define custom sort order for 'Source Portal'
#         source_order = ['LinkedIn', 'IIMJobs']
#         combined_df['Source Portal'] = pd.Categorical(combined_df['Source Portal'], categories=source_order, ordered=True)

#         # Write the final combined, ordered, and deduplicated data to the unified sheet
#         write_unified_data_to_sheet(gc, GOOGLE_SHEET_NAME, FINAL_UNIFIED_SHEET, combined_df)
        
#     else:
#         print("\nNo job data collected from any source. Nothing to save to unified sheet.")

#     print("\n" + "=" * 70)
#     print("                 JOB SCRAPING PROCESS COMPLETED")
#     print("=" * 70)


# if __name__ == "__main__":
#     main()


# (Keep all your existing scraping code from all.py, from the top
# down to the end of the normalization functions)
# ... imports, helper functions, scraper functions, normalization functions ...

def run_full_scrape(linkedin_limit, iimjobs_limit):
    """
    Runs the full scraping process for all job portals and returns a combined DataFrame.
    """
    all_scraped_dataframes = []

    # 1. Scrape LinkedIn Jobs
    print(f"Initiating LinkedIn scraping for {linkedin_limit} jobs...")
    linkedin_df_raw = run_linkedin_scraper_main(
        linkedin_target_jobs=linkedin_limit,
        linkedin_max_workers=2,
        linkedin_slow_mo=1.5,
        linkedin_headless=True
    )
    df_linkedin_normalized = normalize_linkedin_data(linkedin_df_raw)
    if not df_linkedin_normalized.empty:
        all_scraped_dataframes.append(df_linkedin_normalized)

    # 2. Scrape IIMJobs
    print(f"Initiating IIMJobs scraping for {iimjobs_limit} scrolls...")
    # Modify normalize_iimjobs_data to accept the limit
    df_iim_normalized = normalize_iimjobs_data(scroll_times=iimjobs_limit)
    if not df_iim_normalized.empty:
        all_scraped_dataframes.append(df_iim_normalized)

    if not all_scraped_dataframes:
        return pd.DataFrame()

    # Combine and return the final DataFrame
    combined_df = pd.concat(all_scraped_dataframes, ignore_index=True)
    combined_df.drop_duplicates(subset=['Company', 'Role', 'Location', 'Experience'], inplace=True)
    
    return combined_df

# You will also need to slightly modify normalize_iimjobs_data to accept a parameter
# In scraper.py, change the function definition:
# def normalize_iimjobs_data(): -> def normalize_iimjobs_data(scroll_times=5):
# And inside that function, change the call to scrape_iimjobs_infinite_scroll:
# jobs = scrape_iimjobs_infinite_scroll(url, category, scroll_times=scroll_times, ...)