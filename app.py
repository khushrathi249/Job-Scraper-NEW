import streamlit as st
import database as db
import scraper as sc
import asyncio
import sys

# --- FIX for Playwright/Asyncio error on Windows ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

st.set_page_config(page_title="Job Scraper Dashboard", layout="wide")

# --- Database ---
conn = db.create_connection()
if conn:
    db.create_table(conn)
else:
    st.error("Database connection failed.")
    st.stop()

# --- UI ---
st.title("👨‍💻 Startup Job Search")

# --- Search ---
st.header("Search Jobs in Database")
col1, col2 = st.columns(2)
search_role = col1.text_input("Role")
search_location = col2.text_input("Location")

if st.button("Search"):
    with st.spinner("Searching..."):
        results_df = db.search_jobs(conn, search_role, search_location)
    st.subheader(f"{len(results_df)} jobs found")
    st.dataframe(results_df, use_container_width=True)

# --- Sidebar ---
st.sidebar.header("Database Maintenance")
li_limit = st.sidebar.number_input("LinkedIn Jobs to Scrape", 10, 200, 20)
iim_limit = st.sidebar.number_input("IIMJobs Pages to Scroll", 1, 10, 1)

if st.sidebar.button("🚀 Update Database"):
    with st.spinner("Scraping new jobs... This may take a few minutes."):
        scraped_df = sc.run_full_scrape(linkedin_limit=li_limit, iimjobs_limit=iim_limit)
    
    if not scraped_df.empty:
        st.sidebar.write(f"Found {len(scraped_df)} new jobs. Adding to DB...")
        db.add_jobs_df(conn, scraped_df)
        st.sidebar.success("Database updated!")
        st.rerun()
    else:
        st.sidebar.warning("No new jobs found.")
