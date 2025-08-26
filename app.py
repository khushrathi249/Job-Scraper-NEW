import streamlit as st
import pandas as pd
import database as db
import scraper as sc
import asyncio
import sys

# --- FIX for Playwright/Asyncio error on Windows ---
# This must be at the very top of your script
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- Page Configuration ---
st.set_page_config(page_title="Job Scraper Dashboard", layout="wide")

# --- Database Initialization ---
conn = db.create_connection()
if conn:
    db.create_table(conn)
else:
    st.error("Could not connect to the database. Please check the db file.")

# --- UI Layout ---
st.title("👨‍💻 Startup Job Search")
st.write("Search for startup jobs from the database or launch a new scrape.")

# --- Feature 1: Search ---
st.header("Search Jobs in Database")
col1, col2 = st.columns([2, 2])
with col1:
    search_role = st.text_input("Role (e.g., Product Manager)")
with col2:
    search_location = st.text_input("Location (e.g., Bangalore)")

if st.button("Search"):
    if not search_role and not search_location:
        st.warning("Please enter a role or location to search.")
    else:
        with st.spinner("Searching database..."):
            results_df = db.search_jobs(conn, search_role, search_location)

        st.subheader(f"{len(results_df)} jobs found")
        st.dataframe(results_df, use_container_width=True)

# --- Sidebar for Database Updates ---
st.sidebar.header("Database Maintenance")

with st.sidebar.expander("Update Database Manually"):
    st.write("Scrape LinkedIn and IIMJobs to find the latest jobs and add them to the database.")

    li_limit = st.number_input("LinkedIn Jobs to Target", min_value=10, max_value=500, value=50, step=10)
    iim_limit = st.number_input("IIMJobs Pages to Scroll", min_value=1, max_value=20, value=2, step=1)

    if st.button("🚀 Start Scraping and Update DB"):
        with st.spinner("Scraping in progress... This may take several minutes."):
            # Run the full scraper
            scraped_df = sc.run_full_scrape(linkedin_limit=li_limit, iimjobs_limit=iim_limit)

            if scraped_df is not None and not scraped_df.empty:
                st.write(f"Scraped {len(scraped_df)} new unique jobs. Adding to database...")
                # Add to database
                db.add_jobs_df(conn, scraped_df)
                st.success("Database updated successfully!")
                st.experimental_rerun() # Auto-refresh the page to allow a new search
            else:
                st.warning("No new jobs were found in this scrape.")

# The connection is managed within the functions that use it.
# No need to close it here if the app is intended to stay running.