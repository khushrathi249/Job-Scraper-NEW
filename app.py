import streamlit as st
import pandas as pd
import database as db
import scraper as sc
import asyncio
import sys
import io
import time
from datetime import datetime

# This is only needed for running locally on Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

st.set_page_config(page_title="Job Scraper Dashboard", layout="wide")
st.title("🚀 Startup Job Search")

# --- Initial Setup ---
# Connect to Google Sheets once at the start of the app
worksheet = db.connect_to_gsheet()

# Stop the app if connection fails
if worksheet is None:
    st.error("Failed to connect to Google Sheets. Please check your credentials and sheet name in st.secrets.")
    st.stop()

# --- Main Page Search UI ---
st.header("Search Jobs in Database")
col1, col2 = st.columns(2)
search_role = col1.text_input("Role", placeholder="e.g., Product Manager").strip()
search_location = col2.text_input("Location", placeholder="e.g., Bangalore, Remote").strip()

filter_by_date = st.checkbox("Filter by Posted Date")
start_date, end_date = None, None
if filter_by_date:
    d_col1, d_col2 = st.columns(2)
    start_date = d_col1.date_input("Start date", datetime.today())
    end_date = d_col2.date_input("End date", datetime.today())
    st.caption("To search for a single day, set the start and end dates to be the same.")

# --- Search Execution ---
if st.button("Search", key="search_button", type="primary"):
    start_datetime = datetime.combine(start_date, datetime.min.time()) if start_date else None
    end_datetime = datetime.combine(end_date, datetime.max.time()) if end_date else None
    
    with st.spinner("Searching..."):
        results_df = db.search_jobs(
            worksheet,
            role=search_role, 
            location=search_location,
            start_date=start_datetime,
            end_date=end_datetime
        )
    
    st.subheader(f"{len(results_df)} jobs found for your search")
    
    if not results_df.empty:
        st.dataframe(results_df, use_container_width=True, hide_index=True)
    else:
        st.info("No jobs found in the database matching your criteria.")
        
        if search_role or search_location:
            st.write("")
            st.markdown("---")
            st.subheader("Scrape for New Jobs")
            apply_startup_filter = st.checkbox("Only show jobs from startups", value=True, key="filter_checkbox")
            targeted_limit = st.number_input("Number of jobs to scrape", 10, 200, 25, 5, key="targeted_scrape_limit")
            scrape_prompt = f"Scrape for '{search_role or 'any role'}' in '{search_location or 'any location'}'?"
            if st.button(scrape_prompt, key="scrape_now_button"):
                with st.spinner(f"Performing a targeted scrape... this may take a moment."):
                    targeted_df = sc.scrape_targeted_jobs(role=search_role, location=search_location, limit=targeted_limit, apply_filter=apply_startup_filter)
                if not targeted_df.empty:
                    st.write(f"Found {len(targeted_df)} jobs. Adding to spreadsheet...")
                    num_added = db.add_jobs_df(worksheet, targeted_df)
                    st.success(f"Scrape complete! Added {num_added} new jobs. Rerunning search...")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.warning("The targeted scrape did not find any new startup jobs.")

# --- Sidebar ---
st.sidebar.header("Database Maintenance")

with st.sidebar.expander("Update Database (Broad Scrape)", expanded=True):
    li_limit = st.number_input("LinkedIn Jobs to Scrape", 10, 200, 50, step=10, key="broad_li_limit")
    iim_limit = st.number_input("IIMJobs Pages to Scroll", 1, 10, 2, key="broad_iim_limit")

    if st.button("▶️ Start Broad Scrape"):
        progress_bar = st.sidebar.progress(0, text="Starting broad scrape...")
        scraped_df = sc.run_full_scrape(linkedin_limit=li_limit, iimjobs_limit=iim_limit)
        progress_bar.progress(100, text="Scrape complete! Saving to spreadsheet...")
        
        if scraped_df is not None and not scraped_df.empty:
            num_added = db.add_jobs_df(worksheet, scraped_df)
            if num_added > 0:
                st.sidebar.success(f"Success! Added {num_added} new jobs.")
            else:
                st.sidebar.info("No new jobs found. Database is already up to date.")
            time.sleep(3)
            st.rerun()
        else:
            st.sidebar.warning("No jobs were found in the scrape.")
        progress_bar.empty()

with st.sidebar.expander("Download Database"):
    st.write("Download the entire job database as an Excel file.")
    all_jobs_df = db.get_all_jobs_df(worksheet)
    if not all_jobs_df.empty:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            all_jobs_df.to_excel(writer, index=False, sheet_name='Jobs')
        excel_data = output.getvalue()
        st.download_button(
            label="📄 Download as Excel", 
            data=excel_data,
            file_name="job_database.xlsx"
        )
    else:
        st.warning("Database is empty.")

with st.sidebar.expander("Upload to Database"):
    st.write("Upload an Excel or CSV file with new job entries.")
    uploaded_file = st.file_uploader("Choose a file", type=['xlsx', 'xls', 'csv'])
    if uploaded_file:
        try:
            file_extension = uploaded_file.name.split('.')[-1].lower()
            if file_extension == 'csv':
                upload_df = pd.read_csv(uploaded_file)
            else:
                upload_df = pd.read_excel(uploaded_file)

            num_added = db.add_jobs_df(worksheet, upload_df)
            st.success(f"File processed! Added {num_added} new job entries.")
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"An error occurred while processing the file: {e}")
