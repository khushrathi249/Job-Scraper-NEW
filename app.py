# app.py
import streamlit as st
import pandas as pd
import database as db
import scraper as sc
import asyncio
import sys
import io
from setup import setup_playwright

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

setup_successful = setup_playwright()

st.set_page_config(page_title="Job Scraper Dashboard", layout="wide")

st.title("👨‍💻 Startup Job Search")

if not setup_successful:
    st.warning("Scraping functionality is disabled until the setup issue is resolved.")
    st.stop()

st.header("Search Jobs in Database")
col1, col2 = st.columns(2)
search_role = col1.text_input("Role")
search_location = col2.text_input("Location")

if st.button("Search"):
    with st.spinner("Searching..."):
        results_df = db.search_jobs(role=search_role, location=search_location)
    st.subheader(f"{len(results_df)} jobs found")
    st.dataframe(results_df, use_container_width=True)

st.sidebar.header("Database Maintenance")
with st.sidebar.expander("Scrape New Jobs"):
    li_limit = st.number_input("LinkedIn Jobs to Scrape", min_value=10, max_value=500, value=50, step=10)
    iim_limit = st.number_input("IIMJobs Pages to Scroll", min_value=1, max_value=50, value=5, step=1)

    if st.button("🚀 Start Scraping"):
        with st.spinner("Scraping new jobs... This may take a few minutes."):
            scraped_df = sc.run_full_scrape(linkedin_limit=li_limit, iimjobs_limit=iim_limit)
        
        if scraped_df is not None and not scraped_df.empty:
            st.sidebar.write(f"Found {len(scraped_df)} new jobs. Adding to DB...")
            db.add_jobs_df(scraped_df)
            st.sidebar.success("Database updated!")
            st.rerun()
        else:
            st.sidebar.warning("No new jobs found.")

with st.sidebar.expander("Download Database"):
    st.write("Download the entire job database as an Excel file.")
    with st.spinner("Preparing download..."):
        all_jobs_df = db.get_all_jobs()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            all_jobs_df.to_excel(writer, index=False, sheet_name='Jobs')
        excel_data = output.getvalue()

    st.download_button(
        label="📥 Download Excel File",
        data=excel_data,
        file_name="job_database.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.sidebar.expander("Upload to Database"):
    st.write("Upload an Excel file with new job entries.")
    uploaded_file = st.file_uploader("Choose an Excel file", type=['xlsx', 'xls'])

    if uploaded_file is not None:
        try:
            upload_df = pd.read_excel(uploaded_file)
            st.write("Preview of uploaded data:")
            st.dataframe(upload_df.head())
            required_cols = {'Company', 'Role', 'Location'}
            if not required_cols.issubset(upload_df.columns):
                st.error(f"Upload failed. File must contain at least: {', '.join(required_cols)}")
            else:
                if st.button("Add Uploaded Jobs to Database"):
                    with st.spinner("Adding jobs..."):
                        db.add_jobs_df(upload_df)
                    st.success("Successfully added jobs from file!")
                    st.rerun()
        except Exception as e:
            st.error(f"An error occurred: {e}")
