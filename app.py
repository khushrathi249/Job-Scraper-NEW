import streamlit as st
import pandas as pd
import database as db
import scraper as sc
import asyncio
import sys
import io
import time
from setup import setup_playwright

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

setup_successful = setup_playwright()

st.set_page_config(page_title="Job Scraper Dashboard", layout="wide")
st.title("👨‍💻 Startup Job Search")

if not setup_successful:
    st.warning("Scraping functionality is disabled until the setup issue is resolved.")
    st.stop()

# --- Search ---
st.header("Search Jobs in Database")
col1, col2 = st.columns(2)
search_role = col1.text_input("Role").strip()
search_location = col2.text_input("Location").strip()

if st.button("Search", key="search_button"):
    with st.spinner("Searching..."):
        results_df = db.search_jobs(role=search_role, location=search_location)
    
    st.subheader(f"{len(results_df)} jobs found for your search")
    
    if not results_df.empty:
        st.dataframe(results_df, use_container_width=True)
    else:
        st.info("No jobs found in the database matching your criteria.")
        
        if search_role or search_location:
            st.write("")
            
            apply_startup_filter = st.checkbox(
                "Only show jobs from startups", 
                value=True, 
                key="filter_checkbox"
            )

            targeted_limit = st.number_input(
                "Number of jobs to scrape", 
                min_value=10, max_value=200, value=25, step=5,
                key="targeted_scrape_limit"
            )

            scrape_prompt = f"Scrape for '{search_role or 'any role'}' in '{search_location or 'any location'}'?"
            if st.button(scrape_prompt, key="scrape_now_button"):
                with st.spinner(f"Performing a targeted scrape... This may take a moment."):
                    targeted_df = sc.scrape_targeted_jobs(
                        role=search_role, 
                        location=search_location,
                        limit=targeted_limit,
                        apply_filter=apply_startup_filter
                    )
                
                if not targeted_df.empty:
                    st.write(f"Found {len(targeted_df)} new jobs. Adding to database...")
                    db.add_jobs_df(targeted_df)
                    st.success("Scrape complete! Your search results will now be updated.")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.warning("The targeted scrape did not find any new jobs for this search.")

# --- Sidebar ---
st.sidebar.header("Database Maintenance")
with st.sidebar.expander("Update Database (Broad Scrape)"):
    li_limit = st.sidebar.number_input("LinkedIn Jobs to Scrape", 10, 200, 50, step=10, key="broad_li_limit")
    iim_limit = st.sidebar.number_input("IIMJobs Pages to Scroll", 1, 10, 2, key="broad_iim_limit")

    if st.sidebar.button("🚀 Start Broad Scrape"):
        with st.spinner("Scraping new jobs... This may take a few minutes."):
            scraped_df = sc.run_full_scrape(linkedin_limit=li_limit, iimjobs_limit=iim_limit)
        
        if scraped_df is not None and not scraped_df.empty:
            st.sidebar.write(f"Found {len(scraped_df)} jobs. Adding to DB...")
            db.add_jobs_df(scraped_df)
            st.sidebar.success("Database updated!")
            st.rerun()
        else:
            st.sidebar.warning("No new jobs found in the broad scrape.")

with st.sidebar.expander("Download Database"):
    st.write("Download the entire job database as an Excel file.")
    with st.spinner("Preparing download..."):
        all_jobs_df = db.get_all_jobs()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            all_jobs_df.to_excel(writer, index=False, sheet_name='Jobs')
        excel_data = output.getvalue()
    st.download_button(
        label="📥 Download Excel File", data=excel_data,
        file_name="job_database.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with st.sidebar.expander("Upload to Database"):
    st.write("Upload an Excel or CSV file with new job entries.")
    uploaded_file = st.file_uploader("Choose a file", type=['xlsx', 'xls', 'csv'])

    if uploaded_file is not None:
        try:
            file_extension = uploaded_file.name.split('.')[-1].lower()
            if file_extension == 'csv':
                upload_df = pd.read_csv(uploaded_file)
            else:
                upload_df = pd.read_excel(uploaded_file)

            upload_df = upload_df.fillna('')
            
            if 'id' in upload_df.columns:
                upload_df = upload_df.drop(columns=['id'])

            upload_df.columns = [col.lower().replace(' ', '') for col in upload_df.columns]
            
            required_cols = {'company', 'role', 'location'}
            if not required_cols.issubset(upload_df.columns):
                st.error(f"Upload failed. File must contain at least: {', '.join(required_cols)}")
            else:
                existing_jobs_df = db.get_all_jobs_raw()
                if not existing_jobs_df.empty:
                    upload_df['unique_id'] = upload_df['company'].astype(str) + upload_df['role'].astype(str) + upload_df['location'].astype(str)
                    existing_jobs_df['unique_id'] = existing_jobs_df['company'].astype(str) + existing_jobs_df['role'].astype(str) + existing_jobs_df['location'].astype(str)
                    new_jobs_df = upload_df[~upload_df['unique_id'].isin(existing_jobs_df['unique_id'])].drop(columns=['unique_id'])
                else:
                    new_jobs_df = upload_df
                
                num_new_jobs = len(new_jobs_df)
                num_duplicates = len(upload_df) - num_new_jobs
                st.info(f"Found **{num_new_jobs}** new jobs. Ignored **{num_duplicates}** duplicates.")

                if num_new_jobs > 0 and st.button(f"Add {num_new_jobs} New Jobs to Database"):
                    db.add_jobs_df(new_jobs_df)
                    st.success("Successfully added jobs!")
                    st.rerun()
        except Exception as e:
            st.error(f"An error occurred: {e}")

