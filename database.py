import streamlit as st
import pandas as pd
from supabase import create_client, Client

try:
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error("Failed to connect to Supabase. Please check your secrets.toml.")
    st.stop()

def add_jobs_df(df):
    """Adds a DataFrame of jobs to the Supabase 'jobs' table."""
    df.columns = [col.lower().replace(' ', '') for col in df.columns]
    records = df.to_dict(orient='records')
    try:
        supabase.table('jobs').upsert(records, on_conflict='company, role, location').execute()
        print(f"Database upsert complete for {len(records)} records.")
    except Exception as e:
        print(f"Error adding jobs to Supabase: {e}")
        st.error(f"An error occurred while saving jobs: {e}")

def format_columns_for_display(df):
    """Formats DataFrame columns from lowercase to title case for display."""
    df.columns = [col.replace('posteddate', 'Posted Date').replace('sourceportal', 'Source Portal').title() for col in df.columns]
    return df

def get_all_jobs_raw():
    """Fetches all jobs with raw lowercase column names for internal checks."""
    try:
        data = supabase.table('jobs').select('company, role, location').execute().data
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred while fetching raw jobs data: {e}")
        return pd.DataFrame()

def search_jobs(role, location, start_date=None, end_date=None):
    """Searches for jobs by role, location, and optionally filters by date range."""
    try:
        query = supabase.table('jobs').select('*')
        if role:
            query = query.ilike('role', f'%{role}%')
        if location:
            query = query.ilike('location', f'%{location}%')
        
        data = query.execute().data
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Date filtering logic
        if start_date and end_date:
            # Convert posteddate column to datetime objects for comparison
            # errors='coerce' will turn any unparseable dates into NaT (Not a Time)
            df['posteddate_dt'] = pd.to_datetime(df['posteddate'], format='%d-%m-%Y', errors='coerce')
            
            # Filter the DataFrame
            mask = (df['posteddate_dt'] >= start_date) & (df['posteddate_dt'] <= end_date)
            df = df.loc[mask]
            
            # Drop the temporary datetime column
            df = df.drop(columns=['posteddate_dt'])

        return format_columns_for_display(df)
    except Exception as e:
        st.error(f"An error occurred while searching: {e}")
        return pd.DataFrame()

def get_all_jobs():
    """Fetches all jobs from the Supabase database and formats for display."""
    try:
        data = supabase.table('jobs').select('*').execute().data
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        return format_columns_for_display(df)
    except Exception as e:
        st.error(f"An error occurred while fetching all jobs: {e}")
        return pd.DataFrame()
