import streamlit as st
import pandas as pd
from supabase import create_client, Client

try:
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error("Failed to connect to Supabase. Check secrets.toml.")
    st.stop()

def add_jobs_df(df):
    """
    Adds a DataFrame to the Supabase 'jobs' table and returns the count of new rows.
    """
    # Standardize column names to lowercase for Supabase
    df.columns = [col.lower().replace(' ', '') for col in df.columns]
    
    # Ensure required columns exist, even if empty, to prevent errors
    for col in ['company', 'role', 'location', 'experience', 'posteddate', 'sourceportal']:
        if col not in df.columns:
            df[col] = ''

    records = df.to_dict(orient='records')
    
    try:
        # The 'upsert' command correctly ignores duplicates and returns the new data.
        result = supabase.table('jobs').upsert(records, on_conflict='company, role, location').execute()
        # Return the number of rows that were actually inserted.
        return len(result.data)
    except Exception as e:
        st.error(f"An error occurred while saving jobs: {e}")
        return 0

def search_jobs(role, location, start_date=None, end_date=None):
    """Searches jobs with efficient date filtering in the database."""
    try:
        query = supabase.table('jobs').select('*').order('posteddate', desc=True)
        if role:
            query = query.ilike('role', f'%{role}%')
        if location:
            query = query.ilike('location', f'%{location}%')
        if start_date:
            query = query.gte('posteddate', start_date.strftime('%Y-%m-%d'))
        if end_date:
            query = query.lte('posteddate', end_date.strftime('%Y-%m-%d'))
        
        data = query.execute().data
        df = pd.DataFrame(data) if data else pd.DataFrame()
        return format_columns_for_display(df)
    except Exception as e:
        st.error(f"An error occurred while searching: {e}")
        return pd.DataFrame()

def format_columns_for_display(df):
    """Formats DataFrame columns from lowercase to title case for display."""
    if df.empty: return df
    # Rename columns for better readability in the UI
    rename_map = {
        'id': 'ID',
        'company': 'Company',
        'role': 'Role',
        'location': 'Location',
        'experience': 'Experience',
        'posteddate': 'Posted Date',
        'sourceportal': 'Source Portal'
    }
    df = df.rename(columns=rename_map)
    return df

def get_all_jobs_raw():
    """Fetches all jobs with raw lowercase column names for internal checks."""
    try:
        data = supabase.table('jobs').select('company, role, location').execute().data
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred while fetching raw data: {e}")
        return pd.DataFrame()

def get_all_jobs():
    """Fetches all jobs from the Supabase database and formats for display."""
    try:
        data = supabase.table('jobs').select('*').execute().data
        df = pd.DataFrame(data) if data else pd.DataFrame()
        return format_columns_for_display(df)
    except Exception as e:
        st.error(f"An error occurred while fetching jobs: {e}")
        return pd.DataFrame()
