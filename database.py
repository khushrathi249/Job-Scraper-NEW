import streamlit as st
import pandas as pd
from supabase import create_client, Client

# Initialize connection to Supabase
try:
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(supabase_url, supabase_key)
except Exception as e:
    st.error("Failed to connect to Supabase. Please check your secrets.toml file and Supabase credentials.")
    st.stop()


def add_jobs_df(df):
    """Adds a DataFrame of jobs to the Supabase 'jobs' table."""
    # Convert DataFrame to a list of dictionaries
    records = df.to_dict(orient='records')
    
    # Rename columns to match Supabase (snake_case is conventional but not required if table is CamelCase)
    for record in records:
        record['PostedDate'] = record.pop('Posted Date', None)
        record['SourcePortal'] = record.pop('Source Portal', None)

    try:
        # Upsert inserts new rows and ignores duplicates based on the UNIQUE constraint
        supabase.table('jobs').upsert(records, on_conflict='Company, Role, Location').execute()
        print(f"Database update complete. Processed {len(records)} records.")
    except Exception as e:
        print(f"Error adding jobs to Supabase: {e}")
        st.error(f"An error occurred while saving jobs: {e}")


def search_jobs(role, location):
    """Searches for jobs by role and location in Supabase."""
    try:
        query = supabase.table('jobs').select('*')
        if role:
            query = query.ilike('Role', f'%{role}%')
        if location:
            query = query.ilike('Location', f'%{location}%')
        
        data = query.execute().data
        df = pd.DataFrame(data)
        # Rename columns for display
        df.rename(columns={'PostedDate': 'Posted Date', 'SourcePortal': 'Source Portal'}, inplace=True)
        return df
    except Exception as e:
        st.error(f"An error occurred while searching: {e}")
        return pd.DataFrame()


def get_all_jobs():
    """Fetches all jobs from the Supabase database."""
    try:
        data = supabase.table('jobs').select('*').execute().data
        df = pd.DataFrame(data)
        df.rename(columns={'PostedDate': 'Posted Date', 'SourcePortal': 'Source Portal'}, inplace=True)
        return df
    except Exception as e:
        st.error(f"An error occurred while fetching all jobs: {e}")
        return pd.DataFrame()
