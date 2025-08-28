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
    df.columns = [col.lower().replace(' ', '') for col in df.columns]
    records = df.to_dict(orient='records')
    try:
        # The 'upsert' command correctly ignores duplicates and returns the new data.
        result = supabase.table('jobs').upsert(records, on_conflict='company, role, location').execute()
        # Return the number of rows that were actually inserted.
        return len(result.data)
    except Exception as e:
        st.error(f"An error occurred while saving jobs: {e}")
        return 0

# --- The rest of the database.py file remains the same ---
def search_jobs(role, location, start_date=None, end_date=None):
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
    if df.empty: return df
    df.columns = [col.replace('posteddate', 'Posted Date').replace('sourceportal', 'Source Portal').title() for col in df.columns]
    return df

def get_all_jobs_raw():
    try:
        data = supabase.table('jobs').select('company, role, location').execute().data
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred while fetching raw data: {e}")
        return pd.DataFrame()

def get_all_jobs():
    try:
        data = supabase.table('jobs').select('*').execute().data
        df = pd.DataFrame(data) if data else pd.DataFrame()
        return format_columns_for_display(df)
    except Exception as e:
        st.error(f"An error occurred while fetching jobs: {e}")
        return pd.DataFrame()
