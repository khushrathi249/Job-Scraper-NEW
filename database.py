import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def connect_to_gsheet():
    """Connects to the Google Sheet and returns the worksheet object."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        spreadsheet_name = st.secrets["GOOGLE_SHEET_NAME"]
        spreadsheet = client.open(spreadsheet_name)
        
        worksheet = spreadsheet.worksheet("All Jobs") # Assumes your sheet's tab is named "All Jobs"
        print("Successfully connected to Google Sheet.")
        return worksheet
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet '{st.secrets['GOOGLE_SHEET_NAME']}' not found. Check the name and sharing settings.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        st.error("Worksheet named 'All Jobs' not found in your spreadsheet.")
        return None
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None

def get_all_jobs_df(worksheet):
    """Fetches all records from the worksheet and returns a pandas DataFrame."""
    if worksheet is None:
        return pd.DataFrame()
    try:
        data = worksheet.get_all_records()
        df = pd.DataFrame.from_records(data)
        # Ensure standard column names for consistency
        if not df.empty:
            df.columns = [col.title().replace('_', ' ') for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Failed to read data from Google Sheet: {e}")
        return pd.DataFrame()

def add_jobs_df(worksheet, new_jobs_df):
    """Adds a DataFrame of new jobs to the Google Sheet, preventing duplicates."""
    if worksheet is None or new_jobs_df.empty:
        return 0

    # Standardize column names of the new data to match the sheet
    new_jobs_df.columns = [str(col).title().replace('_', ' ') for col in new_jobs_df.columns]
    
    existing_jobs_df = get_all_jobs_df(worksheet)
    
    if not existing_jobs_df.empty:
        # Create a unique ID for each job to check for duplicates
        new_jobs_df['unique_id'] = new_jobs_df['Company'].astype(str) + new_jobs_df['Role'].astype(str) + new_jobs_df['Location'].astype(str)
        existing_jobs_df['unique_id'] = existing_jobs_df['Company'].astype(str) + existing_jobs_df['Role'].astype(str) + existing_jobs_df['Location'].astype(str)
        
        # Filter out jobs that already exist in the sheet
        truly_new_jobs_df = new_jobs_df[~new_jobs_df['unique_id'].isin(existing_jobs_df['unique_id'])]
        truly_new_jobs_df = truly_new_jobs_df.drop(columns=['unique_id'])
    else:
        # If the sheet is empty, all scraped jobs are new
        truly_new_jobs_df = new_jobs_df

    num_new_jobs = len(truly_new_jobs_df)
    
    if num_new_jobs > 0:
        try:
            # Ensure the order of columns matches the sheet's header before appending
            header = worksheet.row_values(1)
            # Fill missing columns with empty strings
            for col in header:
                if col not in truly_new_jobs_df.columns:
                    truly_new_jobs_df[col] = ''
            
            truly_new_jobs_df = truly_new_jobs_df[header]
            
            rows_to_append = truly_new_jobs_df.fillna('').values.tolist()
            worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            print(f"Successfully added {num_new_jobs} new rows to Google Sheet.")
        except Exception as e:
            st.error(f"Failed to write to Google Sheet: {e}")
            return 0
            
    return num_new_jobs

def search_jobs(worksheet, role, location, start_date=None, end_date=None):
    """Searches the worksheet data by role, location, and date."""
    df = get_all_jobs_df(worksheet)
    if df.empty:
        return pd.DataFrame()

    # Filter by role and location (case-insensitive search)
    if role:
        df = df[df['Role'].str.contains(role, case=False, na=False)]
    if location:
        df = df[df['Location'].str.contains(location, case=False, na=False)]
        
    # Filter by date range if provided
    if start_date and end_date and 'Posted Date' in df.columns:
        # Convert 'Posted Date' to datetime objects, handling different formats and errors
        df['Posted Date'] = pd.to_datetime(df['Posted Date'], errors='coerce')
        df = df.dropna(subset=['Posted Date']) # Remove rows where date conversion failed
        
        # Ensure comparison dates are timezone-naive
        start_datetime_naive = pd.to_datetime(start_date).tz_localize(None)
        end_datetime_naive = pd.to_datetime(end_date).tz_localize(None)

        mask = (df['Posted Date'].dt.tz_localize(None) >= start_datetime_naive) & (df['Posted Date'].dt.tz_localize(None) <= end_datetime_naive)
        df = df.loc[mask]

    return df
