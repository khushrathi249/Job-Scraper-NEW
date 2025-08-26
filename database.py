import sqlite3
import pandas as pd

DB_FILE = "jobs.db"

def create_connection():
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False) # Important for Streamlit
        return conn
    except sqlite3.Error as e:
        print(e)
    return None

def create_table(conn):
    try:
        sql = """CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Company TEXT, Role TEXT, Location TEXT,
                    Experience TEXT, PostedDate TEXT, SourcePortal TEXT,
                    UNIQUE(Company, Role, Location)
                );"""
        conn.cursor().execute(sql)
    except sqlite3.Error as e:
        print(e)

def add_jobs_df(conn, df):
    df.rename(columns={'Posted Date': 'PostedDate', 'Source Portal': 'SourcePortal'}, inplace=True)
    df.to_sql('jobs', conn, if_exists='append', index=False)
    print(f"Database update complete. Processed {len(df)} records.")

def search_jobs(conn, role, location):
    query = "SELECT Company, Role, Location, Experience, PostedDate, SourcePortal FROM jobs WHERE 1=1"
    params = []
    if role:
        query += " AND Role LIKE ?"
        params.append(f"%{role}%")
    if location:
        query += " AND Location LIKE ?"
        params.append(f"%{location}%")
    query += " ORDER BY PostedDate DESC"
    return pd.read_sql_query(query, conn, params=params)
