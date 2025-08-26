import sqlite3
import pandas as pd

DB_FILE = "jobs.db"

def create_connection():
    """Create a database connection to the SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn):
    """Create the jobs table if it doesn't exist."""
    try:
        sql_create_jobs_table = """ CREATE TABLE IF NOT EXISTS jobs (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Company TEXT NOT NULL,
                                        Role TEXT NOT NULL,
                                        Location TEXT,
                                        Experience TEXT,
                                        PostedDate TEXT,
                                        SourcePortal TEXT,
                                        UNIQUE(Company, Role, Location, PostedDate)
                                    ); """
        cursor = conn.cursor()
        cursor.execute(sql_create_jobs_table)
    except sqlite3.Error as e:
        print(e)

def add_jobs_df(conn, jobs_dataframe):
    """Add new jobs from a DataFrame to the database, ignoring duplicates."""
    # Rename columns to match the database schema
    jobs_dataframe.rename(columns={
        'Posted Date': 'PostedDate',
        'Source Portal': 'SourcePortal'
    }, inplace=True)

    # Use 'INSERT OR IGNORE' to prevent adding duplicate entries
    jobs_dataframe.to_sql('jobs', conn, if_exists='append', index=False)
    print(f"Database update complete. Processed {len(jobs_dataframe)} records.")


def search_jobs(conn, role, location):
    """Search for jobs by role and location."""
    query = "SELECT Company, Role, Location, Experience, PostedDate, SourcePortal FROM jobs WHERE 1=1"
    params = []

    if role:
        query += " AND Role LIKE ?"
        params.append(f"%{role}%")
    if location:
        query += " AND Location LIKE ?"
        params.append(f"%{location}%")
    
    query += " ORDER BY PostedDate DESC"

    df = pd.read_sql_query(query, conn, params=params)
    return df