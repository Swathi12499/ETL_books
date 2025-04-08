import sqlite3
import pandas as pd
import os
import json
from datetime import datetime

# Safe relative paths using absolute base
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
TABLE_NAME = "books"
DB_PATH = os.path.join(CLEANED_DIR, "books.db")
CSV_PATH = os.path.join(CLEANED_DIR, f"google_books_cleaned_{datetime.today().date()}.csv")
LAST_RUN_FILE = os.path.join(CLEANED_DIR, "last_run.txt")

def create_db_connection(db_path):
    """
    Create a database connection to the SQLite database specified by db_path.
    """
    conn = sqlite3.connect(db_path)
    return conn

def create_table(conn):
    """
    Create a table in the database if it doesn't already exist.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        subtitle TEXT,
        authors TEXT,
        publisher TEXT,
        published_date TEXT,
        description TEXT,
        categories TEXT,
        page_count INTEGER,
        average_rating REAL,
        ratings_count INTEGER,
        language TEXT,
        isbn_10 TEXT,
        isbn_13 TEXT,
        info_link TEXT,
        buy_link TEXT,
        retail_price REAL,
        currency_code TEXT,
        thumbnail_link TEXT,
        is_ebook BOOLEAN,
        genre TEXT
    );
    """
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()

def read_csv_to_dataframe(csv_path):
    """
    Read the CSV file and return a DataFrame.
    """
    print(f"Using file: {csv_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}") 
    return pd.read_csv(csv_path)

def insert_data_to_db(df, conn):
    """
    Insert or update data from the DataFrame into the database using UPSERT.
    """
    try:
        cursor = conn.cursor()
        insert_sql = f"""
        INSERT OR REPLACE INTO {TABLE_NAME} (
            id, title, subtitle, authors, publisher, published_date, description,
            categories, page_count, average_rating, ratings_count, language,
            isbn_10, isbn_13, info_link, buy_link, retail_price, currency_code,
            thumbnail_link, is_ebook, genre
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """    
        rows = df.to_records(index=False)
        for row in rows:
            cursor.execute(insert_sql, tuple(row))
        
        conn.commit()
        print(f"Upserted {len(df)} records into {TABLE_NAME}")
        cursor.close()
    except Exception as e:
        print(f"Error inserting/updating data: {e}")

def check_insertion(conn):
    """
    Check the number of records in the database.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]
    #print(f"Total records in {TABLE_NAME}: {count}")
    return count
    cursor.close()

if __name__ == "__main__":
    # Create a database connection
    print("Creating database connection...")
    conn = create_db_connection(DB_PATH)

    # Create the table if it doesn't exist
    print("Creating the table if it doesn't exist..")
    create_table(conn)

    # Read the CSV file into a DataFrame
    print("Reading CSV file...")
    df = read_csv_to_dataframe(CSV_PATH)

    # Check the number of records in the database before insertion/updation 
    print("Checking the number of records before insertion...")
    before = check_insertion(conn)
    print(f"Total records before insertion: {before}")

    # Insert data into the database
    print("Inserting data into the database...")
    insert_data_to_db(df, conn)

    # Check the number of records in the database after insertion/updation 
    print("Checking the number of records after insertion...")
    after = check_insertion(conn)
    print(f"Total records after insertion: {after}")    

    # Log the last run file with timestamp
    print("Logging the last run...")
    with open(LAST_RUN_FILE, "w") as f:
        f.write(f"Last run: {datetime.now()}\n")

    # Close the database connection
    print("Closing the database connection...")
    conn.close()

    print("Data loaded into the database successfully.")