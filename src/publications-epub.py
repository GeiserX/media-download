import requests
import gzip
import shutil
import sqlite3
import os
import json
import logging
import re
import traceback
import time

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

LANG = os.environ.get('LANG', 'S')
OUTPUT_PATH = os.environ.get('OUTPUT_PATH', '/epubs/')
DB_PATH = os.environ.get('DB_PATH', '/epubs/pubs.db')
UNIT_DB_PATH = os.environ.get('UNIT_DB_PATH', '/app/db/unit.db')  # Path to unit.db

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

def setup_state_database(db_path):
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))
    if not os.path.exists(db_path):
        logging.info(f"Database does not exist at {db_path}. Creating new database.")
        open(db_path, 'w').close()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS PublicationState (
            TagNumber INTEGER,
            Symbol TEXT,
            sym TEXT,
            State TEXT,
            PRIMARY KEY (TagNumber, Symbol)
        )
        ''')
        conn.commit()
        return conn
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        return None

def fetch_log_db():
    try:
        # Step 1: Get the manifest ID and download the log.gz
        logging.info("Fetching manifest ID.")
        manifest_url = "https://place.holder/api/v1/get/json"
        response = requests.get(manifest_url)
        response.raise_for_status()
        manifest_id = response.json().get('current')
        if not manifest_id:
            logging.error("Failed to fetch manifest ID.")
            raise ValueError("Manifest ID is missing")

        log_url = f"https://place.holder/api/v1/get/{manifest_id}/gz"
        logging.info(f"Downloading log from {log_url}.")
        response = requests.get(log_url, stream=True)
        response.raise_for_status()

        # Ensure the output directory exists
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)

        # Define paths for .gz and .db files in the output directory
        gz_path = os.path.join(OUTPUT_PATH, "log.gz")
        db_path = os.path.join(OUTPUT_PATH, "log")

        with open(gz_path, "wb") as log_file:
            log_file.write(response.content)

        # Step 2: Extract log from log.gz
        logging.info("Extracting log from log.gz.")
        with gzip.open(gz_path, "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Step 3: Delete the .gz file after uncompressing
        logging.info("Deleting log.gz after extraction.")
        os.remove(gz_path)

        return db_path
    except Exception as e:
        logging.error(f"Error in fetching or extracting log: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return None

def get_language_id(lang, unit_db_path):
    try:
        # Open the unit.db database
        logging.info(f"Opening unit.db at {unit_db_path}")
        conn = sqlite3.connect(unit_db_path)
        cursor = conn.cursor()
        # Query the Language table
        cursor.execute("SELECT LanguageId FROM Language WHERE Symbol = ?", (lang,))
        result = cursor.fetchone()
        conn.close()
        if result:
            language_id = result[0]
            logging.info(f"Retrieved LanguageId {language_id} for language '{lang}'")
            return language_id
        else:
            logging.error(f"No LanguageId found for language '{lang}' in unit.db")
            return None
    except Exception as e:
        logging.error(f"Error accessing unit.db: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return None

def get_publications(conn_log, language_id):
    try:
        cursor_log = conn_log.cursor()
        logging.info(f"Querying the Publication table for LanguageId {language_id}.")
        cursor_log.execute("SELECT DISTINCT TagNumber, Symbol, sym FROM Publication WHERE LanguageId=?", (language_id,))
        rows = cursor_log.fetchall()
        logging.info(f"Total publications found: {len(rows)}")
        return rows
    except Exception as e:
        logging.error(f"Error querying publications: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return []

def download_epubs():
    # Setup databases
    conn_state = setup_state_database(DB_PATH)
    if conn_state is None:
        logging.error("State database setup failed. Exiting.")
        return
    cursor_state = conn_state.cursor()

    # Get LanguageId corresponding to LANG
    language_id = get_language_id(LANG, UNIT_DB_PATH)
    if language_id is None:
        logging.error("Failed to retrieve LanguageId. Exiting.")
        return

    db_path = fetch_log_db()
    if db_path is None:
        logging.error("Failed to fetch the log database. Exiting.")
        return

    try:
        # Connect to the log SQLite database using the full path
        logging.info("Connecting to the log SQLite database.")
        conn_log = sqlite3.connect(db_path)
    except Exception as e:
        logging.error(f"Error connecting to log database: {e}")
        return

    # Get the list of publications
    publications = get_publications(conn_log, language_id)

    for idx, (tag_number, symbol, sym) in enumerate(publications, 1):
        logging.info(f"Processing publication {idx}/{len(publications)}: Symbol={symbol}, TagNumber={tag_number}, sym={sym}")

        try:
            cursor_state.execute("SELECT State FROM PublicationState WHERE TagNumber=? AND Symbol=?", (tag_number, symbol))
            state_row = cursor_state.fetchone()
            if state_row and state_row[0] == "processed":
                logging.info(f"Skipping already processed entry: Symbol {symbol}, TagNumber {tag_number}")
                continue

            # Determine the URL for the publication
            if tag_number != 0:
                url = f"https://place.holder/{LANG}&pub={sym}&issue={tag_number}&fileformat=epub"
            else:
                url = f"https://place.holder/{LANG}&pub={symbol}&fileformat=epub"

            download_successful = False
            max_retries = 3
            retry_count = 0
            wait_time = 2

            while retry_count < max_retries and not download_successful:
                try:
                    logging.info(f"Fetching media links from {url}")
                    response = requests.get(url)
                    response.raise_for_status()
                    metadata = response.json()

                    # Extract download URL
                    files = metadata.get('files', {}).get(LANG, {}).get('EPUB', [])
                    if not files:
                        logging.warning(f"No EPUB files found for Symbol {symbol}, TagNumber {tag_number}")
                        cursor_state.execute('''
                        INSERT OR REPLACE INTO PublicationState (TagNumber, Symbol, sym, State)
                        VALUES (?, ?, ?, ?)
                        ''', (tag_number, symbol, sym, "no_epub"))
                        conn_state.commit()
                        break  # Exit the retry loop

                    download_url = files[0]['file']['url']

                    # Download the file to OUTPUT_PATH
                    logging.info(f"Downloading file from {download_url}.")
                    file_response = requests.get(download_url, stream=True)
                    file_response.raise_for_status()

                    # Extract filename from headers or construct one
                    filename = None
                    content_disposition = file_response.headers.get('Content-Disposition', '')
                    if 'filename=' in content_disposition:
                        filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
                        if filename_match:
                            filename = filename_match.group(1)
                    if filename is None:
                        filename = f"{symbol}_{tag_number}.epub"

                    output_file_path = os.path.join(OUTPUT_PATH, filename)
                    with open(output_file_path, "wb") as output_file:
                        shutil.copyfileobj(file_response.raw, output_file)
                    logging.info(f"Downloaded file to {output_file_path}.")

                    # Update state as processed in the state database
                    cursor_state.execute('''
                    INSERT OR REPLACE INTO PublicationState (TagNumber, Symbol, sym, State)
                    VALUES (?, ?, ?, ?)
                    ''', (tag_number, symbol, sym, "processed"))
                    conn_state.commit()

                    download_successful = True
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    logging.warning(f"Attempt {retry_count} failed for Symbol {symbol}, TagNumber {tag_number}: {e}")
                    logging.debug(f"Exception details: {traceback.format_exc()}")
                    if retry_count < max_retries:
                        logging.info(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        wait_time *= 2  # Exponential backoff
                    else:
                        logging.error(f"All {max_retries} attempts failed for Symbol {symbol}, TagNumber {tag_number}")
                        cursor_state.execute('''
                        INSERT OR REPLACE INTO PublicationState (TagNumber, Symbol, sym, State)
                        VALUES (?, ?, ?, ?)
                        ''', (tag_number, symbol, sym, "failed"))
                        conn_state.commit()
                except Exception as e:
                    logging.error(f"Unexpected error for Symbol {symbol}, TagNumber {tag_number}: {e}")
                    logging.debug(f"Exception details: {traceback.format_exc()}")
                    cursor_state.execute('''
                    INSERT OR REPLACE INTO PublicationState (TagNumber, Symbol, sym, State)
                    VALUES (?, ?, ?, ?)
                    ''', (tag_number, symbol, sym, "failed"))
                    conn_state.commit()
                    break  # Exit the retry loop
        except Exception as e:
            logging.error(f"Error processing publication Symbol {symbol}, TagNumber {tag_number}: {e}")
            logging.debug(f"Exception details: {traceback.format_exc()}")
            continue  # Proceed to next publication

    # Close the database connections
    conn_log.close()
    conn_state.close()

    # Cleanup complete
    logging.info("Cleanup complete.")

    logging.info("Download complete.")

if __name__ == "__main__":
    try:
        download_epubs()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")