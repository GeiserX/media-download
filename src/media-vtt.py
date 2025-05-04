import requests
import gzip
import shutil
import json
import logging
import os
import sqlite3
import traceback
import time
from urllib.parse import urlparse, unquote

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

LANG = os.environ.get('LANG', 'S')
OUTPUT_PATH = os.environ.get('OUTPUT_PATH', '/vtts/')  # Set to '/vtts/' as per your requirement
DB_PATH = os.environ.get('DB_PATH', '/vtts/media.db')  # Database in '/vtts/'

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# Ensure the database and table are created
def setup_database(db_path):
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    if not os.path.exists(db_path):
        logging.info(f"Database does not exist at {db_path}. Creating new database.")
        open(db_path, 'w').close()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Changed 'pubS' to 'identifier'
        cursor.execute('''CREATE TABLE IF NOT EXISTS downloaded_vtts (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                identifier TEXT NOT NULL,
                                track INTEGER NOT NULL,
                                formatCode TEXT NOT NULL,
                                vtt_url TEXT,
                                status TEXT NOT NULL,
                                UNIQUE(identifier, track, formatCode)
                              )''')
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

# Check if a media item has been processed
def is_vtt_processed(db_path, identifier, track, formatCode):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM downloaded_vtts WHERE identifier = ? AND track = ? AND formatCode = ?",
            (identifier, track, formatCode)
        )
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        logging.error(f"Error checking database for {identifier}, track {track}, format {formatCode}: {e}")
        return None

# Mark a media item as processed
def mark_vtt_as_downloaded(db_path, identifier, track, formatCode, vtt_url, status):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''INSERT OR REPLACE INTO downloaded_vtts (identifier, track, formatCode, vtt_url, status)
               VALUES (?, ?, ?, ?, ?)''',
            (identifier, track, formatCode, vtt_url, status)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting into database for {identifier} track {track} format {formatCode}: {e}")

def download_extract_json(catalog_url, output_path):
    try:
        logging.info(f"Downloading catalog from {catalog_url}.")
        response = requests.get(catalog_url, stream=True)
        response.raise_for_status()
        
        gz_path = os.path.join(output_path, f"{LANG}.json.gz")
        json_path = os.path.join(output_path, f"{LANG}.json")

        with open(gz_path, "wb") as gz_file:
            gz_file.write(response.content)

        logging.info("Extracting the JSON")

        with gzip.open(gz_path, "rb") as f_in:
            with open(json_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Delete the .gz file after extraction
        logging.info(f"Deleting {gz_path} after extraction.")
        os.remove(gz_path)

        return json_path

    except Exception as e:
        logging.error(f"Error in downloading or extracting JSON: {e}")
        return None

def extract_media_info(json_path):
    media_info = []
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            for line in file:
                item = json.loads(line)
                if item['type'] == 'media-item':
                    o = item.get('o', {})
                    key_parts = o.get('keyParts', {})

                    # Get 'identifier' as either 'pubS' or 'docID'
                    identifier = key_parts.get('pubS') or key_parts.get('docID')
                    track = key_parts.get('track')
                    formatCode = key_parts.get('formatCode')

                    if identifier and track is not None and formatCode:
                        media_info.append((identifier, track, formatCode, key_parts))
    except Exception as e:
        logging.error(f"Error in extracting media info: {e}")

    return media_info

def get_pub_media_links(identifier, track, formatCode, key_parts):
    base_url = "https://place.holder/api/v1/get"

    # Determine whether to use 'pub' or 'docid' parameter
    params = {
        'langwritten': LANG,
        'track': track,
        # 'fileformat' parameter is removed as per your instructions
    }

    if 'pubS' in key_parts:
        params['pub'] = key_parts['pubS']
    elif 'docID' in key_parts:
        params['docid'] = key_parts['docID']
    else:
        logging.error(f"No 'pubS' or 'docID' found in key parts: {key_parts}")
        return None

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error for {identifier} track {track} format {formatCode}: {http_err}")
        return None
    except Exception as e:
        logging.error(f"Error in accessing media links API: {e}")
        return None

def download_vtt_files(media_info, max_retries=3):
    for identifier, track, formatCode, key_parts in media_info:
        status = is_vtt_processed(DB_PATH, identifier, track, formatCode)

        if status == 'success':
            logging.info(f"Already successfully processed {identifier} track {track} format {formatCode}, skipping.")
            continue
        elif status == 'failed':
            logging.info(f"Already attempted but failed {identifier} track {track} format {formatCode}, skipping.")
            continue
        else:
            # Proceed to attempt to get media links and download
            media_links = get_pub_media_links(identifier, track, formatCode, key_parts)

            if media_links and "files" in media_links:
                vtt_file_url = None

                # We no longer skip items based on title
                formats = media_links["files"].get(LANG, {})

                found_vtt = False
                # Iterate over available formats and files
                for file_format_entries in formats.values():
                    for file in file_format_entries:
                        # Check if 'subtitles' are available
                        subtitles = file.get('subtitles')
                        if subtitles and 'url' in subtitles:
                            vtt_file_url = subtitles['url']
                            found_vtt = True
                            break
                    if found_vtt:
                        break

                if vtt_file_url:
                    retry_count = 0
                    while retry_count < max_retries:
                        try:
                            vtt_response = requests.get(vtt_file_url, stream=True)
                            vtt_response.raise_for_status()

                            # Extract the filename from the URL
                            parsed_url = urlparse(vtt_file_url)
                            filename = os.path.basename(parsed_url.path)
                            filename = unquote(filename)  # Decode URL-encoded characters

                            # Save VTT file with the original filename
                            vtt_filename = os.path.join(OUTPUT_PATH, filename)

                            with open(vtt_filename, 'wb') as vtt_output:
                                vtt_output.write(vtt_response.content)

                            logging.info(f"Downloaded: {vtt_filename}")

                            # Mark the VTT as successfully downloaded
                            mark_vtt_as_downloaded(DB_PATH, identifier, track, formatCode, vtt_file_url, 'success')
                            break  # Success, exit retry loop

                        except requests.exceptions.RequestException as e:
                            retry_count += 1
                            logging.warning(f"Attempt {retry_count} failed for {identifier} track {track}: {e}")
                            logging.debug(f"Exception details: {traceback.format_exc()}")
                            if retry_count < max_retries:
                                wait_time = 2 ** retry_count
                                logging.info(f"Retrying in {wait_time} seconds...")
                                time.sleep(wait_time)
                            else:
                                logging.error(f"All {max_retries} attempts failed for {identifier} track {track}")
                                # Mark the VTT as failed
                                mark_vtt_as_downloaded(DB_PATH, identifier, track, formatCode, vtt_file_url, 'failed')
                        except Exception as e:
                            logging.error(f"Unexpected error for {identifier} track {track}: {e}")
                            logging.debug(f"Exception details: {traceback.format_exc()}")
                            # Mark the VTT as failed
                            mark_vtt_as_downloaded(DB_PATH, identifier, track, formatCode, vtt_file_url, 'failed')
                            break  # Exit the retry loop

                else:
                    logging.warning(f"No subtitles found for {identifier} track {track} format {formatCode}")
                    # Optionally, record this as 'no_subtitles' in the database
                    mark_vtt_as_downloaded(DB_PATH, identifier, track, formatCode, None, 'no_subtitles')
            else:
                logging.error(f"No media links available for {identifier} track {track} format {formatCode}")
                logging.debug(f"Response from get_pub_media_links for {identifier} track {track} format {formatCode}: {media_links}")
                # Record this as failed attempt
                mark_vtt_as_downloaded(DB_PATH, identifier, track, formatCode, None, 'failed')

if __name__ == "__main__":
    setup_database(DB_PATH)
    catalog_url = f"https://place.holder/api/v1/get/{LANG}.json.gz"
    json_path = download_extract_json(catalog_url, OUTPUT_PATH)

    if json_path:
        media_info = extract_media_info(json_path)
        logging.info(f"Total media items to process: {len(media_info)}")
        download_vtt_files(media_info)

    logging.info("Finished processing all media items.")