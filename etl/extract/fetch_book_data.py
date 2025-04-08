import requests
import json
import os
import time
import glob

# Parameters
API_URL = "https://www.googleapis.com/books/v1/volumes"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

GENRES = ["fiction","non-fiction","business","psychology","science","history","literature","children","romance","fantasy"]
PAGES_PER_GENRE = 5  # Number of pages to fetch per genre
MAX_RESULTS = 40  # Max allowed per request
#OUTPUT_PATH = "data/raw/google_books_fiction.json"


def cleanup_old_files():
    '''
    Remove old JSON files in the RAW_DIR
    '''
    for f in glob.glob(os.path.join(RAW_DIR, "*.json")):
        os.remove(f)


def fetch_books(query, start_index=0, max_results=40):
    params = {
        'q': query,
        'maxResults': max_results,
        'startIndex': start_index,
        'printType': 'books'
    }

    try:
        response = requests.get(API_URL, params=params, timeout=10)
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code)
        return None

def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"Saved to {path}")

if __name__ == "__main__":
    # Cleanup old files
    cleanup_old_files()
    # Fetch books data
    for genre in GENRES:
        print(f"Fetching books for genre: {genre}")
        for page in range(PAGES_PER_GENRE):
            start_index = page * MAX_RESULTS
            result = fetch_books(genre, start_index, MAX_RESULTS)
            if result:
                sanitized = genre.lower().replace(" ", "_").replace("-", "_")
                save_path = os.path.join(RAW_DIR, f"google_books_{sanitized}_page_{page}.json")
                save_json(result, save_path)
                print(f"Genre {genre} : Page {page + 1} data saved successfully.")
                time.sleep(1)  # To avoid hitting the API rate limit
            else:
                print("Failed to fetch data.")
                break
        print(f"Completed fetching for genre: {genre}")
    print("All data fetched successfully.")
