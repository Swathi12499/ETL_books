import os
import json
import re
import pandas as pd
from rapidfuzz import fuzz
from datetime import datetime

# Dynamically build safe paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "cleaned")
OUTPUT_FILE = f"google_books_cleaned_{datetime.today().date()}.csv"
GENRE_COUNT_FILE = f"genre_counts_{datetime.today().date()}.csv"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_PATH, OUTPUT_FILE)
GENRE_COUNTS_PATH = os.path.join(OUTPUT_PATH, GENRE_COUNT_FILE)


def clean_book_item(book_item):
    """
    Cleans a single book item by extracting relevant fields and handling missing values.
    """
    # Extract relevant fields
    volume_info = book_item.get('volumeInfo', {})
    sale_data = book_item.get('saleInfo', {})
    image_links = book_item.get('imageLinks', {})
    industry_identifiers = volume_info.get('industryIdentifiers', [])

    # Extract ISBNs
    isbn_10, isbn_13 = '', ''
    for identifier in industry_identifiers:
        if identifier.get('type') == 'ISBN_13':
            isbn_13 = identifier.get('identifier', '')
        elif identifier.get('type') == 'ISBN_10':
            isbn_10 = identifier.get('identifier', '')

    return {
        'id': book_item.get('id', ''),
        'title': volume_info.get('title', '').strip().title(),
        'subtitle': volume_info.get('subtitle', ''),
        'authors': ", ".join([author.strip().title() for author in volume_info.get('authors', [])]),
        'publisher': volume_info.get('publisher', ''),
        'published_date': volume_info.get('publishedDate', ''),
        'description': volume_info.get('description', '')[:500],
        'categories': ", ".join(volume_info.get('categories', [])),
        'page_count': volume_info.get('pageCount', 0),
        'average_rating': volume_info.get('averageRating', ""),
        'ratings_count': volume_info.get('ratingsCount', 0),
        'language': volume_info.get('language', ''),
        'isbn_10': isbn_10,
        'isbn_13': isbn_13,
        'info_link': volume_info.get('infoLink', ''),
        'buy_link': sale_data.get('buyLink', ''),
        'retail_price': sale_data.get('retailPrice', {}).get('amount', ""),
        'currency_code': sale_data.get('retailPrice', {}).get('currencyCode', ""),
        'thumbnail_link': image_links.get('thumbnail', ''),
        'is_ebook': sale_data.get('isEbook', False)
    }

def is_duplicate(row1, row2):
    """
    Checks if the current row is a duplicate of an existing row based on fuzzy title match and author.
    """
    if row1['authors'] == row2['authors']:
        return fuzz.token_sort_ratio(row1['title'], row2['title']) > 90
    return False

def extract_genre_from_filename(filename):
    match = re.match(r"google_books_(.+?)_page", filename)
    if match:
        genre_raw = match.group(1)  # e.g. "non_fiction"
        # Format nicely: "non_fiction" → "Non-Fiction"
        genre_formatted = genre_raw.replace("_", " ").title().replace(" And ", " and ")
        return genre_formatted
    return "unknown"

    
if __name__ == "__main__":
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Initialize a list to hold all book data
    all_cleaned_books = []

    # Loop through all JSON files in the raw data directory
    for filename in os.listdir(DATA_PATH):
        if filename.endswith(".json"):
            file_path = os.path.join(DATA_PATH, filename)
            genre = extract_genre_from_filename(filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    items = data.get('items', [])
                    for item in items:
                        cleaned_book_item = clean_book_item(item)
                        # Add genre to the cleaned book item
                        cleaned_book_item['genre'] = genre
                        # Append the cleaned book item to the list
                        all_cleaned_books.append(cleaned_book_item)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {file_path}: {e}")

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_cleaned_books)

    print(f"Total records before deduplication: {len(df)}")

    # De-duplicate based on fuzzy title match + same author
    unique_rows = []
    for idx, row in df.iterrows():
        is_dup = any(is_duplicate(row, existing) for existing in unique_rows)
        if not is_dup:
            unique_rows.append(row)
    
    print(f"Duplicates removed: {len(df) - len(unique_rows)}")
    df = pd.DataFrame(unique_rows)
    print(f"Total records after deduplication: {len(df)}")

    # Save the cleaned DataFrame to a CSV file
    print(f"Saving cleaned data to: {OUTPUT_FILE_PATH}")
    df.to_csv(OUTPUT_FILE_PATH, index=False, encoding='utf-8')
    print(f"Cleaned data saved to {OUTPUT_FILE_PATH}")

    genre_counts = df['genre'].value_counts()

    # Save to CSV
    genre_counts.to_csv(GENRE_COUNTS_PATH)
    print(f"Genre counts saved to {GENRE_COUNTS_PATH}")

    # Also print summary
    print("\n📊 Books per genre:")
    print(genre_counts)
