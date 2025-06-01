import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from urllib.parse import urlparse, urlunparse
import re
from datetime import datetime
import os

# Airtable setup
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "tbl0oMenJHGTTrOoi"
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")

AIRTABLE_ENDPOINT = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# Airtable Field IDs for writing
FIELD_ID_EVENT = "fldB8AMCIKRKWKPZo"        # Event Title
FIELD_ID_MONTH_DAY = "fldjIX0swJ9gk5ERo"     # Month & Day
FIELD_ID_LOCATION = "fld9h9ViSpsEEAlbZ"      # Location
FIELD_ID_URL = "fldOaE6gMEsiKBe4q"           # eventurl
FIELD_ID_TIMESTAMP = "fld71QOP6Ivug3PpT"     # Added At

def normalize_url(url):
    try:
        parsed = urlparse(url.strip())
        clean_path = parsed.path.rstrip('/')
        return urlunparse((parsed.scheme, parsed.netloc, clean_path, '', '', ''))
    except Exception:
        return url.strip().rstrip('/')

# ✅ FIXED: using field name "eventurl" for reading from Airtable
def fetch_existing_urls():
    existing_urls = set()
    offset = None

    while True:
        params = {}
        if offset:
            params['offset'] = offset

        r = requests.get(AIRTABLE_ENDPOINT, headers=HEADERS, params=params)
        if r.status_code != 200:
            print("❌ Failed to fetch Airtable records:", r.text)
            break

        data = r.json()
        for record in data.get("records", []):
            fields = record.get("fields", {})
            # Debug: print available fields to see what we're getting
            print(f"🔍 Available fields: {list(fields.keys())}")
            
            # Try both field ID and field name
            url = fields.get(FIELD_ID_URL) or fields.get("eventurl")
            if url:
                existing_urls.add(normalize_url(url))
                print(f"✅ Found existing URL: {url}")

        offset = data.get("offset")
        if not offset:
            break

    print(f"✅ Loaded {len(existing_urls)} existing event URLs from Airtable")
    return existing_urls

def format_date_for_airtable(month, day):
    """Format month and day for Airtable - assume 2025"""
    return f"{month} {day}, 2025"

def parse_events():
    base_url = 'https://www.iese.edu/search/events'
    all_events = []
    seen_events = set()  # Track unique events by URL
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}/{page}/"
        print(f"🔍 Scraping page {page}: {url}")
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        boxes = soup.select('.box-events')

        if not boxes:
            print("🛑 No more events found.")
            break

        page_events_added = 0
        for box in boxes:
            try:
                day = box.select_one('.event-date__day').text.strip()
                month = box.select_one('.event-date__month').text.strip().replace('.', '')
                title = box.select_one('.content a').text.strip()
                event_url = box.select_one('.content a')['href']
                location_tag = box.select_one('.categories')
                location = location_tag.text.strip() if location_tag else "N/A"

                # Create a unique identifier for this event
                normalized_url = normalize_url(event_url)
                
                # Check if we've already seen this event
                if normalized_url in seen_events:
                    print(f"🔄 Skipping duplicate event: {title}")
                    continue
                
                # Add to our tracking set
                seen_events.add(normalized_url)
                
                # Format date assuming 2025
                date_display = format_date_for_airtable(month, day)
                print(f"✅ Using date: {date_display}")
                
                all_events.append({
                    "title": title,
                    "date": date_display,
                    "location": location,
                    "url": event_url
                })
                page_events_added += 1
                
            except Exception as e:
                print("❌ Error parsing event box:", e)

        print(f"📄 Page {page}: Added {page_events_added} unique events")
        page += 1

    print(f"✅ Total unique events found: {len(all_events)}")
    return all_events

def send_to_airtable(events):
    added_count = 0
    existing_urls = fetch_existing_urls()

    for e in events:
        cleaned_url = normalize_url(e['url'])
        if cleaned_url in existing_urls:
            continue

        # Generate unique timestamp for each record
        unique_timestamp = datetime.utcnow().isoformat()

        data = {
            "fields": {
                FIELD_ID_EVENT: e['title'],
                FIELD_ID_MONTH_DAY: e['date'],
                FIELD_ID_LOCATION: e['location'],
                FIELD_ID_URL: cleaned_url,
                FIELD_ID_TIMESTAMP: unique_timestamp
            }
        }

        r = requests.post(AIRTABLE_ENDPOINT, headers=HEADERS, data=json.dumps(data))
        if r.status_code in [200, 201]:
            print("✅ Uploaded:", e['title'])
            added_count += 1
        else:
            print("❌ Failed to upload:", e['title'], r.text)

    print(f"\n📊 {added_count} new events added to Airtable.")

if __name__ == "__main__":
    events = parse_events()
    send_to_airtable(events)
