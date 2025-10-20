import os
import time
import json
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# =========================
# CONFIG
# =========================
BASEROW_API_BASE = "https://api.baserow.io"
BASEROW_TABLE_ID = 709546  # your MC Daily table id
USE_USER_FIELD_NAMES = True

# GitHub secret
BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN")


# =========================
# BASEROW HELPERS
# =========================
def _auth_headers() -> Dict[str, str]:
    if not BASEROW_TOKEN:
        raise RuntimeError("Missing BASEROW_TOKEN environment variable.")
    return {
        "Authorization": f"Token {BASEROW_TOKEN}",
        "Content-Type": "application/json",
    }


def baserow_create_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    params = {}
    if USE_USER_FIELD_NAMES:
        params["user_field_names"] = "true"

    url = f"{BASEROW_API_BASE}/api/database/rows/table/{BASEROW_TABLE_ID}/"
    resp = requests.post(
        url, headers=_auth_headers(), params=params, data=json.dumps(payload), timeout=30
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Baserow create failed ({resp.status_code}): {resp.text}")
    return resp.json()


# =========================
# SCRAPER
# =========================
def time_is_after_nine_am(time_str: str) -> bool:
    """Return True if time is at or after 9:00 AM."""
    try:
        t = datetime.strptime(time_str.strip(), "%I:%M %p").time()
        nine_am = datetime.strptime("9:00 AM", "%I:%M %p").time()
        return t >= nine_am
    except Exception:
        return False


def scrape_data() -> List[Dict[str, Any]]:
    """Scrapes the daily court calendar just like the Airtable version."""
    url = "https://www.superiorcourt.maricopa.gov/calendar/today/"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch calendar page (status {response.status_code})")

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", id="tblZebra")
    if not table:
        print("⚠️ No table found on the page. The site layout might have changed.")
        return []

    rows = table.find_all("tr")[1:]  # Skip header row
    entries: List[Dict[str, Any]] = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue

        name = cols[0].get_text(strip=True)
        floor = cols[2].get_text(strip=True)
        room = cols[3].get_text(strip=True)
        time_str = cols[4].get_text(strip=True)
        case_number = cols[5].get_text(strip=True)

        # Filter out floors 2 and 3, and times before 9:00 AM
        if floor in ["2", "3"]:
            continue
        if not time_is_after_nine_am(time_str):
            continue

        entries.append({
            "Suspect Name": name,
            "Building": room,  # Room maps to "Building" field
            "Time": time_str,
            "Case #": case_number
        })

    print(f"✅ Scraped {len(entries)} qualifying court entries.")
    return entries


# =========================
# PUSH TO BASEROW
# =========================
def push_to_baserow(entries: List[Dict[str, Any]]) -> None:
    """Push scraped entries to the Baserow 'MC Daily' table."""
    if not entries:
        print("No entries to push.")
        return

    for i, record in enumerate(entries, start=1):
        clean: Dict[str, Any] = {}
        for key in ["Suspect Name", "Building", "Time", "Case #"]:
            if key in record and record[key]:
                clean[key] = record[key]

        created = baserow_create_row(clean)
        print(f"[{i}/{len(entries)}] ✅ Created Baserow row id={created.get('id')}")
        time.sleep(0.2)  # small delay to avoid hitting rate limits


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    data = scrape_data()
    push_to_baserow(data)
