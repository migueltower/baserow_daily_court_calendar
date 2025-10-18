import os
import time
import json
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from datetime import datetime

# =========================
# CONFIG — EDIT AS NEEDED
# =========================
BASEROW_API_BASE = "https://api.baserow.io"
BASEROW_TABLE_ID = 709546  # "MC Daily" table id from your API doc
# We'll use user-friendly field names in our payloads:
USE_USER_FIELD_NAMES = True

# The GitHub Action passes this via env:
#   BASEROW_TOKEN: ${{ secrets.BASEROW_TOKEN }}
BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN")


# =========================
# BASEROW CLIENT HELPERS
# =========================
def _auth_headers() -> Dict[str, str]:
    if not BASEROW_TOKEN:
        raise RuntimeError(
            "Missing BASEROW_TOKEN environment variable. "
            "Add it in GitHub → Settings → Secrets → Actions."
        )
    return {"Authorization": f"Token {BASEROW_TOKEN}", "Content-Type": "application/json"}


def baserow_create_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a single row in the MC Daily table.
    Payload must use user field names if USE_USER_FIELD_NAMES is True.
    """
    params = {}
    if USE_USER_FIELD_NAMES:
        params["user_field_names"] = "true"

    url = f"{BASEROW_API_BASE}/api/database/rows/table/{BASEROW_TABLE_ID}/"
    resp = requests.post(url, headers=_auth_headers(), params=params, data=json.dumps(payload), timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Baserow create failed ({resp.status_code}): {resp.text}")
    return resp.json()


def baserow_list_rows(
    page: int = 1, size: int = 100, search: Optional[str] = None
) -> Dict[str, Any]:
    """
    Optional: read rows back from Baserow (for dedupe checks etc.).
    Returns {'count', 'next', 'previous', 'results': [ ... ]}.
    """
    params = {"page": page, "size": size}
    if USE_USER_FIELD_NAMES:
        params["user_field_names"] = "true"
    if search:
        params["search"] = search

    url = f"{BASEROW_API_BASE}/api/database/rows/table/{BASEROW_TABLE_ID}/"
    resp = requests.get(url, headers=_auth_headers(), params=params, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Baserow list failed ({resp.status_code}): {resp.text}")
    return resp.json()


# =========================
# YOUR SCRAPER
# =========================
def time_is_after_nine_am(time_str: str) -> bool:
    """
    Keep any helper logic you had. If you previously compared "9:00 AM" etc.,
    re-implement here. Return True if the time is >= 09:00.
    """
    try:
        dt = datetime.strptime(time_str.strip(), "%I:%M %p")
        return (dt.hour, dt.minute) >= (9, 0)
    except Exception:
        return True  # if unsure, include


def scrape_data() -> List[Dict[str, Any]]:
    """
    ⬇️ REPLACE THIS BODY WITH YOUR EXISTING SCRAPING LOGIC ⬇️

    Your existing code should return a list of dictionaries, one per court event, like:
        {
            "Suspect Name": "Jane Doe",
            "Building": "Central Court",
            "Time": "9:30 AM",
            "Case #": "CR-2024-123456",
            "Crime": "Aggravated Assault",
            "Case Number Links": "https://...",
            "High Profile": False,
            "High Profile Cases": "",
            "High Profile Cases 2": "",
            "MC Daily Sentencing Auto-Pull": ""
        }

    Keep the keys exactly matching your Baserow field names.
    If you only have a subset (e.g., Suspect Name, Time, Case #), that’s fine too.
    """

    # --- BEGIN: placeholder example that returns an empty list ---
    # Replace this entire block with your existing parsing code.
    url = "https://example.com/your/court/calendar"  # your real URL
    _ = requests.get(url, timeout=30)
    # soup = BeautifulSoup(_.text, "html.parser")
    # Parse...
    entries: List[Dict[str, Any]] = []
    # --- END: placeholder ---

    return entries


# =========================
# PIPELINE: PUSH TO BASEROW
# =========================
def push_to_baserow(entries: List[Dict[str, Any]]) -> None:
    """
    Creates rows in Baserow for each entry.
    If you want to dedupe, you can first baserow_list_rows(...) and check for an existing Case #.
    """
    if not entries:
        print("No entries to push.")
        return

    for i, record in enumerate(entries, start=1):
        clean: Dict[str, Any] = {}

        # Only include known fields you actually scraped. These names match your table schema.
        for key in [
            "Suspect Name",
            "Building",
            "Time",
            "Case #",
            "Crime",
            "Case Number Links",
            "High Profile",
            "High Profile Cases",
            "High Profile Cases 2",
            "MC Daily Sentencing Auto-Pull",
        ]:
            if key in record and record[key] not in (None, ""):
                clean[key] = record[key]

        created = baserow_create_row(clean)
        print(f"[{i}/{len(entries)}] Created row id={created.get('id')}")

        # Gentle pacing to be nice to the API (optional)
        time.sleep(0.2)


if __name__ == "__main__":
    data = scrape_data()
    push_to_baserow(data)
