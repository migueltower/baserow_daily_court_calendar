import os
import time
import json
from typing import List, Dict, Any, Set, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import urllib3
from requests.exceptions import SSLError, RequestException, ReadTimeout

# Disable warnings when we fall back to verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# CONFIG
# =========================
BASEROW_API_BASE = "https://api.baserow.io"
BASEROW_TABLE_ID = 709546  # MC Daily table id
USE_USER_FIELD_NAMES = True

# Batch size: Baserow commonly supports batch operations;
# 100 is conservative and safer than pushing hundreds one-by-one.
BASEROW_BATCH_SIZE = 100

# If true, the script will check existing Baserow rows and skip exact duplicates.
# This is helpful because failed GitHub Action runs may already have created some rows.
SKIP_EXISTING_ROWS = True

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


def _baserow_params() -> Dict[str, str]:
    params: Dict[str, str] = {}

    if USE_USER_FIELD_NAMES:
        params["user_field_names"] = "true"

    return params


def row_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Creates a simple duplicate-check key.

    This prevents the same exact court-calendar row from being added again
    if a previous GitHub Action run partially succeeded.
    """
    return (
        str(row.get("Suspect Name", "")).strip(),
        str(row.get("Building", "")).strip(),
        str(row.get("Time", "")).strip(),
        str(row.get("Case #", "")).strip(),
    )


def chunk_list(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    """Split a list into smaller chunks."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def baserow_request_with_retries(
    method: str,
    url: str,
    *,
    params: Dict[str, str] | None = None,
    payload: Dict[str, Any] | None = None,
    timeout: int = 90,
    max_attempts: int = 5,
) -> requests.Response:
    """
    Makes a Baserow request with retries for temporary API failures.

    Retries:
    - 429: rate limited
    - 500, 502, 503, 504: temporary server/gateway errors
    - timeout/network errors
    """
    retryable_statuses = {429, 500, 502, 503, 504}
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=_auth_headers(),
                params=params,
                data=json.dumps(payload) if payload is not None else None,
                timeout=timeout,
            )

            if response.status_code in retryable_statuses:
                last_error = RuntimeError(
                    f"Baserow temporary error {response.status_code}: {response.text}"
                )
                wait_seconds = 10 * attempt
                print(
                    f"⚠️ Baserow temporary error {response.status_code} "
                    f"on attempt {attempt}/{max_attempts}. "
                    f"Waiting {wait_seconds} seconds before retrying..."
                )
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"Baserow request failed ({response.status_code}): {response.text}"
                )

            return response

        except (ReadTimeout, RequestException) as e:
            last_error = e
            wait_seconds = 10 * attempt
            print(
                f"⚠️ Baserow request error on attempt {attempt}/{max_attempts}: {e}. "
                f"Waiting {wait_seconds} seconds before retrying..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"Baserow request failed after {max_attempts} attempts: {last_error}")


def fetch_existing_baserow_keys() -> Set[Tuple[str, str, str, str]]:
    """
    Fetch existing Baserow rows so the script does not create exact duplicates.

    This is especially useful after a failed GitHub Action run that created
    only part of the day's rows before crashing.
    """
    existing_keys: Set[Tuple[str, str, str, str]] = set()

    url = f"{BASEROW_API_BASE}/api/database/rows/table/{BASEROW_TABLE_ID}/"
    page = 1
    size = 200

    print("🔎 Checking existing Baserow rows to avoid duplicates...")

    while True:
        params = _baserow_params()
        params["page"] = str(page)
        params["size"] = str(size)
        params["include"] = "Suspect Name,Building,Time,Case #"

        response = baserow_request_with_retries(
            "GET",
            url,
            params=params,
            timeout=90,
            max_attempts=5,
        )

        data = response.json()
        results = data.get("results", [])

        for row in results:
            existing_keys.add(row_key(row))

        if not data.get("next"):
            break

        page += 1

    print(f"✅ Found {len(existing_keys)} existing Baserow row keys.")
    return existing_keys


def baserow_create_rows_batch(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create rows in Baserow using the batch endpoint instead of one row at a time.
    """
    url = f"{BASEROW_API_BASE}/api/database/rows/table/{BASEROW_TABLE_ID}/batch/"

    payload = {
        "items": rows
    }

    response = baserow_request_with_retries(
        "POST",
        url,
        params=_baserow_params(),
        payload=payload,
        timeout=120,
        max_attempts=5,
    )

    return response.json()


# =========================
# SCRAPER
# =========================
def scrape_data() -> List[Dict[str, Any]]:
    """Scrape the Maricopa County Superior Court daily court calendar."""
    url = "https://www.superiorcourt.maricopa.gov/calendar/today/"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

    except SSLError as e:
        print(f"⚠️ SSL error when fetching {url}: {e}")
        print("⚠️ Retrying without SSL verification.")
        response = requests.get(url, timeout=30, verify=False)
        response.raise_for_status()

    except RequestException as e:
        raise RuntimeError(f"Failed to fetch calendar page: {e}") from e

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
        floor = cols[2].get_text(strip=True).upper()
        room = cols[3].get_text(strip=True).upper()
        time_str = cols[4].get_text(strip=True)
        case_number = cols[5].get_text(strip=True)

        # Filter out actual floors 2 and 3, plus rooms that are 2/3 or start with 2/3.
        # Examples skipped: floor 2, floor 3, room 2, room 3, 2A, 2B, 2D, 3A, 3B, 301, 302.
        if floor in {"2", "3"} or room in {"2", "3"} or room.startswith(("2", "3")):
            print(
                f"⏭️ Skipping floor/room 2 or 3: "
                f"{name} | floor {floor} | room {room} | {case_number}"
            )
            continue

        entry = {
            "Suspect Name": name,
            "Building": room,
            "Time": time_str,
            "Case #": case_number,
        }

        entries.append(entry)

    print(f"✅ Scraped {len(entries)} qualifying court entries.")
    return entries


# =========================
# PUSH TO BASEROW
# =========================
def clean_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep only the fields that exist in the MC Daily Baserow table."""
    allowed_keys = ["Suspect Name", "Building", "Time", "Case #"]
    cleaned: List[Dict[str, Any]] = []

    for record in entries:
        clean: Dict[str, Any] = {}

        for key in allowed_keys:
            value = record.get(key)
            if value is not None and str(value).strip() != "":
                clean[key] = str(value).strip()

        if clean:
            cleaned.append(clean)

    return cleaned


def dedupe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove exact duplicates from the scraped data itself."""
    seen: Set[Tuple[str, str, str, str]] = set()
    deduped: List[Dict[str, Any]] = []

    for entry in entries:
        key = row_key(entry)

        if key in seen:
            continue

        seen.add(key)
        deduped.append(entry)

    skipped = len(entries) - len(deduped)

    if skipped:
        print(f"ℹ️ Skipped {skipped} duplicate rows within the scraped calendar data.")

    return deduped


def push_to_baserow(entries: List[Dict[str, Any]]) -> None:
    """Push scraped entries to the Baserow MC Daily table in batches."""
    if not entries:
        print("No entries to push.")
        return

    cleaned_entries = clean_entries(entries)
    cleaned_entries = dedupe_entries(cleaned_entries)

    if not cleaned_entries:
        print("No cleaned entries to push.")
        return

    if SKIP_EXISTING_ROWS:
        existing_keys = fetch_existing_baserow_keys()

        before_count = len(cleaned_entries)
        cleaned_entries = [
            entry for entry in cleaned_entries
            if row_key(entry) not in existing_keys
        ]

        skipped_existing = before_count - len(cleaned_entries)

        if skipped_existing:
            print(f"ℹ️ Skipped {skipped_existing} rows that already exist in Baserow.")

    if not cleaned_entries:
        print("✅ Nothing new to push to Baserow.")
        return

    batches = chunk_list(cleaned_entries, BASEROW_BATCH_SIZE)

    print(
        f"📦 Sending {len(cleaned_entries)} new rows to Baserow "
        f"in {len(batches)} batch(es)."
    )

    total_created = 0

    for i, batch in enumerate(batches, start=1):
        result = baserow_create_rows_batch(batch)

        # Baserow batch responses usually include an "items" array.
        # If the response shape changes, fall back to counting the submitted batch.
        if isinstance(result, dict) and isinstance(result.get("items"), list):
            created_count = len(result["items"])
        else:
            created_count = len(batch)

        total_created += created_count

        print(
            f"[batch {i}/{len(batches)}] ✅ Created about {created_count} row(s)."
        )

        # Small pause between batches to be gentle on the API
        time.sleep(2)

    print(f"✅ Finished. Created about {total_created} new Baserow row(s).")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    data = scrape_data()
    push_to_baserow(data)
