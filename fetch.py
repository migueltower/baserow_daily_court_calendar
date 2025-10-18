import os
import requests
from bs4 import BeautifulSoup
from pyairtable import Api, Table
from datetime import datetime

# Airtable config
AIRTABLE_BASE_ID = "appklERHZIa2OuacR"
AIRTABLE_TABLE_ID = "tblb0yIYr91PzghXQ"

def connect_to_airtable():
    api_key = os.environ["KEY"]
    api = Api(api_key)
    return api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

def time_is_after_nine_am(time_str):
    try:
        time_obj = datetime.strptime(time_str.strip(), "%I:%M %p").time()
        return time_obj >= datetime.strptime("9:00 AM", "%I:%M %p").time()
    except:
        return False

def scrape_data():
    url = "https://www.superiorcourt.maricopa.gov/calendar/today/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", id="tblZebra")
    rows = table.find_all("tr")[1:]  # Skip header row

    entries = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue

        name = cols[0].get_text(strip=True)
        floor = cols[2].get_text(strip=True)
        room = cols[3].get_text(strip=True)
        time = cols[4].get_text(strip=True)
        case_number = cols[5].get_text(strip=True)

        # Filter conditions
        if floor in ["2", "3"]:
            continue
        if not time_is_after_nine_am(time):
            continue

        entries.append({
            "Suspect Name": name,
            "Building": room,          # sending ROOM info as BUILDING
            "Time": time,
            "Case #": case_number
        })

    return entries

def push_to_airtable(entries):
    table = connect_to_airtable()
    for record in entries:
        table.create(record)

if __name__ == "__main__":
    data = scrape_data()
    push_to_airtable(data)
