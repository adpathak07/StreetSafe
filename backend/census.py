"""
Pulls Census ACS data for Beverly MA + Lawrence MA (equity comparison).
Beverly = Cyvl data city | Lawrence = high foreign-born Gateway City
"""

import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["streetsafe"]

CENSUS_KEY = os.getenv("CENSUS_API_KEY")  # get free key at api.census.gov/data/key_signup.html
BASE = "https://api.census.gov/data/2023/acs/acs5/profile"

CITIES = {
    "Beverly":  "05645",   # MA place FIPS
    "Lawrence": "34550",
}

VARS = {
    "DP05_0086E":  "total_population",
    "DP05_0093E":  "foreign_born_total",
    "DP05_0093PE": "foreign_born_pct",
    "DP03_0062E":  "median_household_income",
}


def fetch_census(place_fips):
    var_str = ",".join(VARS.keys())
    url = (f"{BASE}?get=NAME,{var_str}"
           f"&for=place:{place_fips}&in=state:25&key={CENSUS_KEY}")
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    headers, values = data[0], data[1]
    row = dict(zip(headers, values))
    result = {}
    for code, label in VARS.items():
        try:
            result[label] = float(row[code])
        except:
            result[label] = None
    return result


def load_census():
    col = db["census"]
    col.drop()
    docs = []
    for city, fips in CITIES.items():
        print(f"Fetching Census for {city}...")
        data = fetch_census(fips)
        data["city"] = city
        docs.append(data)
        print(f"  {city}: {data['foreign_born_pct']}% foreign-born, "
              f"income ${data['median_household_income']:,.0f}")
    col.insert_many(docs)
    print("✅ Census data saved to MongoDB")
    return docs


def get_equity_summary():
    """Returns equity comparison dict for frontend display."""
    beverly  = db["census"].find_one({"city": "Beverly"},  {"_id": 0})
    lawrence = db["census"].find_one({"city": "Lawrence"}, {"_id": 0})
    if not beverly or not lawrence:
        return {}
    return {
        "beverly":  beverly,
        "lawrence": lawrence,
        "insight": (
            f"Beverly is {100 - (beverly['foreign_born_pct'] or 0):.0f}% "
            f"US-born with median income ${beverly['median_household_income']:,.0f}. "
            f"Lawrence — 12 miles away — is {lawrence['foreign_born_pct']:.0f}% foreign-born "
            f"with median income ${lawrence['median_household_income']:,.0f}. "
            f"Both cities share similar road infrastructure challenges, "
            f"but Lawrence residents have far fewer resources to absorb the impact."
        )
    }


if __name__ == "__main__":
    load_census()