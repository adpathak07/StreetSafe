"""
Fetches Beverly, MA Cyvl GeoJSON data and computes a simple risk score.
Run once to populate MongoDB, then serve from DB.
"""

import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["streetsafe"]

BASE = "https://dcygqrjfsypox.cloudfront.net/cyvl_public_data/11_2025/beverly_data/gis"

URLS = {
    "pavements": f"{BASE}/pavement/pavements.geojson",
    "assets":    f"{BASE}/assets/aboveGroundAssets.geojson",
}

CONDITION_PENALTY = {"Poor": 3, "Fair": 1, "Good": 0, "Cant Verify": 1, "None": 1}


def fetch(url):
    print(f"  Fetching {url.split('/')[-1]}...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("features", [])


def compute_risk(pci, condition):
    """Simple 0–10 risk score. Higher = worse."""
    pavement_risk  = (100 - (pci or 50)) / 10
    asset_penalty  = CONDITION_PENALTY.get(condition, 1)
    return round(min(pavement_risk + asset_penalty, 10), 1)


def load_and_score():
    col = db["segments"]
    col.drop()  # fresh load each run

    pavements = fetch(URLS["pavements"])
    assets    = fetch(URLS["assets"])

    # Build a quick lookup: street name → worst asset condition
    asset_conditions = {}
    for f in assets:
        p = f["properties"]
        if p.get("asset_type") in ("SIDEWALK", "CURB_CUT", "RAMP"):
            # Use lat/lon as rough street key (round to 3 decimals)
            key = f"{round(p.get('lat',0) or f['geometry']['coordinates'][1], 2)}"
            cond = p.get("condition") or p.get("Condition") or "None"
            # Keep worst condition seen
            rank = {"Poor": 0, "Fair": 1, "Good": 2, "None": 3}
            if key not in asset_conditions or rank.get(cond, 3) < rank.get(asset_conditions[key], 3):
                asset_conditions[key] = cond

    docs = []
    for f in pavements:
        p      = f["properties"]
        pci    = p.get("score")
        label  = p.get("label", "Unknown")
        street = p.get("address_st", "Unknown Street")
        lat    = p.get("lat", 0)
        lon    = p.get("lon", 0)

        # Find nearest asset condition
        key   = f"{round(lat, 2)}"
        cond  = asset_conditions.get(key, "None")
        risk  = compute_risk(pci, cond)

        doc = {
            "street":     street,
            "pci":        pci,
            "pci_label":  label,
            "condition":  cond,
            "risk_score": risk,
            "lat":        lat,
            "lon":        lon,
            "geometry":   f["geometry"],   # keep original LineString
            "color":      risk_to_color(risk),
        }
        docs.append(doc)

    col.insert_many(docs)
    print(f"\n✅ Loaded {len(docs)} segments into MongoDB")
    print(f"   Avg risk: {sum(d['risk_score'] for d in docs)/len(docs):.1f}/10")
    print(f"   High risk (>7): {sum(1 for d in docs if d['risk_score'] > 7)}")
    return docs


def risk_to_color(score):
    """Green → Yellow → Red based on risk score 0–10."""
    if score <= 3:   return "#22c55e"   # green
    elif score <= 5: return "#eab308"   # yellow
    elif score <= 7: return "#f97316"   # orange
    else:            return "#ef4444"   # red


def get_top5():
    return list(db["segments"].find(
        {}, {"_id": 0}
    ).sort("risk_score", -1).limit(5))


def get_all_geojson():
    """Return all segments as GeoJSON FeatureCollection for the map."""
    features = []
    for doc in db["segments"].find({}, {"_id": 0}):
        features.append({
            "type": "Feature",
            "geometry": doc["geometry"],
            "properties": {
                "street":     doc["street"],
                "pci":        doc["pci"],
                "pci_label":  doc["pci_label"],
                "condition":  doc["condition"],
                "risk_score": doc["risk_score"],
                "color":      doc["color"],
            }
        })
    return {"type": "FeatureCollection", "features": features}


if __name__ == "__main__":
    load_and_score()
    print("\nTop 5 riskiest streets:")
    for s in get_top5():
        print(f"  {s['street']}: risk={s['risk_score']}, PCI={s['pci']}, condition={s['condition']}")