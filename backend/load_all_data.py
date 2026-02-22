"""
Run this ONCE before starting the server.
Fetches Beverly MA Cyvl GeoJSON from CDN → stores in MongoDB.
"""

import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["streetsafe"]

BASE = "https://dcygqrjfsypox.cloudfront.net/cyvl_public_data/11_2025/beverly_data"

FILES = {
    "pavement_segments": f"{BASE}/gis/pavement/pavements.geojson",
    "assets":            f"{BASE}/gis/assets/aboveGroundAssets.geojson",
    "signs":             f"{BASE}/gis/assets/signs.geojson",
    # Note: distresses file skipped (too large for free tier MongoDB)
}

# Map PCI label → risk score (more accurate than inverting PCI number)
LABEL_RISK = {
    "Failed":       10,
    "Very Poor":     9,
    "Serious":       8,
    "Poor":          7,
    "Fair":          5,
    "Satisfactory":  3,
    "Good":          2,
    "Not Scored":    5,
    "Unknown":       5,
}

def pci_label_from_score(pci):
    """Override label using actual PCI number when available."""
    if pci is None: return None
    if pci >= 85:   return "Good"
    if pci >= 70:   return "Satisfactory"
    if pci >= 55:   return "Fair"
    if pci >= 40:   return "Poor"
    if pci >= 25:   return "Very Poor"
    if pci >= 10:   return "Serious"
    return "Failed"

CONDITION_PENALTY = {"Poor": 2, "Fair": 1, "Good": 0, "Cant Verify": 1, "None": 0}


def fetch_and_store(collection_name, url):
    print(f"\n📥 Fetching {collection_name}...")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        features = r.json().get("features", [])
        if not features:
            print(f"   ⚠ No features found")
            return 0
        docs = []
        for f in features:
            doc = {
                "geometry": f.get("geometry"),
                **f.get("properties", {}),
            }
            docs.append(doc)
        col = db[collection_name]
        col.drop()
        col.insert_many(docs)
        print(f"   ✅ Stored {len(docs)} records → '{collection_name}'")
        return len(docs)
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return 0


def compute_risk(pci, label, condition):
    # Use PCI number to get accurate label when available
    accurate_label = pci_label_from_score(pci) if pci else label
    base_risk      = LABEL_RISK.get(accurate_label, 5)
    asset_penalty  = CONDITION_PENALTY.get(condition, 0)
    return round(min(base_risk + asset_penalty, 10), 1), accurate_label or label


def risk_to_color(score):
    if score <= 3:   return "#22c55e"   # green
    elif score <= 5: return "#eab308"   # yellow
    elif score <= 7: return "#f97316"   # orange
    else:            return "#ef4444"   # red


def score_segments():
    print("\n⚙️  Scoring street segments...")
    pavements = list(db["pavement_segments"].find({}))
    if not pavements:
        print("   ❌ No pavement data found")
        return

    docs = []
    for p in pavements:
        pci       = p.get("score")
        label     = p.get("label", "Unknown")
        condition = p.get("condition", "None")
        risk, accurate_label = compute_risk(pci, label, condition)

        doc = {
            "street":     p.get("address_st", "Unknown Street"),
            "pci":        round(pci, 1) if pci else None,
            "pci_label":  accurate_label,
            "condition":  condition,
            "risk_score": risk,
            "lat":        p.get("lat", 0),
            "lon":        p.get("lon", 0),
            "geometry":   p.get("geometry"),
            "color":      risk_to_color(risk),
        }
        docs.append(doc)

    db["segments"].drop()
    db["segments"].insert_many(docs)

    avg  = sum(d["risk_score"] for d in docs) / len(docs)
    high = sum(1 for d in docs if d["risk_score"] > 7)
    print(f"   ✅ Scored {len(docs)} segments")
    print(f"   📊 Avg risk: {avg:.1f}/10 | Critical segments: {high}")

    print("\n🏆 Top 5 Riskiest Streets:")
    top5 = sorted(docs, key=lambda x: x["risk_score"], reverse=True)[:5]
    for i, s in enumerate(top5):
        print(f"   {i+1}. {s['street']} — Risk: {s['risk_score']}/10 "
              f"| Label: {s['pci_label']} | PCI: {s['pci']}")


def load_census_basic():
    print("\n📊 Loading census data...")
    docs = [
        {
            "city": "Beverly",
            "foreign_born_pct": 8.2,
            "median_household_income": 74500,
            "total_population": 43068,
        },
        {
            "city": "Lawrence",
            "foreign_born_pct": 42.1,
            "median_household_income": 34200,
            "total_population": 80376,
        },
    ]
    db["census"].drop()
    db["census"].insert_many(docs)
    print("   ✅ Census data loaded")


if __name__ == "__main__":
    print("🚀 StreetSafe Data Loader")
    print("=" * 45)

    total = 0
    for name, url in FILES.items():
        total += fetch_and_store(name, url)

    print(f"\n✅ Total raw records: {total}")
    score_segments()
    load_census_basic()

    print("\n🎉 Done! Now run:")
    print("   uvicorn backend.main:app --reload --port 8000")