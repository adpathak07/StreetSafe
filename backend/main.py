from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(title="StreetSafe Equity Lens")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

client = MongoClient(os.getenv("MONGO_URI"))
db = client["streetsafe"]


@app.on_event("startup")
async def startup():
    try:
        from backend.gemini import generate_top_insight
        from backend.voice import generate_voice
        top = list(db["segments"].find({}, {"_id": 0}).sort("risk_score", -1).limit(1))
        if top:
            print(f"🔊 Generating voice for: {top[0]['street']}")
            text = generate_top_insight(top[0])
            generate_voice(text)
            print("✅ Voice ready")
    except Exception as e:
        print(f"⚠ Startup warning (non-fatal): {e}")


@app.get("/")
def root():
    return FileResponse("frontend/index.html")


@app.get("/api/segments")
def segments():
    try:
        features = []
        for doc in db["segments"].find({}, {"_id": 0}):
            if not doc.get("geometry"):
                continue
            features.append({
                "type": "Feature",
                "geometry": doc["geometry"],
                "properties": {
                    "street":     doc.get("street", "Unknown"),
                    "pci":        doc.get("pci"),
                    "pci_label":  doc.get("pci_label", ""),
                    "condition":  doc.get("condition", ""),
                    "risk_score": doc.get("risk_score", 5),
                    "color":      doc.get("color", "#eab308"),
                }
            })
        return {"type": "FeatureCollection", "features": features}
    except Exception as e:
        print(f"❌ Segments error: {e}")
        return {"type": "FeatureCollection", "features": []}


@app.get("/api/top5")
def top5():
    try:
        # Get worst segment per unique street name
        pipeline = [
            {"$sort": {"risk_score": -1}},
            {"$group": {
                "_id": "$street",
                "street":     {"$first": "$street"},
                "pci":        {"$first": "$pci"},
                "pci_label":  {"$first": "$pci_label"},
                "condition":  {"$first": "$condition"},
                "risk_score": {"$first": "$risk_score"},
                "lat":        {"$first": "$lat"},
                "lon":        {"$first": "$lon"},
                "color":      {"$first": "$color"},
            }},
            {"$sort": {"risk_score": -1}},
            {"$limit": 5},
            {"$project": {"_id": 0}}
        ]
        return list(db["segments"].aggregate(pipeline))
    except Exception as e:
        print(f"❌ Top5 error: {e}")
        return []


@app.get("/api/equity")
def equity():
    try:
        beverly  = db["census"].find_one({"city": "Beverly"},  {"_id": 0})
        lawrence = db["census"].find_one({"city": "Lawrence"}, {"_id": 0})
        if not beverly or not lawrence:
            return {}
        return {
            "beverly":  beverly,
            "lawrence": lawrence,
            "insight": (
                f"Beverly is {100 - (beverly['foreign_born_pct'] or 0):.0f}% US-born "
                f"with median income ${beverly['median_household_income']:,.0f}. "
                f"Lawrence — just 12 miles away — is {lawrence['foreign_born_pct']:.0f}% "
                f"foreign-born with median income ${lawrence['median_household_income']:,.0f}. "
                f"Both cities share similar road infrastructure challenges, "
                f"but Lawrence residents have far fewer resources to absorb the impact."
            )
        }
    except Exception as e:
        print(f"❌ Equity error: {e}")
        return {}


class ExplainRequest(BaseModel):
    street:     str
    pci:        float = 50
    pci_label:  str = "Unknown"
    condition:  str = "Unknown"
    risk_score: float = 5.0


@app.post("/api/explain")
def explain(req: ExplainRequest):
    try:
        from backend.gemini import explain_street
        text = explain_street(
            street     = req.street,
            pci        = req.pci,
            pci_label  = req.pci_label,
            condition  = req.condition,
            risk_score = req.risk_score,
        )
        return {"explanation": text}
    except Exception as e:
        print(f"❌ Explain error: {e}")
        return {"explanation": f"Analysis temporarily unavailable: {str(e)}"}


@app.get("/api/voice")
def voice():
    path = "static/top_insight.mp3"
    if os.path.exists(path):
        return FileResponse(path, media_type="audio/mpeg")
    raise HTTPException(404, "Audio not ready")