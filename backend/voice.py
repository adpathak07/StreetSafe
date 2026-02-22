import requests
import os
from dotenv import load_dotenv

load_dotenv()

ELEVEN_KEY = os.getenv("ELEVENLABS_API_KEY")
VOICE_ID   = "21m00Tcm4TlvDq8ikWAM"  # Rachel voice
OUT_PATH   = "static/top_insight.mp3"


def generate_voice(text: str):
    if not ELEVEN_KEY:
        print("⚠ No ElevenLabs key — skipping voice")
        return False

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"
    headers = {
        "xi-api-key":   ELEVEN_KEY,
        "Content-Type": "application/json",
        "Accept":       "audio/mpeg",
    }
    body = {
        "text":       text[:500],  # limit length
        "model_id":   "eleven_turbo_v2_5",
        "voice_settings": {
            "stability":        0.5,
            "similarity_boost": 0.75
        }
    }
    try:
        r = requests.post(url, headers=headers, json=body, timeout=30)
        r.raise_for_status()
        os.makedirs("static", exist_ok=True)
        with open(OUT_PATH, "wb") as f:
            f.write(r.content)
        print(f"✅ Voice saved → {OUT_PATH} ({len(r.content)} bytes)")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"⚠ ElevenLabs HTTP error: {e.response.status_code} — {e.response.text}")
        return False
    except Exception as e:
        print(f"⚠ ElevenLabs error: {e}")
        return False