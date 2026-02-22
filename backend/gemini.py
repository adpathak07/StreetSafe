import os
from dotenv import load_dotenv
from google import genai

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

MODEL = "gemini-3-flash-preview"


def explain_street(street, pci, pci_label, condition, risk_score):
    prompt = f"""
You are a civic infrastructure analyst advising a city planner.

Street: {street}
Pavement Condition Index (PCI): {pci}/100 — rated "{pci_label}" (lower = worse)
Sidewalk/Asset Condition: {condition}
Overall Risk Score: {risk_score}/10 (higher = more urgent)

In exactly 3 short sentences:
1. Describe the infrastructure problem using the numbers.
2. Explain who is most harmed if this stays unfixed.
3. Give one specific actionable recommendation.

Be direct. Cite the numbers. No filler words.
"""
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        return f"Analysis unavailable: {str(e)}"


def generate_top_insight(top_street):
    prompt = f"""
Write a 2-sentence civic report narration suitable for text-to-speech.
Beverly MA worst street: {top_street['street']}.
PCI: {top_street['pci']}/100 rated {top_street['pci_label']}, Risk: {top_street['risk_score']}/10.
No bullet points or special characters. Plain sentences only.
"""
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        return (f"{top_street['street']} is Beverly's highest risk street "
                f"with a risk score of {top_street['risk_score']} out of 10. "
                f"Immediate city investment is recommended.")