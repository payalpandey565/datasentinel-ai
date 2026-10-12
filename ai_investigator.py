import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google import genai


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports"

load_dotenv(BASE_DIR / ".env")

MODEL_NAME = "gemini-3.8-flash"


# --------------------------------------------------
# Find the latest data-quality report
# --------------------------------------------------

def get_latest_quality_report():
    """Find and load the most recent data-quality report."""

    report_files = list(
        REPORTS_DIR.glob("data_quality_report_*.json")
    )

    if not report_files:
        raise FileNotFoundError(
            "No data-quality reports found. "
            "Run 'python pipeline.py' first."
        )

    latest_report = max(
        report_files,
        key=lambda file: file.stat().st_mtime
    )

    with latest_report.open("r", encoding="utf-8") as file:
        report_data = json.load(file)

    return latest_report, report_data


# --------------------------------------------------
# AI Investigation Engine
# --------------------------------------------------

def investigate_data_quality(report_data):
    """
    Use Gemini to investigate data-quality alerts.

    AI findings are hypotheses, not confirmed root causes.
    """

    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise ValueError(
            "GEMINI_API_KEY is missing. "
            "Check your .env file."
        )

    client = genai.Client(api_key=api_key)

    # Limit the input to the quality report rather than
    # sending all raw product and cart records.
    report_json = json.dumps(
        report_data,
        indent=2,
        ensure_ascii=False,
        default=str
    )

    if len(report_json) > 15000:
        report_json = report_json[:15000] + "\n[Report truncated]"

    prompt = f"""
You are the AI Investigation Engine for DataSentinel AI,
a data engineering and data quality monitoring platform.

Your task is to investigate the supplied data-quality report.

IMPORTANT RULES:
- Use only evidence available in the report.
- Never invent metrics, records, or root causes.
- Clearly label possible explanations as hypotheses.
- Do not claim that an anomaly is definitely an error
  without sufficient evidence.
- If information is missing, explicitly say so.
- Recommend practical, actionable data engineering checks.
- Distinguish immediate remediation from long-term prevention.

Analyze the report using these sections:

1. EXECUTIVE SUMMARY
   Summarize the overall data-quality status.

2. ALERT INVESTIGATION
   For each significant alert, explain:
   - What was detected?
   - What evidence supports the alert?
   - What could explain it?
   - What additional checks would confirm the explanation?

3. ROOT-CAUSE HYPOTHESES
   Rank plausible explanations by priority.
   Explain the evidence and uncertainty for each.

4. RECOMMENDED ACTIONS
   Provide prioritized, practical remediation steps.

5. PIPELINE PREVENTION
   Recommend validation rules, monitoring checks,
   or pipeline improvements to prevent recurrence.

6. INVESTIGATION LIMITATIONS
   State what cannot be concluded from the available evidence.

DATA-QUALITY REPORT:
{report_json}
"""

    response = client.interactions.create(
        model=MODEL_NAME,
        input=prompt
    )

    result = response.output_text

    if not result:
        raise RuntimeError(
            "The AI returned an empty investigation."
        )

    return result


# --------------------------------------------------
# Run an investigation from the terminal
# --------------------------------------------------

def run_ai_investigation():
    """Load the latest report and generate AI findings."""

    report_path, report_data = get_latest_quality_report()

    print("\n" + "=" * 60)
    print("DATASENTINEL AI — INVESTIGATION ENGINE")
    print("=" * 60)

    print(f"\nReport: {report_path.name}")
    print("\nInvestigating data-quality alerts...")

    findings = investigate_data_quality(report_data)

    print("\n" + "-" * 60)
    print("AI INVESTIGATION FINDINGS")
    print("-" * 60)
    print(findings)

    return findings


if __name__ == "__main__":
    run_ai_investigation()