"""
datafilter.py — minimal Catapult puller

• Requires the env var  MSOC_API_KEY  to hold a valid “Read activities & sensor” token.
• Adjust REGION if your OpenField Cloud URL is connect-eu or connect-au.
"""

import os
import json
import time
import requests
import dotenv
from pathlib import Path


dotenv.load_dotenv()
TOKEN   = os.getenv("MSOC_API_KEY")
REGION  = "us"        
BASE    = f"https://connect-{REGION}.catapultsports.com/api/v6"
HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}
DATE    = "2024-08-16" # 16/8/24 Game
OUTDIR  = Path("athlete-activity-data")
OUTDIR.mkdir(exist_ok=True)

def get_activities(date: str):
    """Return list of activity dicts that fall on a specific calendar day."""
    params = {"start_date": date, "end_date": date}
    r = requests.get(f"{BASE}/activities", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("items", [])

def get_roster(activity_id: int):
    """Return list of athlete dicts for a given activity."""
    r = requests.get(f"{BASE}/activities/{activity_id}/athletes",
                    headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("items", [])

def get_sensor(activity_id: int, athlete_id: int, stream="gps"):
    """Return the raw sensor JSON for one athlete in one session."""
    url = f"{BASE}/activities/{activity_id}/athletes/{athlete_id}/sensor"
    r = requests.get(url, headers=HEADERS, params={"stream_type": stream}, timeout=60)
    r.raise_for_status()
    return r.json()

#  Main flow
def main():
    if not TOKEN:
        raise RuntimeError("MSOC_API_KEY is not set in the environment.")

    activities = get_activities(DATE)
    if not activities:
        print(f"No activities found on {DATE}.")
        return

    print(f"{len(activities)} activity/activities found on {DATE}.")

    for act in activities:
        act_id = act["id"]
        roster = get_roster(act_id)
        if not roster:
            print(f"  Activity {act_id}: no athletes returned.")
            continue

        print(f"  Activity {act_id}: {len(roster)} athletes.")

        for ath in roster:
            ath_id = ath["id"]
            try:
                payload = get_sensor(act_id, ath_id)
            except Exception as exc:
                print(f"    Athlete {ath_id}: error → {exc}")
                continue

            outfile = OUTDIR / f"{DATE}_{ath_id}_{act_id}.json"
            with outfile.open("w") as f:
                json.dump(payload, f)
            print(f"    Athlete {ath_id}: saved → {outfile.name}")

            time.sleep(0.5)  # stay under the 60-req/min throttle

if __name__ == "__main__":
    main()