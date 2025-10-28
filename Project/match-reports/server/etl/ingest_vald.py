from __future__ import annotations
import requests
from db import SessionLocal
from models import ValdTest
from config import load_config

def main(team_code="MSOC"):
    cfg = load_config()
    vald_key = cfg.secrets.get(f"{team_code}_VALD_KEY")
    if not vald_key:
        raise SystemExit(f"Missing {team_code}_VALD_KEY in data/secrets.json")

    headers = { "Authorization": f"Bearer {vald_key}" }
    url = f'{cfg.urls.get("valdNordBord","")}/tests?limit=10'
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    items = payload if isinstance(payload, list) else payload.get("items", [])

    with SessionLocal() as s:
        for row in items:
            s.add(ValdTest(team_code=team_code, test_id=str(row.get("testId")), payload=row))
        s.commit()

if __name__ == "__main__":
    main()
