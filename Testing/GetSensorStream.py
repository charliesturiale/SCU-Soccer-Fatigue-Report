import requests, os
import pandas as pd

#!/usr/bin/env python3

def main():
    wsoc_apikey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJhN2U5MjBhN2RhMDJkMjFkMDVhNTljN2E0ZTVmYzAxMDkyMGE0NDRhNTY2Mjg1ZDJkMDk4Y2Y1OWQ5OWEwODhkYmQ0NTVjOGQyNGYxNzkwMyIsImlhdCI6MTczNzY2OTUxMC42MDI1OTUsIm5iZiI6MTczNzY2OTUxMC42MDI1OTcsImV4cCI6NDg5MTI2OTUxMC41OTMyODgsInN1YiI6IjdhMmQ3MDM4LTMxYzgtNDUxNS1iZTM4LTkwNDRiOWY3ZWQ0OSIsInNjb3BlcyI6WyJjb25uZWN0Iiwic2Vuc29yLXJlYWQtb25seSIsImF0aGxldGVzLXVwZGF0ZSIsInRhZ3MtdXBkYXRlIiwiYWN0aXZpdGllcy11cGRhdGUiLCJhbm5vdGF0aW9ucy11cGRhdGUiLCJwYXJhbWV0ZXJzLXVwZGF0ZSJdfQ.vUaUio4sdG1fxs9FgO1DHtSRDsInCUiHdbKh01o8o4AjzIcoKXcg_S0vnJr7yO5TWv5wn5_9wYbY4b3bO-48WDr3lZY4EsTEKNCwQe0PzyJ11JJEMZpqylisp2oM97PnqVZWIDXD1tatWrAq0i0BBX93TCfanvzQf93eVKEzCD89S8UwFW5gyrwTH2Zx0HcXUo7PIOJTc-Ie1NDoItAYrSyIfzkJxHV_vOadiTuHuDyl6DFuyJrPwJMJW4MGhb3l1L88HlYNw297ePeq1HOy8vl1cBgvjjpxqp9lTSGj1nZjUjGFkCT6jqh2DmZyrVr0F-Pi3QSwPIOV3ziYYeyenRLBDZddY50ioLC9kk5Y2tFOKmBT6rFK881ek7RyNwt0i8k-RSVCrcgm1QLnPCk6DeOWUPSquSQY5TrUmSboyCMss6mNml_dyw7Kme5uZhv37H8B82ZIal73q_zxIuoX6wvu-pUinrarCAfyDJi37T72gG1w9EndE5eF-uhHUINtVIRuRVPzNlPvGUsnBIoqsm_6N_jS4tQsDSPewGbzZ-PcX2bzL3j5QcSZuu0zJjCJQ740pjr1wqXVWW_qsPj-Fauq54aGfHTDJZfI0U2dW9E5TeG4-2yw3en9-79_IIVoi0u4fxMzI8FeeD_h1CCrNNDg5ZcuhpXpruucQhpR3fE"
    msoc_apikey = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJiZmMzNTMxOGFkNzJmZDE0ZDdjZTMyMmNiMWFmMmRhNGM2OWQ2YWY4YmIyZWU1MWFkZTFkNjIyMjU1YTE4MjhhZDA2OTY5YjU4YzRiYTBjZiIsImlhdCI6MTczNzUwMDE3Mi4zNDU2OCwibmJmIjoxNzM3NTAwMTcyLjM0NTY4MiwiZXhwIjo0ODkxMTAwMTcyLjMzNzE2OCwic3ViIjoiMWIxOThhY2UtZWM1NC00NTEwLWI2YjktM2UwMWYzMTgxNzYyIiwic2NvcGVzIjpbImNvbm5lY3QiLCJzZW5zb3ItcmVhZC1vbmx5IiwiYXRobGV0ZXMtdXBkYXRlIiwidGFncy11cGRhdGUiLCJhY3Rpdml0aWVzLXVwZGF0ZSIsImFubm90YXRpb25zLXVwZGF0ZSIsInBhcmFtZXRlcnMtdXBkYXRlIl19.KT5lz3tadAwMTR4A04nmFxQLPS8tlKEzGxm5os9TucX8aro1wsqTIFW77tvJITjsDeUuhRgNh9oZuc0bzIWJcNjx_zdht1hKH1bX0HodzhN5Nf791BezQOwPdtTOZH9fEZtCzT5auhh64Et4YC9S9_iu2YtUWr8NCVfi7acchY1oScZEm_Dqd_fewB3oza9auKmppDBtMkghLsS8zmINxGLxFCpZo3LTCiKZlbZGoRL_lLGRlrtxG_bVGFqsOoGYH9MjT0cBCYIDAOI8lHuQhmoedcFUW-qBb7szd3ol07b6jQB5glDHSTaoNtHwTXSlVkkUKKU6DKcXN60G7k5Fi8ky_m9eJSWWrrta6xX8RkX_xn_RZmSwSoUkAsXquGTIW0ujYmtd7d3c-M2UMBleXD3cUiY7xgAoac1C486SNESuKaTf58SQk-bzTL1V7HhnBzdr3OtE-8zPi1VeKzqEB3brvkWIwPwv9DJNDO-i6tG013PABcmRcwWN2ckelCY8EmfayMz5XdBJNBDjxfJB_5QSiXlFJpgvb4DuJ_fs2ktDfUNgoDL0-Jpxgqc228AzAa1kY4vkyKhkcG4AtqnwbKz334qKj_jB9kI-SAqVx00IyLO3rbbJxJlD2keUvKWsqXiDyllLhG93Zq0mZruooyS_ysHfIucaYoWZaoR-6sM"

    activity_id = "e92f11e7-6b5b-4629-a024-4a79d6cff89e" # vs Pepperdine 10/8/25
    athlete_id = "0353a909-21c6-473e-99da-19679a3cf628" # Malia Yamamoto
    
    sensor_api_url = f"https://connect-us.catapultsports.com/api/v6/activities/{activity_id}/athletes/{athlete_id}/sensor?frequency=1.0"

    used_api = sensor_api_url
    used_key = wsoc_apikey

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {used_key}"
    }

    try:
        response = requests.get(used_api, headers=headers)
        response.raise_for_status()

        # Step 1: Parse JSON
        data = response.json()  # gives you list of dicts
        # print(data)

        # Step 2: Convert to DataFrame
        df = pd.DataFrame(data)

        # Step 3: Export to Excel or CSV
        # df.to_excel("wsoc.xlsx", index=False)  # Excel
        df.to_csv("output/sensor.csv", index=False)     # CSV

        print("Data exported successfully!")

    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")

from typing import Any, Dict, Iterable, List, Tuple, Union
import math, ast

def _coerce_samples(obj: Any) -> List[dict]:
    """
    Accepts:
    - list[dict] of samples
    - dict with 'data': list[dict]
    - str representing a Python-literal list of dicts (e.g., CSV 'data' column)
    Returns list[dict] samples.
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
        return obj["data"]
    if isinstance(obj, str):
        # Catapult CSVs often store the samples as a Python-literal string with single quotes.
        try:
            parsed = ast.literal_eval(obj)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return []

def average_xy(
    sensor: Union[List[dict], Dict[str, Any], str],
    *,
    min_satellites: int = None,  # e.g., 6–7 for GNSS QC
    min_speed: float = None,     # m/s; e.g., 0.5 to drop near-stationary
    method: str = "time_weighted"  # 'uniform' or 'time_weighted'
) -> Dict[str, float]:
    """
    Compute average (x, y) from Catapult 10Hz sensor samples.

    Sample schema (per item in list):
    { 'ts': int (unix seconds), 'x': float, 'y': float, 'cs': int (sat count), 'v': float (m/s), ... }

    Returns:
    dict with x_mean, y_mean, n_used, n_total, method
    """
    samples = _coerce_samples(sensor)
    n_total = len(samples)

    # Apply optional QC/activity filters
    kept: List[dict] = []
    for s in samples:
        x = s.get("x"); y = s.get("y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            continue
        if min_satellites is not None:
            cs = s.get("cs")
            if not isinstance(cs, (int, float)) or cs < min_satellites:
                continue
        if min_speed is not None:
            v = s.get("v")
            if not isinstance(v, (int, float)) or v < min_speed:
                continue
        kept.append(s)

    if not kept:
        return {"x_mean": math.nan, "y_mean": math.nan, "n_used": 0, "n_total": n_total, "method": method}

    if method == "uniform":
        sx = sum(s["x"] for s in kept)
        sy = sum(s["y"] for s in kept)
        n = len(kept)
        return {"x_mean": sx / n, "y_mean": sy / n, "n_used": n, "n_total": n_total, "method": "uniform"}

    # time-weighted (default): weight each sample by the time until the next sample
    # If 'ts' is missing or non-monotonic, we fall back to uniform for that pair.
    kept_sorted = sorted(kept, key=lambda s: s.get("ts", 0))
    weights: List[float] = []
    xs: List[float] = []
    ys: List[float] = []

    for i, s in enumerate(kept_sorted):
        t = kept_sorted[i+1].get("ts") if i+1 < len(kept_sorted) else None
        ts = s.get("ts")
        # default to 1.0s if we can't compute a delta
        w = float(t - ts) if isinstance(ts, (int, float)) and isinstance(t, (int, float)) and t >= ts else 1.0
        weights.append(w)
        xs.append(s["x"] * w)
        ys.append(s["y"] * w)

    wsum = sum(weights) if sum(weights) > 0 else len(kept_sorted)
    x_mean = sum(xs) / wsum
    y_mean = sum(ys) / wsum
    return {"x_mean": x_mean, "y_mean": y_mean, "n_used": len(kept_sorted), "n_total": n_total, "method": "time_weighted"}






main()

df = pd.read_csv("output/sensor.csv")
row = df.iloc[0]                 # one athlete/activity per row in your export
result = average_xy(row["data"], min_satellites=6, min_speed=None, method="time_weighted")
print(result)  # {'x_mean': ..., 'y_mean': ..., 'n_used': ..., 'n_total': ..., 'method': ...}