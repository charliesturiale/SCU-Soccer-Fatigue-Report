from __future__ import annotations
import requests, os, time, math, json, requests
import pandas as pd
import numpy as np
from typing import Any, Iterable, List, Dict, Union, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.cluster import DBSCAN
from shapely.geometry import Polygon, Point, MultiPoint
from math import atan2, cos, sin, degrees, isfinite
import matplotlib.pyplot as plt
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Catapult keys
WSOC_APIKEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJhN2U5MjBhN2RhMDJkMjFkMDVhNTljN2E0ZTVmYzAxMDkyMGE0NDRhNTY2Mjg1ZDJkMDk4Y2Y1OWQ5OWEwODhkYmQ0NTVjOGQyNGYxNzkwMyIsImlhdCI6MTczNzY2OTUxMC42MDI1OTUsIm5iZiI6MTczNzY2OTUxMC42MDI1OTcsImV4cCI6NDg5MTI2OTUxMC41OTMyODgsInN1YiI6IjdhMmQ3MDM4LTMxYzgtNDUxNS1iZTM4LTkwNDRiOWY3ZWQ0OSIsInNjb3BlcyI6WyJjb25uZWN0Iiwic2Vuc29yLXJlYWQtb25seSIsImF0aGxldGVzLXVwZGF0ZSIsInRhZ3MtdXBkYXRlIiwiYWN0aXZpdGllcy11cGRhdGUiLCJhbm5vdGF0aW9ucy11cGRhdGUiLCJwYXJhbWV0ZXJzLXVwZGF0ZSJdfQ.vUaUio4sdG1fxs9FgO1DHtSRDsInCUiHdbKh01o8o4AjzIcoKXcg_S0vnJr7yO5TWv5wn5_9wYbY4b3bO-48WDr3lZY4EsTEKNCwQe0PzyJ11JJEMZpqylisp2oM97PnqVZWIDXD1tatWrAq0i0BBX93TCfanvzQf93eVKEzCD89S8UwFW5gyrwTH2Zx0HcXUo7PIOJTc-Ie1NDoItAYrSyIfzkJxHV_vOadiTuHuDyl6DFuyJrPwJMJW4MGhb3l1L88HlYNw297ePeq1HOy8vl1cBgvjjpxqp9lTSGj1nZjUjGFkCT6jqh2DmZyrVr0F-Pi3QSwPIOV3ziYYeyenRLBDZddY50ioLC9kk5Y2tFOKmBT6rFK881ek7RyNwt0i8k-RSVCrcgm1QLnPCk6DeOWUPSquSQY5TrUmSboyCMss6mNml_dyw7Kme5uZhv37H8B82ZIal73q_zxIuoX6wvu-pUinrarCAfyDJi37T72gG1w9EndE5eF-uhHUINtVIRuRVPzNlPvGUsnBIoqsm_6N_jS4tQsDSPewGbzZ-PcX2bzL3j5QcSZuu0zJjCJQ740pjr1wqXVWW_qsPj-Fauq54aGfHTDJZfI0U2dW9E5TeG4-2yw3en9-79_IIVoi0u4fxMzI8FeeD_h1CCrNNDg5ZcuhpXpruucQhpR3fE"
MSOC_APIKEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0NjFiMTExMS02ZjdhLTRkYmItOWQyOS0yMzAzOWZlMjI4OGUiLCJqdGkiOiJiZmMzNTMxOGFkNzJmZDE0ZDdjZTMyMmNiMWFmMmRhNGM2OWQ2YWY4YmIyZWU1MWFkZTFkNjIyMjU1YTE4MjhhZDA2OTY5YjU4YzRiYTBjZiIsImlhdCI6MTczNzUwMDE3Mi4zNDU2OCwibmJmIjoxNzM3NTAwMTcyLjM0NTY4MiwiZXhwIjo0ODkxMTAwMTcyLjMzNzE2OCwic3ViIjoiMWIxOThhY2UtZWM1NC00NTEwLWI2YjktM2UwMWYzMTgxNzYyIiwic2NvcGVzIjpbImNvbm5lY3QiLCJzZW5zb3ItcmVhZC1vbmx5IiwiYXRobGV0ZXMtdXBkYXRlIiwidGFncy11cGRhdGUiLCJhY3Rpdml0aWVzLXVwZGF0ZSIsImFubm90YXRpb25zLXVwZGF0ZSIsInBhcmFtZXRlcnMtdXBkYXRlIl19.KT5lz3tadAwMTR4A04nmFxQLPS8tlKEzGxm5os9TucX8aro1wsqTIFW77tvJITjsDeUuhRgNh9oZuc0bzIWJcNjx_zdht1hKH1bX0HodzhN5Nf791BezQOwPdtTOZH9fEZtCzT5auhh64Et4YC9S9_iu2YtUWr8NCVfi7acchY1oScZEm_Dqd_fewB3oza9auKmppDBtMkghLsS8zmINxGLxFCpZo3LTCiKZlbZGoRL_lLGRlrtxG_bVGFqsOoGYH9MjT0cBCYIDAOI8lHuQhmoedcFUW-qBb7szd3ol07b6jQB5glDHSTaoNtHwTXSlVkkUKKU6DKcXN60G7k5Fi8ky_m9eJSWWrrta6xX8RkX_xn_RZmSwSoUkAsXquGTIW0ujYmtd7d3c-M2UMBleXD3cUiY7xgAoac1C486SNESuKaTf58SQk-bzTL1V7HhnBzdr3OtE-8zPi1VeKzqEB3brvkWIwPwv9DJNDO-i6tG013PABcmRcwWN2ckelCY8EmfayMz5XdBJNBDjxfJB_5QSiXlFJpgvb4DuJ_fs2ktDfUNgoDL0-Jpxgqc228AzAa1kY4vkyKhkcG4AtqnwbKz334qKj_jB9kI-SAqVx00IyLO3rbbJxJlD2keUvKWsqXiDyllLhG93Zq0mZruooyS_ysHfIucaYoWZaoR-6sM"

# Input activity id to gather stats for
# ACTIVITY_ID = "e92f11e7-6b5b-4629-a024-4a79d6cff89e" # vs Pepperdine 10/8/25
ACTIVITY_ID = "e299a017-7ce2-475b-ade4-61385572a6d6" # vs Seattle 10/4/25
# ACTIVITY_ID = "9c67d53c-0f8c-47c9-9edd-7af2945f0be2" # vs Cal 4/13/25
# ACTIVITY_ID = "4684fb68-7354-4430-8bb3-4efe5af180ae" # vs Texas Tech 8/21/25

# Which key are we using?
USED_KEY = WSOC_APIKEY

# Catapult sensor stream frequency - how many data points per sec? (Hz)
FREQUENCY: float = 1.0

LA = ZoneInfo("America/Los_Angeles")

Record = Dict[str, Any]

#!/usr/bin/env python3

def main():
    data = get_activity_json()
    activity_summary = parse_activity_json(data)           # list[dict]
    raw_samples = fetch_all_sensor_streams(activity_summary, frequency=FREQUENCY)

    # Clean + select top-18 + infer times
    df_clean, samples_xy, times, kept_athletes = refine_points_v2(
        raw_samples,
        activity_summary,
        k_keep=18,
        expected_players=11,
        min_active_speed=1.1,
        dbscan_eps=2.8,
        dbscan_min_samples=120,
        pre_kick_prior_min=(30*60, 55*60),
        halftime_len_bounds=(10*60, 25*60),
        buffers=(90, 120),
    )

    # Fit orientation + polygon on the CLEAN samples
    theta_best, rect_best = refine_angle_by_edges(
        samples_xy,
        theta_seed=None,
        sweep_deg=25,
        step_deg=0.25,
        qy=0.03, qx=0.03,
        band=2.0,
        penalty=2.0,
    )

    # Visual QA
    plot_overlay(samples_xy, rect_best, theta_best)

    print("TIMES: ", times)
    print("KEPT ATHLETES: ", kept_athletes)

    # Clean up times
    for label in ["kickoff", "halftime_start", "halftime_end", "fulltime"]:
        ts = times.get(label)
        if ts is None:
            print(f"{label}: None")
        else:
            utc_str, la_str = fmt(ts)
            print(f"{label}: UTC {utc_str}   |   LA {la_str}")

    return 0






def _headers(api_key: str) -> Dict[str, str]:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

def get_activity_json():
    print("Retrieving activity info from API...")
    headers = _headers(USED_KEY)
    payload = {
        "parameters": ["total_distance", "average_player_load"],
        "filters": [
            {
                "comparison": "=",
                "name": "activity_id",
                "values": [ACTIVITY_ID]
            }
        ],
        "group_by": ["athlete"],
        "source": "cached_stats"
    }

    activityStats_url = "https://connect-us.catapultsports.com/api/v6/stats"
    try:
        response = requests.post(activityStats_url, json=payload, headers=headers)
        response.raise_for_status()

        # Step 1: Parse JSON
        data = response.json()  # gives you list of dicts

        # Step 2: Convert to DataFrame
        # df = pd.DataFrame(data)
        return data

    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
        return err
    
def parse_activity_json(
    payload: Union[str, Iterable[Record]],
) -> List[Record]:
    """
    Extract athlete_id, start_time, end_time, total_distance from Catapult daily stats JSON.

    Args:
        payload: list[dict] or JSON string representing that list.
        aggregate: if True, combine multiple rows per athlete_id:
        start_time = min, end_time = max, total_distance = sum.

    Returns:
        List of dicts with keys: athlete_id, start_time, end_time, total_distance.
    """
    print("Parsing ativity data...")
    # Coerce input to list[dict]
    if isinstance(payload, str):
        data = json.loads(payload)
    else:
        data = list(payload)

    out: List[Record] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        aid = row.get("athlete_id")
        nm = row.get("athlete_name")
        st  = row.get("start_time")
        et  = row.get("end_time")
        td  = row.get("total_distance")
        if aid is None or nm is None or st is None or et is None or td is None:
            # skip incomplete rows
            continue
        try:
            out.append({
                "athlete_name": str(nm),
                "athlete_id": str(aid),
                "start_time": float(st),        # unix seconds (as in your sample)
                "end_time": float(et),
                "total_distance": float(td),    # meters in your sample
            })
        except (TypeError, ValueError):
            # skip rows with non-coercible types
            continue
    return out


def get_ath_sensor_data(athlete_id):
    headers = _headers(USED_KEY)
    url = f"https://connect-us.catapultsports.com/api/v6/activities/{ACTIVITY_ID}/athletes/{athlete_id}/sensor"
    params = {"frequency": str(FREQUENCY)}
    s= requests.Session()

    timeout: int = 60
    max_retries: int = 4
    retry_backoff: float = 1.5

    for attempt in range(max_retries):
        try:
            response = requests.get(url=url, headers=headers, params=params)
            response.raise_for_status()

            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{response.status_code} {response.reason}")
            
            data = response.json()

            if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                return data["data"]
            if isinstance(data, list):
                return data
            return list(data) if data else []
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                    raise
            sleep_s = (retry_backoff ** attempt) + (0.1 * attempt)
            time.sleep(sleep_s)

    return []


def build_xy(parsed):
    # CONFIG:
    min_speed = 0.8
    pts = []

    print("Collecting x and y values...")
    print("Working on athlete number:")
    counter = 1
    for athlete in parsed:
        ath_stream = get_ath_sensor_data(athlete["athlete_id"])
        if not ath_stream:
            continue
        ath_data = ath_stream[0].get('data', []) or []

        print(counter)
        counter+=1

        for snapshot in ath_data:
            # Skip invalid coords
            lat = snapshot.get("lat")
            lon = snapshot.get("long")
            x   = snapshot.get("x")
            y   = snapshot.get("y")
            v   = snapshot.get("v", 0.0)  # m/s

            # ignore clearly invalid points
            if (lat in (None, 0) and x in (None, 0)) or (lon in (None, 0) and y in (None, 0)):
                continue

            # Light activity filter to reduce bench/locker-room contamination
            if v is None or v < min_speed:
                continue

            # Prefer native meter coords if available
            if x not in (None, 0) and y not in (None, 0):
                pts.append((float(x), float(y)))
                continue

            # Fallback: convert lat/lon to local meters
            # if lat in (None, 0) or lon in (None, 0):
            #     continue
            # if origin_lat is None:
            #     origin_lat, origin_lon = float(lat), float(lon)
            # X, Y = latlon_to_local(float(lat), float(lon), origin_lat, origin_lon)
            # pts.append((X, Y))

    if not pts:
        return np.empty((0, 2), dtype=float)

    return np.array(pts, dtype=float)



def rotate_pts(X, angle, origin):
    c, s = cos(angle), sin(angle)
    R = np.array([[c, -s],[s, c]])
    return (X - origin) @ R.T + origin

def plot_overlay(X, rect, angle):
    print("Plotting overlay...")

    ctr = rect.mean(axis=0)
    poly = np.vstack([rect, rect[0]])
    idx = np.linspace(0, len(X)-1, min(len(X), 20000)).astype(int)

    plt.figure(figsize=(6,6))
    plt.scatter(X[idx,0], X[idx,1], s=1, alpha=0.5)
    plt.plot(poly[:,0], poly[:,1], lw=2)
    ray = np.array([cos(angle), sin(angle)])*40
    plt.plot([ctr[0], ctr[0]+ray[0]], [ctr[1], ctr[1]+ray[1]], lw=2)
    plt.axis('equal'); plt.title(f"Raw (angle ≈ {degrees(angle):.1f}°)"); plt.tight_layout(); plt.show()

    Xr = rotate_pts(X, -angle, ctr)
    rect_r = rotate_pts(rect, -angle, ctr)
    poly_r = np.vstack([rect_r, rect_r[0]])
    plt.figure(figsize=(6,6))
    plt.scatter(Xr[idx,0], Xr[idx,1], s=1, alpha=0.5)
    plt.plot(poly_r[:,0], poly_r[:,1], lw=2)
    dims = np.ptp(rect_r, axis=0)
    plt.title(f"Rotated (≈ {dims[0]:.1f}×{dims[1]:.1f} m)"); plt.axis('equal'); plt.tight_layout(); plt.show()



def rotate(X, theta, ctr):
    c, s = cos(theta), sin(theta)
    R = np.array([[c, -s],[s, c]])
    return (X - ctr) @ R.T + ctr

def edge_score_for_angle(X, theta, ctr, qy=0.02, qx=0.02, band=2.0, penalty=2.0):
    """
    Score how well angle 'theta' aligns the cloud with a rectangle by
    counting points in thin bands near top/bottom (y) and right (x) edges.
    """
    Xr = rotate(X, -theta, ctr)
    x, y = Xr[:,0], Xr[:,1]

    # Trim a little to ignore outliers when locating edges
    y_lo, y_hi = np.quantile(y, [qy, 1-qy])
    x_lo, x_hi = np.quantile(x, [qx, 1-qx])

    # Count points near edges (within 'band' meters)
    near_bot = (y >= y_lo - band) & (y <= y_lo + band)
    near_top = (y >= y_hi - band) & (y <= y_hi + band)
    near_right = (x >= x_hi - band) & (x <= x_hi + band)

    # Penalize points that fall clearly outside the three edges
    outside_y = (y < y_lo - band) | (y > y_hi + band)
    outside_x = (x > x_hi + band)  # right-side tail (locker-room/conditioning)
    # (left edge is often messy with the tunnel—don’t penalize it as hard)

    score = near_bot.sum() + near_top.sum() + 0.7*near_right.sum()
    score -= penalty * (outside_y.sum() + 0.7*outside_x.sum())
    return score, (x_lo, x_hi, y_lo, y_hi)

def refine_angle_by_edges(X, theta_seed=None, sweep_deg=30, step_deg=0.25, **kw):
    """
    Grid-search around theta_seed (or around 0 if None) for the best edge score.
    Returns best_theta, rect4x2 (in original frame).
    """
    print("Refining field rectangle...")

    ctr = X.mean(axis=0)
    if theta_seed is None:
        theta_seed = 0.0

    best = (None, -np.inf, None)  # theta, score, edges
    grid = np.deg2rad(np.arange(-sweep_deg, sweep_deg + 1e-9, step_deg))
    for dth in grid:
        th = theta_seed + dth
        sc, edges = edge_score_for_angle(X, th, ctr, **kw)
        if sc > best[1]:
            best = (th, sc, edges)

    theta = best[0]
    x_lo, x_hi, y_lo, y_hi = best[2]

    # Build rectangle in rotated frame then rotate back
    rect_r = np.array([[x_lo, y_lo],
                        [x_hi, y_lo],
                        [x_hi, y_hi],
                        [x_lo, y_hi]])
    rect = rotate(rect_r, theta, ctr)
    return theta, rect



def to_df(raw_samples):
    """Flatten raw API payload into a DataFrame."""
    rows = []
    for a in raw_samples:
        aid = str(a.get('athlete_id'))
        for s in a.get('data', []):
            rows.append({
                'athlete_id': aid,
                'ts': float(s.get('ts', np.nan)),
                'x': float(s.get('x', np.nan)),
                'y': float(s.get('y', np.nan)),
                'v': float(s.get('v', np.nan)),         # m/s
                'a': float(s.get('a', np.nan)),         # accel (m/s^2) if present
                'hr': float(s.get('hr', np.nan)),       # heart rate if present
                'pl_acc': float(s.get('pl', np.nan)),   # accumulated load (gives activity “shape”)
                'lat': float(s.get('lat', np.nan)),
                'lon': float(s.get('long', np.nan)),
            })
    df = pd.DataFrame(rows)
    # basic validity
    df = df.dropna(subset=['ts'])
    # drop explicit zero coordinates
    bad_xy = ((df['x'] == 0) & (df['y'] == 0))
    bad_ll = ((df['lat'] == 0) & (df['lon'] == 0))
    df = df[~(bad_xy | bad_ll)].copy()
    df = df.sort_values(['ts','athlete_id']).reset_index(drop=True)
    return df

def pick_top_athletes(activity_summary, k=18):
    import re
    df = pd.DataFrame(activity_summary).copy()

    # Normalize column names once
    df.columns = [str(c).strip() for c in df.columns]

    # Prefer APL
    metric = None
    if "average_player_load" in df.columns:
        metric = "average_player_load"
    else:
        # Try common distance keys
        cand = [c for c in df.columns
                if re.fullmatch(r"(total_)?distance(_m|_meters)?", c) or c.lower() in {"distance","total_distance"}]
        if cand:
            metric = cand[0]

    if metric is None:
        raise KeyError(f"Could not find a metric column in activity_summary. "
                        f"Available columns: {list(df.columns)}. "
                        f"Expected 'average_player_load' or a distance field.")

    # Clean distance strings like "9345****"
    if df[metric].dtype == object:
        df[metric] = df[metric].astype(str).str.replace(r"\*+", "", regex=True)
        df[metric] = pd.to_numeric(df[metric], errors="coerce")

    df = df.dropna(subset=[metric])
    if df.empty:
        raise ValueError(f"Metric '{metric}' contains only NaN after cleaning. Check activity_summary values.")

    # Secondary tiebreaker if present
    tiebreak = None
    for alt in ["total_distance_m", "total_distance", "distance", "distance_meters"]:
        if alt in df.columns and alt != metric:
            tiebreak = alt
            if df[alt].dtype == object:
                df[alt] = pd.to_numeric(df[alt].astype(str).str.replace(r"\*+","",regex=True), errors="coerce")
            break

    sort_cols = [metric] + ([tiebreak] if tiebreak else [])
    top = df.sort_values(sort_cols, ascending=False).head(k)
    return set(top["athlete_id"].astype(str)), top

def main_cluster_mask(df, eps=2.5, min_samples=120):
    """
    Keep only the largest spatial cluster (discard locker-room/tunnel).
    eps in meters; 2.5–4.0 works well for 1 Hz.
    """
    X = df[['x','y']].to_numpy()
    labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(X)
    df['_label'] = labels
    lab_vals = [l for l in set(labels) if l != -1]
    if not lab_vals:
        df.drop(columns=['_label'], inplace=True, errors='ignore')
        return np.ones(len(df), dtype=bool)
    sizes = {l: (labels==l).sum() for l in lab_vals}
    keep = max(sizes, key=sizes.get)
    mask = (labels == keep)
    df.drop(columns=['_label'], inplace=True, errors='ignore')
    return mask

# ------------------------------
# kickoff / halftime / fulltime inference
# ------------------------------
def infer_match_windows(df_clustered,
                        expected_players=11,
                        min_active_speed=1.1,           # m/s; 1.0–1.2 is typical
                        pre_kick_prior_min=(30*60, 55*60),  # data starts ~35–45 min before KO
                        halftime_len_bounds=(8*60, 30*60),
                        smooth_sec=15,
                        sustain_ko=180,                  # need ≥3 min sustained "in play"
                        sustain_ft=120,                  # ≥2 min at end
                        buffers=(90, 120)):              # pre/post buffers
    """
    Works only on the main spatial cluster to avoid bench/tunnel bias.
    """
    df = df_clustered.copy()
    df['t'] = df['ts'].round().astype(int)
    # active if moving or accelerating a bit (handles keepers/slow phases)
    df['is_active'] = ((df['v'] >= min_active_speed) | (df['a'].abs() >= 0.3)).astype(int)

    # active unique athletes per second
    active_count = (df[df['is_active']==1]
                    .groupby('t')['athlete_id'].nunique()
                    .sort_index())
    if active_count.empty:
        tmin, tmax = int(df['t'].min()), int(df['t'].max())
        return {'kickoff': tmin, 'halftime_start': None, 'halftime_end': None, 'fulltime': tmax}

    # team intensity: median speed across all samples/athletes at each second
    med_speed = (df.groupby('t')['v'].median().reindex(active_count.index).fillna(0))

    # smooth
    k = smooth_sec
    a_s = active_count.rolling(k, center=True, min_periods=1).median()
    v_s = med_speed.rolling(k, center=True, min_periods=1).median()

    # Prior: kickoff occurs roughly after 30–55 min from the first timestamp
    t0 = int(df['t'].min())
    prior_lo, prior_hi = t0 + pre_kick_prior_min[0], t0 + pre_kick_prior_min[1]
    window = a_s[(a_s.index >= prior_lo) & (a_s.index <= prior_hi)]
    # Thresholds
    thr_players = max(9, math.ceil(0.85 * expected_players))   # usually 10 or 11
    thr_speed   = max(0.8, v_s.quantile(0.60))                 # elevated team median speed

    # find first sustained block that meets both player-count & speed
    kickoff = None
    cnt = 0
    times = window.index.to_numpy()
    for i, t in enumerate(times):
        ok = (a_s.loc[t] >= thr_players) and (v_s.loc[t] >= thr_speed)
        cnt = cnt + 1 if ok else 0
        if cnt >= sustain_ko:
            kickoff = int(times[i - sustain_ko + 1])
            break
    if kickoff is None:
        # fallback: global search
        cnt = 0
        for t in a_s.index:
            ok = (a_s.loc[t] >= thr_players) and (v_s.loc[t] >= thr_speed)
            cnt = cnt + 1 if ok else 0
            if cnt >= sustain_ko:
                kickoff = int(t - sustain_ko + 1); break
        if kickoff is None:
            kickoff = int(a_s.idxmax())

    # Halftime: longest valley of very low active count in the mid-game
    tmin, tmax = int(a_s.index.min()), int(a_s.index.max())
    mid_lo, mid_hi = tmin + (tmax - tmin)//3, tmin + 2*(tmax - tmin)//3
    mid = a_s[(a_s.index >= mid_lo) & (a_s.index <= mid_hi)]
    valley = (mid <= 2).astype(int)  # nearly everyone off the field
    best_len, best_end = 0, None; run = 0
    mid_times = mid.index.to_numpy()
    for i, t in enumerate(mid_times):
        if valley.iat[i] == 1:
            run += 1
            if run > best_len:
                best_len, best_end = run, t
        else:
            run = 0
    half_start = half_end = None
    if best_len >= halftime_len_bounds[0] and best_len <= halftime_len_bounds[1]:
        half_end = int(best_end)
        half_start = int(best_end - best_len + 1)

    # Full-time: last sustained period above thresholds after halftime (or kickoff)
    search_start = half_end or kickoff
    after = a_s[a_s.index >= search_start]
    last_run_end = int(after.index.max())
    cnt = 0
    for i in range(len(after)-1, -1, -1):
        t = after.index[i]
        ok = (a_s.loc[t] >= thr_players) and (v_s.loc[t] >= thr_speed)
        cnt = cnt + 1 if ok else 0
        if cnt >= sustain_ft:
            last_run_end = int(t + sustain_ft - 1)
            break

    pre_buf, post_buf = buffers
    kickoff_b   = max(int(kickoff - pre_buf), tmin)
    fulltime_b  = min(int(last_run_end + post_buf), tmax)

    return {
        'kickoff': kickoff_b,
        'halftime_start': half_start,
        'halftime_end': half_end,
        'fulltime': fulltime_b
    }


def refine_points_v2(raw_samples,
                    activity_summary,
                     *,
                    k_keep=18,
                    expected_players=11,          # soccer
                    min_active_speed=1.1,         # m/s
                    dbscan_eps=2.8,               # meters; tune 2.5–4.0
                    dbscan_min_samples=120,
                    halftime_len_bounds=(10*60, 25*60),
                    pre_kick_prior_min=(30*60, 55*60),
                    buffers=(90, 120)):
    """
    Returns:
        df_clean: per-sample dataframe (top-18, in-play only, main cluster)
        samples_xy: Nx2 array from df_clean[['x','y']]
        inferred_times: dict(kickoff, halftime_start, halftime_end, fulltime)
        kept_athletes: list of athlete_ids kept
    """
    print("Refining points and dropping reserves...")

    # 0) flatten
    df0 = to_df(raw_samples)

    # 1) keep most-active 18 by average_player_load
    keep_ids, _ = pick_top_athletes(activity_summary, k=k_keep)
    df1 = df0[df0['athlete_id'].isin(keep_ids)].copy()

    # 2) spatial main cluster only
    mask = main_cluster_mask(df1, eps=dbscan_eps, min_samples=dbscan_min_samples)
    df2 = df1.loc[mask].copy()

    # 3) infer times using active counts + team intensity with your prior
    times = infer_times_from_segments(df2, expected_players=11)

    # 4) keep only [kickoff .. fulltime], excluding halftime valley if detected
    df3 = df2[(df2['ts'] >= times['kickoff']) & (df2['ts'] <= times['fulltime'])].copy()
    if times['halftime_start'] and times['halftime_end']:
        s, e = times['halftime_start'], times['halftime_end']
        df3 = df3[(df3['ts'] <= s) | (df3['ts'] >= e)].copy()

    # 5) drop loafing: only samples with some activity (speed or accel)
    df3 = df3[((df3['v'] >= min_active_speed) | (df3['a'].abs() >= 0.3))].copy()

    # 6) final deliverables
    samples_xy = df3[['x','y']].to_numpy()
    kept_athletes = sorted(df3['athlete_id'].unique().tolist())
    df_clean = df3.sort_values(['ts','athlete_id']).reset_index(drop=True)

    return df_clean, samples_xy, times, kept_athletes


def fetch_all_sensor_streams(activity_summary: List[Dict[str, Any]],
                            frequency: float = None,
                            max_workers: int = 4) -> List[Dict[str, Any]]:
    """
    Returns raw_samples: [{ 'athlete_id': str, 'data': [ {ts,x,y,v,a,lat,long,...}, ... ] }, ...]
    Robust to different API response shapes.
    """
    print("Fetching athlete data...")
    if frequency is None:
        frequency = FREQUENCY

    def _fetch(aid: str) -> Dict[str, Any]:
        res = get_ath_sensor_data(aid)  # may be list[ {athlete_id, data:[...] } ] or dict or list of samples
        # normalize shapes
        if isinstance(res, list):
            if len(res) == 0:
                return {"athlete_id": aid, "data": []}
            # common case: [ { 'athlete_id':..., 'data': [...] } ]
            first = res[0]
            if isinstance(first, dict) and 'data' in first:
                return {"athlete_id": aid, "data": first.get('data', [])}
            # fallback: assume list is already data-sample dicts
            return {"athlete_id": aid, "data": res}
        if isinstance(res, dict):
            if 'data' in res:
                return {"athlete_id": aid, "data": res.get('data', [])}
            # unknown dict shape
            return {"athlete_id": aid, "data": []}
        # anything else
        return {"athlete_id": aid, "data": []}

    # fetch sequentially (API rate limits can be touchy; switch to threads if safe)
    print("Working on athlete number:")
    counter = 1
    raw_samples: List[Dict[str, Any]] = []
    for row in activity_summary:
        print(counter); counter+=1
        aid = str(row.get("athlete_id"))
        if not aid: 
            continue
        raw_samples.append(_fetch(aid))
    return raw_samples


def to_dt(ts):
    """
    Convert Unix timestamp (seconds or milliseconds) to aware datetimes.
    Returns (utc_dt, la_dt).
    """
    # auto-detect ms
    if ts > 1e12:  # looks like milliseconds
        ts = ts / 1000.0

    utc_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    la_dt  = utc_dt.astimezone(LA)
    return utc_dt, la_dt


def fmt(ts):
    utc_dt, la_dt = to_dt(float(ts))
    return utc_dt.isoformat(timespec="seconds"), la_dt.isoformat(timespec="seconds")

def _roll_med(s, w=15):
    return s.rolling(w, center=True, min_periods=max(1, w//3)).median()

def _pc_lengthwise_spread(df_t: pd.DataFrame) -> float:
    """
    Lengthwise spread proxy at time t:
    PCA on (x,y) across active players; return sqrt of top eigenvalue (~meters).
    With 1 Hz and ~11 players, this is stable and rotation-agnostic.
    """
    X = df_t[['x','y']].to_numpy()
    if len(X) < 3:
        return 0.0
    X = X - X.mean(axis=0)
    C = np.cov(X.T)
    vals = np.linalg.eigvalsh(C)  # ascending
    top = float(max(vals[-1], 0.0))
    return float(np.sqrt(top))

def build_in_play_score(df: pd.DataFrame,
                        expected_players: int = 11,
                        min_active_speed: float = 1.1,
                        w_count: float = 0.45,
                        w_speed: float = 0.25,
                        w_spread: float = 0.30) -> pd.Series:
    """
    df must have columns: ts (rounded seconds), athlete_id, x, y, v
    Returns a per-second score in [0,1] where >= 0.6 typically means 'in play'.
    """
    # Per-second active unique players
    df['t'] = df['ts'].round().astype(int)
    # Active if moving a bit
    active = df[df['v'] >= min_active_speed]
    cnt = active.groupby('t')['athlete_id'].nunique().astype(float)

    # Team intensity (median speed across all samples at t)
    med_v = df.groupby('t')['v'].median()

    # Lengthwise spread (rotation free): compute on active players at t
    # (this is the heavy part; vectorized with groupby.apply)
    def _spread_for_group(g):
        try:
            return _pc_lengthwise_spread(g)
        except Exception:
            return 0.0
    spread = (active.groupby('t')[['x','y']]
                .apply(lambda g: _spread_for_group(g.reset_index(level=0, drop=True)))
                .astype(float))

    # Align indices
    idx = cnt.index.union(med_v.index).union(spread.index)
    cnt = cnt.reindex(idx, fill_value=0.0)
    med_v = med_v.reindex(idx, fill_value=0.0)
    spread = spread.reindex(idx, fill_value=0.0)

    # Smooth a bit
    cnt_s  = _roll_med(cnt, 15)
    v_s    = _roll_med(med_v, 15)
    spr_s  = _roll_med(spread, 15)

    # Normalize features to [0,1] with sensible scales
    f_count  = np.clip(cnt_s / float(expected_players), 0.0, 1.0)
    # Speed: 0 at 0.6 m/s, 1 near 2.5 m/s median
    f_speed  = np.clip((v_s - 0.6) / (2.5 - 0.6), 0.0, 1.0)
    # Spread: 0 at 15 m (tight warmup cluster), 1 at 55 m (match-wide dispersion)
    f_spread = np.clip((spr_s - 15.0) / (55.0 - 15.0), 0.0, 1.0)

    score = (w_count * f_count) + (w_speed * f_speed) + (w_spread * f_spread)
    return score.fillna(0.0)

def segments_from_score(score: pd.Series,
                        hi_thr: float = 0.60,
                        lo_thr: float = 0.45,
                        min_dur: int = 5*60,
                        bridge_gap: int = 60) -> list[tuple[int,int]]:
    """
    Hysteresis thresholding + gap-bridging to turn the score into [start,end] segments.
    Returns list of (t_start, t_end) in epoch seconds.
    """
    s = score.sort_index()
    on = False
    t_start = None
    segments = []
    last_on_t = None

    for t, val in s.items():
        if not on and val >= hi_thr:
            on = True; t_start = int(t)
        elif on and val < lo_thr:
            # maybe a short dip; finalize later
            last_on_t = int(t)
            on = False
            # bridge short gaps by looking ahead—handled by allowing quick re-entry
            segments.append((t_start, last_on_t))
            t_start = None

    if on and t_start is not None:
        segments.append((t_start, int(s.index[-1])))

    # Bridge nearby segments separated by <= bridge_gap
    if not segments:
        return []
    merged = [segments[0]]
    for a,b in segments[1:]:
        prev_a, prev_b = merged[-1]
        if a - prev_b <= bridge_gap:
            merged[-1] = (prev_a, b)
        else:
            merged.append((a,b))

    # Keep only long enough
    merged = [(a,b) for a,b in merged if (b - a) >= min_dur]
    return merged

def infer_times_from_segments(df: pd.DataFrame,
                                expected_players: int = 11) -> dict:
    """
    Master routine: compute score -> segments -> KO/HT/FT.
    """
    score = build_in_play_score(df, expected_players=expected_players)
    segs  = segments_from_score(score, hi_thr=0.60, lo_thr=0.45, min_dur=5*60, bridge_gap=75)

    if not segs:
        # Fallback to full span
        tmin, tmax = int(df['ts'].min()), int(df['ts'].max())
        return {'kickoff': tmin, 'halftime_start': None, 'halftime_end': None, 'fulltime': tmax, 'score': score}

    # Typically 2 segments (first & second half). If more, pick the two longest.
    segs_sorted = sorted(segs, key=lambda ab: (ab[1]-ab[0]), reverse=True)
    if len(segs_sorted) == 1:
        ko, ft = segs_sorted[0]
        return {'kickoff': int(ko), 'halftime_start': None, 'halftime_end': None, 'fulltime': int(ft), 'score': score}

    # Choose two longest around the middle of the timeline
    segs_sorted = segs_sorted[:2]
    segs_sorted.sort(key=lambda ab: ab[0])
    (ko, h1_end), (h2_start, ft) = segs_sorted[0], segs_sorted[1]
    return {'kickoff': int(ko), 'halftime_start': int(h1_end), 'halftime_end': int(h2_start), 'fulltime': int(ft), 'score': score}

# Run program.
main()