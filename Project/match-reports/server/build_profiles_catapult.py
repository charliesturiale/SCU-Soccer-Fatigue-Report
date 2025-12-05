import requests, os
import pandas as pd
import time
import ast
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, Team, Player, Roster, PlayerMetricValue
from db import SessionLocal
from derived_metrics import compute_derived_metrics, DERIVED_FUNCS

#!/usr/bin/env python3

# Load environment variables from .env file
load_dotenv()

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# DERIVED METRICS CONFIGURATION
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# Map metric codes to their derived functions from derived_metrics.py
# Only include Catapult-specific derived metrics (provider="derived-catapult")
DERIVED_METRIC_CONFIG = {
    "high_intensity_efforts": DERIVED_FUNCS["high_intensity_efforts"],
}

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# TESTING CONFIGURATION
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# Override "today" for testing purposes - set to None to use actual current date
TESTING_TODAY = pd.Timestamp("2025-09-28")  # Example: test as if today is Nov 15, 2024
# TESTING_TODAY = None  # Use actual current date: time.time()

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAIN FUNCTION - Organizes workflow
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
def build_profiles_main():
    # Environment variable stuff - hard coded for now, will become dynamic later
    key = os.environ.get("WSOC_API_KEY")
    TEAM = "WSOC"

    # Get a list of all activities in the past months, determined by an env variable
    # Return as a dataframe
    activities_df = get_activities(key)
    if activities_df is None or not isinstance(activities_df, pd.DataFrame) or activities_df.empty:
        print("No activities to process.")
        return

    # Periodize the activities into a list of groups, roughly representing matchweeks.
    acitivity_periods = createActivityPeriods(activities_df)

    # Debug: Print period date ranges
    print_period_debug_info(acitivity_periods)

    # Get averages per metric on each period, then average the periods to get a single reference metric for each player
    reference_metrics, recent_period_metrics = build_reference_metrics(acitivity_periods)

    # Store metrics in SQL (both reference and recent period metrics)
    store_metrics(reference_metrics, recent_period_metrics, team=TEAM)

    # Export the profiles to a CSV for dev testing
    export_profiles_to_csv(reference_metrics)

    return




# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAJOR STEP FUNCTIONS - Called directly from main
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_

def get_activities(apikey):
    url = os.environ.get("ACTIVITIES_API_URL")

    # Look back 6 weeks (42 days) plus a buffer for period alignment
    weeks_past = 6
    days_lookback = weeks_past * 7 + 7  # Extra week for buffer

    # Use testing date if configured, otherwise use actual current time
    if TESTING_TODAY is not None:
        current_time = TESTING_TODAY.timestamp()
        print(f"[TESTING MODE] Using test date: {TESTING_TODAY.date()}")
    else:
        current_time = time.time()  # Actual current time

    start_time = int(current_time - days_lookback * 24 * 3600)

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {apikey}"
    }

    try:
        print(url)
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Step 1: Parse JSON
        data = response.json()  # gives you list of dicts

        # Step 2: Convert to DataFrame
        df = pd.DataFrame(data)

        return df

    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
        return
    



def createActivityPeriods(activities_df):

    # Group activities into roughly week-long periods.
    # Prioritize anchoring on weekend matches + MD+1 when available,
    # but also create 7-day periods for preseason/off-season without matches.
    # Only look back 6 weeks maximum, stopping at large gaps (e.g., summer break).

    # Returns
    # -------
    # periods : list[dict]
    #     Each dict has:
    #         - period_id: int
    #         - start: Timestamp (start of period)
    #         - end: Timestamp (end of period = MD+1 or match day)
    #         - match_id: id of the weekend match anchoring this period (None if no match)
    #         - md_plus1_id: id of the MD+1 activity if found, else None
    #         - activity_ids: list of activity ids in this period

    df = activities_df.copy()
    df["start_dt"] = pd.to_datetime(df["start_time"], unit="s")

    if df.empty:
        return []

    # --- 0. Filter activities to test date if in testing mode ---
    if TESTING_TODAY is not None:
        # Only include activities that occurred before or on the test date
        df = df[df["start_dt"] <= TESTING_TODAY].copy()
        print(f"[TESTING MODE] Filtered to {len(df)} activities occurring on or before {TESTING_TODAY.date()}")

        if df.empty:
            print("[TESTING MODE] No activities found before test date")
            return []

    # --- 1. Detect large gaps (>14 days) to avoid bridging summer/off-season ---
    df_sorted = df.sort_values("start_dt")
    df_sorted["gap_days"] = df_sorted["start_dt"].diff().dt.total_seconds() / (24 * 3600)

    # Find the most recent large gap (if any)
    large_gaps = df_sorted[df_sorted["gap_days"] > 14]
    if not large_gaps.empty:
        # Only consider activities after the most recent large gap
        last_gap_idx = large_gaps.index[-1]
        cutoff_date = df_sorted.loc[last_gap_idx, "start_dt"]
        df = df[df["start_dt"] >= cutoff_date].copy()
        print(f"Detected large gap before {cutoff_date.date()}, excluding earlier activities")

    if df.empty:
        return []

    # --- 2. Parse tags and classify basic types ---
    # tags column might be a list already or a string representation
    def parse_tags(tags):
        if tags is None or (isinstance(tags, float) and pd.isna(tags)):
            return []
        if isinstance(tags, list):
            return tags
        if isinstance(tags, str):
            try:
                return ast.literal_eval(tags)
            except (ValueError, SyntaxError):
                return []
        return []

    df["tags_list"] = df["tags"].apply(parse_tags)

    df["is_match"] = df["tags_list"].apply(lambda t: "MD" in t)
    df["is_md_plus1"] = df["tags_list"].apply(lambda t: "MD+1" in t)

    # Day-of-week: Monday=0 ... Sunday=6
    df["weekday"] = df["start_dt"].dt.weekday
    df["day"] = df["start_dt"].dt.normalize()  # date-only (midnight) boundary

    # --- 4. Find weekend matches (Sat/Sun) as potential anchors ---
    weekend_matches = df[(df["is_match"]) & (df["weekday"] >= 5)].copy()
    weekend_matches = weekend_matches.sort_values("start_dt")

    anchors = []

    if not weekend_matches.empty:
        # We have matches - create match-anchored periods
        for _, match_row in weekend_matches.iterrows():
            match_day = match_row["day"]

            # Find MD+1 within 1–2 days after the match
            md1_candidates = df[
                (df["is_md_plus1"]) &
                (df["day"] >= match_day + pd.Timedelta(days=1)) &
                (df["day"] <= match_day + pd.Timedelta(days=2))
            ].sort_values("start_dt")

            if not md1_candidates.empty:
                md1_row = md1_candidates.iloc[0]
                anchor_end_day = md1_row["day"]
                md_plus1_id = md1_row["id"]
            else:
                # No MD+1 found: use match day itself as anchor end
                anchor_end_day = match_day
                md_plus1_id = None

            anchors.append({
                "anchor_end_day": anchor_end_day,
                "match_id": match_row["id"],
                "md_plus1_id": md_plus1_id,
            })

    # --- 5. For periods without match anchors (preseason), create 7-day periods ---
    # Find the earliest and latest activity dates
    min_date = df["day"].min()
    max_date = df["day"].max()

    if anchors:
        # Ensure anchors are in chronological order
        anchors = sorted(anchors, key=lambda a: a["anchor_end_day"])
        earliest_anchor = anchors[0]["anchor_end_day"]

        # Fill in any gaps before the first anchor with 7-day periods
        if min_date < earliest_anchor - pd.Timedelta(days=7):
            print(f"Creating preseason periods from {min_date.date()} to {earliest_anchor.date()}")
            current_end = earliest_anchor - pd.Timedelta(days=1)
            while current_end >= min_date:
                anchors.insert(0, {
                    "anchor_end_day": current_end,
                    "match_id": None,
                    "md_plus1_id": None,
                })
                current_end = current_end - pd.Timedelta(days=7)
    else:
        # No matches at all - create 7-day periods for entire range
        print(f"No matches found, creating 7-day periods from {min_date.date()} to {max_date.date()}")
        current_end = max_date
        while current_end >= min_date:
            anchors.append({
                "anchor_end_day": current_end,
                "match_id": None,
                "md_plus1_id": None,
            })
            current_end = current_end - pd.Timedelta(days=7)

        # Reverse to chronological order
        anchors = sorted(anchors, key=lambda a: a["anchor_end_day"])

    if not anchors:
        return []

    # --- 6. Limit to 6 most recent periods ---
    # Only use the last 6 anchors to create profiles
    if len(anchors) > 6:
        anchors = anchors[-6:]
        print(f"Limiting to 6 most recent periods")

    # --- 7. Assign period IDs by walking backward from each anchor ---
    # Each period should span ~7 days leading UP TO the anchor (not all history)
    df_sorted = df.sort_values("start_dt").copy()
    df_sorted["period_id"] = pd.NA

    for pid, anchor in enumerate(anchors):
        anchor_end_day = anchor["anchor_end_day"]
        # Period starts 7 days before the anchor end (typical training week)
        period_start_day = anchor_end_day - pd.Timedelta(days=7)

        # Find all activities in this 7-day window that haven't been assigned yet
        # Only assign to first matching period to avoid double-counting
        mask = (
            (df_sorted["day"] >= period_start_day) &
            (df_sorted["day"] <= anchor_end_day) &
            (df_sorted["period_id"].isna())  # Only unassigned activities
        )
        df_sorted.loc[mask, "period_id"] = pid

    # Drop any activities that didn't get assigned to a period
    df_periods = df_sorted.dropna(subset=["period_id"]).copy()
    if df_periods.empty:
        return []

    df_periods["period_id"] = df_periods["period_id"].astype(int)

    # --- 8. Build output structure: list of period dicts ---
    # Filter to only periods with at least 5 days of activities (existing logic)
    periods = []
    grouped = df_periods.groupby("period_id")

    for pid, group in grouped:
        anchor = anchors[pid]  # pid corresponds to this anchor in order

        # Calculate period start as 7 days before anchor end
        period_start = anchor["anchor_end_day"] - pd.Timedelta(days=7)

        # Count unique days in this period
        unique_days = group["day"].nunique()

        # Only include periods with at least 5 days of activities
        if unique_days >= 5:
            period_info = {
                "period_id": pid,
                "start": period_start,
                "end": anchor["anchor_end_day"],
                "match_id": anchor["match_id"],
                "md_plus1_id": anchor["md_plus1_id"],
                "activity_ids": group["id"].tolist(),
                "num_days": unique_days
            }
            periods.append(period_info)
        else:
            print(f"Skipping period {pid} - only {unique_days} days (minimum 5 required)")

    return periods


def build_reference_metrics(activity_periods):
    """
    Build reference metrics for all players across all periods.

    Returns
    -------
    tuple : (player_profiles, recent_period_metrics)
        player_profiles : dict
            Dictionary mapping player_id to their profile data
        recent_period_metrics : dict
            Dictionary mapping player_id to their most recent period metrics
    """
    # 1: Get a list of all players on this team
    all_players = discover_players(activity_periods)
    print(f"Building profiles for {len(all_players)} players")

    # 2: Get the detailed stats for each period
    period_stats = []
    print(f"Fetching stats for {len(activity_periods)} periods...")
    for i, period in enumerate(activity_periods):
        print(f"  Processing period {i+1}/{len(activity_periods)} (ID: {period['period_id']})")
        period_data = get_period_stats(period)
        period_stats.append(period_data)

    # 3: Build profiles for each player
    player_profiles = {}
    recent_period_metrics = {}

    for player in all_players:
        # Get all their stats across all periods
        player_period_averages = calculate_player_period_averages(player, period_stats)

        # Skip players with no data
        if not player_period_averages:
            continue

        # Average the period averages to get reference metrics
        reference_metrics = calculate_reference_metrics(player_period_averages)

        # Get the most recent period's metrics (last period in the list)
        most_recent_period = player_period_averages[-1]
        recent_metrics = most_recent_period["metrics"]

        player_profiles[player["id"]] = {
            "player_name": player["name"],
            "position": player.get("position"),  # Include position in profile
            "metrics": reference_metrics,
            "period_averages": player_period_averages
        }

        recent_period_metrics[player["id"]] = {
            "player_name": player["name"],
            "position": player.get("position"),  # Include position in recent metrics
            "metrics": recent_metrics
        }

    print(f"\n{'='*60}")
    print(f"Built profiles for {len(player_profiles)} players")
    print(f"{'='*60}\n")

    # Export to CSV
    export_profiles_to_csv(player_profiles)

    # Print sample profiles for testing
    sample_count = min(3, len(player_profiles))
    for profile in list(player_profiles.values())[:sample_count]:
        print(f"Player: {profile['player_name']}")
        print(f"  Reference Metrics (avg per day across {len(profile['period_averages'])} periods):")
        for metric_code, metric_stats in profile['metrics'].items():
            print(f"    {metric_code}: avg={metric_stats['average']:.2f}, std={metric_stats['std_dev']:.2f}, n={metric_stats['num_samples']}")
        print()

    return player_profiles, recent_period_metrics


def export_profiles_to_csv(player_profiles):
    """
    Export player profiles to a CSV file.

    Parameters
    ----------
    player_profiles : dict
        Dictionary mapping player_id to their profile data
    """
    if not player_profiles:
        print("No player profiles to export")
        return

    # Prepare data for CSV
    rows = []
    for player_id, profile in player_profiles.items():
        row = {
            "player_id": player_id,
            "player_name": profile["player_name"],
            "num_periods": len(profile["period_averages"])
        }
        # Add each metric as columns (average, std_dev, num_samples)
        for metric_code, metric_stats in profile["metrics"].items():
            row[f"{metric_code}_avg"] = metric_stats["average"]
            row[f"{metric_code}_std"] = metric_stats["std_dev"]
            row[f"{metric_code}_n"] = metric_stats["num_samples"]
        rows.append(row)

    # Convert to DataFrame and export
    df = pd.DataFrame(rows)

    # Create output directory if it doesn't exist
    output_dir = "Project/match-reports/data"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "catapult_profiles.csv")
    df.to_csv(output_path, index=False)

    print(f"Exported {len(rows)} player profiles to {output_path}")


def discover_players(activity_periods):
    """
    Fetch all athletes from the team roster.

    Returns
    -------
    players : list[dict]
        List of player dicts with 'id' and 'name' keys
    """
    key = os.environ.get("WSOC_API_KEY")
    url = "https://connect-us.catapultsports.com/api/v6/athletes"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {key}"
    }

    try:
        print(url)
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse JSON - should be a list of athlete dicts
        data = response.json()

        # Convert to our standard format: list of {id, name, position}
        players = []
        for athlete in data:
            player_id = athlete.get("id")
            first_name = athlete.get("first_name", "")
            last_name = athlete.get("last_name", "")
            position = athlete.get("position")  # Extract position from Catapult API

            # Build full name
            full_name = f"{first_name} {last_name}".strip()
            if not full_name:
                full_name = athlete.get("nickname") or "Unknown"

            if player_id:
                players.append({
                    "id": player_id,
                    "name": full_name,
                    "position": position  # Include position in player dict
                })

        print(f"Discovered {len(players)} players")
        return players

    except requests.exceptions.RequestException as err:
        print(f"Error fetching athletes: {err}")
        return []

def get_period_stats(period):
    """
    Get player-level stats for all activities in a period.

    Parameters
    ----------
    period : dict
        A period dict with 'period_id' and 'activity_ids'

    Returns
    -------
    period_data : dict
        {
            "period_id": int,
            "activity_stats": [
                {
                    "activity_id": "...",
                    "stats_df": DataFrame with columns [athlete_id, athlete_name, metric1, metric2, ...]
                },
                ...
            ]
        }
    """
    key = os.environ.get("WSOC_API_KEY")
    stats_url = "https://connect-us.catapultsports.com/api/v6/stats"

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {key}"
    }

    # Load metrics dynamically from the database
    metrics = get_catapult_metrics_from_db()
    parameters = [metric["code"] for metric in metrics]

    # print(f"Using {len(parameters)} metrics: {', '.join(parameters)}")

    activity_stats = []

    print(f"Fetching stats for period {period['period_id']} ({len(period['activity_ids'])} activities)")

    for activity_id in period["activity_ids"]:
        payload = {
            "parameters": parameters,
            "filters": [
                {
                    "comparison": "=",
                    "name": "activity_id",
                    "values": [activity_id]
                }
            ],
            "group_by": ["athlete"],
            "source": "cached_stats"
        }

        try:
            response = requests.post(stats_url, json=payload, headers=headers)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Convert to DataFrame
            stats_df = pd.DataFrame(data)

            if not stats_df.empty:
                activity_stats.append({
                    "activity_id": activity_id,
                    "stats_df": stats_df
                })

            # Small delay to avoid rate limiting
            time.sleep(0.1)

        except requests.exceptions.RequestException as err:
            print(f"Error fetching stats for activity {activity_id}: {err}")
            continue

    return {
        "period_id": period["period_id"],
        "activity_stats": activity_stats
    }

def calculate_player_period_averages(player, period_stats):
    """
    Calculate average metrics per day for a player across all periods.

    Parameters
    ----------
    player : dict
        Player dict with 'id' and 'name' keys
    period_stats : list[dict]
        List of period data dicts from get_period_stats()

    Returns
    -------
    period_averages : list[dict]
        List of dicts, one per period, containing:
        {
            "period_id": int,
            "days_active": int,
            "metrics": {
                "total_distance": float,
                "high_speed_distance": float,
                ...
            }
        }
    """
    # Get the list of metric codes we care about from the database
    metrics = get_catapult_metrics_from_db()
    metric_codes = [m["code"] for m in metrics]

    period_averages = []

    for period_data in period_stats:
        period_id = period_data["period_id"]

        # Collect all stats for this player across all activities in this period
        player_rows = []
        unique_days = set()

        for activity_stat in period_data["activity_stats"]:
            stats_df = activity_stat["stats_df"]

            # Filter to this player only
            player_data = stats_df[stats_df["athlete_id"] == player["id"]]

            if not player_data.empty:
                player_rows.append(player_data)

                # Track unique days (assuming 'date' column exists)
                if "date" in player_data.columns:
                    unique_days.update(player_data["date"].unique())

        if not player_rows:
            # Player didn't participate in this period
            continue

        # Combine all rows for this player in this period
        combined_df = pd.concat(player_rows, ignore_index=True)

        # Calculate the number of unique days
        days_active = len(unique_days) if unique_days else len(combined_df)

        # Calculate average per day for each raw metric
        metric_averages = {}
        for metric_code in metric_codes:
            if metric_code in combined_df.columns:
                total = combined_df[metric_code].sum()
                avg_per_day = total / days_active if days_active > 0 else 0
                metric_averages[metric_code] = avg_per_day
            else:
                # Metric not in data, default to 0
                metric_averages[metric_code] = 0.0

        # Compute derived metrics for each activity and then average them
        # Derived metrics need access to raw metrics per-activity (not summed)
        derived_values_per_activity = []
        for activity_stat in period_data["activity_stats"]:
            stats_df = activity_stat["stats_df"]
            player_data = stats_df[stats_df["athlete_id"] == player["id"]]

            if not player_data.empty:
                # For each row (activity), build a trial dict with raw metrics
                for _, row in player_data.iterrows():
                    trial = row.to_dict()
                    # Compute derived metrics for this activity
                    derived = compute_derived_metrics(trial, body_mass=None)
                    derived_values_per_activity.append(derived)

        # Average derived metrics across all activities, then divide by days_active
        for metric_code in DERIVED_METRIC_CONFIG.keys():
            # Collect all values for this derived metric across activities
            values = [d[metric_code] for d in derived_values_per_activity if metric_code in d]
            if values:
                # Sum across activities, then divide by days to get per-day average
                total_derived = sum(values)
                avg_per_day = total_derived / days_active if days_active > 0 else 0
                metric_averages[metric_code] = avg_per_day
            else:
                metric_averages[metric_code] = 0.0

        period_averages.append({
            "period_id": period_id,
            "days_active": days_active,
            "metrics": metric_averages
        })

    return period_averages

def calculate_reference_metrics(player_period_averages):
    """
    Calculate reference metrics by averaging the period averages and computing standard deviation.

    Parameters
    ----------
    player_period_averages : list[dict]
        List of period average dicts from calculate_player_period_averages()

    Returns
    -------
    reference_metrics : dict
        Dictionary with keys for each metric containing:
        - 'average': mean value across all periods
        - 'std_dev': standard deviation across periods
        - 'num_samples': number of periods used
        Example: {
            "total_distance": {"average": 4523.5, "std_dev": 312.1, "num_samples": 6},
            "high_speed_distance": {"average": 234.2, "std_dev": 45.3, "num_samples": 6},
            ...
        }
    """
    if not player_period_averages:
        return {}

    # Get all metric codes from the first period
    metric_codes = list(player_period_averages[0]["metrics"].keys())

    reference_metrics = {}

    # For each metric, calculate average and std dev across all periods
    for metric_code in metric_codes:
        values = [
            period["metrics"][metric_code]
            for period in player_period_averages
            if metric_code in period["metrics"]
        ]

        if values:
            avg_value = sum(values) / len(values)

            # Calculate standard deviation
            if len(values) > 1:
                variance = sum((x - avg_value) ** 2 for x in values) / len(values)
                std_dev = variance ** 0.5
            else:
                std_dev = 0.0

            reference_metrics[metric_code] = {
                "average": avg_value,
                "std_dev": std_dev,
                "num_samples": len(values)
            }
        else:
            reference_metrics[metric_code] = {
                "average": 0.0,
                "std_dev": 0.0,
                "num_samples": 0
            }

    return reference_metrics


def store_metrics(reference_metrics, recent_period_metrics, team="WSOC"):
    """
    Store reference metrics and recent period metrics in the SQL database.

    Parameters
    ----------
    reference_metrics : dict
        Dictionary mapping player_id to their profile data (reference values)
        Example: {
            "player123": {
                "player_name": "John Doe",
                "metrics": {"total_distance": 4523.5, ...},
                ...
            }
        }
    recent_period_metrics : dict
        Dictionary mapping player_id to their most recent period metrics (previous values)
        Same structure as reference_metrics
    team : str
        Name of the team to store metrics for
    """
    if not reference_metrics:
        print("No reference metrics to store")
        return

    print(f"\n{'='*60}")
    print(f"Storing metrics for {len(reference_metrics)} players to database")
    print(f"{'='*60}\n")

    db_url = os.environ.get("DATABASE_URL")

    # Create database engine and session
    engine = create_engine(db_url)
    session = Session(engine)

    try:
        # Step 1: Get or create the team
        team_obj = session.query(Team).filter_by(name=team).one_or_none()
        if team_obj is None:
            print(f"Creating new team: {team}")
            team_obj = Team(name=team)
            session.add(team_obj)
            session.flush()
        else:
            print(f"Found existing team: {team}")

        # Step 2: Get all metrics from the database (indexed by code)
        metrics_by_code = {}
        all_metrics = session.query(Metric).filter_by(provider="catapult").all()
        for metric in all_metrics:
            metrics_by_code[metric.code] = metric

        print(f"Loaded {len(metrics_by_code)} metrics from database")

        # Step 3: Process each player
        players_created = 0
        players_updated = 0
        rosters_created = 0
        metrics_stored = 0

        for player_catapult_id, profile in reference_metrics.items():
            player_name = profile["player_name"]
            position = profile.get("position")  # Get position from profile
            metrics = profile["metrics"]

            # Parse the player name into first and last name
            name_parts = player_name.strip().split(maxsplit=1)
            first_name = name_parts[0] if len(name_parts) > 0 else "Unknown"
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            # Get or create the player
            player = session.query(Player).filter_by(catapult_id=player_catapult_id).one_or_none()
            if player is None:
                print(f"  Creating new player: {player_name} (Catapult ID: {player_catapult_id})")
                player = Player(
                    first_name=first_name,
                    last_name=last_name,
                    catapult_id=player_catapult_id
                )
                session.add(player)
                session.flush()
                players_created += 1
            else:
                print(f"  Found existing player: {player_name}")
                players_updated += 1

            # Get or create the roster membership
            roster = session.query(Roster).filter_by(
                team_id=team_obj.id,
                player_id=player.id
            ).one_or_none()

            if roster is None:
                print(f"    Adding {player_name} to team roster (Position: {position or 'N/A'})")
                roster = Roster(
                    team_id=team_obj.id,
                    player_id=player.id,
                    position=position,  # Store position in roster
                    status="active"
                )
                session.add(roster)
                session.flush()
                rosters_created += 1
            else:
                # Update position if it's changed or was previously None
                if position and roster.position != position:
                    print(f"    Updating position: {roster.position} -> {position}")
                    roster.position = position

            # Get recent period metrics for this player (if available)
            recent_metrics = {}
            if player_catapult_id in recent_period_metrics:
                recent_metrics = recent_period_metrics[player_catapult_id]["metrics"]

            # Store the reference metrics
            for metric_code, metric_stats in metrics.items():
                # Get the metric from the database
                metric = metrics_by_code.get(metric_code)
                if metric is None:
                    print(f"    Warning: Metric '{metric_code}' not found in database, skipping")
                    continue

                # Extract average, std_dev, and num_samples from metric_stats dict
                average_value = metric_stats["average"]
                std_dev_value = metric_stats["std_dev"]
                num_samples_value = metric_stats["num_samples"]

                # Get the recent period value for this metric (if available)
                recent_value = recent_metrics.get(metric_code)

                # Get or create the PlayerMetricValue
                pmv = session.query(PlayerMetricValue).filter_by(
                    player_id=player.id,
                    metric_id=metric.id
                ).one_or_none()

                if pmv is None:
                    # Create new metric value with average, std_dev, num_samples, and recent period values
                    pmv = PlayerMetricValue(
                        player_id=player.id,
                        metric_id=metric.id,
                        average_value=average_value,
                        std_deviation=std_dev_value,
                        num_samples=num_samples_value,
                        previous_value=recent_value
                    )
                    session.add(pmv)
                    metrics_stored += 1
                else:
                    # Update existing metric value
                    pmv.average_value = average_value
                    pmv.std_deviation = std_dev_value
                    pmv.num_samples = num_samples_value
                    pmv.previous_value = recent_value
                    metrics_stored += 1

        # Commit all changes
        session.commit()

        print(f"\n{'='*60}")
        print(f"Database storage complete:")
        print(f"  Players created: {players_created}")
        print(f"  Players updated: {players_updated}")
        print(f"  Roster entries created: {rosters_created}")
        print(f"  Metric values stored: {metrics_stored}")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"Error storing metrics: {e}")
        session.rollback()
        raise
    finally:
        session.close()

    return


# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# HELPER FUNCTIONS - called from the major step functions
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_

def print_period_debug_info(periods):
    """
    Print debug information about activity periods including date ranges.

    Parameters
    ----------
    periods : list[dict]
        List of period dictionaries from createActivityPeriods()
    """
    if not periods:
        print("\n[DEBUG] No periods created")
        return

    print(f"\n{'='*70}")
    print(f"PERIOD DEBUG INFO - {len(periods)} periods created")
    print(f"{'='*70}")

    for period in periods:
        period_id = period["period_id"]
        start_date = period["start"].date()
        end_date = period["end"].date()
        num_activities = len(period["activity_ids"])
        num_days = period.get("num_days", "N/A")
        match_id = period["match_id"]
        md_plus1_id = period["md_plus1_id"]

        period_type = "Match-anchored" if match_id else "Non-match (preseason/off-season)"

        print(f"\nPeriod {period_id}: {start_date} to {end_date} ({period_type})")
        print(f"  Duration: {(period['end'] - period['start']).days + 1} days")
        print(f"  Active days: {num_days}")
        print(f"  Activities: {num_activities}")
        if match_id:
            print(f"  Match ID: {match_id}")
            if md_plus1_id:
                print(f"  MD+1 ID: {md_plus1_id}")

    print(f"\n{'='*70}\n")


def get_catapult_metrics_from_db():
    """
    Read all Catapult metrics from the database.

    Returns
    -------
    metrics : list[dict]
        List of dicts with 'code' and 'name' keys for each Catapult metric
    """
    # Get database URL from environment
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Warning: DATABASE_URL not set, using default metrics")
        # Fallback to hardcoded metrics
        return [
            {"code": "total_distance", "name": "Total Distance"},
            {"code": "high_speed_running_distance", "name": "HSR"},
            {"code": "sprint_distance", "name": "Sprint Distance"},
            {"code": "average_speed", "name": "Average Speed"},
            {"code": "max_speed", "name": "Max Speed"},
            {"code": "average_player_load", "name": "Average Player Load"},
            {"code": "total_player_load", "name": "Total Player Load"},
            {"code": "acceleration_count", "name": "Acceleration Count"},
            {"code": "deceleration_count", "name": "Deceleration Count"}
        ]

    try:
        # Create database engine and session
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Query all metrics where provider = 'catapult'
            catapult_metrics = session.query(Metric).filter(
                Metric.provider == "catapult"
            ).all()

            metrics = [
                {"code": metric.code, "name": metric.name}
                for metric in catapult_metrics
            ]

            print(f"Loaded {len(metrics)} Catapult metrics from database")
            return metrics

    except Exception as e:
        print(f"Error loading metrics from database: {e}")
        print("Falling back to default metrics")
        return [
            {"code": "total_distance", "name": "Total Distance"},
            {"code": "high_speed_running_distance", "name": "HSR"},
            {"code": "sprint_distance", "name": "Sprint Distance"},
            {"code": "average_speed", "name": "Average Speed"},
            {"code": "max_speed", "name": "Max Speed"},
            {"code": "average_player_load", "name": "Average Player Load"},
            {"code": "total_player_load", "name": "Total Player Load"},
            {"code": "acceleration_count", "name": "Acceleration Count"},
            {"code": "deceleration_count", "name": "Deceleration Count"}
        ]

# RUN THE FILE - main function (at top) controls whole workflow.
build_profiles_main()