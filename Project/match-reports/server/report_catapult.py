import requests, os
import pandas as pd
import time
import ast
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, DEFAULT_METRICS
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
TESTING_TODAY = None
# TESTING_TODAY = pd.Timestamp("2025-09-28")  # Example: test as if today is Sept 28, 2025

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAIN FUNCTION - Organizes workflow
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
def get_catapult_report_metrics_main(save_csv=True, match_date=None):
    """
    Generate a report of player metrics for the most recent complete period.

    Parameters
    ----------
    save_csv : bool
        Whether to save averaged metrics to CSV file (default: True)
    match_date : datetime, optional
        The date to anchor the report period search. If None, uses current date.

    Returns
    -------
    tuple[DataFrame, dict | None]
        A tuple containing:
        - DataFrame with players as rows and metrics as columns (DAILY AVERAGES)
        - The identified report period dictionary, or None
    """
    
    # Get a list of all activities in the past months, determined by an env variable
    # Return as a dataframe
    key = os.environ.get("WSOC_API_KEY")
    activities_df = get_activities(key, match_date=match_date)
    if activities_df is None or not isinstance(activities_df, pd.DataFrame) or activities_df.empty:
        print("No activities to process.")
        return pd.DataFrame(), None

    # Identify the most recent complete period
    report_period = identify_report_period(activities_df, match_date=match_date)

    if report_period is None:
        print("Could not identify a complete report period.")
        return pd.DataFrame(), None

    # Get player stats for this period
    player_metrics = get_report_period_stats(report_period)

    # Convert to DataFrame (totals)
    metrics_df = build_metrics_dataframe(player_metrics)

    # Calculate daily averages
    num_days = (report_period["end"] - report_period["start"]).days + 1
    averages_df = calculate_averages_for_csv(metrics_df, num_days)

    # Optionally save averaged metrics to CSV
    if save_csv and not averages_df.empty:
        output_path = "../data/catapult_report.csv"
        averages_df.to_csv(output_path, index=False)
        print(f"\nSaved AVERAGED metrics to {output_path} ({num_days} days)")

    # Return daily averages and the report period
    return averages_df, report_period


# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAJOR STEP FUNCTIONS - Called directly from main
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
def get_activities(apikey, match_date=None):
    url = os.environ.get("ACTIVITIES_API_URL")

    # Only get activities from the past month for report generation
    months_past = 1.0
    # approximate 1 month = 30 days

    # Use match_date if provided, then testing date, otherwise use actual current time
    if match_date is not None:
        current_time = match_date.timestamp()
        print(f"[REPORT MODE] Using match date for lookback: {match_date.date()}")
    elif TESTING_TODAY is not None:
        current_time = TESTING_TODAY.timestamp()
        print(f"[TESTING MODE] Using test date: {TESTING_TODAY.date()}")
    else:
        current_time = time.time()  # Actual current time

    start_time = int(current_time - months_past * 30 * 24 * 3600)

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

        # Step 3: Parse timestamps
        df["start_dt"] = pd.to_datetime(df["start_time"], unit="s")
        df["end_dt"] = pd.to_datetime(df["end_time"], unit="s")

        # Filter to test date if in testing mode (and match_date isn't overriding)
        if match_date is None and TESTING_TODAY is not None:
            df = df[df["start_dt"] <= TESTING_TODAY].copy()
            print(f"[TESTING MODE] Filtered to {len(df)} activities occurring on or before {TESTING_TODAY.date()}")

        # Step 4: Parse tags
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

        print(f"Loaded {len(df)} activities from API")
        return df

    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
        return


def load_activities_from_csv():
    """
    Load activities from the testing CSV file.

    Returns
    -------
    DataFrame
        Activities dataframe with parsed timestamps
    """
    csv_path = "Testing/output/activities.csv"

    if not os.path.exists(csv_path):
        print(f"Error: Could not find activities CSV at {csv_path}")
        return None

    try:
        df = pd.read_csv(csv_path)

        # Parse timestamps
        df["start_dt"] = pd.to_datetime(df["start_time"], unit="s")
        df["end_dt"] = pd.to_datetime(df["end_time"], unit="s")

        # Parse tags
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

        print(f"Loaded {len(df)} activities from CSV")
        return df

    except Exception as e:
        print(f"Error loading activities CSV: {e}")
        return None


def identify_report_period(activities_df, match_date=None):
    """
    Identify the most recent complete period for reporting.

    A complete period includes:
    - The most recent weekend match (MD) and its MD+1 recovery day
    - At least 5 days of activities leading up to the match
    - Excludes the previous weekend's MD and MD+1

    Parameters
    ----------
    activities_df : DataFrame
        Activities dataframe with parsed timestamps and tags
    match_date : datetime, optional
        The date to anchor the search. The report will be for the match week
        ending on or just before this date.

    Returns
    -------
    dict or None
        Period information with start, end, and activity_ids, or None if no complete period found
    """
    df = activities_df.copy()

    if match_date:
        # Filter activities to be on or before the match_date
        df = df[df['start_dt'].dt.date <= match_date.date()].copy()

    # Add day-of-week and day fields
    df["weekday"] = df["start_dt"].dt.weekday  # Monday=0, Sunday=6
    df["day"] = df["start_dt"].dt.normalize()

    # Identify match days and MD+1 days
    df["is_match"] = df["tags_list"].apply(lambda t: "MD" in t)
    df["is_md_plus1"] = df["tags_list"].apply(lambda t: "MD+1" in t)

    # Sort by start time (most recent first)
    df = df.sort_values("start_dt", ascending=False)

    # Find the most recent weekend match (Saturday or Sunday)
    weekend_matches = df[(df["is_match"]) & (df["weekday"] >= 5)].copy()

    if weekend_matches.empty:
        print("No weekend matches found in activities")
        return None

    # Get the most recent weekend match
    most_recent_match = weekend_matches.iloc[0]
    match_day = most_recent_match["day"]
    match_id = most_recent_match["id"]

    print(f"\nMost recent weekend match: {most_recent_match['name']} on {match_day.date()}")

    # Find MD+1 after this match (within 1-2 days)
    md_plus1_candidates = df[
        (df["is_md_plus1"]) &
        (df["day"] >= match_day + pd.Timedelta(days=1)) &
        (df["day"] <= match_day + pd.Timedelta(days=2))
    ].sort_values("start_dt")

    if not md_plus1_candidates.empty:
        md_plus1 = md_plus1_candidates.iloc[0]
        period_end_day = md_plus1["day"]
        md_plus1_id = md_plus1["id"]
        print(f"Found MD+1: {md_plus1['name']} on {period_end_day.date()}")
    else:
        # No MD+1 found, period ends on match day
        period_end_day = match_day
        md_plus1_id = None
        print(f"No MD+1 found, period ends on match day")

    # Work backward to find the start of the period
    # Look for the previous weekend's MD and MD+1 to exclude
    previous_weekend_matches = df[
        (df["is_match"]) &
        (df["weekday"] >= 5) &
        (df["day"] < match_day)
    ].sort_values("start_dt", ascending=False)

    if not previous_weekend_matches.empty:
        prev_match = previous_weekend_matches.iloc[0]
        prev_match_day = prev_match["day"]

        # Find previous MD+1
        prev_md_plus1 = df[
            (df["is_md_plus1"]) &
            (df["day"] >= prev_match_day + pd.Timedelta(days=1)) &
            (df["day"] <= prev_match_day + pd.Timedelta(days=2))
        ].sort_values("start_dt")

        if not prev_md_plus1.empty:
            # Start after previous MD+1
            period_start_day = prev_md_plus1.iloc[0]["day"] + pd.Timedelta(days=1)
            print(f"Previous MD+1 found, period starts after {prev_md_plus1.iloc[0]['day'].date()}")
        else:
            # Start after previous match day
            period_start_day = prev_match_day + pd.Timedelta(days=1)
            print(f"No previous MD+1, period starts after {prev_match_day.date()}")
    else:
        # No previous match, use at least 5 days before current match
        period_start_day = match_day - pd.Timedelta(days=7)
        print(f"No previous match found, using 7-day window starting {period_start_day.date()}")

    # Ensure we have at least 5 days of activities
    min_start_day = period_end_day - pd.Timedelta(days=5)
    if period_start_day > min_start_day:
        period_start_day = min_start_day
        print(f"Adjusted period start to ensure minimum 5 days: {period_start_day.date()}")

    # Get all activities in this period
    period_activities = df[
        (df["day"] >= period_start_day) &
        (df["day"] <= period_end_day)
    ].sort_values("start_dt")

    if period_activities.empty:
        print("No activities found in the identified period")
        return None

    activity_ids = period_activities["id"].tolist()

    print(f"\n{'='*60}")
    print(f"Report Period Identified:")
    print(f"  Start: {period_start_day.date()}")
    print(f"  End: {period_end_day.date()}")
    print(f"  Match: {most_recent_match['name']}")
    print(f"  Activities: {len(activity_ids)}")
    print(f"\n  Activity IDs:")
    for i, act_id in enumerate(activity_ids, 1):
        act_name = period_activities[period_activities["id"] == act_id]["name"].iloc[0]
        act_date = period_activities[period_activities["id"] == act_id]["start_dt"].iloc[0]
        print(f"    {i}. {act_name} ({act_date.date()}) - {act_id}")
    print(f"{'='*60}\n")

    return {
        "start": period_start_day,
        "end": period_end_day,
        "match_id": match_id,
        "md_plus1_id": md_plus1_id,
        "activity_ids": activity_ids
    }


def get_report_period_stats(period):
    """
    Get player stats for all activities in the report period.

    Parameters
    ----------
    period : dict
        Period information with activity_ids

    Returns
    -------
    dict
        Dictionary mapping player_id to their total metrics for the period
    """
    key = os.environ.get("WSOC_API_KEY")
    stats_url = "https://connect-us.catapultsports.com/api/v6/stats"

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {key}"
    }

    # Load metrics from database
    metrics = get_catapult_metrics_from_db()
    parameters = [metric["code"] for metric in metrics]

    print(f"Fetching stats for {len(period['activity_ids'])} activities...")

    # Store all player stats (raw metrics)
    player_totals = {}
    # Store derived metrics separately - need to compute per-activity then sum
    player_derived_totals = {}

    for i, activity_id in enumerate(period["activity_ids"]):
        print(f"  Processing activity {i+1}/{len(period['activity_ids'])}")

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

            data = response.json()
            stats_df = pd.DataFrame(data)

            if not stats_df.empty:
                # Add stats to player totals
                for _, row in stats_df.iterrows():
                    athlete_id = row.get("athlete_id")
                    athlete_name = row.get("athlete_name", "Unknown")

                    if athlete_id not in player_totals:
                        player_totals[athlete_id] = {
                            "player_name": athlete_name,
                            "metrics": {metric_code: 0.0 for metric_code in parameters}
                        }

                    if athlete_id not in player_derived_totals:
                        player_derived_totals[athlete_id] = {
                            metric_code: 0.0 for metric_code in DERIVED_METRIC_CONFIG.keys()
                        }

                    # Sum up raw metrics
                    for metric_code in parameters:
                        if metric_code in row and pd.notna(row[metric_code]):
                            player_totals[athlete_id]["metrics"][metric_code] += float(row[metric_code])

                    # Compute derived metrics for this activity
                    trial = row.to_dict()
                    derived = compute_derived_metrics(trial, body_mass=None)

                    # Sum up derived metrics
                    for derived_code, value in derived.items():
                        if derived_code in player_derived_totals[athlete_id]:
                            player_derived_totals[athlete_id][derived_code] += value

            # Small delay to avoid rate limiting
            time.sleep(0.1)

        except requests.exceptions.RequestException as err:
            print(f"Error fetching stats for activity {activity_id}: {err}")
            continue

    # Merge derived metrics into player_totals
    for athlete_id in player_totals:
        if athlete_id in player_derived_totals:
            for derived_code, value in player_derived_totals[athlete_id].items():
                player_totals[athlete_id]["metrics"][derived_code] = value

    print(f"\nCollected stats for {len(player_totals)} players")
    print(f"Derived metrics: {list(DERIVED_METRIC_CONFIG.keys())}")
    return player_totals


def build_metrics_dataframe(player_metrics):
    """
    Build a DataFrame from player metrics.

    Parameters
    ----------
    player_metrics : dict
        Dictionary mapping player_id to metrics

    Returns
    -------
    DataFrame
        DataFrame with players as rows and metrics as columns
    """
    if not player_metrics:
        print("No player metrics to build DataFrame")
        return pd.DataFrame()

    rows = []
    for player_id, data in player_metrics.items():
        row = {
            "player_id": player_id,
            "player_name": data["player_name"]
        }
        # Add metrics
        for metric_code, value in data["metrics"].items():
            row[metric_code] = value
        rows.append(row)

    df = pd.DataFrame(rows)

    print(f"\n{'='*60}")
    print(f"Built metrics DataFrame:")
    print(f"  Players: {len(df)}")
    print(f"  Metrics: {len(df.columns) - 2}")  # Subtract player_id and player_name
    print(f"{'='*60}\n")

    return df


# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# HELPER FUNCTIONS
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_

def get_catapult_metrics_from_db():
    """
    Read all Catapult metrics from the database.

    Returns
    -------
    list[dict]
        List of dicts with 'code' and 'name' keys for each Catapult metric
    """
    db_url = os.environ.get("DATABASE_URL", "sqlite:///../data/project.db")
    if not db_url:
        print("Warning: DATABASE_URL not set, using default metrics")
        return get_default_metrics()

    try:
        engine = create_engine(db_url)
        with Session(engine) as session:
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
        return get_default_metrics()


def get_default_metrics():
    """Fallback default metrics if database is unavailable."""
    # Filter DEFAULT_METRICS from models.py to get only catapult provider
    return [
        {"code": code, "name": name}
        for name, provider, code, unit, lower_is_better in DEFAULT_METRICS
        if provider == "catapult"
    ]


def calculate_averages_for_csv(totals_df, num_days):
    """
    Calculate per-day averages for CSV output.

    Parameters
    ----------
    totals_df : DataFrame
        DataFrame with total metrics
    num_days : int
        Number of days in the report period

    Returns
    -------
    DataFrame
        DataFrame with averaged metrics
    """
    if totals_df.empty or num_days <= 0:
        return totals_df

    averages_df = totals_df.copy()

    # Don't average player_id and player_name columns
    metric_columns = [col for col in averages_df.columns if col not in ["player_id", "player_name"]]

    # Divide each metric by the number of days
    for col in metric_columns:
        averages_df[col] = averages_df[col] / num_days

    return averages_df


# Run file
if __name__ == "__main__":
    result_df = get_catapult_report_metrics_main(save_csv=True)

    if not result_df.empty:
        print("\nSample data (first 3 players - DAILY AVERAGES):")
        print(result_df.head(3).to_string())