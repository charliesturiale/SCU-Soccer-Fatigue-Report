import requests, os
import pandas as pd
import time
import ast
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, Team, Roster, Player, PlayerMetricValue
from db import engine
from datetime import datetime, timezone, timedelta
from derived_metrics import compute_derived_metrics, DERIVED_FUNCS

# Load environment variables from .env file
load_dotenv()

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# DERIVED METRICS CONFIGURATION
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# Map metric codes to their derived functions from derived_metrics.py
# ForceDecks-specific derived metrics (provider="derived-forcedecks")
FORCEDECKS_DERIVED_CONFIG = {
    # Empty for now - no ForceDecks derived metrics yet
    # Future: "fd_stiffness": DERIVED_FUNCS["fd_stiffness"],
    #         "fd_cmf_rel": DERIVED_FUNCS["fd_cmf_rel"],
}

# NordBord-specific derived metrics (provider="derived-nordbord")
NORDBORD_DERIVED_CONFIG = {
    "nordbord_strength_rel": DERIVED_FUNCS["nordbord_strength_rel"],  # Requires body_mass from ForceDecks
    "nordbord_asym": DERIVED_FUNCS["nordbord_asym"],
}


def get_vald_report_metrics_main(save_csv=True):
    clientId = os.environ.get("CLIENT_ID")
    clientSecret = os.environ.get("CLIENT_SECRET")
    teamName = "WSOC"

    token = get_bearer(clientId, clientSecret)

    forcedecks_df = get_forcedecks_report(token, teamName)
    forcedecks_df.to_csv("Project/match-reports/data/forcedecks_report.csv", index=False)

    nordbord_df = get_nordbord_report(token, teamName)
    nordbord_df.to_csv("Project/match-reports/data/nordbord_report.csv", index=False)

    return forcedecks_df, nordbord_df




def get_forcedecks_report(token, teamName):
    db_url = os.environ.get("DATABASE_URL")

    # List to collect all player data
    player_data = []

    try:
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Get team by name
            team = session.query(Team).filter_by(name=teamName).one_or_none()

            if not team:
                raise ValueError(f"Team '{teamName}' not found in database")

            # Get all players rostered to this team
            roster_entries = session.query(Roster).filter(Roster.team_id == team.id).all()

            # modifiedFrom = two months ago (UTC) as ISO8601 with 'Z' suffix
            two_months_ago = datetime.now(timezone.utc) - timedelta(days=60)
            modified_from = two_months_ago.replace(microsecond=0).isoformat().replace("+00:00", "Z")

            # Loop through all players on the team
            for roster_entry in roster_entries:
                player = roster_entry.player
                vald_id = player.vald_id

                if not vald_id:
                    print(f"Warning: Player {player.full_name} has no vald_id, skipping...")
                    continue

                recent_testId = get_recent_fd_test(token, vald_id, modified_from)

                # If no tests found in the last month, testId returned as 0
                if (recent_testId == "0"):
                    continue

                recent_test_values, trials_data = get_fd_test_metrics(token, recent_testId)

                # Create a row with player info and their test values
                row_data = {
                    'player_name': player.full_name,
                    'vald_id': vald_id,
                    'test_id': recent_testId
                }

                # Add each raw metric value to the row
                if recent_test_values:
                    for metric_code, values in recent_test_values.items():
                        # Use mean of all trial values for each metric
                        if values:
                            row_data[metric_code] = sum(values) / len(values)
                        else:
                            row_data[metric_code] = None

                # Compute derived metrics from the test trials (if any configured)
                # Note: FORCEDECKS_DERIVED_CONFIG is currently empty
                if FORCEDECKS_DERIVED_CONFIG and trials_data:
                    derived_values = {code: [] for code in FORCEDECKS_DERIVED_CONFIG.keys()}

                    # Compute derived metrics for each trial
                    for trial in trials_data:
                        # Build trial dict from results
                        trial_dict = {}
                        for result in trial.get('results', []):
                            result_id = str(result.get('resultId', ''))
                            value = result.get('value')
                            if value is not None:
                                trial_dict[result_id] = float(value)

                        # Compute derived metrics for this trial
                        derived = compute_derived_metrics(trial_dict, body_mass=trial_dict.get('655386'))

                        # Collect derived values
                        for code, value in derived.items():
                            if code in derived_values:
                                derived_values[code].append(value)

                    # Average derived metrics across trials and add to row
                    for code, values in derived_values.items():
                        if values:
                            row_data[code] = sum(values) / len(values)
                        else:
                            row_data[code] = None

                player_data.append(row_data)

    except Exception as e:
        print(f"Error: {e}")

    # Convert to DataFrame
    if player_data:
        df = pd.DataFrame(player_data)
        return df
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def get_nordbord_report(token, team):
    
    nordbord_url = os.environ.get("VALD_NORDBORD_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")

    # Metric field names in the NordBord API response
    metric_fields = [
        'leftAvgForce', 'leftImpulse', 'leftMaxForce', 'leftTorque',
        'rightAvgForce', 'rightImpulse', 'rightMaxForce', 'rightTorque'
    ]

    # TODO: LASNGFLASNFGLASNGLAKSNG

    # List to collect player data
    player_data = []

    # Step 1: Get players with VALD IDs from database
    try:
        db_url = os.environ.get("DATABASE_URL")
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Get the team
            team_obj = session.query(Team).filter(Team.name == team).one_or_none()
            if not team_obj:
                print(f"Team '{team}' not found in database")
                return
            
            # Get all players on team with VALD IDs
            players = session.query(Player).join(Roster).filter(
                Roster.team_id == team_obj.id,
                Player.vald_id.isnot(None)
            ).all()

            if not players:
                print(f"No players with VALD IDs found for team {team}")
                return

            print(f"\nFound {len(players)} players with VALD IDs for NordBord metrics")

            # Get all NordBord raw metrics from database
            nordbord_metrics = session.query(Metric).filter(
                Metric.provider == "vald_nordbord"
            ).all()

            if not nordbord_metrics:
                print("No NordBord metrics found in database. Please seed metrics first.")
                return

            print(f"Found {len(nordbord_metrics)} NordBord raw metrics in database")

            # Create a mapping of metric code to metric object
            metric_code_map = {m.code: m for m in nordbord_metrics}

            # Get the list of metric field names that we're actually tracking
            metric_fields = [m.code for m in nordbord_metrics]

            # modifiedFrom = two months ago (UTC) as ISO8601 with 'Z' suffix
            two_months_ago = datetime.now(timezone.utc) - timedelta(days=60)
            modified_from = two_months_ago.replace(microsecond=0).isoformat().replace("+00:00", "Z")

            # Step 2: For each player, get tests and extract metrics
            for player in players:
                print(f"\nProcessing {player.first_name} {player.last_name} (VALD ID: {player.vald_id})")

                # Get player's body mass from ForceDecks data if available (needed for derived metrics)
                body_mass = None
                body_weight_metric = session.query(Metric).filter(
                    Metric.provider == "vald_forcedecks",
                    Metric.code == "655386"  # Body Weight code
                ).one_or_none()

                if body_weight_metric:
                    pmv = session.query(PlayerMetricValue).filter(
                        PlayerMetricValue.player_id == player.id,
                        PlayerMetricValue.metric_id == body_weight_metric.id
                    ).one_or_none()

                    if pmv and pmv.average_value:
                        body_mass = float(pmv.average_value)
                        print(f"  Using body mass: {body_mass:.1f} kg (from ForceDecks)")
                    else:
                        print(f"  No body mass data available for this player")
                else:
                    print(f"  Body weight metric not found in database")

                # Get player's NordBord tests
                tests_url = f"{nordbord_url}/tests/v2"
                params = {
                    "TenantId": tenantId,
                    "modifiedFromUtc": modified_from,
                    "profileId": player.vald_id
                }


                try:
                    r = requests.get(tests_url, headers=auth_header(token), params=params, timeout=30)
                    r.raise_for_status()

                    # Handle 204 No Content response (no tests in timeframe)
                    if r.status_code == 204 or not r.content:
                        print(f"  No NordBord tests found for {player.first_name} {player.last_name}")
                        continue

                    tests_data = r.json()

                    # Handle empty or None response
                    if not tests_data:
                        print(f"  No NordBord tests found for {player.first_name} {player.last_name}")
                        continue

                    # Extract tests array if wrapped
                    tests = tests_data.get('tests', tests_data) if isinstance(tests_data, dict) else tests_data

                    # Skip if no tests found in the timeframe
                    if not tests or len(tests) == 0:
                        print(f"  No NordBord tests found for {player.first_name} {player.last_name}")
                        continue

                    # Sort tests by test date (most recent first)
                    tests_df = pd.DataFrame(tests)
                    tests_df = tests_df.sort_values('testDateUtc', ascending=False)

                    # Most recent test - use iloc to get the first row as a Series
                    recent_test = tests_df.iloc[0]

                    test_values = {field: [] for field in metric_fields}

                    # Extract raw metrics from the most recent test
                    for field in metric_fields:
                        value = recent_test.get(field)

                        if value is not None and value != 0:
                            test_values[field].append(float(value))

                    # Create a row with player info and their test values
                    row_data = {
                        'player_name': player.full_name,
                        'vald_id': player.vald_id,
                        'test_id': recent_test['testId']
                    }

                    # Add each raw metric value to the row
                    if test_values:
                        for metric_code, values in test_values.items():
                            # Use mean of all trial values for each metric
                            if values:
                                row_data[metric_code] = sum(values) / len(values)
                            else:
                                row_data[metric_code] = None

                    # Compute derived metrics from the most recent test
                    if NORDBORD_DERIVED_CONFIG:
                        # Build trial dict from the most recent test's raw values
                        trial = {
                            'leftMaxForce': recent_test.get('leftMaxForce'),
                            'rightMaxForce': recent_test.get('rightMaxForce'),
                            'leftAvgForce': recent_test.get('leftAvgForce'),
                            'rightAvgForce': recent_test.get('rightAvgForce'),
                            'leftImpulse': recent_test.get('leftImpulse'),
                            'rightImpulse': recent_test.get('rightImpulse'),
                        }

                        # Compute derived metrics for this test
                        # Pass body_mass from ForceDecks data if available
                        derived = compute_derived_metrics(trial, body_mass=body_mass)

                        # Add derived metrics to row
                        for code, value in derived.items():
                            if code in NORDBORD_DERIVED_CONFIG:
                                row_data[code] = value
                                print(f"  {code}: {value:.2f}")

                    player_data.append(row_data)
                except Exception as e:
                    print(f"Error in nordbord processing for {player.first_name} {player.last_name}: {type(e).__name__}: {e}")
                    continue  # Continue to next player instead of returning
                
    except:
        print("error :(")
        return
    
    # Convert to DataFrame
    if player_data:
        df = pd.DataFrame(player_data)
        return df
    else:
        return pd.DataFrame()

# -----------------------------------------------
# HELPER FNS


def get_bearer(clientId, clientSecret):
    # mint a token with client-credentials
    auth_url = os.environ.get("VALD_AUTH_URL")

    if not clientId or not clientSecret:
        raise RuntimeError("No VALD_BEARER JWT and no client credentials provided.")

    r = requests.post(
        auth_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "client_id": clientId, "client_secret": clientSecret},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()

    return j["access_token"]


def auth_header(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def get_recent_fd_test(token, profileId, modified_from):
    print("getting recent fd tests for ", profileId)
    forcedecks_url = os.environ.get("VALD_FORCEDECKS_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")
    tests_url = f"{forcedecks_url}/tests"

    params = {"TenantId": tenantId, "modifiedFromUtc": modified_from, "profileId": profileId}

    try:
        r = requests.get(tests_url, headers=auth_header(token), params=params, timeout=30)
        r.raise_for_status()
        tests_data = r.json()

        # Extract tests array if wrapped
        tests = tests_data.get('tests', tests_data) if isinstance(tests_data, dict) else tests_data

        if not tests:
            print(f"  No ForceDecks tests found for profileId {profileId}")
            return

        # Sort by modifiedDateUtc descending to get most recent test first
        sorted_tests = sorted(tests, key=lambda x: x.get('modifiedDateUtc', ''), reverse=True)
        most_recent_test = sorted_tests[0]

        return most_recent_test['testId']

    except Exception as e:
        print(f"  Error fetching ForceDecks tests for profileId {profileId}: {e}")
        return "0"
    
def get_fd_test_metrics(token, testId):
    forcedecks_url = os.environ.get("VALD_FORCEDECKS_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")

    # Get metric values to collect from SQL
    try:
        db_url = os.environ.get("DATABASE_URL")
        engine = create_engine(db_url)
        with Session(engine) as session:
            forcedecks_metrics = session.query(Metric).filter(
                Metric.provider == "vald_forcedecks"
            ).all()

            # Create a mapping of metric code to metric object
            metric_code_map = {m.code: m for m in forcedecks_metrics}
            test_values = {code: [] for code in metric_code_map.keys()}

    except:
        print("Error getting the fd metrics from sql")
        return None, None

    # Get trials for this test
    trials_url = f"{forcedecks_url}/v2019q3/teams/{tenantId}/tests/{testId}/trials"

    try:
        r_trials = requests.get(trials_url, headers=auth_header(token), timeout=30)
        r_trials.raise_for_status()
        trials_data = r_trials.json()

        # Extract trials array if wrapped
        trials = trials_data.get('trials', trials_data) if isinstance(trials_data, dict) else trials_data

        if not trials:
            return None, None

        # Process each trial to extract metric values
        for trial in trials:
            # Trials contain a 'results' array with metric values
            results = trial.get('results', [])

            for result in results:
                result_id = str(result.get('resultId', ''))
                value = result.get('value')

                # Check if this result matches any of our tracked metrics
                if result_id in test_values and value is not None:
                    test_values[result_id].append(float(value))

        # Return both test_values (aggregated) and trials (raw data for derived metrics)
        return test_values, trials
    except Exception as e:
        print(f"    Error fetching trials for test {testId}: {e}")
    return None, None

# RUN FILE
get_vald_report_metrics_main()