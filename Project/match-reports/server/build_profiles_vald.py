import requests, os
import pandas as pd
import time
import ast
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, Team, Player, Roster, PlayerMetricValue
from db import SessionLocal

#!/usr/bin/env python3

# Load environment variables from .env file
load_dotenv()

# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAIN FUNCTION - Organizes workflow
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
def build_profiles_main():
    # Environment variable stuff - hard coded for now, will become dynamic later
    clientId = os.environ.get("CLIENT_ID")
    clientSecret = os.environ.get("CLIENT_SECRET")
    teamName = "WSOC"

    # Step 1: Get a temporary VALD token
    token = get_bearer(clientId, clientSecret)

    # Step 2: Update sql with players' vald ids. Requires population from catapult build first.
    get_roster(token, teamName)

    # Step 3: For each player, get ForceDecks metrics
    get_forceDecks_metrics(token, teamName)

    # Step 4: For each player, get NordBord metrics
    get_nordbord_metrics(token, teamName) 

    return


# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# MAJOR STEP FUNCTIONS - Called directly from main
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_

def get_bearer(clientId, clientSecret):
    # mint a token with client-credentials
    auth_url = os.environ.get("VALD_AUTH_URL")

    cid = clientId
    csec = clientSecret
    if not cid or not csec:
        raise RuntimeError("No VALD_BEARER JWT and no client credentials provided.")

    r = requests.post(
        auth_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "client_id": cid, "client_secret": csec},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()

    return j["access_token"]


def get_roster(token, team):
    profiles_url = os.environ.get("VALD_PROFILES_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")

    url = f"{profiles_url}/profiles"
    params = {"tenantId": tenantId}
    r = requests.get(url, headers=auth_header(token), params=params, timeout=30)
    r.raise_for_status()

    data=r.json()

    # Extract the profiles array from the wrapper object
    profiles_data = data['profiles'] if 'profiles' in data else data

    # Create DataFrame from the profiles array
    profiles_df = pd.DataFrame(profiles_data)

    print("DataFrame columns:", profiles_df.columns.tolist())
    print("DataFrame shape:", profiles_df.shape)
    print(profiles_df.head())

    try:
        db_url = os.environ.get("DATABASE_URL")
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Get the team object first
            team_obj = session.query(Team).filter(Team.name == team).one_or_none()
            if not team_obj:
                print(f"Team '{team}' not found in database")
                return

            # Query all players rostered on this team via the Roster table
            roster = session.query(Player).join(Roster).filter(
                Roster.team_id == team_obj.id
            ).all()

            # Match players from roster with VALD profiles and store profileId
            matched_count = 0
            for player in roster:
                # Try to find matching profile by name
                matching_profile = profiles_df[
                    (profiles_df['givenName'].str.lower() == player.first_name.lower()) &
                    (profiles_df['familyName'].str.lower() == player.last_name.lower())
                ]

                if not matching_profile.empty:
                    # Get the profileId from the first matching row
                    profile_id = matching_profile.iloc[0]['profileId']

                    # Update the player's vald_id in the database
                    player.vald_id = profile_id
                    matched_count += 1
                    print(f"Matched {player.first_name} {player.last_name} -> profileId: {profile_id}")
                else:
                    print(f"No VALD profile found for {player.first_name} {player.last_name}")

            # Commit all changes to the database
            session.commit()
            print(f"\nUpdated VALD IDs for {matched_count} players")

    except Exception as e:
        print(f"Database error in get_roster: {e}")
        return

    return


def get_forceDecks_metrics(token, team):
    """
    Retrieves ForceDecks metrics for all players on a team and stores them in the database.

    Workflow:
    1. Get all players and their VALD IDs from the database
    2. For each player, fetch their ForceDecks tests
    3. For each test, fetch trials containing metric data
    4. Extract metric values using codes from database
    5. Calculate and store average & most recent values
    """
    forcedecks_url = os.environ.get("VALD_FORCEDECKS_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")

    # List to collect all player metric data for CSV export
    all_player_data = []

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

            print(f"Found {len(players)} players with VALD IDs")

            # Step 4: Get all ForceDecks metrics from database
            forcedecks_metrics = session.query(Metric).filter(
                Metric.provider == "vald_forcedecks"
            ).all()

            if not forcedecks_metrics:
                print("No ForceDecks metrics found in database. Please seed metrics first.")
                return

            print(f"Found {len(forcedecks_metrics)} ForceDecks metrics in database")

            # Create a mapping of metric code to metric object
            metric_code_map = {m.code: m for m in forcedecks_metrics}

            # Step 2 & 3: For each player, get tests and trials
            for player in players:
                print(f"\nProcessing {player.first_name} {player.last_name} (VALD ID: {player.vald_id})")

                # Get player's ForceDecks tests
                tests_url = f"{forcedecks_url}/tests"
                modified_from="2000-01-01T00:00:00.000Z"
                params = {"TenantId": tenantId, "modifiedFromUtc": modified_from, "profileId": player.vald_id}

                try:
                    r = requests.get(tests_url, headers=auth_header(token), params=params, timeout=30)
                    r.raise_for_status()
                    tests_data = r.json()

                    # Extract tests array if wrapped
                    tests = tests_data.get('tests', tests_data) if isinstance(tests_data, dict) else tests_data

                    if not tests:
                        print(f"  No ForceDecks tests found for {player.first_name} {player.last_name}")
                        continue

                    # Sort tests by recorded date (most recent first)
                    tests_df = pd.DataFrame(tests)
                    tests_df = tests_df.sort_values('recordedDateUtc', ascending=False)

                    print(f"  Found {len(tests_df)} tests")

                    # Get the most recent test date for display
                    if len(tests_df) > 0:
                        most_recent_date_str = tests_df.iloc[0]['recordedDateUtc']
                        print(f"  Most recent test: {most_recent_date_str}")

                    # Dictionary to store all values for each metric
                    metric_values = {code: [] for code in metric_code_map.keys()}
                    most_recent_test_values = {code: [] for code in metric_code_map.keys()}

                    # Process each test (limit to recent tests for performance)
                    is_first_test = True
                    for idx, test in tests_df.head(20).iterrows():
                        test_id = test['testId']

                        # Get trials for this test
                        trials_url = f"{forcedecks_url}/v2019q3/teams/{tenantId}/tests/{test_id}/trials"

                        try:
                            r_trials = requests.get(trials_url, headers=auth_header(token), timeout=30)
                            r_trials.raise_for_status()
                            trials_data = r_trials.json()

                            # Extract trials array if wrapped
                            trials = trials_data.get('trials', trials_data) if isinstance(trials_data, dict) else trials_data

                            if not trials:
                                continue

                            # Process each trial to extract metric values
                            for trial in trials:
                                # Trials contain a 'results' array with metric values
                                results = trial.get('results', [])

                                for result in results:
                                    result_id = str(result.get('resultId', ''))
                                    value = result.get('value')

                                    # Check if this result matches any of our tracked metrics
                                    if result_id in metric_values and value is not None:
                                        metric_values[result_id].append(float(value))

                                        # Track values from most recent test only
                                        if is_first_test:
                                            most_recent_test_values[result_id].append(float(value))

                            # Mark that we've processed the first test
                            is_first_test = False

                        except Exception as e:
                            print(f"    Error fetching trials for test {test_id}: {e}")
                            continue

                    # Step 6 & 7: Calculate averages and store in database
                    player_row = {
                        'player_name': f"{player.first_name} {player.last_name}",
                        'player_id': player.id,
                        'vald_id': player.vald_id
                    }

                    for code, values in metric_values.items():
                        if values:  # Only process if we have data
                            avg_value = sum(values) / len(values)

                            # Calculate recent value as average of most recent test
                            recent_test_vals = most_recent_test_values.get(code, [])
                            if recent_test_vals:
                                recent_value = sum(recent_test_vals) / len(recent_test_vals)
                            else:
                                recent_value = avg_value

                            metric = metric_code_map[code]

                            # Store in database
                            from models import upsert_player_metric_value
                            upsert_player_metric_value(
                                session,
                                player_id=player.id,
                                metric_id=metric.id,
                                reference_value=avg_value,
                                previous_value=recent_value
                            )

                            # Add to CSV data
                            player_row[f"{metric.name}_avg"] = avg_value
                            player_row[f"{metric.name}_recent"] = recent_value
                            player_row[f"{metric.name}_n"] = len(values)

                            # Show data points for debugging
                            print(f"    {metric.name}: avg={avg_value:.2f} (n={len(values)}), recent={recent_value:.2f} (n={len(recent_test_vals)})")

                    all_player_data.append(player_row)

                    # Commit after each player
                    session.commit()

                except Exception as e:
                    print(f"  Error processing player {player.first_name} {player.last_name}: {e}")
                    continue

            print(f"\nCompleted ForceDecks metrics update for team {team}")

            # Export to CSV
            if all_player_data:
                output_dir = "output"
                os.makedirs(output_dir, exist_ok=True)
                csv_path = os.path.join(output_dir, "forcedecks_profiles.csv")

                df = pd.DataFrame(all_player_data)
                df.to_csv(csv_path, index=False)
                print(f"\nForceDecks data exported to {csv_path}")

    except Exception as e:
        print(f"Database error: {e}")
        return

def get_nordbord_metrics(token, team):
    """
    Retrieves NordBord metrics for all players on a team and stores them in the database.

    Workflow:
    1. Get all players and their VALD IDs from the database
    2. For each player, fetch their NordBord tests from API
    3. Extract the 8 metrics from each test (left/right avgForce, impulse, maxForce, torque)
    4. Calculate average across all tests and most recent test values
    5. Store in database (placeholder for now)
    """
    nordbord_url = os.environ.get("VALD_NORDBORD_URL")
    tenantId = os.environ.get("VALD_TENANT_ID")

    # Metric field names in the NordBord API response
    metric_fields = [
        'leftAvgForce', 'leftImpulse', 'leftMaxForce', 'leftTorque',
        'rightAvgForce', 'rightImpulse', 'rightMaxForce', 'rightTorque'
    ]

    # List to collect all player metric data for CSV export
    all_player_data = []

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

            # Get all NordBord metrics from database
            nordbord_metrics = session.query(Metric).filter(
                Metric.provider == "vald_nordbord"
            ).all()

            if not nordbord_metrics:
                print("No NordBord metrics found in database. Please seed metrics first.")
                return

            print(f"Found {len(nordbord_metrics)} NordBord metrics in database")

            # Create a mapping of metric code to metric object
            metric_code_map = {m.code: m for m in nordbord_metrics}

            # Step 2: For each player, get tests and extract metrics
            for player in players:
                print(f"\nProcessing {player.first_name} {player.last_name} (VALD ID: {player.vald_id})")

                # Get player's NordBord tests
                tests_url = f"{nordbord_url}/tests/v2"
                modified_from = "2000-01-01T00:00:00.000Z"
                params = {
                    "TenantId": tenantId,
                    "modifiedFromUtc": modified_from,
                    "profileId": player.vald_id
                }

                try:
                    r = requests.get(tests_url, headers=auth_header(token), params=params, timeout=30)
                    r.raise_for_status()
                    tests_data = r.json()

                    # Extract tests array if wrapped
                    tests = tests_data.get('tests', tests_data) if isinstance(tests_data, dict) else tests_data

                    if not tests:
                        print(f"  No NordBord tests found for {player.first_name} {player.last_name}")
                        continue

                    # Sort tests by test date (most recent first)
                    tests_df = pd.DataFrame(tests)
                    tests_df = tests_df.sort_values('testDateUtc', ascending=False)

                    print(f"  Found {len(tests_df)} tests")

                    # Get the most recent test date for display
                    if len(tests_df) > 0:
                        most_recent_date_str = tests_df.iloc[0]['testDateUtc']
                        print(f"  Most recent test: {most_recent_date_str}")

                    # Dictionary to store all values for each metric
                    metric_values = {field: [] for field in metric_fields}
                    most_recent_test_values = {field: [] for field in metric_fields}

                    # Process each test
                    is_first_test = True
                    for _, test in tests_df.iterrows():
                        # Extract each metric from the test
                        for field in metric_fields:
                            value = test.get(field)

                            # Only include non-null, non-zero values
                            if value is not None and value != 0:
                                metric_values[field].append(float(value))

                                # Track most recent test values
                                if is_first_test:
                                    most_recent_test_values[field].append(float(value))

                        # Mark that we've processed the first test
                        if is_first_test:
                            is_first_test = False

                    # Step 3 & 4: Calculate averages and store in database
                    print(f"  Metrics:")
                    player_row = {
                        'player_name': f"{player.first_name} {player.last_name}",
                        'player_id': player.id,
                        'vald_id': player.vald_id
                    }

                    for field in metric_fields:
                        all_vals = metric_values[field]
                        recent_vals = most_recent_test_values[field]

                        if all_vals:  # Only process if we have data
                            avg_value = sum(all_vals) / len(all_vals)

                            # Calculate recent value
                            if recent_vals:
                                recent_value = sum(recent_vals) / len(recent_vals)
                            else:
                                recent_value = avg_value

                            # Store in database if this metric is tracked
                            if field in metric_code_map:
                                metric = metric_code_map[field]

                                from models import upsert_player_metric_value
                                upsert_player_metric_value(
                                    session,
                                    player_id=player.id,
                                    metric_id=metric.id,
                                    reference_value=avg_value,
                                    previous_value=recent_value
                                )

                            # Add to CSV data
                            player_row[f"{field}_avg"] = avg_value
                            player_row[f"{field}_recent"] = recent_value
                            player_row[f"{field}_n"] = len(all_vals)

                            print(f"    {field}: avg={avg_value:.2f} (n={len(all_vals)}), recent={recent_value:.2f} (n={len(recent_vals)})")

                    all_player_data.append(player_row)

                    # Commit after each player
                    session.commit()

                except Exception as e:
                    print(f"  Error processing player {player.first_name} {player.last_name}: {e}")
                    continue

            print(f"\nCompleted NordBord metrics update for team {team}")

            # Export to CSV
            if all_player_data:
                output_dir = "output"
                os.makedirs(output_dir, exist_ok=True)
                csv_path = os.path.join(output_dir, "nordbord_profiles.csv")

                df = pd.DataFrame(all_player_data)
                df.to_csv(csv_path, index=False)
                print(f"\nNordBord data exported to {csv_path}")

    except Exception as e:
        print(f"Database error: {e}")
        return


# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_
# HELPER FUNCTIONS - called from the major step functions
# —_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_—_

def auth_header(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

# RUN THE FILE - main function (at top) controls whole workflow.
build_profiles_main()