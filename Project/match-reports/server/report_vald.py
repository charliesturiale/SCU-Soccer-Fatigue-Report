import requests, os
import pandas as pd
import time
import ast
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, Team, Roster, Player
from db import engine
from datetime import datetime, timezone, timedelta

# Load environment variables from .env file
load_dotenv()


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

                recent_test_values = get_fd_test_metrics(token, recent_testId)

                # Create a row with player info and their test values
                row_data = {
                    'player_name': player.full_name,
                    'vald_id': vald_id,
                    'test_id': recent_testId
                }

                # Add each metric value to the row
                if recent_test_values:
                    for metric_code, values in recent_test_values.items():
                        # Use mean of all trial values for each metric
                        if values:
                            row_data[metric_code] = sum(values) / len(values)
                        else:
                            row_data[metric_code] = None

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

            # modifiedFrom = two months ago (UTC) as ISO8601 with 'Z' suffix
            two_months_ago = datetime.now(timezone.utc) - timedelta(days=60)
            modified_from = two_months_ago.replace(microsecond=0).isoformat().replace("+00:00", "Z")

            # Step 2: For each player, get tests and extract metrics
            for player in players:
                print(f"\nProcessing {player.first_name} {player.last_name} (VALD ID: {player.vald_id})")

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

                    for field in metric_fields:
                        value = recent_test.get(field)

                        if value is not None and value !=0:
                            test_values[field].append(float(value))

                    # Create a row with player info and their test values
                    row_data = {
                        'player_name': player.full_name,
                        'vald_id': player.vald_id,
                        'test_id': recent_test['testId']
                    }

                    # Add each metric value to the row
                    if test_values:
                        for metric_code, values in test_values.items():
                            # Use mean of all trial values for each metric
                            if values:
                                row_data[metric_code] = sum(values) / len(values)
                            else:
                                row_data[metric_code] = None

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
        return

    # Get trials for this test
    trials_url = f"{forcedecks_url}/v2019q3/teams/{tenantId}/tests/{testId}/trials"

    try:
        r_trials = requests.get(trials_url, headers=auth_header(token), timeout=30)
        r_trials.raise_for_status()
        trials_data = r_trials.json()

        # Extract trials array if wrapped
        trials = trials_data.get('trials', trials_data) if isinstance(trials_data, dict) else trials_data

        if not trials:
            return

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

        return test_values
    except Exception as e:
        print(f"    Error fetching trials for test {testId}: {e}")
    return

# RUN FILE
get_vald_report_metrics_main()