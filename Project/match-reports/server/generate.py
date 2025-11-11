#!/usr/bin/env python3
"""
Main report generation script.
Handles both building player profiles and generating match report PDFs.

Usage:
    python generate.py build-profiles --window-days 42
    python generate.py generate --match-date 2025-10-24
"""

import sys
import argparse
from datetime import datetime, timedelta
from config import load_config
from db import SessionLocal, engine
from models import Base, ValdTest, CatapultSession


def build_profiles(window_days: int = 42):
    """
    Build player profiles based on recent data within the specified window.

    Args:
        window_days: Number of days to look back for data
    """
    print(f"Building player profiles with {window_days}-day window...")

    config = load_config()
    session = SessionLocal()
    return
    try:
        # Query recent data
        cutoff_date = datetime.now() - timedelta(days=window_days)

        vald_count = session.query(ValdTest).count()
        catapult_count = session.query(CatapultSession).count()

        print(f"Found {vald_count} VALD tests in database")
        print(f"Found {catapult_count} Catapult sessions in database")

        # TODO: Implement profile building logic
        # 1. Aggregate player metrics from VALD and Catapult data
        # 2. Calculate baselines and trends
        # 3. Store profiles in database or cache

        print("Profile building complete!")
        return 0

    except Exception as e:
        print(f"Error building profiles: {e}", file=sys.stderr)
        return 1
    finally:
        session.close()


def generate_report(match_date: str):
    """
    Generate a match report PDF for the specified date.

    Args:
        match_date: Date of the match in YYYY-MM-DD format
    """
    print(f"Generating report for match on {match_date}...")

    config = load_config()
    session = SessionLocal()

    try:
        # Parse the match date
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")

        # TODO: Implement report generation logic
        # 1. Load player profiles
        # 2. Query match-day data
        # 3. Generate visualizations
        # 4. Create PDF report
        # 5. Save to reports/ directory

        output_path = f"reports/match_report_{match_date}.pdf"
        print(f"Report would be saved to: {output_path}")
        print("Report generation complete!")
        return 0

    except ValueError as e:
        print(f"Invalid date format: {match_date}. Use YYYY-MM-DD", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error generating report: {e}", file=sys.stderr)
        return 1
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Match Report Generation Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # build-profiles command
    profiles_parser = subparsers.add_parser("build-profiles", help="Build player profiles")
    profiles_parser.add_argument("--window-days", type=int, default=42,
                                 help="Number of days to look back (default: 42)")

    # generate command
    generate_parser = subparsers.add_parser("generate", help="Generate match report")
    generate_parser.add_argument("--match-date", required=True,
                                 help="Match date in YYYY-MM-DD format")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "build-profiles":
        return build_profiles(args.window_days)
    elif args.command == "generate":
        return generate_report(args.match_date)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
