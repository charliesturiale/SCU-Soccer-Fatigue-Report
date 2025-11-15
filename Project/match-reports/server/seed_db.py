#!/usr/bin/env python3
"""
Seed the database with initial data.
This script:
1. Initializes the database schema (creates tables)
2. Seeds default metrics from models.py

Usage:
    python seed_db.py
"""

import os
from dotenv import load_dotenv
from db import engine, SessionLocal
from models import Base, seed_default_metrics

# Load environment variables
load_dotenv()


def seed_database():
    """Initialize database and seed with default data."""
    print("=" * 60)
    print("DATABASE SEEDING")
    print("=" * 60)

    # Step 1: Create all tables
    print("\n[1/2] Creating database tables...")

    # Ensure data directory exists
    db_path = os.getenv("DATABASE_URL", "sqlite:///../data/project.db")
    if db_path.startswith("sqlite:///"):
        db_file = db_path.replace("sqlite:///", "")
        db_dir = os.path.dirname(db_file)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"  [OK] Created directory: {db_dir}")

    # Create all tables
    Base.metadata.create_all(bind=engine)

    # List created tables
    tables = Base.metadata.tables.keys()
    print(f"  [OK] Initialized {len(tables)} table(s):")
    for table_name in tables:
        print(f"    - {table_name}")

    # Step 2: Seed default metrics
    print("\n[2/2] Seeding default metrics...")
    session = SessionLocal()
    try:
        seed_default_metrics(session)
        print("  [OK] Seeded default Catapult metrics")

        # Verify metrics were created
        from models import Metric
        metric_count = session.query(Metric).count()
        catapult_count = session.query(Metric).filter(Metric.provider == "catapult").count()
        print(f"  [OK] Total metrics in database: {metric_count}")
        print(f"  [OK] Catapult metrics: {catapult_count}")

    except Exception as e:
        print(f"  [ERROR] Error seeding metrics: {e}")
        session.rollback()
    finally:
        session.close()

    print("\n" + "=" * 60)
    print("[OK] DATABASE SEEDING COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    seed_database()