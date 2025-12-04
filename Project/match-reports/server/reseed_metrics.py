#!/usr/bin/env python3
"""
Utility script to reseed the metrics table with new metric definitions.
Run this after updating DEFAULT_METRICS in models.py
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import seed_default_metrics

# Load environment variables
load_dotenv()

def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not found in environment")
        return

    print(f"Connecting to database: {db_url}")
    engine = create_engine(db_url)

    with Session(engine) as session:
        from models import Metric, DEFAULT_METRICS

        # Get current metrics codes that should exist
        valid_codes = {code for _, _, code, _, _ in DEFAULT_METRICS}

        # Find metrics in database that are no longer in DEFAULT_METRICS
        all_metrics = session.query(Metric).all()
        old_metrics = [m for m in all_metrics if m.code not in valid_codes]

        if old_metrics:
            print(f"\nFound {len(old_metrics)} old/unused metrics to delete:")
            for m in old_metrics:
                print(f"  - {m.name} (code: {m.code}, provider: {m.provider})")

            # Delete old metrics
            for m in old_metrics:
                session.delete(m)
            session.commit()
            print(f"✓ Deleted {len(old_metrics)} old metrics")
        else:
            print("\nNo old metrics to delete")

        print("\nSeeding default metrics...")
        seed_default_metrics(session)
        print("✓ Metrics seeded successfully!")

        # Show what metrics are now in the database
        metrics = session.query(Metric).all()

        print(f"\nTotal metrics in database: {len(metrics)}")
        print("\nMetrics by provider:")

        providers = {}
        for metric in metrics:
            if metric.provider not in providers:
                providers[metric.provider] = []
            providers[metric.provider].append(metric)

        for provider, provider_metrics in providers.items():
            print(f"\n{provider}:")
            for m in provider_metrics:
                print(f"  - {m.name} (code: {m.code}, unit: {m.unit})")

if __name__ == "__main__":
    main()
