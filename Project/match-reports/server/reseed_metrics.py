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
        print("Seeding default metrics...")
        seed_default_metrics(session)
        print("✓ Metrics seeded successfully!")

        # Show what metrics are now in the database
        from models import Metric
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
