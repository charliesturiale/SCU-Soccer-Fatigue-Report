#!/usr/bin/env python3
"""
Initialize the database schema.
Creates all tables defined in models.py if they don't exist.

Usage:
    python init_db.py
"""

import os
from db import engine
from models import Base


def init_database():
    """Create all database tables."""
    print("Initializing database...")

    # Ensure data directory exists
    db_path = os.getenv("DATABASE_URL", "sqlite:///../data/project.db")
    if db_path.startswith("sqlite:///"):
        db_file = db_path.replace("sqlite:///", "")
        db_dir = os.path.dirname(db_file)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"Created directory: {db_dir}")

    # Create all tables
    Base.metadata.create_all(bind=engine)

    # List created tables
    tables = Base.metadata.tables.keys()
    print(f"Database initialized with {len(tables)} table(s):")
    for table_name in tables:
        print(f"  - {table_name}")

    print("✓ Database initialization complete!")


if __name__ == "__main__":
    init_database()
