import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///../data/project.db")
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
