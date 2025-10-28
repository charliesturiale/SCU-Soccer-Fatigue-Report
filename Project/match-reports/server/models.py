from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, JSON

class Base(DeclarativeBase): pass

class ValdTest(Base):
    __tablename__ = "vald_test"
    id: Mapped[int] = mapped_column(primary_key=True)
    team_code: Mapped[str] = mapped_column(String(16))
    test_id: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)

class CatapultSession(Base):
    __tablename__ = "catapult_session"
    id: Mapped[int] = mapped_column(primary_key=True)
    team_code: Mapped[str] = mapped_column(String(16))
    session_id: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
