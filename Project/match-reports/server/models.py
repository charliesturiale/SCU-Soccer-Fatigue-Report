"""
Sports Science / Soccer – SQLAlchemy 2.0 models
------------------------------------------------
- Teams have Rosters (membership of Players on a Team).
- Tracked metrics are defined in Metric.
- For each Player x Metric, we store reference_value and previous_value.

Notes
-----
* Numeric precision can be tuned per your data; defaults here are DECIMAL(14,4).
* Postgres recommended. If you're on SQLite for local dev, everything still works.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional, List

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ---------------------------
# Base
# ---------------------------
class Base(DeclarativeBase):
    pass


# ---------------------------
# Team
# ---------------------------
class Team(Base):
    __tablename__ = "team"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    short_name: Mapped[Optional[str]] = mapped_column(String(24))
    org: Mapped[Optional[str]] = mapped_column(String(120))  # e.g., school/club

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    roster_memberships: Mapped[List[Roster]] = relationship(
        back_populates="team", cascade="all, delete-orphan"
    )


# ---------------------------
# Player & Roster (membership)
# ---------------------------
class Player(Base):
    __tablename__ = "player"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(60), index=True)
    last_name: Mapped[str] = mapped_column(String(60), index=True)
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date)

    # Optional external keys for integrations
    catapult_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    vald_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    roster_memberships: Mapped[List[Roster]] = relationship(
        back_populates="player", cascade="all, delete-orphan"
    )
    metric_values: Mapped[List[PlayerMetricValue]] = relationship(
        back_populates="player", cascade="all, delete-orphan"
    )

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


class Roster(Base):
    __tablename__ = "roster"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("team.id", ondelete="CASCADE"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("player.id", ondelete="CASCADE"), index=True)

    squad_number: Mapped[Optional[int]] = mapped_column(Integer)  # jersey number
    position: Mapped[Optional[str]] = mapped_column(String(24))   # e.g., CB, CM, W, ST, GK
    status: Mapped[str] = mapped_column(String(24), default="active", server_default="active")
    joined_on: Mapped[Optional[date]] = mapped_column(Date)
    left_on: Mapped[Optional[date]] = mapped_column(Date)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    team: Mapped[Team] = relationship(back_populates="roster_memberships")
    player: Mapped[Player] = relationship(back_populates="roster_memberships")

    __table_args__ = (
        # Ensure a player appears at most once per team
        UniqueConstraint("team_id", "player_id", name="uq_roster_team_player"),
        Index("ix_roster_team", "team_id"),
    )


# ---------------------------
# Metric catalog
# ---------------------------
class Metric(Base):
    __tablename__ = "metric"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Stable identifier for code (use snake_case)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    unit: Mapped[Optional[str]] = mapped_column(String(24))  # e.g., m, m/s, %, min, N, cm
    description: Mapped[Optional[str]] = mapped_column(Text)
    lower_is_better: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    precision: Mapped[int] = mapped_column(Integer, default=2, server_default="2")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    player_values: Mapped[List[PlayerMetricValue]] = relationship(
        back_populates="metric", cascade="all, delete-orphan"
    )


# ---------------------------
# Player x Metric values (reference & previous)
# ---------------------------
class PlayerMetricValue(Base):
    __tablename__ = "player_metric_value"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    player_id: Mapped[int] = mapped_column(ForeignKey("player.id", ondelete="CASCADE"), index=True)
    metric_id: Mapped[int] = mapped_column(ForeignKey("metric.id", ondelete="CASCADE"), index=True)

    # Core requirement: both a reference and a previous value per metric
    reference_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4))
    previous_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4))

    # Useful metadata
    last_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_source: Mapped[Optional[str]] = mapped_column(String(64))  # e.g., "catapult", "vald", "manual"

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    player: Mapped[Player] = relationship(back_populates="metric_values")
    metric: Mapped[Metric] = relationship(back_populates="player_values")

    __table_args__ = (
        # Unique per player x metric
        UniqueConstraint("player_id", "metric_id", name="uq_player_metric"),
        Index("ix_player_metric", "player_id", "metric_id"),
    )


# ---------------------------
# Convenience helpers
# ---------------------------
from sqlalchemy.orm import Session


def get_or_create_metric(session: Session, code: str, name: Optional[str] = None, unit: Optional[str] = None,
                         description: Optional[str] = None, lower_is_better: bool = False, precision: int = 2) -> Metric:
    metric = session.query(Metric).filter_by(code=code).one_or_none()
    if metric is None:
        metric = Metric(
            code=code,
            name=name or code.replace("_", " ").title(),
            unit=unit,
            description=description,
            lower_is_better=lower_is_better,
            precision=precision,
        )
        session.add(metric)
        session.flush()
    return metric


def upsert_player_metric_value(
    session: Session,
    *,
    player_id: int,
    metric_code: str,
    reference_value: Optional[float] = None,
    previous_value: Optional[float] = None,
    last_observed_at: Optional[datetime] = None,
    last_source: Optional[str] = None,
) -> PlayerMetricValue:
    """Create or update the PlayerMetricValue for (player, metric)."""
    metric = get_or_create_metric(session, code=metric_code)

    pmv = (
        session.query(PlayerMetricValue)
        .filter_by(player_id=player_id, metric_id=metric.id)
        .one_or_none()
    )
    if pmv is None:
        pmv = PlayerMetricValue(
            player_id=player_id,
            metric_id=metric.id,
            reference_value=reference_value,
            previous_value=previous_value,
            last_observed_at=last_observed_at,
            last_source=last_source,
        )
        session.add(pmv)
    else:
        if reference_value is not None:
            pmv.reference_value = reference_value
        if previous_value is not None:
            pmv.previous_value = previous_value
        if last_observed_at is not None:
            pmv.last_observed_at = last_observed_at
        if last_source is not None:
            pmv.last_source = last_source
    session.flush()
    return pmv


# ---------------------------
# Optional: quick metric seed
# ---------------------------
DEFAULT_METRICS = [
    ("total_distance_m", "Total Distance", "m", "Total distance covered in a session."),
    ("hsr_distance_m", "High-Speed Running Distance", "m", "Distance above HSR threshold."),
    ("pct_max_velocity", "% Max Velocity Reached", "%", "Percent of max velocity reached."),
    ("hr_high_band_min", "High-Band Heart Rate Time", "min", "Minutes spent above HR threshold."),
    ("accel_decel_high", "High-Band Accel/Decel Count", None, "Sum of high-band accel+decel events."),
    ("rsi_mod", "RSI-Modified", None, "Reactive Strength Index (modified)."),
    ("jump_height_cm", "CMJ Jump Height", "cm", "Countermovement jump height."),
]


def seed_default_metrics(session: Session) -> None:
    for code, name, unit, desc in DEFAULT_METRICS:
        get_or_create_metric(session, code=code, name=name, unit=unit, description=desc)
    session.commit()


# ---------------------------
# End of models
# ---------------------------

