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
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    # short_name: Mapped[Optional[str]] = mapped_column(String(24))
    # org: Mapped[Optional[str]] = mapped_column(String(120))  # e.g., school/club

    last_profile_update: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=func.now(),
        nullable=False,
    )
    last_report_gen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=func.now(),
        nullable=False,
    )

    # created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    roster_memberships: Mapped[List[Roster]] = relationship(
        back_populates="team", cascade="all, delete-orphan"
    )


# ---------------------------
# Player & Roster (membership)
# ---------------------------
class Player(Base):
    __tablename__ = "player"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(60), index=True)
    last_name: Mapped[str] = mapped_column(String(60), index=True)
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date)

    # Optional external keys for integrations
    catapult_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    vald_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)

    #Date they joined the team / earliest data
    join_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    provider: Mapped[str] = mapped_column(String(24))
    # Stable identifier for code (use snake_case)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    unit: Mapped[Optional[str]] = mapped_column(String(24))  # e.g., m, m/s, %, min, N, cm
    lower_is_better: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    player_values: Mapped[List[PlayerMetricValue]] = relationship(
        back_populates="metric", cascade="all, delete-orphan"
    )


# ---------------------------
# Player x Metric values (reference & previous)
# ---------------------------
class PlayerMetricValue(Base):
    __tablename__ = "player_metric_value"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    player_id: Mapped[int] = mapped_column(ForeignKey("player.id", ondelete="CASCADE"), index=True)
    metric_id: Mapped[int] = mapped_column(ForeignKey("metric.id", ondelete="CASCADE"), index=True)

    # Core requirement: both a reference and a previous value per metric
    reference_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4))
    previous_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4))

    # Useful metadata
    # last_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    # last_source: Mapped[Optional[str]] = mapped_column(String(64))  # e.g., "catapult", "vald", "manual"

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


def get_or_create_metric(
    session: Session,
    *,
    provider: str,
    code: str,
    name: str,
    unit: Optional[str] = None,
    lower_is_better: bool = False
) -> Metric:
    # Get or create a Metric by code (unique identifier).
    metric = session.query(Metric).filter_by(code=code).one_or_none()
    if metric is None:
        metric = Metric(
            provider=provider,
            code=code,
            name=name,
            unit=unit,
            lower_is_better=lower_is_better,
        )
        session.add(metric)
        session.flush()
    return metric


def upsert_player_metric_value(
    session: Session,
    *,
    player_id: int,
    metric_id: int,
    reference_value: Optional[float] = None,
    previous_value: Optional[float] = None
) -> PlayerMetricValue:
    # Create or update the PlayerMetricValue for (player, metric) using metric_id.
    pmv = (
        session.query(PlayerMetricValue)
        .filter_by(player_id=player_id, metric_id=metric_id)
        .one_or_none()
    )
    if pmv is None:
        pmv = PlayerMetricValue(
            player_id=player_id,
            metric_id=metric_id,
            reference_value=reference_value,
            previous_value=previous_value
        )
        session.add(pmv)
    else:
        if reference_value is not None:
            pmv.reference_value = reference_value
        if previous_value is not None:
            pmv.previous_value = previous_value
    session.flush()
    return pmv


#METRIC SEEDING: This is where we decide what metrics are tracked and stored in the DB
DEFAULT_METRICS = [
    # Catapult metrics
    ("Total Distance",       "catapult", "total_distance",                                 "m",   False),
    ("HSR",                  "catapult", "high_speed_distance",                            "m",   False),
    ("Percent Max Velocity", "catapult", "percentage_max_velocity",                        "%",   False),
    ("High Band HR Time",    "catapult", "red_zone",                                       "min", False),
    ("High Band Accel",      "catapult", "gen2_acceleration_band6plus_total_effort_count", "ct",  False),
    ("High Band Decel",      "catapult", "gen2_acceleration_band3plus_total_effort_count", "ct",  False),

    ("Total Player Load",        "catapult", "total_player_load",         "",  False),
    ("Player Load Per Minute",   "catapult", "player_load_per_minute",    "",  False),
    ("Meterage Per Minute",      "catapult", "meterage_per_minute",       "",  False),
    ("Total Acceleration Load",  "catapult", "total_acceleration_load",   "",  False),
    ("Average Trimp",            "catapult", "avg_trimp",                 "",  False),
    ("Percent Max Heart Rate",   "catapult", "percentage_max_heart_rate", "",  False),


    # VALD ForceDecks metrics (using resultId as code for easy access in trials)
    ("Jump Height (Flight Time)",    "vald_forcedecks", "6553607",  "cm",   False),
    ("RSI-Modified",                 "vald_forcedecks", "6553698",  "m/s",  False),
    ("Peak Power / BM",              "vald_forcedecks", "6553604",  "W/kg", False),
    ("Countermovement Depth",        "vald_forcedecks", "6553603",  "cm",   False),
    ("Concentric Mean Force",        "vald_forcedecks", "6553619",  "N",    False),

    # VALD NordBord metrics - raw data, will use in calculations later
    ("Left Average Force",  "vald_nordbord", "leftAvgForce", "cm",   False),
    ("Left Impulse",        "vald_nordbord", "leftImpulse",  "cm",   False),
    ("Left Max Force",      "vald_nordbord", "leftMaxForce", "m/s",  False),
    ("Left Torque",         "vald_nordbord", "leftTorque",   "W/kg", False),
    
    ("Right Average Force",  "vald_nordbord", "rightAvgForce", "N",   False),
    ("RIght Impulse",        "vald_nordbord", "rightImpulse",  "N*s",   False),
    ("RIght Max Force",      "vald_nordbord", "rightMaxForce", "N",  False),
    ("RIght Torque",         "vald_nordbord", "rightTorque",   "N*m", False),
]

def seed_default_metrics(session: Session) -> None:
    for name, provider, code, unit, lower_is_better in DEFAULT_METRICS:
        get_or_create_metric(session, name=name, provider=provider, code=code, unit=unit, lower_is_better=lower_is_better)
    session.commit()


# ---------------------------
# End of models
# ---------------------------

