# derived_metrics.py
"""
Per-trial derived metrics registry.

These functions are intended to be called from the profile-building step.
Given a dict of raw trial values (Catapult / VALD) and optional player
context (e.g. body mass), they return scalar derived metrics keyed by the
same code strings that appear in the Metric table.

Usage pattern in build_profiles
-------------------------------
1. Build a dict of raw metrics for a single trial/test:
   trial = {
       "leftMaxForce": ...,
       "rightMaxForce": ...,
       "leftAvgForce": ...,
       "rightAvgForce": ...,
       "leftImpulse": ...,
       "rightImpulse": ...,
       "6553607": ...,   # Jump Height (Flight Time)
       "6553603": ...,   # Countermovement Depth
       "6553619": ...,   # Concentric Mean Force
       "gen2_acceleration_band7plus_total_effort_count": ...,
       "gen2_acceleration_band2plus_total_effort_count": ...,
       ...
   }

2. Call:
   derived = compute_derived_metrics(trial, body_mass=player_body_mass)

3. Merge `derived` into your per-trial metric set before aggregating:
   trial_metrics.update(derived)

4. On the DB side, seed Metric rows like:
   ("High-Intensity Efforts", "derived", "high_intensity_efforts", "ct", False)
   ("FD Stiffness",           "derived", "fd_stiffness",          "",   False)
   ("FD CMF / BM",            "derived", "fd_cmf_rel",            "N/kg", False)
   ("NordBord Strength (rel)","derived", "nordbord_strength_rel", "N/kg", False)
   ("NordBord Asymmetry",     "derived", "nordbord_asym",         "%",   True)
   ("NordBord Total Impulse", "derived", "nordbord_total_impulse","N*s", False)

Then your existing aggregation → PlayerMetricValue pipeline can treat
these like any other metric.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Callable
import math


TrialDict = Dict[str, Any]
# Signature: (trial, body_mass) -> optional scalar
DerivedFunc = Callable[[TrialDict, Optional[float]], Optional[float]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(val: Any) -> Optional[float]:
    """Safely convert a numeric-like value to float; return None if not possible."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _sanitize(value: Optional[float]) -> Optional[float]:
    """Drop NaN/inf; return a clean float or None."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


# ---------------------------------------------------------------------------
# Catapult-derived metrics
# ---------------------------------------------------------------------------

def compute_high_intensity_efforts(
    trial: TrialDict, body_mass: Optional[float] = None
) -> Optional[float]:
    """
    High-Intensity Efforts = High-band accelerations + high-band decelerations.

    Uses Catapult Gen2 band 7+ for accels and 2+ for decels.
    """
    accel = _to_float(trial.get("gen2_acceleration_band7plus_total_effort_count"))
    decel = _to_float(trial.get("gen2_acceleration_band2plus_total_effort_count"))
    if accel is None or decel is None:
        return None
    return _sanitize(accel + decel)


# ---------------------------------------------------------------------------
# ForceDecks-derived metrics
# ---------------------------------------------------------------------------

# def compute_fd_stiffness(
#     trial: TrialDict, body_mass: Optional[float] = None
# ) -> Optional[float]:
#     """
#     Stiffness / strategy index for CMJ:
#         stiffness_index = jump_height / countermovement_depth

#     Both in cm, so the ratio is unitless.
#     """
#     jh = _to_float(trial.get("6553607"))   # Jump Height (Flight Time)
#     cmd = _to_float(trial.get("6553603"))  # Countermovement Depth
#     if jh is None or cmd is None or cmd <= 0:
#         return None
#     return _sanitize(jh / cmd)


# def compute_fd_cmf_rel(
#     trial: TrialDict, body_mass: Optional[float] = None
# ) -> Optional[float]:
#     """
#     Concentric Mean Force normalized to body mass:
#         CMF_rel = concentric_mean_force / body_mass

#     This is useful for combining with Peak Power / BM in a meta "explosive z".
#     """
#     cmf = _to_float(trial.get("6553619"))  # Concentric Mean Force (N)
#     if cmf is None or body_mass is None or body_mass <= 0:
#         return None
#     return _sanitize(cmf / body_mass)


# ---------------------------------------------------------------------------
# NordBord-derived metrics
# ---------------------------------------------------------------------------

def _compute_nordbord_leg_strengths(
    trial: TrialDict,
) -> Optional[tuple[float, float]]:
    """
    Compute composite strength for left/right legs for a single NordBord trial.

    Base formulation (no impulse):
        L_strength = 0.6 * L_max + 0.4 * L_avg
        R_strength = 0.6 * R_max + 0.4 * R_avg
    """
    L_max = _to_float(trial.get("leftMaxForce"))
    R_max = _to_float(trial.get("rightMaxForce"))
    L_avg = _to_float(trial.get("leftAvgForce"))
    R_avg = _to_float(trial.get("rightAvgForce"))

    if L_max is None or R_max is None or L_avg is None or R_avg is None:
        return None

    L_strength = 0.6 * L_max + 0.4 * L_avg
    R_strength = 0.6 * R_max + 0.4 * R_avg
    return L_strength, R_strength


def compute_nordbord_strength_rel(
    trial: TrialDict, body_mass: Optional[float] = None
) -> Optional[float]:
    """
    Bilateral NordBord strength normalized to body mass:

        L_strength, R_strength = composite(left/right)  # see helper above
        bilateral_strength      = (L_strength + R_strength) / 2          [N]
        strength_rel            = bilateral_strength / body_mass         [N/kg]

    Returns N/kg. Requires a non-zero body mass.
    """
    if body_mass is None or body_mass <= 0:
        return None

    strengths = _compute_nordbord_leg_strengths(trial)
    if strengths is None:
        return None

    L_strength, R_strength = strengths
    bilateral_strength = (L_strength + R_strength) / 2.0
    return _sanitize(bilateral_strength / body_mass)


def compute_nordbord_asym(
    trial: TrialDict, body_mass: Optional[float] = None
) -> Optional[float]:
    """
    NordBord asymmetry (%), based on composite leg strengths:

        asym_pct = 100 * |L_strength - R_strength| / max(L_strength, R_strength)
    """
    strengths = _compute_nordbord_leg_strengths(trial)
    if strengths is None:
        return None

    L_strength, R_strength = strengths
    denom = max(L_strength, R_strength)
    if denom <= 0:
        return None

    asym_pct = 100.0 * abs(L_strength - R_strength) / denom
    return _sanitize(asym_pct)


# ---------------------------------------------------------------------------
# Registry + convenience API
# ---------------------------------------------------------------------------

DERIVED_FUNCS: Dict[str, DerivedFunc] = {
    # Catapult
    "high_intensity_efforts": compute_high_intensity_efforts,

    # ForceDecks
    # "fd_stiffness":  compute_fd_stiffness,
    # "fd_cmf_rel":    compute_fd_cmf_rel,

    # NordBord
    "nordbord_strength_rel":  compute_nordbord_strength_rel,
    "nordbord_asym":          compute_nordbord_asym,
    # "nordbord_total_impulse": compute_nordbord_total_impulse,
}


def is_derived_metric(code: str) -> bool:
    """Return True if this metric code is backed by a derived function."""
    return code in DERIVED_FUNCS


def compute_derived_metrics(
    trial: TrialDict,
    *,
    body_mass: Optional[float] = None,
) -> Dict[str, float]:
    """
    Compute all derived metrics for a single trial.

    Returns:
        dict mapping Metric.code -> float, for any derived metrics that are
        computable given the fields present in `trial` and the supplied
        `body_mass`.
    """
    out: Dict[str, float] = {}
    for code, func in DERIVED_FUNCS.items():
        value = func(trial, body_mass)
        value = _sanitize(value)
        if value is None:
            continue
        out[code] = float(value)
    return out
