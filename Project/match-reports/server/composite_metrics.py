# composite_metrics.py
"""
Z-score-based composite metrics registry.

These functions compute derived metrics from Z-scores of other metrics rather than
raw values. They are intended to be computed AFTER all z-scores are calculated
in the report generation step.

Usage pattern in GenReport.py
------------------------------
1. Collect all metrics and compute z-scores for each metric vs player profile
2. For each player, build a dict of z-scores:
   z_scores = {
       "total_distance": 1.5,
       "high_speed_distance": 0.8,
       "6553607": -0.3,  # Jump Height
       ...
   }
3. Call:
   composite_z_scores = compute_composite_metrics(z_scores)
4. Add the composite z-scores to the report as new columns

Example composite metric:
-------------------------
Explosiveness Index = average of z-scores for:
  - Jump Height (ForceDecks 6553607)
  - RSI-Modified (ForceDecks 6553698)
  - Peak Power / BM (ForceDecks 6553604)
"""

from __future__ import annotations
from typing import Dict, Optional, Callable, List


ZScoreDict = Dict[str, float]
# Signature: (z_scores) -> optional float
CompositeFunc = Callable[[ZScoreDict], Optional[float]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_average(z_scores: ZScoreDict, metric_codes: List[str]) -> Optional[float]:
    """
    Calculate average of z-scores for the given metric codes.

    Returns None if no valid z-scores are available.
    """
    values = []
    for code in metric_codes:
        z_score = z_scores.get(code)
        if z_score is not None:
            values.append(z_score)

    if not values:
        return None

    return sum(values) / len(values)


# ---------------------------------------------------------------------------
# Composite Metric Functions
# ---------------------------------------------------------------------------

def compute_explosiveness_index(z_scores: ZScoreDict) -> Optional[float]:
    """
    Explosiveness Index: Average z-score of explosive power metrics.

    Combines:
    - 6553607: Jump Height (Flight Time)
    - 6553698: RSI-Modified
    - 6553604: Peak Power / BM

    Returns the average z-score across available metrics.
    """
    metric_codes = [
        "6553607",  # Jump Height (Flight Time)
        "6553698",  # RSI-Modified
        "6553604",  # Peak Power / BM
    ]

    return _safe_average(z_scores, metric_codes)

# def compute_explosive_output(z_scores: ZScoreDict) -> Optional[float]:
#     """
#     Explosive Output: Weighted combination of power and force z-scores.
#
#     Equation: 0.5 * (Peak Power / BM z-score) + 0.5 * (Concentric Mean Force z-score)
#
#     Combines:
#     - 6553604: Peak Power / BM (W/kg)
#     - 6553619: Concentric Mean Force (N)
#
#     Returns the weighted average of these two z-scores (50/50 split).
#     """
#     pp_bm_z = z_scores.get("6553604")  # Peak Power / BM
#     cmf_z = z_scores.get("6553619")     # Concentric Mean Force
#
#     # Both metrics are required for this calculation
#     if pp_bm_z is None or cmf_z is None:
#         return None
#
#     # Weighted combination (50/50)
#     explosive_output = 0.5 * pp_bm_z + 0.5 * cmf_z
#
#     return explosive_output


# ---------------------------------------------------------------------------
# Registry + convenience API
# ---------------------------------------------------------------------------

COMPOSITE_FUNCS: Dict[str, CompositeFunc] = {
    # Z-score composite metrics
    "explosiveness_index": compute_explosiveness_index,
    # "explosive_output": compute_explosive_output,

    # Add more composite metrics here as needed:
    # "endurance_index": compute_endurance_index,
    # "strength_index": compute_strength_index,
}


def is_composite_metric(code: str) -> bool:
    """Return True if this metric code is a composite z-score metric."""
    return code in COMPOSITE_FUNCS


def compute_composite_metrics(z_scores: ZScoreDict) -> Dict[str, float]:
    """
    Compute all composite metrics from z-scores.

    Parameters
    ----------
    z_scores : dict
        Dictionary mapping metric codes to their z-scores for a single player

    Returns
    -------
    dict
        Dictionary mapping composite metric codes to their computed z-scores
    """
    composite_values: Dict[str, float] = {}

    for code, func in COMPOSITE_FUNCS.items():
        value = func(z_scores)
        if value is not None:
            composite_values[code] = float(value)

    return composite_values


# ---------------------------------------------------------------------------
# Metadata for report generation
# ---------------------------------------------------------------------------

COMPOSITE_METRIC_METADATA = {
    "explosiveness_index": {
        "name": "Explosiveness Index",
        "description": "Composite z-score of explosive power metrics (Jump Height, RSI-Modified, Peak Power)",
        "provider": "composite",
        "unit": "z-score",
        "component_metrics": ["6553607", "6553698", "6553604"],
    },
    # "explosive_output": {
    #     "name": "Explosive Output",
    #     "description": "Weighted combination of Peak Power/BM and Concentric Mean Force z-scores (50/50 split)",
    #     "provider": "composite",
    #     "unit": "z-score",
    #     "component_metrics": ["6553604", "6553619"],
    # },
    # Add metadata for future composite metrics here
}


def get_composite_metric_name(code: str) -> str:
    """Get the display name for a composite metric."""
    metadata = COMPOSITE_METRIC_METADATA.get(code, {})
    return metadata.get("name", code)


def get_required_metrics_for_composite(code: str) -> List[str]:
    """Get the list of component metric codes required for a composite metric."""
    metadata = COMPOSITE_METRIC_METADATA.get(code, {})
    return metadata.get("component_metrics", [])
