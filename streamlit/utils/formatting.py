"""
streamlit/utils/formatting.py
------------------------------
Presentation formatting helpers for the Spotify Music Intelligence Application.
Adheres strictly to zero-transformation principles: these helpers only format
pre-computed metrics for UI rendering (e.g., number commas, durations, badges).
"""

from typing import Any, Optional


def format_number(val: Any) -> str:
    """Formats an integer or float with thousands separators."""
    if val is None:
        return "—"
    try:
        num = float(val)
        if num.is_integer():
            return f"{int(num):,}"
        return f"{num:,.1f}"
    except (ValueError, TypeError):
        return str(val)


def format_compact_number(val: Any) -> str:
    """Formats large numbers into compact human-readable representations (e.g. 3.8K, 1.2M)."""
    if val is None:
        return "—"
    try:
        num = float(val)
        if abs(num) >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        if abs(num) >= 1_000:
            return f"{num / 1_000:.1f}K"
        return f"{int(num)}" if num.is_integer() else f"{num:.1f}"
    except (ValueError, TypeError):
        return str(val)


def format_duration_ms(ms: Optional[int]) -> str:
    """
    Converts milliseconds into MM:SS format.
    Example: 185000 ms -> '03:05'
    """
    if ms is None or ms <= 0:
        return "00:00"
    try:
        total_seconds = int(ms) // 1000
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes:02d}:{seconds:02d}"
    except (ValueError, TypeError):
        return "00:00"


def format_percentage(val: Any, is_ratio: bool = False, decimals: int = 1) -> str:
    """
    Formats a numeric percentage.
    If is_ratio=True, expects 0.274 -> '27.4%'.
    If is_ratio=False, expects 27.4 -> '27.4%'.
    """
    if val is None:
        return "—"
    try:
        num = float(val)
        if is_ratio:
            num = num * 100.0
        return f"{num:.{decimals}f}%"
    except (ValueError, TypeError):
        return str(val)


def momentum_badge(score: Optional[float]) -> str:
    """
    Returns an emoji and tier label based on the calibrated Catalog Momentum Index.
    Pillars calibrated: [0.0, 100.0]
    """
    if score is None:
        return "⚪ Unranked"
    try:
        s = float(score)
        if s >= 75.0:
            return "🔥 Hyper-Paced"
        if s >= 50.0:
            return "⚡ High Velocity"
        if s >= 25.0:
            return "📈 Steady Expansion"
        return "⏳ Dormant / Archival"
    except (ValueError, TypeError):
        return "⚪ Unknown"


def activity_tier_badge(tier: Optional[str]) -> str:
    """Returns a formatted badge for artist activity tier."""
    if not tier:
        return "⚪ Unknown"
    tier_lower = str(tier).lower()
    if "hyper" in tier_lower:
        return f"🔥 {tier}"
    if "active" in tier_lower:
        return f"⚡ {tier}"
    if "moderate" in tier_lower:
        return f"📈 {tier}"
    return f"⏳ {tier}"


def strategy_badge(strategy: Optional[str]) -> str:
    """Returns a formatted badge for catalog publishing strategy."""
    if not strategy:
        return "⚪ Unclassified"
    strat_lower = str(strategy).lower()
    if "single" in strat_lower:
        return f"🎵 {strategy}"
    if "hybrid" in strat_lower or "balanced" in strat_lower:
        return f"⚖️ {strategy}"
    return f"💿 {strategy}"
