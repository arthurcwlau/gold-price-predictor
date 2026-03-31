"""

Repeatable scoring model for daily gold CSV updates.

What it does
------------
- Loads a CSV containing gold spot, macro data, and Polymarket tier probabilities
- Computes:
  1) 20-period correlation matrix
  2) Sentiment divergence / bull-trap check
  3) Leading-indicator checks for miners and copper
  4) Recession-floor assessment
  5) Momentum scoring from *_signal columns
  6) Market-implied fair value from tier probabilities
  7) Rule-based regime classification
  8) 7-day and 30-day target ranges with confidence

Usage
-----
python gold_scoring_model.py /path/to/gold_investment_pro.csv

Notes
-----
- This is a transparent rule-based quant model, not a black-box ML forecaster.
- It is designed for small and growing daily datasets.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Config
# ----------------------------

CORR_WINDOW = 20
TREND_WINDOW = 10
SHORT_HORIZON_DAYS = 7
MEDIUM_HORIZON_DAYS = 30

# Default tier midpoint map for market-implied fair value
DEFAULT_TIER_MIDPOINTS = {
    "gold_3_800_prob": 3600.0,
    "gold_3_800_4_200_prob": 4000.0,
    "gold_4_200_4_600_prob": 4400.0,
    "gold_4_600_5_000_prob": 4800.0,
    "gold_5_000_5_400_prob": 5200.0,
    "gold_5_400_5_800_prob": 5600.0,
    "gold_5_800_6_200_prob": 6000.0,
    "gold_6_200_prob": 6400.0,
}


@dataclass
class ForecastResult:
    as_of_date: Optional[str]
    current_spot: float
    regime: str
    corr_gold_dxy: Optional[float]
    corr_gold_vix: Optional[float]
    corr_gold_gdx: Optional[float]
    return_corr_gold_dxy: Optional[float]
    return_corr_gold_vix: Optional[float]
    sentiment_divergence: str
    bull_trap_flag: bool
    miners_signal: str
    copper_signal: str
    recession_floor_signal: str
    conviction_momentum_score: int
    conviction_momentum_label: str
    market_implied_fair_value: Optional[float]
    fair_value_gap_vs_spot: Optional[float]
    short_term_target_7d: str
    short_term_confidence_pct: int
    medium_term_target_30d: str
    medium_term_confidence_pct: int
    invalidation_risk: str


def _safe_last(series: pd.Series) -> Optional[float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.iloc[-1])


def _safe_corr(a: pd.Series, b: pd.Series, window: int = CORR_WINDOW) -> Optional[float]:
    df = pd.concat([pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")], axis=1).dropna()
    if len(df) < max(5, window // 2):
        return None
    if len(df) >= window:
        df = df.iloc[-window:]
    val = df.iloc[:, 0].corr(df.iloc[:, 1])
    return None if pd.isna(val) else float(val)


def _returns_corr(a: pd.Series, b: pd.Series, window: int = CORR_WINDOW) -> Optional[float]:
    a = pd.to_numeric(a, errors="coerce").pct_change()
    b = pd.to_numeric(b, errors="coerce").pct_change()
    return _safe_corr(a, b, window)


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _trend_direction(series: pd.Series, window: int = TREND_WINDOW) -> Optional[float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < max(5, window):
        return None
    s = s.iloc[-window:]
    x = np.arange(len(s), dtype=float)
    slope = np.polyfit(x, s.values.astype(float), 1)[0]
    return float(slope)


def _sum_signal_cols(df: pd.DataFrame) -> Tuple[int, List[str]]:
    signal_cols = [c for c in df.columns if c.startswith("gold_") and c.endswith("_signal")]
    total = 0
    used = []
    for c in signal_cols:
        val = _safe_last(df[c])
        if val is not None:
            total += int(round(val))
            used.append(c)
    return total, used


def _momentum_label(score: int) -> str:
    if score >= 5:
        return "High Conviction Momentum"
    if score >= 2:
        return "Moderate Conviction Momentum"
    if score >= 0:
        return "Neutral Momentum"
    return "Negative Momentum"


def _infer_probability_cols(df: pd.DataFrame) -> List[str]:
    cols = [c for c in df.columns if c in DEFAULT_TIER_MIDPOINTS]
    return cols


def _market_implied_fair_value(df: pd.DataFrame) -> Optional[float]:
    prob_cols = _infer_probability_cols(df)
    if not prob_cols:
        return None
    last = df[prob_cols].apply(pd.to_numeric, errors="coerce").iloc[-1]
    valid = last.dropna()
    if valid.empty:
        return None
    total = valid.sum()
    if total <= 0:
        return None
    weights = valid / total
    fair_value = 0.0
    for c, w in weights.items():
        fair_value += float(w) * DEFAULT_TIER_MIDPOINTS[c]
    return float(fair_value)


def _probability_shift_assessment(df: pd.DataFrame, spot_col: str) -> Tuple[str, bool]:
    prob_cols = _infer_probability_cols(df)
    if len(prob_cols) < 2:
        return ("Insufficient tier probability data", False)

    valid = df[[spot_col] + prob_cols].apply(pd.to_numeric, errors="coerce").dropna()
    if len(valid) < 8:
        return ("Insufficient aligned history for divergence check", False)

    sub = valid.iloc[-min(20, len(valid)):]
    spot_move = float(sub[spot_col].iloc[-1] - sub[spot_col].iloc[0])

    # Split tiers into "higher" and "lower/nearby" using midpoint vs current spot
    current_spot = float(sub[spot_col].iloc[-1])
    higher_cols = [c for c in prob_cols if DEFAULT_TIER_MIDPOINTS[c] > current_spot]
    near_lower_cols = [c for c in prob_cols if DEFAULT_TIER_MIDPOINTS[c] <= current_spot]

    higher_shift = float(sub[higher_cols].iloc[-1].sum() - sub[higher_cols].iloc[0].sum()) if higher_cols else 0.0
    near_lower_shift = float(sub[near_lower_cols].iloc[-1].sum() - sub[near_lower_cols].iloc[0].sum()) if near_lower_cols else 0.0

    bull_trap = bool(spot_move > 0 and higher_shift < -1.5)

    if spot_move > 0 and higher_shift < 0 and near_lower_shift > 0:
        text = "Rising spot with fading upside-tier probabilities: caution for a local bull trap."
    elif spot_move > 0 and higher_shift >= 0:
        text = "Spot rise is broadly confirmed by stable-to-rising upside-tier probabilities."
    elif spot_move < 0 and higher_shift > 0:
        text = "Spot weakness while upside-tier probabilities hold up: latent bullish divergence."
    else:
        text = "No strong sentiment divergence signal."
    return text, bull_trap


def _leading_indicator_signal(df: pd.DataFrame, spot_col: str, miners_col: Optional[str], copper_col: Optional[str]) -> Tuple[str, str]:
    spot_slope = _trend_direction(df[spot_col], TREND_WINDOW)
    miners_signal = "Insufficient miners data"
    copper_signal = "Insufficient copper data"

    if miners_col:
        miners_slope = _trend_direction(df[miners_col], TREND_WINDOW)
        if spot_slope is not None and miners_slope is not None:
            if miners_slope > spot_slope * 0.8 and miners_slope > 0:
                miners_signal = "Miners are confirming / modestly leading spot gold."
            elif miners_slope <= 0 and (spot_slope or 0) > 0:
                miners_signal = "Miners are lagging spot gold."
            else:
                miners_signal = "Miners are mixed versus spot gold."

    if copper_col:
        copper_slope = _trend_direction(df[copper_col], TREND_WINDOW)
        if copper_slope is not None:
            if copper_slope > 0:
                copper_signal = "Copper trend is supportive of broader macro strength."
            elif copper_slope < 0:
                copper_signal = "Copper is not confirming broad cyclical strength."
            else:
                copper_signal = "Copper is flat / neutral."

    return miners_signal, copper_signal


def _recession_floor_signal(df: pd.DataFrame, recession_prob_col: Optional[str], recession_vel_col: Optional[str]) -> str:
    if not recession_prob_col:
        return "Insufficient recession-market data"

    last_prob = _safe_last(df[recession_prob_col])
    last_vel = _safe_last(df[recession_vel_col]) if recession_vel_col else None

    if last_prob is None:
        return "Insufficient recession-market data"

    if last_vel is None:
        # fallback to local slope of the probability itself
        slope = _trend_direction(df[recession_prob_col], TREND_WINDOW)
        if slope is None:
            return "Insufficient recession-market data"
        if slope > 0:
            return "Recession hedge premium is increasing."
        if slope < 0:
            return "Recession hedge premium is decreasing."
        return "Recession hedge premium is flat."
    else:
        if last_vel > 0:
            return "Recession hedge premium is increasing."
        if last_vel < 0:
            return "Recession hedge premium is decreasing."
        return "Recession hedge premium is flat."


def _classify_regime(corr_dxy: Optional[float], corr_vix: Optional[float], recession_signal: str) -> str:
    if corr_dxy is not None and corr_dxy <= -0.55 and (corr_vix is None or abs(corr_dxy) > abs(corr_vix)):
        if "increasing" in recession_signal.lower():
            return "USD Devaluation Play with recession-floor support"
        return "USD Devaluation Play"
    if corr_vix is not None and corr_vix >= 0.35:
        return "Risk-Off Hedge"
    return "Mixed Macro Hedge"


def _forecast_ranges(
    spot: float,
    fair_value: Optional[float],
    corr_dxy: Optional[float],
    bull_trap: bool,
    momentum_score: int,
    miners_signal: str,
) -> Tuple[str, int, str, int, str]:
    base_7d_up = 0.012
    base_30d_up = 0.035
    conf_7d = 55
    conf_30d = 52

    if fair_value is not None and fair_value > spot:
        base_7d_up += min((fair_value - spot) / max(spot, 1.0), 0.025)
        base_30d_up += min((fair_value - spot) / max(spot, 1.0), 0.05)
        conf_7d += 4
        conf_30d += 4

    if corr_dxy is not None and corr_dxy <= -0.6:
        base_7d_up += 0.006
        base_30d_up += 0.012
        conf_7d += 3
        conf_30d += 3

    if bull_trap:
        base_7d_up -= 0.012
        base_30d_up -= 0.018
        conf_7d -= 8
        conf_30d -= 8

    if momentum_score >= 5:
        base_7d_up += 0.008
        base_30d_up += 0.018
        conf_7d += 5
        conf_30d += 4
    elif momentum_score >= 2:
        base_7d_up += 0.004
        base_30d_up += 0.01
        conf_7d += 3
        conf_30d += 2

    if "lagging" in miners_signal.lower():
        conf_7d -= 2
        conf_30d -= 3

    base_7d_down = max(0.006, base_7d_up * 0.55)
    base_30d_down = max(0.015, base_30d_up * 0.55)

    low_7d = spot * (1 - base_7d_down)
    high_7d = spot * (1 + base_7d_up)
    low_30d = spot * (1 - base_30d_down)
    high_30d = spot * (1 + base_30d_up)

    short_range = f"{low_7d:,.1f} to {high_7d:,.1f}"
    medium_range = f"{low_30d:,.1f} to {high_30d:,.1f}"

    invalidation = "A decisive DXY spike and rollover in >spot Polymarket tier probabilities"
    if "lagging" in miners_signal.lower():
        invalidation += ", especially if miners continue to underperform"

    return short_range, int(max(35, min(75, conf_7d))), medium_range, int(max(35, min(72, conf_30d))), invalidation


def analyze_gold_csv(csv_path: str | Path) -> ForecastResult:
    df = pd.read_csv(csv_path)

    spot_col = _find_col(df, ["gold_price", "gold_close", "close", "gc=F_close"])
    dxy_col = _find_col(df, ["dxy_index", "dxy", "dx-y.nyb_close"])
    vix_col = _find_col(df, ["vix_index", "vix", "^vix_close"])
    miners_col = _find_col(df, ["gold_miners", "gdx", "gdx_close"])
    copper_col = _find_col(df, ["copper_price", "copper", "hg=f_close"])
    recession_prob_col = _find_col(df, ["recession_us_recession_by_end_of_2026_prob"])
    recession_vel_col = _find_col(df, ["recession_us_recession_by_end_of_2026_velocity"])

    if not spot_col:
        raise ValueError("Could not find a gold spot column like 'gold_price'.")

    current_spot = float(pd.to_numeric(df[spot_col], errors="coerce").dropna().iloc[-1])
    as_of_date = None
    if "date" in df.columns:
        try:
            as_of_date = str(pd.to_datetime(df["date"]).dropna().iloc[-1].date())
        except Exception:
            as_of_date = str(df["date"].dropna().iloc[-1])

    corr_gold_dxy = _safe_corr(df[spot_col], df[dxy_col], CORR_WINDOW) if dxy_col else None
    corr_gold_vix = _safe_corr(df[spot_col], df[vix_col], CORR_WINDOW) if vix_col else None
    corr_gold_gdx = _safe_corr(df[spot_col], df[miners_col], CORR_WINDOW) if miners_col else None
    return_corr_gold_dxy = _returns_corr(df[spot_col], df[dxy_col], CORR_WINDOW) if dxy_col else None
    return_corr_gold_vix = _returns_corr(df[spot_col], df[vix_col], CORR_WINDOW) if vix_col else None

    sentiment_divergence, bull_trap_flag = _probability_shift_assessment(df, spot_col)
    miners_signal, copper_signal = _leading_indicator_signal(df, spot_col, miners_col, copper_col)
    recession_floor_signal = _recession_floor_signal(df, recession_prob_col, recession_vel_col)

    momentum_score, _ = _sum_signal_cols(df)
    momentum_label = _momentum_label(momentum_score)

    fair_value = _market_implied_fair_value(df)
    fair_gap = None if fair_value is None else float(fair_value - current_spot)

    regime = _classify_regime(corr_gold_dxy, corr_gold_vix, recession_floor_signal)

    short_range, short_conf, medium_range, medium_conf, invalidation = _forecast_ranges(
        spot=current_spot,
        fair_value=fair_value,
        corr_dxy=corr_gold_dxy,
        bull_trap=bull_trap_flag,
        momentum_score=momentum_score,
        miners_signal=miners_signal,
    )

    return ForecastResult(
        as_of_date=as_of_date,
        current_spot=current_spot,
        regime=regime,
        corr_gold_dxy=corr_gold_dxy,
        corr_gold_vix=corr_gold_vix,
        corr_gold_gdx=corr_gold_gdx,
        return_corr_gold_dxy=return_corr_gold_dxy,
        return_corr_gold_vix=return_corr_gold_vix,
        sentiment_divergence=sentiment_divergence,
        bull_trap_flag=bull_trap_flag,
        miners_signal=miners_signal,
        copper_signal=copper_signal,
        recession_floor_signal=recession_floor_signal,
        conviction_momentum_score=momentum_score,
        conviction_momentum_label=momentum_label,
        market_implied_fair_value=fair_value,
        fair_value_gap_vs_spot=fair_gap,
        short_term_target_7d=short_range,
        short_term_confidence_pct=short_conf,
        medium_term_target_30d=medium_range,
        medium_term_confidence_pct=medium_conf,
        invalidation_risk=invalidation,
    )


def format_report(result: ForecastResult) -> str:
    def fmt(x):
        if x is None:
            return "N/A"
        if isinstance(x, float):
            return f"{x:.2f}"
        return str(x)

    lines = [
        "GOLD SCORING MODEL REPORT",
        "=" * 28,
        f"As of date: {fmt(result.as_of_date)}",
        f"Current spot: {result.current_spot:,.2f}",
        "",
        f"Current regime: {result.regime}",
        "",
        "Correlation block",
        f"- 20p corr gold vs DXY: {fmt(result.corr_gold_dxy)}",
        f"- 20p corr gold vs VIX: {fmt(result.corr_gold_vix)}",
        f"- 20p corr gold vs GDX: {fmt(result.corr_gold_gdx)}",
        f"- 20p return corr gold vs DXY: {fmt(result.return_corr_gold_dxy)}",
        f"- 20p return corr gold vs VIX: {fmt(result.return_corr_gold_vix)}",
        "",
        "Sentiment divergence",
        f"- {result.sentiment_divergence}",
        f"- Bull trap flag: {result.bull_trap_flag}",
        "",
        "Leading indicators",
        f"- Miners: {result.miners_signal}",
        f"- Copper: {result.copper_signal}",
        "",
        "Recession floor",
        f"- {result.recession_floor_signal}",
        "",
        "Momentum",
        f"- Score: {result.conviction_momentum_score}",
        f"- Label: {result.conviction_momentum_label}",
        "",
        "Market-implied fair value",
        f"- Fair value: {fmt(result.market_implied_fair_value)}",
        f"- Gap vs spot: {fmt(result.fair_value_gap_vs_spot)}",
        "",
        "Forecast",
        f"- 7-day target: {result.short_term_target_7d} ({result.short_term_confidence_pct}% confidence)",
        f"- 30-day target: {result.medium_term_target_30d} ({result.medium_term_confidence_pct}% confidence)",
        "",
        "Invalidation risk",
        f"- {result.invalidation_risk}",
    ]
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: python gold_scoring_model.py /path/to/file.csv")
        sys.exit(1)

    csv_path = sys.argv[1]
    result = analyze_gold_csv(csv_path)
    print(format_report(result))

    json_path = Path(csv_path).with_suffix(".analysis.json")
    json_path.write_text(json.dumps(asdict(result), indent=2), encoding="utf-8")
    print(f"\nSaved JSON summary to: {json_path}")


if __name__ == "__main__":
    main()
