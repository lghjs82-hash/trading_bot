#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETH INTRADAY TREND ENGINE v8.1 RECLAIM-FIRST DUAL
- ETH 전용 기본값 (환경변수로 다른 심볼도 가능)
- 당일 청산
- reclaim 중심 / pullback 보조 / 하루 0~3회 지향
- 4h 방향 / 1h 구조 / 15min reclaim + EMA pullback setup / 5min trigger
- CSV 없으면 Binance 공개 OHLCV 자동 백필
- Engine C dual:
  Sweep Reclaim LONG/SHORT 중심 + 엄격한 EMA Pullback Continuation 보조
"""

from __future__ import annotations

import os
import time
import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import ccxt


# ============================================================
# CONFIG
# ============================================================

BTC_BACKTEST_CSV = os.getenv("BTC_BACKTEST_CSV", "").strip()
BTC_OUTPUT_DIR = os.getenv("BTC_OUTPUT_DIR", "./eth_intraday_trend_out").strip()

BACKFILL_IF_MISSING = os.getenv("BACKFILL_IF_MISSING", "1").strip() == "1"
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "180"))
BACKFILL_EXCHANGE = os.getenv("BACKFILL_EXCHANGE", "binanceusdm").strip()
BACKFILL_SYMBOL = os.getenv("BACKFILL_SYMBOL", "ETH/USDT:USDT").strip()
AUTO_BACKFILL_CSV_NAME = os.getenv("AUTO_BACKFILL_CSV_NAME", "eth_5m_backfill_generated.csv").strip()

START_CAPITAL = float(os.getenv("START_CAPITAL", "10000"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.005"))
MAX_NOTIONAL_USDT = float(os.getenv("MAX_NOTIONAL_USDT", "5000"))
LEVERAGE = float(os.getenv("LEVERAGE", "3"))

TAKER_FEE_BPS = float(os.getenv("TAKER_FEE_BPS", "5.0"))
ENTRY_SLIPPAGE_BPS = float(os.getenv("ENTRY_SLIPPAGE_BPS", "1.0"))
EXIT_SLIPPAGE_BPS = float(os.getenv("EXIT_SLIPPAGE_BPS", "1.2"))

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "4"))
COOLDOWN_BARS_5M = int(os.getenv("COOLDOWN_BARS_5M", "6"))

ALLOW_LONG = os.getenv("ALLOW_LONG", "1").strip() == "1"
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "1").strip() == "1"
ALLOW_LONG_RECLAIM = os.getenv("ALLOW_LONG_RECLAIM", "1").strip() == "1"
ALLOW_SHORT_RECLAIM = os.getenv("ALLOW_SHORT_RECLAIM", "1").strip() == "1"
ALLOW_LONG_PULLBACK = os.getenv("ALLOW_LONG_PULLBACK", "1").strip() == "1"
ALLOW_SHORT_PULLBACK = os.getenv("ALLOW_SHORT_PULLBACK", "0").strip() == "1"

EMA_FAST_4H = 20
EMA_SLOW_4H = 50
EMA_FAST_1H = 20
EMA_SLOW_1H = 50
EMA_FAST_15M = 20
EMA_SLOW_15M = 50

ATR_LEN = 14
RV_LOOKBACK_5M = 24
VWAP_LEN_15M = 20

# common filters (v5보다 다시 약간 조임)
MIN_VOL_RATIO_15M = float(os.getenv("MIN_VOL_RATIO_15M", "0.70"))
MIN_RV_5M = float(os.getenv("MIN_RV_5M", "0.00025"))
MAX_RV_5M = float(os.getenv("MAX_RV_5M", "0.00900"))
MIN_BOX_WIDTH_PCT_15M = float(os.getenv("MIN_BOX_WIDTH_PCT_15M", "0.22"))
MAX_BOX_WIDTH_PCT_15M = float(os.getenv("MAX_BOX_WIDTH_PCT_15M", "6.50"))

# Engine C only
C_RECLAIM_LOOKBACK_15M = int(os.getenv("C_RECLAIM_LOOKBACK_15M", "6"))
C_FAKE_BREAK_TOL_PCT = float(os.getenv("C_FAKE_BREAK_TOL_PCT", "0.03"))
C_RECLAIM_BUFFER_PCT = float(os.getenv("C_RECLAIM_BUFFER_PCT", "0.00"))
C_REQUIRE_VWAP_RECLAIM = os.getenv("C_REQUIRE_VWAP_RECLAIM", "0").strip() == "1"
C_REQUIRE_EMA20_RECLAIM = os.getenv("C_REQUIRE_EMA20_RECLAIM", "1").strip() == "1"
C_MAX_CLOSE_FROM_LEVEL_PCT = float(os.getenv("C_MAX_CLOSE_FROM_LEVEL_PCT", "0.55"))
C_REQUIRE_BULLISH_5M_CLOSE = os.getenv("C_REQUIRE_BULLISH_5M_CLOSE", "0").strip() == "1"
C_REQUIRE_15M_BULLISH = os.getenv("C_REQUIRE_15M_BULLISH", "0").strip() == "1"

# reclaim 이후 재가속 확인
C_REQUIRE_REACC = os.getenv("C_REQUIRE_REACC", "1").strip() == "1"
C_REACC_LOOKBACK_5M = int(os.getenv("C_REACC_LOOKBACK_5M", "3"))
C_REACC_BREAK_BUFFER_BPS = float(os.getenv("C_REACC_BREAK_BUFFER_BPS", "0.3"))

# ETH adaptation
C_PULLBACK_EMA_TOUCH_PCT = float(os.getenv("C_PULLBACK_EMA_TOUCH_PCT", "0.24"))
C_PULLBACK_CLOSE_ABOVE_EMA_PCT = float(os.getenv("C_PULLBACK_CLOSE_ABOVE_EMA_PCT", "0.03"))
C_LEVEL_BLEND_WITH_SWING_LOW = os.getenv("C_LEVEL_BLEND_WITH_SWING_LOW", "1").strip() == "1"
C_REQUIRE_1H_STRENGTH = os.getenv("C_REQUIRE_1H_STRENGTH", "1").strip() == "1"
C_MIN_15M_EMA_SLOPE_BPS = float(os.getenv("C_MIN_15M_EMA_SLOPE_BPS", "0.0"))
C_MAX_15M_EMA_SLOPE_BPS_SHORT = float(os.getenv("C_MAX_15M_EMA_SLOPE_BPS_SHORT", "0.0"))
SHORT_REQUIRE_VWAP_RECLAIM = os.getenv("SHORT_REQUIRE_VWAP_RECLAIM", "1").strip() == "1"
SHORT_REQUIRE_BEARISH_5M_CLOSE = os.getenv("SHORT_REQUIRE_BEARISH_5M_CLOSE", "0").strip() == "1"
SHORT_REQUIRE_15M_BEARISH = os.getenv("SHORT_REQUIRE_15M_BEARISH", "0").strip() == "1"

# reclaim-first / strict-pullback guards
PULLBACK_REQUIRE_VWAP_ALIGN = os.getenv("PULLBACK_REQUIRE_VWAP_ALIGN", "1").strip() == "1"
PULLBACK_REQUIRE_15M_CONFIRM = os.getenv("PULLBACK_REQUIRE_15M_CONFIRM", "1").strip() == "1"
PULLBACK_REQUIRE_PREV_BAR_BREAK = os.getenv("PULLBACK_REQUIRE_PREV_BAR_BREAK", "1").strip() == "1"
PULLBACK_MAX_CLOSE_FROM_EMA_PCT = float(os.getenv("PULLBACK_MAX_CLOSE_FROM_EMA_PCT", "0.28"))
PULLBACK_1H_ALLOWANCE_PCT = float(os.getenv("PULLBACK_1H_ALLOWANCE_PCT", "0.0015"))

# early invalidation
EARLY_SCRATCH_BARS = int(os.getenv("EARLY_SCRATCH_BARS", "4"))
EARLY_SCRATCH_MIN_MFE_BPS = float(os.getenv("EARLY_SCRATCH_MIN_MFE_BPS", "10.0"))
EARLY_SCRATCH_MAX_LOSS_R = float(os.getenv("EARLY_SCRATCH_MAX_LOSS_R", "0.35"))

# exits
INITIAL_STOP_ATR_MULT = float(os.getenv("INITIAL_STOP_ATR_MULT", "1.15"))
TP1_R = float(os.getenv("TP1_R", "1.4"))
TP1_CLOSE_PCT = float(os.getenv("TP1_CLOSE_PCT", "0.40"))
RUNNER_ARM_R = float(os.getenv("RUNNER_ARM_R", "1.40"))
RUNNER_GIVEBACK_PCT = float(os.getenv("RUNNER_GIVEBACK_PCT", "0.25"))
MAX_HOLD_BARS_5M = int(os.getenv("MAX_HOLD_BARS_5M", "60"))
FORCE_FLAT_HOUR_UTC = int(os.getenv("FORCE_FLAT_HOUR_UTC", "23"))

MIN_BARS_WARMUP = 300


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class Trade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: str
    entry_price: float
    exit_price: float
    qty: float
    notional: float
    gross_pnl: float
    fee: float
    slip_cost: float
    net_pnl: float
    hold_bars: int
    mfe_bps: float
    mae_bps: float
    exit_reason: str
    entry_reason: str
    stop_price: float
    risk_per_unit: float
    regime_4h: str
    setup_type: str
    tp1_hit: bool
    engine: str


@dataclass
class OpenPosition:
    side: str
    entry_idx: int
    entry_time: pd.Timestamp
    entry_price: float
    qty: float
    remaining_qty: float
    notional: float
    stop_price: float
    risk_per_unit: float
    entry_reason: str
    regime_4h: str
    setup_type: str
    engine: str
    tp1_price: float
    runner_armed: bool = False
    peak_price: float = 0.0
    trough_price: float = 0.0
    mfe_bps: float = 0.0
    mae_bps: float = 0.0
    tp1_hit: bool = False
    realized_partial_pnl: float = 0.0


# ============================================================
# UTILS
# ============================================================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or pd.isna(b):
        return default
    return a / b


def safe_series_div(a: pd.Series, b: pd.Series, default: float = 0.0) -> pd.Series:
    out = a / b.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).fillna(default)


def bps(a: float, b: float) -> float:
    if b == 0 or pd.isna(a) or pd.isna(b):
        return 0.0
    return (a / b - 1.0) * 10000.0


def parse_timestamp_column(s: pd.Series) -> pd.Series:
    s = s.copy()
    numeric = pd.to_numeric(s, errors="coerce")
    numeric_ratio = numeric.notna().mean()

    if numeric_ratio > 0.90:
        v = numeric.dropna().astype("float64")
        if len(v) == 0:
            return pd.to_datetime(s, errors="coerce", utc=True)
        median_abs = float(np.nanmedian(np.abs(v.values)))
        if median_abs > 1e17:
            unit = "ns"
        elif median_abs > 1e14:
            unit = "us"
        elif median_abs > 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(numeric, unit=unit, errors="coerce", utc=True)

    return pd.to_datetime(s.astype("string"), errors="coerce", utc=True)


def normalize_resample_rule(rule: str) -> str:
    rule = str(rule).strip()
    mapping = {
        "1H": "1h",
        "4H": "4h",
        "15T": "15min",
        "5T": "5min",
        "1h": "1h",
        "4h": "4h",
        "15m": "15min",
        "5m": "5min",
    }
    return mapping.get(rule, rule)


def resolve_backfill_csv_path() -> str:
    if BTC_BACKTEST_CSV:
        return BTC_BACKTEST_CSV
    ensure_dir(BTC_OUTPUT_DIR)
    return os.path.join(BTC_OUTPUT_DIR, AUTO_BACKFILL_CSV_NAME)


def make_exchange(name: str):
    name = name.lower().strip()
    if name == "binanceusdm":
        ex = ccxt.binanceusdm({"enableRateLimit": True})
    elif name == "binance":
        ex = ccxt.binance({"enableRateLimit": True})
    else:
        raise ValueError(f"Unsupported BACKFILL_EXCHANGE: {name}")
    ex.load_markets()
    return ex


def auto_backfill_btc_csv(csv_path: str) -> str:
    ensure_dir(os.path.dirname(csv_path) or ".")
    ex = make_exchange(BACKFILL_EXCHANGE)

    now_ms = ex.milliseconds()
    since_ms = now_ms - BACKFILL_DAYS * 24 * 60 * 60 * 1000

    all_rows: List[List[float]] = []
    fetch_since = since_ms
    limit = 1500

    print(
        f"[INFO] Auto downloading ETH 5m history "
        f"| exchange={BACKFILL_EXCHANGE} | symbol={BACKFILL_SYMBOL} | days={BACKFILL_DAYS}"
    )

    while True:
        batch = ex.fetch_ohlcv(BACKFILL_SYMBOL, timeframe="5m", since=fetch_since, limit=limit)
        if not batch:
            break

        all_rows.extend(batch)
        last_ts = int(batch[-1][0])
        next_since = last_ts + 5 * 60_000

        if next_since <= fetch_since:
            break
        fetch_since = next_since

        if len(batch) < limit or fetch_since >= now_ms:
            break

        time.sleep(ex.rateLimit / 1000.0)

    if not all_rows:
        raise RuntimeError("Auto backfill failed: no OHLCV rows fetched.")

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df.to_csv(csv_path, index=False)

    print(
        f"[INFO] Generated backfill CSV saved: {csv_path} "
        f"| rows={len(df)} | {df['timestamp'].min()} -> {df['timestamp'].max()}"
    )
    return csv_path


def ensure_input_csv() -> str:
    csv_path = resolve_backfill_csv_path()
    if os.path.exists(csv_path):
        print(f"[INFO] Using existing CSV: {csv_path}")
        return csv_path
    if not BACKFILL_IF_MISSING:
        raise FileNotFoundError(f"CSV not found and BACKFILL_IF_MISSING=0: {csv_path}")
    return auto_backfill_btc_csv(csv_path)


def load_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path)
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {sorted(missing)}")

    df = df.copy()
    df["timestamp"] = parse_timestamp_column(df["timestamp"])
    for col in df.columns:
        if col != "timestamp":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"]).copy()
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    return df


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    base = df.set_index("timestamp").copy()
    rule = normalize_resample_rule(rule)

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    out = (
        base.resample(rule)
        .agg(agg)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    return out


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def atr(df: pd.DataFrame, n: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()


def rolling_vwap(df: pd.DataFrame, n: int) -> pd.Series:
    pv = df["close"] * df["volume"]
    return pv.rolling(n, min_periods=n).sum() / df["volume"].rolling(n, min_periods=n).sum()


def realized_vol(close: pd.Series, n: int) -> pd.Series:
    r = np.log(close / close.shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return r.rolling(n, min_periods=n).std().fillna(0.0)


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def add_features_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema20"] = ema(out["close"], 20)
    out["ema50"] = ema(out["close"], 50)
    out["ema200"] = ema(out["close"], 200)
    out["atr14"] = atr(out, ATR_LEN)
    out["rv24"] = realized_vol(out["close"], RV_LOOKBACK_5M)

    out["local_high_3"] = out["high"].shift(1).rolling(C_REACC_LOOKBACK_5M, min_periods=C_REACC_LOOKBACK_5M).max()
    out["local_low_3"] = out["low"].shift(1).rolling(C_REACC_LOOKBACK_5M, min_periods=C_REACC_LOOKBACK_5M).min()

    out["break_long_reacc"] = (
        out["high"] >= out["local_high_3"] * (1.0 + C_REACC_BREAK_BUFFER_BPS / 10000.0)
    ).astype(int)
    out["break_short_reacc"] = (
        out["low"] <= out["local_low_3"] * (1.0 - C_REACC_BREAK_BUFFER_BPS / 10000.0)
    ).astype(int)

    return out


def add_features_15m(df_5m: pd.DataFrame) -> pd.DataFrame:
    d15 = resample_ohlcv(df_5m, "15min")
    d15["ema20"] = ema(d15["close"], EMA_FAST_15M)
    d15["ema50"] = ema(d15["close"], EMA_SLOW_15M)
    d15["atr14"] = atr(d15, ATR_LEN)
    d15["vwap20"] = rolling_vwap(d15, VWAP_LEN_15M)
    d15["vol_med_20"] = d15["volume"].rolling(20, min_periods=20).median()
    d15["vol_ratio"] = safe_series_div(d15["volume"], d15["vol_med_20"])

    d15["swing_high_6"] = d15["high"].shift(1).rolling(6, min_periods=6).max()
    d15["swing_low_6"] = d15["low"].shift(1).rolling(6, min_periods=6).min()
    d15["recent_low_8"] = d15["low"].shift(1).rolling(C_RECLAIM_LOOKBACK_15M, min_periods=C_RECLAIM_LOOKBACK_15M).min()
    d15["recent_high_8"] = d15["high"].shift(1).rolling(C_RECLAIM_LOOKBACK_15M, min_periods=C_RECLAIM_LOOKBACK_15M).max()

    box_width = safe_series_div(d15["swing_high_6"] - d15["swing_low_6"], d15["close"]) * 100.0
    d15["box_width_pct"] = box_width
    d15["bullish_close"] = (d15["close"] >= d15["open"]).astype(int)
    d15["bearish_close"] = (d15["close"] <= d15["open"]).astype(int)
    d15["ema20_slope_bps"] = safe_series_div(d15["ema20"] - d15["ema20"].shift(1), d15["ema20"].shift(1)) * 10000.0
    return d15


def add_features_1h(df_5m: pd.DataFrame) -> pd.DataFrame:
    d1h = resample_ohlcv(df_5m, "1h")
    d1h["ema20"] = ema(d1h["close"], EMA_FAST_1H)
    d1h["ema50"] = ema(d1h["close"], EMA_SLOW_1H)
    d1h["atr14"] = atr(d1h, ATR_LEN)
    return d1h


def add_features_4h(df_5m: pd.DataFrame) -> pd.DataFrame:
    d4h = resample_ohlcv(df_5m, "4h")
    d4h["ema20"] = ema(d4h["close"], EMA_FAST_4H)
    d4h["ema50"] = ema(d4h["close"], EMA_SLOW_4H)
    d4h["atr14"] = atr(d4h, ATR_LEN)
    return d4h


def merge_all(df_5m: pd.DataFrame, d15: pd.DataFrame, d1h: pd.DataFrame, d4h: pd.DataFrame) -> pd.DataFrame:
    out = pd.merge_asof(
        df_5m.sort_values("timestamp"),
        d15[[
            "timestamp", "close", "open", "ema20", "ema50", "atr14", "vwap20", "vol_ratio",
            "swing_high_6", "swing_low_6", "recent_low_8", "recent_high_8", "box_width_pct", "bullish_close", "bearish_close", "ema20_slope_bps"
        ]].rename(columns={
            "close": "close_15m",
            "open": "open_15m",
            "ema20": "ema20_15m",
            "ema50": "ema50_15m",
            "atr14": "atr14_15m",
            "vwap20": "vwap20_15m",
            "vol_ratio": "vol_ratio_15m",
            "swing_high_6": "swing_high_15m",
            "swing_low_6": "swing_low_15m",
            "recent_low_8": "recent_low_15m",
            "recent_high_8": "recent_high_15m",
            "box_width_pct": "box_width_pct_15m",
            "bullish_close": "bullish_close_15m",
            "bearish_close": "bearish_close_15m",
            "ema20_slope_bps": "ema20_slope_bps_15m",
        }).sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    out = pd.merge_asof(
        out.sort_values("timestamp"),
        d1h[["timestamp", "close", "ema20", "ema50", "atr14"]].rename(columns={
            "close": "close_1h",
            "ema20": "ema20_1h",
            "ema50": "ema50_1h",
            "atr14": "atr14_1h",
        }).sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    out = pd.merge_asof(
        out.sort_values("timestamp"),
        d4h[["timestamp", "close", "ema20", "ema50", "atr14"]].rename(columns={
            "close": "close_4h",
            "ema20": "ema20_4h",
            "ema50": "ema50_4h",
            "atr14": "atr14_4h",
        }).sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    return out


# ============================================================
# REGIME / FILTERS
# ============================================================

def classify_regime_4h_long(row: pd.Series) -> str:
    score = 0
    if row["close_4h"] > row["ema20_4h"]:
        score += 1
    if row["ema20_4h"] > row["ema50_4h"]:
        score += 1
    if row["close_1h"] > row["ema20_1h"]:
        score += 1
    if row["ema20_1h"] > row["ema50_1h"]:
        score += 1
    if score >= 3:
        return "BULL"
    if score == 2:
        return "NEUTRAL_UP"
    return "NO_LONG"


def classify_regime_4h_short(row: pd.Series) -> str:
    score = 0
    if row["close_4h"] < row["ema20_4h"]:
        score += 1
    if row["ema20_4h"] < row["ema50_4h"]:
        score += 1
    if row["close_1h"] < row["ema20_1h"]:
        score += 1
    if row["ema20_1h"] < row["ema50_1h"]:
        score += 1
    if score >= 3:
        return "BEAR"
    if score == 2:
        return "NEUTRAL_DOWN"
    return "NO_SHORT"


def common_day_filter(row: pd.Series) -> Tuple[bool, str]:
    if pd.isna(row["rv24"]) or pd.isna(row["vol_ratio_15m"]) or pd.isna(row["box_width_pct_15m"]):
        return False, "NAN_FILTER"
    if not (MIN_RV_5M <= row["rv24"] <= MAX_RV_5M):
        return False, "RV_FILTER"
    if row["vol_ratio_15m"] < MIN_VOL_RATIO_15M:
        return False, "VOL_FILTER"
    if row["box_width_pct_15m"] < MIN_BOX_WIDTH_PCT_15M:
        return False, "BOX_TOO_TIGHT"
    if row["box_width_pct_15m"] > MAX_BOX_WIDTH_PCT_15M:
        return False, "BOX_TOO_WIDE"
    return True, "OK"


# ============================================================
# ENGINE C DUAL
# ============================================================

def c_reacc_confirm_long(df: pd.DataFrame, i: int) -> bool:
    if i < 1:
        return False
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not C_REQUIRE_REACC:
        return True
    broke_local_high = bool(row["break_long_reacc"] == 1)
    close_above_ema = row["close"] >= row["ema20"] * 0.998
    no_weak_close = row["close"] >= prev["close"] * 0.997
    prev_high_break = row["close"] >= prev["high"] * (1.0 - 0.0002)
    return (broke_local_high or prev_high_break) and close_above_ema and no_weak_close


def c_reacc_confirm_short(df: pd.DataFrame, i: int) -> bool:
    if i < 1:
        return False
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not C_REQUIRE_REACC:
        return True
    broke_local_low = bool(row["break_short_reacc"] == 1)
    close_below_ema = row["close"] <= row["ema20"] * 1.002
    no_strong_close = row["close"] <= prev["close"] * 1.003
    prev_low_break = row["close"] <= prev["low"] * (1.0 + 0.0002)
    return (broke_local_low or prev_low_break) and close_below_ema and no_strong_close


def signal_engine_c_long_reclaim(df: pd.DataFrame, i: int) -> Tuple[bool, str, Dict[str, float]]:
    if not ALLOW_LONG or not ALLOW_LONG_RECLAIM:
        return False, "LONG_DISABLED", {}
    row = df.iloc[i]
    ok, reason = common_day_filter(row)
    if not ok:
        return False, reason, {}
    regime = classify_regime_4h_long(row)
    if regime == "NO_LONG":
        return False, "REGIME_FAIL", {"regime_4h": regime}
    if pd.isna(row["recent_low_15m"]):
        return False, "C_NO_LEVEL", {"regime_4h": regime}
    low_level = float(row["recent_low_15m"])
    broke_below = row["low"] <= low_level * (1.0 - C_FAKE_BREAK_TOL_PCT / 100.0)
    reclaimed = row["close"] >= low_level * (1.0 + C_RECLAIM_BUFFER_PCT / 100.0)
    ema_ok = (row["close"] >= row["ema20"] * 0.999) if C_REQUIRE_EMA20_RECLAIM else True
    vwap_ok = (row["close_15m"] >= row["vwap20_15m"] * 0.998) if C_REQUIRE_VWAP_RECLAIM else True
    bullish_5m = (row["close"] >= row["open"]) if C_REQUIRE_BULLISH_5M_CLOSE else True
    bullish_15m = (row["bullish_close_15m"] == 1) if C_REQUIRE_15M_BULLISH else True
    close_from_level_pct = safe_div(abs(row["close"] - low_level), low_level) * 100.0
    not_too_far = close_from_level_pct <= C_MAX_CLOSE_FROM_LEVEL_PCT
    reacc_ok = c_reacc_confirm_long(df, i)
    if not (broke_below and reclaimed and ema_ok and vwap_ok and bullish_5m and bullish_15m and not_too_far and reacc_ok):
        return False, "C_NO_RECLAIM_LONG", {"regime_4h": regime, "close_from_level_pct": close_from_level_pct}
    meta = {"regime_4h": regime, "level": low_level, "close_from_level_pct": float(close_from_level_pct), "setup_type": "FAILED_BREAKDOWN_RECLAIM_LONG", "engine": "C"}
    return True, "ALLOW_LONG_C_RECLAIM", meta


def signal_engine_c_long_pullback(df: pd.DataFrame, i: int) -> Tuple[bool, str, Dict[str, float]]:
    if not ALLOW_LONG or not ALLOW_LONG_PULLBACK:
        return False, "LONG_DISABLED", {}
    row = df.iloc[i]
    ok, reason = common_day_filter(row)
    if not ok:
        return False, reason, {}
    regime = classify_regime_4h_long(row)
    if regime == "NO_LONG":
        return False, "REGIME_FAIL", {"regime_4h": regime}
    if C_REQUIRE_1H_STRENGTH and row["close_1h"] < row["ema20_1h"] * (1.0 - PULLBACK_1H_ALLOWANCE_PCT):
        return False, "C_FAIL_1H_STRENGTH", {"regime_4h": regime}
    touch_ema = row["low"] <= row["ema20"] * (1.0 + C_PULLBACK_EMA_TOUCH_PCT / 100.0)
    reclaim_ema = row["close"] >= row["ema20"] * (1.0 + C_PULLBACK_CLOSE_ABOVE_EMA_PCT / 100.0)
    trend_15m = row["close_15m"] >= row["ema20_15m"] * 1.000
    slope_ok = row["ema20_slope_bps_15m"] >= max(C_MIN_15M_EMA_SLOPE_BPS, 0.5)
    vwap_ok = (row["close_15m"] >= row["vwap20_15m"] * 1.000) if PULLBACK_REQUIRE_VWAP_ALIGN else True
    bar_confirm = (row["bullish_close_15m"] == 1) if PULLBACK_REQUIRE_15M_CONFIRM else True
    prev_break = (row["close"] >= df.iloc[i - 1]["high"] * 0.9998) if (PULLBACK_REQUIRE_PREV_BAR_BREAK and i >= 1) else True
    close_from_ema_pct = safe_div(abs(row["close"] - row["ema20"]), row["ema20"]) * 100.0
    not_too_far = close_from_ema_pct <= PULLBACK_MAX_CLOSE_FROM_EMA_PCT
    reacc_ok = c_reacc_confirm_long(df, i)
    if not (touch_ema and reclaim_ema and trend_15m and slope_ok and vwap_ok and bar_confirm and prev_break and not_too_far and reacc_ok):
        return False, "C_NO_PULLBACK_LONG", {"regime_4h": regime, "close_from_level_pct": float(close_from_ema_pct)}
    meta = {"regime_4h": regime, "level": float(row["ema20"]), "close_from_level_pct": float(close_from_ema_pct), "setup_type": "EMA_PULLBACK_CONT_LONG", "engine": "C"}
    return True, "ALLOW_LONG_C_PULLBACK", meta


def signal_engine_c_short_reclaim(df: pd.DataFrame, i: int) -> Tuple[bool, str, Dict[str, float]]:
    if not ALLOW_SHORT or not ALLOW_SHORT_RECLAIM:
        return False, "SHORT_DISABLED", {}
    row = df.iloc[i]
    ok, reason = common_day_filter(row)
    if not ok:
        return False, reason, {}
    regime = classify_regime_4h_short(row)
    if regime == "NO_SHORT":
        return False, "REGIME_FAIL", {"regime_4h": regime}
    if pd.isna(row["recent_high_15m"]):
        return False, "C_NO_LEVEL", {"regime_4h": regime}
    high_level = float(row["recent_high_15m"])
    broke_above = row["high"] >= high_level * (1.0 + C_FAKE_BREAK_TOL_PCT / 100.0)
    reclaimed_fail = row["close"] <= high_level * (1.0 - C_RECLAIM_BUFFER_PCT / 100.0)
    ema_ok = (row["close"] <= row["ema20"] * 1.001) if C_REQUIRE_EMA20_RECLAIM else True
    vwap_ok = (row["close_15m"] <= row["vwap20_15m"] * 1.002) if SHORT_REQUIRE_VWAP_RECLAIM else True
    bearish_5m = (row["close"] <= row["open"]) if SHORT_REQUIRE_BEARISH_5M_CLOSE else True
    bearish_15m = (row["bearish_close_15m"] == 1) if SHORT_REQUIRE_15M_BEARISH else True
    close_from_level_pct = safe_div(abs(row["close"] - high_level), high_level) * 100.0
    not_too_far = close_from_level_pct <= C_MAX_CLOSE_FROM_LEVEL_PCT
    reacc_ok = c_reacc_confirm_short(df, i)
    if not (broke_above and reclaimed_fail and ema_ok and vwap_ok and bearish_5m and bearish_15m and not_too_far and reacc_ok):
        return False, "C_NO_RECLAIM_SHORT", {"regime_4h": regime, "close_from_level_pct": close_from_level_pct}
    meta = {"regime_4h": regime, "level": high_level, "close_from_level_pct": float(close_from_level_pct), "setup_type": "FAILED_BREAKOUT_RECLAIM_SHORT", "engine": "C"}
    return True, "ALLOW_SHORT_C_RECLAIM", meta


def signal_engine_c_short_pullback(df: pd.DataFrame, i: int) -> Tuple[bool, str, Dict[str, float]]:
    if not ALLOW_SHORT or not ALLOW_SHORT_PULLBACK:
        return False, "SHORT_DISABLED", {}
    row = df.iloc[i]
    ok, reason = common_day_filter(row)
    if not ok:
        return False, reason, {}
    regime = classify_regime_4h_short(row)
    if regime == "NO_SHORT":
        return False, "REGIME_FAIL", {"regime_4h": regime}
    if C_REQUIRE_1H_STRENGTH and row["close_1h"] > row["ema20_1h"] * (1.0 + PULLBACK_1H_ALLOWANCE_PCT):
        return False, "C_FAIL_1H_WEAKNESS", {"regime_4h": regime}
    touch_ema = row["high"] >= row["ema20"] * (1.0 - C_PULLBACK_EMA_TOUCH_PCT / 100.0)
    reject_ema = row["close"] <= row["ema20"] * (1.0 - C_PULLBACK_CLOSE_ABOVE_EMA_PCT / 100.0)
    trend_15m = row["close_15m"] <= row["ema20_15m"] * 1.000
    slope_ok = row["ema20_slope_bps_15m"] <= min(C_MAX_15M_EMA_SLOPE_BPS_SHORT, -0.5)
    vwap_ok = (row["close_15m"] <= row["vwap20_15m"] * 1.000) if PULLBACK_REQUIRE_VWAP_ALIGN else True
    bar_confirm = (row["bearish_close_15m"] == 1) if PULLBACK_REQUIRE_15M_CONFIRM else True
    prev_break = (row["close"] <= df.iloc[i - 1]["low"] * 1.0002) if (PULLBACK_REQUIRE_PREV_BAR_BREAK and i >= 1) else True
    close_from_ema_pct = safe_div(abs(row["close"] - row["ema20"]), row["ema20"]) * 100.0
    not_too_far = close_from_ema_pct <= PULLBACK_MAX_CLOSE_FROM_EMA_PCT
    reacc_ok = c_reacc_confirm_short(df, i)
    if not (touch_ema and reject_ema and trend_15m and slope_ok and vwap_ok and bar_confirm and prev_break and not_too_far and reacc_ok):
        return False, "C_NO_PULLBACK_SHORT", {"regime_4h": regime, "close_from_level_pct": float(close_from_ema_pct)}
    meta = {"regime_4h": regime, "level": float(row["ema20"]), "close_from_level_pct": float(close_from_ema_pct), "setup_type": "EMA_PULLBACK_CONT_SHORT", "engine": "C"}
    return True, "ALLOW_SHORT_C_PULLBACK", meta


def gather_signals(df: pd.DataFrame, i: int) -> List[Tuple[str, str, Dict[str, float]]]:
    signals: List[Tuple[str, str, Dict[str, float]]] = []
    for fn, side in [
        (signal_engine_c_long_reclaim, "LONG"),
        (signal_engine_c_short_reclaim, "SHORT"),
        (signal_engine_c_long_pullback, "LONG"),
        (signal_engine_c_short_pullback, "SHORT"),
    ]:
        ok, dec, meta = fn(df, i)
        if ok:
            signals.append((side, dec, meta))
    return signals


def choose_signal(signals: List[Tuple[str, str, Dict[str, float]]]) -> Optional[Tuple[str, str, Dict[str, float]]]:
    if not signals:
        return None
    priority = {
        "ALLOW_LONG_C_RECLAIM": 0,
        "ALLOW_SHORT_C_RECLAIM": 1,
        "ALLOW_LONG_C_PULLBACK": 2,
        "ALLOW_SHORT_C_PULLBACK": 3,
    }
    return sorted(signals, key=lambda x: priority.get(x[1], 99))[0]


# ============================================================
# BACKTEST ENGINE
# ============================================================

def calc_qty_from_risk(entry: float, stop: float, equity: float) -> Tuple[float, float]:
    risk_usdt = equity * RISK_PER_TRADE_PCT
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0, 0.0
    qty_by_risk = risk_usdt / risk_per_unit
    notional_by_risk = qty_by_risk * entry
    capped_notional = min(notional_by_risk, MAX_NOTIONAL_USDT, equity * LEVERAGE)
    qty = capped_notional / entry if entry > 0 else 0.0
    return qty, risk_per_unit


def fill_entry_price(side: str, row: pd.Series) -> float:
    if side == "LONG":
        return float(row["open"]) * (1.0 + ENTRY_SLIPPAGE_BPS / 10000.0)
    return float(row["open"]) * (1.0 - ENTRY_SLIPPAGE_BPS / 10000.0)


def fill_exit_price(side: str, row: pd.Series) -> float:
    if side == "LONG":
        return float(row["open"]) * (1.0 - EXIT_SLIPPAGE_BPS / 10000.0)
    return float(row["open"]) * (1.0 + EXIT_SLIPPAGE_BPS / 10000.0)


def trade_fee(notional: float) -> float:
    return notional * TAKER_FEE_BPS / 10000.0


def mark_to_mfe_mae(pos: OpenPosition, row: pd.Series) -> Tuple[float, float]:
    if pos.side == "LONG":
        mfe = bps(float(row["high"]), pos.entry_price)
        mae = bps(float(row["low"]), pos.entry_price)
    else:
        mfe = bps(pos.entry_price, float(row["low"]))
        mae = bps(pos.entry_price, float(row["high"]))
    return mfe, mae


def current_profit_r(pos: OpenPosition, row: pd.Series) -> float:
    if pos.side == "LONG":
        return safe_div(float(row["close"]) - pos.entry_price, pos.risk_per_unit, default=0.0)
    return safe_div(pos.entry_price - float(row["close"]), pos.risk_per_unit, default=0.0)


def partial_close(pos: OpenPosition, row: pd.Series, close_pct: float) -> float:
    qty_to_close = pos.remaining_qty * close_pct
    if qty_to_close <= 0:
        return 0.0
    exit_price = fill_exit_price(pos.side, row)
    if pos.side == "LONG":
        gross = (exit_price - pos.entry_price) * qty_to_close
    else:
        gross = (pos.entry_price - exit_price) * qty_to_close
    fee = trade_fee(qty_to_close * pos.entry_price) + trade_fee(qty_to_close * exit_price)
    net = gross - fee
    pos.remaining_qty -= qty_to_close
    pos.realized_partial_pnl += net
    return net


def close_position(pos: OpenPosition, df: pd.DataFrame, i: int, reason: str) -> Trade:
    row = df.iloc[i]
    exit_price = fill_exit_price(pos.side, row)
    if pos.side == "LONG":
        gross = (exit_price - pos.entry_price) * pos.remaining_qty
    else:
        gross = (pos.entry_price - exit_price) * pos.remaining_qty
    fee = trade_fee(pos.remaining_qty * pos.entry_price) + trade_fee(pos.remaining_qty * exit_price)
    slip_cost = pos.notional * (ENTRY_SLIPPAGE_BPS + EXIT_SLIPPAGE_BPS) / 10000.0
    net = gross - fee + pos.realized_partial_pnl
    return Trade(
        entry_time=pos.entry_time,
        exit_time=row["timestamp"],
        side=pos.side,
        entry_price=pos.entry_price,
        exit_price=exit_price,
        qty=pos.qty,
        notional=pos.notional,
        gross_pnl=gross + pos.realized_partial_pnl,
        fee=fee,
        slip_cost=slip_cost,
        net_pnl=net,
        hold_bars=i - pos.entry_idx,
        mfe_bps=pos.mfe_bps,
        mae_bps=pos.mae_bps,
        exit_reason=reason,
        entry_reason=pos.entry_reason,
        stop_price=pos.stop_price,
        risk_per_unit=pos.risk_per_unit,
        regime_4h=pos.regime_4h,
        setup_type=pos.setup_type,
        tp1_hit=pos.tp1_hit,
        engine=pos.engine,
    )


def backtest(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float], pd.DataFrame]:
    decisions: List[Dict] = []
    trades: List[Trade] = []
    equity = START_CAPITAL
    equity_curve: List[Dict] = []
    pos: Optional[OpenPosition] = None
    last_exit_idx = -10**9
    trades_today: Dict[str, int] = {}

    for i in range(MIN_BARS_WARMUP, len(df) - 1):
        row = df.iloc[i]
        next_row = df.iloc[i + 1]
        day_key = str(pd.Timestamp(row["timestamp"]).date())
        trades_today.setdefault(day_key, 0)

        if pos is not None:
            mfe, mae = mark_to_mfe_mae(pos, row)
            pos.mfe_bps = max(pos.mfe_bps, mfe)
            pos.mae_bps = min(pos.mae_bps, mae)
            if pos.side == "LONG":
                pos.peak_price = max(pos.peak_price, float(row["high"]))
            else:
                pos.trough_price = min(pos.trough_price, float(row["low"]))
            profit_r = current_profit_r(pos, row)

            if (not pos.tp1_hit):
                hit_tp1 = float(row["high"]) >= pos.tp1_price if pos.side == "LONG" else float(row["low"]) <= pos.tp1_price
                if hit_tp1:
                    _ = partial_close(pos, row, TP1_CLOSE_PCT)
                    pos.tp1_hit = True
                    pos.stop_price = max(pos.stop_price, pos.entry_price) if pos.side == "LONG" else min(pos.stop_price, pos.entry_price)

            if (not pos.runner_armed) and profit_r >= RUNNER_ARM_R:
                pos.runner_armed = True

            hold_bars = i - pos.entry_idx
            if hold_bars <= EARLY_SCRATCH_BARS:
                weak_follow = pos.mfe_bps < EARLY_SCRATCH_MIN_MFE_BPS
                soft_loss_r = profit_r <= -EARLY_SCRATCH_MAX_LOSS_R
                lost_ema = float(row["close"]) < float(row["ema20"]) * 0.998 if pos.side == "LONG" else float(row["close"]) > float(row["ema20"]) * 1.002
                if weak_follow and soft_loss_r and lost_ema:
                    tr = close_position(pos, df, i + 1, "EARLY_INVALIDATION")
                    equity += tr.net_pnl
                    trades.append(tr)
                    pos = None
                    last_exit_idx = i + 1
                    equity_curve.append({"timestamp": next_row["timestamp"], "equity": equity})
                    continue

            stop_hit = float(row["low"]) <= pos.stop_price if pos.side == "LONG" else float(row["high"]) >= pos.stop_price
            if stop_hit:
                tr = close_position(pos, df, i + 1, "STOP_OR_BE")
                equity += tr.net_pnl
                trades.append(tr)
                pos = None
                last_exit_idx = i + 1
                equity_curve.append({"timestamp": next_row["timestamp"], "equity": equity})
                continue

            if pos.runner_armed:
                if pos.side == "LONG":
                    giveback_level = pos.peak_price - (pos.peak_price - pos.entry_price) * RUNNER_GIVEBACK_PCT
                    trigger = float(row["close"]) <= giveback_level
                else:
                    giveback_level = pos.trough_price + (pos.entry_price - pos.trough_price) * RUNNER_GIVEBACK_PCT
                    trigger = float(row["close"]) >= giveback_level
                if trigger:
                    tr = close_position(pos, df, i + 1, "RUNNER_GIVEBACK")
                    equity += tr.net_pnl
                    trades.append(tr)
                    pos = None
                    last_exit_idx = i + 1
                    equity_curve.append({"timestamp": next_row["timestamp"], "equity": equity})
                    continue

            if (i - pos.entry_idx) >= MAX_HOLD_BARS_5M:
                tr = close_position(pos, df, i + 1, "MAX_HOLD")
                equity += tr.net_pnl
                trades.append(tr)
                pos = None
                last_exit_idx = i + 1
                equity_curve.append({"timestamp": next_row["timestamp"], "equity": equity})
                continue

            if pd.Timestamp(row["timestamp"]).hour >= FORCE_FLAT_HOUR_UTC:
                tr = close_position(pos, df, i + 1, "FORCE_FLAT_EOD")
                equity += tr.net_pnl
                trades.append(tr)
                pos = None
                last_exit_idx = i + 1
                equity_curve.append({"timestamp": next_row["timestamp"], "equity": equity})
                continue

        if pos is None:
            if trades_today[day_key] >= MAX_TRADES_PER_DAY:
                decisions.append({"timestamp": row["timestamp"], "decision": "DAILY_LIMIT"})
                equity_curve.append({"timestamp": row["timestamp"], "equity": equity})
                continue
            if (i - last_exit_idx) < COOLDOWN_BARS_5M:
                decisions.append({"timestamp": row["timestamp"], "decision": "COOLDOWN"})
                equity_curve.append({"timestamp": row["timestamp"], "equity": equity})
                continue
            signals = gather_signals(df, i)
            chosen = choose_signal(signals)
            if chosen is None:
                decisions.append({"timestamp": row["timestamp"], "decision": "NO_SIGNAL"})
            else:
                chosen_side, chosen_decision, chosen_meta = chosen
                decisions.append({"timestamp": row["timestamp"], "decision": chosen_decision, **chosen_meta})
                entry_price = fill_entry_price(chosen_side, next_row)
                if chosen_side == "LONG":
                    fallback_stop = entry_price - INITIAL_STOP_ATR_MULT * float(row["atr14"])
                    structural_candidates = [fallback_stop]
                    if not pd.isna(row["local_low_3"]):
                        structural_candidates.append(float(row["local_low_3"]))
                    if not pd.isna(row.get("swing_low_15m", np.nan)):
                        structural_candidates.append(float(row["swing_low_15m"]))
                    stop_price = min(structural_candidates)
                    tp1_price = entry_price + TP1_R * abs(entry_price - stop_price)
                    peak_price, trough_price = entry_price, entry_price
                else:
                    fallback_stop = entry_price + INITIAL_STOP_ATR_MULT * float(row["atr14"])
                    structural_candidates = [fallback_stop]
                    if not pd.isna(row["local_high_3"]):
                        structural_candidates.append(float(row["local_high_3"]))
                    if not pd.isna(row.get("swing_high_15m", np.nan)):
                        structural_candidates.append(float(row["swing_high_15m"]))
                    stop_price = max(structural_candidates)
                    tp1_price = entry_price - TP1_R * abs(stop_price - entry_price)
                    peak_price, trough_price = entry_price, entry_price
                qty, risk_per_unit = calc_qty_from_risk(entry_price, stop_price, equity)
                if qty > 0 and risk_per_unit > 0:
                    notional = qty * entry_price
                    pos = OpenPosition(
                        side=chosen_side,
                        entry_idx=i + 1,
                        entry_time=next_row["timestamp"],
                        entry_price=entry_price,
                        qty=qty,
                        remaining_qty=qty,
                        notional=notional,
                        stop_price=stop_price,
                        risk_per_unit=risk_per_unit,
                        entry_reason=chosen_decision,
                        regime_4h=str(chosen_meta.get("regime_4h", "")),
                        setup_type=str(chosen_meta.get("setup_type", "")),
                        engine=str(chosen_meta.get("engine", "")),
                        tp1_price=tp1_price,
                        peak_price=peak_price,
                        trough_price=trough_price,
                    )
                    trades_today[day_key] += 1
        equity_curve.append({"timestamp": row["timestamp"], "equity": equity})

    if pos is not None:
        tr = close_position(pos, df, len(df) - 1, "FORCED_EOF")
        equity += tr.net_pnl
        trades.append(tr)
        equity_curve.append({"timestamp": df.iloc[-1]["timestamp"], "equity": equity})

    trades_df = pd.DataFrame([asdict(t) for t in trades])
    decisions_df = pd.DataFrame(decisions)
    equity_df = pd.DataFrame(equity_curve)
    summary = summarize(trades_df, equity_df, decisions_df)
    return trades_df, decisions_df, summary, equity_df
# ============================================================
# SUMMARY / SAVE
# ============================================================

def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def summarize(trades_df: pd.DataFrame, equity_df: pd.DataFrame, decisions_df: pd.DataFrame) -> Dict[str, float]:
    if trades_df.empty:
        summary = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "avg_net_pnl": 0.0,
            "profit_factor": 0.0,
            "avg_mfe_bps": 0.0,
            "avg_mae_bps": 0.0,
            "final_equity": START_CAPITAL,
            "return_pct": 0.0,
            "max_drawdown_pct": 0.0,
        }
    else:
        wins = int((trades_df["net_pnl"] > 0).sum())
        losses = int((trades_df["net_pnl"] <= 0).sum())
        gross_profit = float(trades_df.loc[trades_df["net_pnl"] > 0, "net_pnl"].sum())
        gross_loss = float(-trades_df.loc[trades_df["net_pnl"] <= 0, "net_pnl"].sum())
        pf = safe_div(gross_profit, gross_loss, default=0.0)

        final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else START_CAPITAL
        ret_pct = safe_div(final_equity, START_CAPITAL, 1.0) - 1.0
        mdd = max_drawdown(equity_df["equity"]) if not equity_df.empty else 0.0

        summary = {
            "trades": int(len(trades_df)),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": float(wins / len(trades_df) * 100.0),
            "gross_pnl": float(trades_df["gross_pnl"].sum()),
            "net_pnl": float(trades_df["net_pnl"].sum()),
            "avg_net_pnl": float(trades_df["net_pnl"].mean()),
            "profit_factor": float(pf),
            "avg_mfe_bps": float(trades_df["mfe_bps"].mean()),
            "avg_mae_bps": float(trades_df["mae_bps"].mean()),
            "final_equity": float(final_equity),
            "return_pct": float(ret_pct * 100.0),
            "max_drawdown_pct": float(mdd * 100.0),
        }

        if "engine" in trades_df.columns:
            for eng, cnt in trades_df["engine"].value_counts().items():
                summary[f"engine_{eng}_trades"] = int(cnt)

    if not decisions_df.empty:
        summary["decisions_total"] = int(len(decisions_df))
        for dec, cnt in decisions_df["decision"].astype(str).value_counts().head(20).items():
            summary[f"decision_{dec}"] = int(cnt)

    return summary


def save_outputs(
    trades_df: pd.DataFrame,
    decisions_df: pd.DataFrame,
    summary: Dict[str, float],
    equity_df: pd.DataFrame,
    output_dir: str,
    input_csv_path: str
) -> None:
    ensure_dir(output_dir)

    trades_path = os.path.join(output_dir, "trades.csv")
    decisions_path = os.path.join(output_dir, "decisions.csv")
    equity_path = os.path.join(output_dir, "equity.csv")
    summary_path = os.path.join(output_dir, "summary.txt")
    summary_json_path = os.path.join(output_dir, "summary.json")

    trades_df.to_csv(trades_path, index=False)
    decisions_df.to_csv(decisions_path, index=False)
    equity_df.to_csv(equity_path, index=False)

    lines = ["ETH INTRADAY TREND ENGINE v8.1 RECLAIM-FIRST DUAL", "=" * 60]
    lines.append(f"input_csv: {input_csv_path}")
    for k, v in summary.items():
        lines.append(f"{k}: {v}")
    lines.append("")
    lines.append(f"trades_path: {trades_path}")
    lines.append(f"decisions_path: {decisions_path}")
    lines.append(f"equity_path: {equity_path}")

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    with open(summary_json_path, "w", encoding="utf-8") as f:
        payload = {"input_csv": input_csv_path, **summary}
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ensure_dir(BTC_OUTPUT_DIR)

    print("[INFO] ensuring input CSV...")
    csv_path = ensure_input_csv()

    print("[INFO] loading CSV...")
    raw_df = load_csv(csv_path)
    print(f"[INFO] loaded rows={len(raw_df)} | {raw_df['timestamp'].min()} -> {raw_df['timestamp'].max()}")

    print("[INFO] building features...")
    df_5m = add_features_5m(raw_df)
    d15 = add_features_15m(raw_df)
    d1h = add_features_1h(raw_df)
    d4h = add_features_4h(raw_df)
    df = merge_all(df_5m, d15, d1h, d4h)

    required_cols = [
        "ema20", "ema50", "atr14", "rv24",
        "close_15m", "open_15m", "ema20_15m", "vwap20_15m", "vol_ratio_15m", "box_width_pct_15m",
        "close_1h", "ema20_1h", "ema50_1h",
        "close_4h", "ema20_4h", "ema50_4h",
        "local_high_3", "local_low_3",
        "recent_low_15m", "recent_high_15m", "bullish_close_15m", "bearish_close_15m", "swing_low_15m", "swing_high_15m", "ema20_slope_bps_15m",
    ]
    df = df.dropna(subset=required_cols).reset_index(drop=True)

    print(f"[INFO] feature rows after cleanup={len(df)}")
    if len(df) < MIN_BARS_WARMUP + 100:
        raise ValueError("Not enough data after feature warmup.")

    print("[INFO] running backtest...")
    trades_df, decisions_df, summary, equity_df = backtest(df)

    save_outputs(trades_df, decisions_df, summary, equity_df, BTC_OUTPUT_DIR, csv_path)

    print("\n[SUMMARY]")
    for k, v in summary.items():
        print(f"{k}: {v}")

    if not decisions_df.empty:
        print("\n[TOP DECISIONS]")
        print(decisions_df["decision"].astype(str).value_counts().head(20).to_string())

    if not trades_df.empty and "engine" in trades_df.columns:
        print("\n[ENGINE USAGE]")
        print(trades_df["engine"].astype(str).value_counts().to_string())

    print(f"\n[INPUT CSV] {csv_path}")
    print(f"[OUTPUT DIR] {BTC_OUTPUT_DIR}")


if __name__ == "__main__":
    main()