#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETH STRUCTURE SHOCK BOT v17.3
------------------------------------------------------------
목적
- 사용자가 표시한 1h 차트의 동그라미 지점처럼,
  구조 변화 / 충격봉 / 첫 되돌림 / 저점 재이탈 실패 / EMA 거절 / 붕괴 지속을
  백테스트 가능한 형태로 구현한 연구용 백테스트 봇.

v17.3 최적화 포인트
- 숏 엔진 세분화:
  1) BREAKDOWN_PULLBACK_SHORT
  2) EMA_FAST_REJECT_SHORT
  3) EMA_MID_REJECT_SHORT
  4) BREAKDOWN_RETEST_SHORT
  5) BREAKDOWN_CONTINUATION_SHORT
- 롱 엔진 강화:
  4) CAPITULATION_RECLAIM_LONG
  5) BREAKOUT_FIRST_PULLBACK_LONG
- 엔진별 time stop
- partial 이후 EMA10+ATR 트레일 강화
- opposite shock exit 축소/강화 조건
- ETH/BTC 리짐 필터 옵션
- entry relaxed mode
- CSV 경로가 없거나 잘못돼도 자동 fallback 다운로드
- summary/report 강화:
  summary.json / monthly_report.csv / engine_report.csv / direction_report.csv / exit_reason_report.csv

주의
- 연구/백테스트용이며 실거래용 아님
- 수수료/슬리피지/체결 현실은 단순화되어 있음
- ENV ONLY / Windows 경로 문자열 안전 / pandas StringDtype 재발 오류 방지

권장 설치
pip install ccxt pandas numpy matplotlib

실행 예시
$env:ETH_MODE="BACKTEST"
$env:ETH_BACKTEST_CSV="C:\\Users\\Administrator\\Desktop\\hjsung\\backtest\\ethusdt_1h_180d_downloaded.csv"
$env:ETH_OUTPUT_DIR="C:\\Users\\Administrator\\Desktop\\hjsung\\eth_structure_out_v17_2"
$env:ETH_AUTO_DOWNLOAD="0"
python C:\\Users\\Administrator\\Desktop\\hjsung\\backtest\\eth_structure_shock_bot_v17_2.py
"""

from __future__ import annotations

import os
import math
import json
import time
import traceback
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import ccxt  # type: ignore
except Exception:
    ccxt = None


# ============================================================
# ENV / CONFIG
# ============================================================

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else str(v).strip()

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(float(str(v).strip()))
    except Exception:
        return int(default)

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

BOT_NAME = env_str("BOT_NAME", "ETH_STRUCTURE_SHOCK_BOT_V17_3")
ETH_MODE = env_str("ETH_MODE", "BACKTEST").upper()
ETH_SYMBOL = env_str("ETH_SYMBOL", "ETH/USDT:USDT")
ETH_TIMEFRAME = env_str("ETH_TIMEFRAME", "1h")
ETH_BACKTEST_CSV = env_str("ETH_BACKTEST_CSV", "")
ETH_OUTPUT_DIR = env_str("ETH_OUTPUT_DIR", "./eth_structure_shock_out_v17_3")
ETH_AUTO_DOWNLOAD = env_bool("ETH_AUTO_DOWNLOAD", True)
ETH_FALLBACK_DOWNLOAD_IF_CSV_MISSING = env_bool("ETH_FALLBACK_DOWNLOAD_IF_CSV_MISSING", True)
ETH_DOWNLOAD_DAYS = env_int("ETH_DOWNLOAD_DAYS", 365)
ETH_PLOT = env_bool("ETH_PLOT", True)

INITIAL_CAPITAL = env_float("ETH_INITIAL_CAPITAL", 10000.0)
RISK_PER_TRADE_PCT = env_float("ETH_RISK_PER_TRADE_PCT", 0.01)
FEE_RATE = env_float("ETH_FEE_RATE", 0.0004)
SLIPPAGE_RATE = env_float("ETH_SLIPPAGE_RATE", 0.0002)
LONG_ENGINE_ENABLED = env_bool("LONG_ENGINE_ENABLED", False)
ALLOW_LONG = env_bool("ETH_ALLOW_LONG", LONG_ENGINE_ENABLED)
ALLOW_SHORT = env_bool("ETH_ALLOW_SHORT", True)

USE_REGIME_FILTER = env_bool("USE_REGIME_FILTER", True)
ETH_REGIME_ONLY_SHORT_WHEN_BEARISH = env_bool("ETH_REGIME_ONLY_SHORT_WHEN_BEARISH", True)
BTC_REGIME_FILTER = env_bool("BTC_REGIME_FILTER", False)
BTC_SYMBOL = env_str("BTC_SYMBOL", "BTC/USDT:USDT")
BTC_REGIME_DIR = env_str("BTC_REGIME_DIR", "")
BTC_REGIME_CSV = env_str("BTC_REGIME_CSV", "")
BTC_AUTO_DOWNLOAD = env_bool("BTC_AUTO_DOWNLOAD", True)
BTC_LOOKBACK_RETURN_BARS = env_int("BTC_LOOKBACK_RETURN_BARS", 24)
OPPOSITE_SHOCK_MIN_BARS = env_int("OPPOSITE_SHOCK_MIN_BARS", 4)
OPPOSITE_SHOCK_MIN_MFE_R = env_float("OPPOSITE_SHOCK_MIN_MFE_R", 0.80)
OPPOSITE_SHOCK_REQUIRE_PARTIAL = env_bool("OPPOSITE_SHOCK_REQUIRE_PARTIAL", True)

ENTRY_RELAXED_MODE = env_bool("ENTRY_RELAXED_MODE", False)
ONE_POSITION_ONLY = env_bool("ONE_POSITION_ONLY", True)

ATR_LEN = env_int("ATR_LEN", 14)
EMA_FAST = env_int("EMA_FAST", 10)
EMA_MID = env_int("EMA_MID", 50)
EMA_SLOW = env_int("EMA_SLOW", 99)
VOL_MA_LEN = env_int("VOL_MA_LEN", 20)
RSI_FAST_LEN = env_int("RSI_FAST_LEN", 6)
RSI_LEN = env_int("RSI_LEN", 14)
STRUCTURE_LOOKBACK = env_int("STRUCTURE_LOOKBACK", 12)
RECENT_IMPULSE_LOOKBACK = env_int("RECENT_IMPULSE_LOOKBACK", 20)

BREAKDOWN_IMPULSE_ATR = env_float("BREAKDOWN_IMPULSE_ATR", 1.15 if ENTRY_RELAXED_MODE else 1.25)
BREAKDOWN_VOL_MULT = env_float("BREAKDOWN_VOL_MULT", 1.05 if ENTRY_RELAXED_MODE else 1.20)
BREAKDOWN_PULLBACK_MAX_BARS = env_int("BREAKDOWN_PULLBACK_MAX_BARS", 4 if ENTRY_RELAXED_MODE else 3)
BREAKDOWN_RETEST_ZONE_ATR = env_float("BREAKDOWN_RETEST_ZONE_ATR", 0.45 if ENTRY_RELAXED_MODE else 0.35)
BREAKDOWN_STOP_ATR = env_float("BREAKDOWN_STOP_ATR", 0.15)

EMA_REJECT_LOOKBACK = env_int("EMA_REJECT_LOOKBACK", 4)
EMA_REJECT_STOP_ATR = env_float("EMA_REJECT_STOP_ATR", 0.20)
EMA_REJECT_ZONE_ATR = env_float("EMA_REJECT_ZONE_ATR", 0.30 if ENTRY_RELAXED_MODE else 0.22)

CONTINUATION_STOP_ATR = env_float("CONTINUATION_STOP_ATR", 0.18)
CONTINUATION_MAX_BARS = env_int("CONTINUATION_MAX_BARS", 2)

CAPITULATION_DROP_ATR = env_float("CAPITULATION_DROP_ATR", 1.60 if ENTRY_RELAXED_MODE else 1.85)
CAPITULATION_RSI_MAX = env_float("CAPITULATION_RSI_MAX", 28.0 if ENTRY_RELAXED_MODE else 24.0)
CAPITULATION_WICK_RATIO = env_float("CAPITULATION_WICK_RATIO", 0.35 if ENTRY_RELAXED_MODE else 0.45)
CAPITULATION_CONFIRM_BARS = env_int("CAPITULATION_CONFIRM_BARS", 3 if ENTRY_RELAXED_MODE else 2)
CAPITULATION_STOP_ATR = env_float("CAPITULATION_STOP_ATR", 0.12)

BREAKOUT_IMPULSE_ATR = env_float("BREAKOUT_IMPULSE_ATR", 1.10 if ENTRY_RELAXED_MODE else 1.25)
BREAKOUT_VOL_MULT = env_float("BREAKOUT_VOL_MULT", 1.05 if ENTRY_RELAXED_MODE else 1.20)
BREAKOUT_PULLBACK_MAX_BARS = env_int("BREAKOUT_PULLBACK_MAX_BARS", 4 if ENTRY_RELAXED_MODE else 3)
BREAKOUT_RETEST_ZONE_ATR = env_float("BREAKOUT_RETEST_ZONE_ATR", 0.45 if ENTRY_RELAXED_MODE else 0.35)
BREAKOUT_STOP_ATR = env_float("BREAKOUT_STOP_ATR", 0.15)

PARTIAL1_R = env_float("PARTIAL1_R", 1.0)
PARTIAL1_CLOSE_PCT = env_float("PARTIAL1_CLOSE_PCT", 0.50)
PARTIAL2_R = env_float("PARTIAL2_R", 2.0)
PARTIAL2_CLOSE_PCT = env_float("PARTIAL2_CLOSE_PCT", 0.25)

DEFAULT_MAX_HOLD_BARS = env_int("MAX_HOLD_BARS", 36)
DEFAULT_TIME_STOP_BARS = env_int("TIME_STOP_BARS", 12)
MIN_PROGRESS_R_BY_TIME_STOP = env_float("MIN_PROGRESS_R_BY_TIME_STOP", 0.25)
OPPOSITE_SHOCK_EXIT = env_bool("OPPOSITE_SHOCK_EXIT", True)


# ============================================================
# UTIL
# ============================================================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def safe_json_dump(obj, path: str) -> None:
    def default(o):
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            x = float(o)
            return x if math.isfinite(x) else None
        if isinstance(o, (np.bool_, bool)):
            return bool(o)
        if isinstance(o, pd.Timestamp):
            return o.isoformat()
        if isinstance(o, np.ndarray):
            return o.tolist()
        return str(o)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=default)

def parse_timestamp_column(s: pd.Series) -> pd.Series:
    x = s.copy()
    if pd.api.types.is_datetime64_any_dtype(x):
        return pd.to_datetime(x, utc=True, errors="coerce")

    if pd.api.types.is_numeric_dtype(x):
        xn = pd.to_numeric(x, errors="coerce")
        median_abs = np.nanmedian(np.abs(xn.values.astype(float))) if len(xn) else np.nan
        if np.isfinite(median_abs):
            if median_abs > 1e14:
                return pd.to_datetime(xn, unit="ns", utc=True, errors="coerce")
            if median_abs > 1e11:
                return pd.to_datetime(xn, unit="ms", utc=True, errors="coerce")
            if median_abs > 1e9:
                return pd.to_datetime(xn, unit="s", utc=True, errors="coerce")
        return pd.to_datetime(xn, utc=True, errors="coerce")

    xs = x.astype("string")
    dt = pd.to_datetime(xs, utc=True, errors="coerce")
    if dt.notna().mean() >= 0.80:
        return dt

    xn = pd.to_numeric(xs, errors="coerce")
    if xn.notna().mean() >= 0.80:
        return parse_timestamp_column(xn)

    return pd.to_datetime(xs, utc=True, errors="coerce")

def load_ohlcv_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols_lower = {c.lower(): c for c in df.columns}

    ts_col = None
    for cand in ["timestamp", "datetime", "date", "time", "open_time"]:
        if cand in cols_lower:
            ts_col = cols_lower[cand]
            break
    if ts_col is None:
        raise ValueError("timestamp 컬럼을 찾지 못했습니다. 예: timestamp / datetime / date / time / open_time")

    rename_map = {}
    for cand in ["open", "high", "low", "close", "volume"]:
        if cand in cols_lower:
            rename_map[cols_lower[cand]] = cand
    df = df.rename(columns=rename_map)

    required = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    df["timestamp"] = parse_timestamp_column(df[ts_col])
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna().copy()
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    return df

def maybe_download_binance_ohlcv(symbol: str, timeframe: str, days: int, save_path: str) -> pd.DataFrame:
    if ccxt is None:
        raise RuntimeError("ccxt가 설치되어 있지 않습니다. pip install ccxt")

    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    tf_ms_map = {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "30m": 30 * 60_000,
        "1h": 60 * 60_000,
        "4h": 4 * 60 * 60_000,
        "1d": 24 * 60 * 60_000,
    }
    if timeframe not in tf_ms_map:
        raise ValueError(f"지원하지 않는 timeframe: {timeframe}")

    since_ms = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    all_rows: List[List[float]] = []
    limit = 1500
    tf_ms = tf_ms_map[timeframe]

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
        if not batch:
            break
        all_rows.extend(batch)
        last_ts = int(batch[-1][0])
        next_since = last_ts + tf_ms
        if next_since <= since_ms:
            break
        since_ms = next_since
        if len(batch) < limit:
            break
        time.sleep(exchange.rateLimit / 1000.0)

    if not all_rows:
        raise RuntimeError("Binance에서 데이터를 받아오지 못했습니다.")

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna().drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df.to_csv(save_path, index=False)
    return df

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)

def atr(df: pd.DataFrame, length: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema_fast"] = ema(out["close"], EMA_FAST)
    out["ema_mid"] = ema(out["close"], EMA_MID)
    out["ema_slow"] = ema(out["close"], EMA_SLOW)
    out["atr"] = atr(out, ATR_LEN)
    out["vol_ma"] = out["volume"].rolling(VOL_MA_LEN, min_periods=VOL_MA_LEN).mean()
    out["rsi_fast"] = rsi(out["close"], RSI_FAST_LEN)
    out["rsi"] = rsi(out["close"], RSI_LEN)
    out["body"] = (out["close"] - out["open"]).abs()
    out["body_signed"] = out["close"] - out["open"]
    out["range"] = (out["high"] - out["low"]).replace(0, np.nan)
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["body_to_range"] = (out["body"] / out["range"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["close_pos_in_bar"] = ((out["close"] - out["low"]) / out["range"]).replace([np.inf, -np.inf], np.nan).fillna(0.5)
    out["hh_struct"] = out["high"].rolling(STRUCTURE_LOOKBACK, min_periods=STRUCTURE_LOOKBACK).max().shift(1)
    out["ll_struct"] = out["low"].rolling(STRUCTURE_LOOKBACK, min_periods=STRUCTURE_LOOKBACK).min().shift(1)
    out["avg_body_recent"] = out["body"].rolling(RECENT_IMPULSE_LOOKBACK, min_periods=RECENT_IMPULSE_LOOKBACK).mean()
    out["return_1"] = out["close"].pct_change()
    out["atr_pct"] = (out["atr"] / out["close"]).replace([np.inf, -np.inf], np.nan)
    out["vol_ratio"] = (out["volume"] / out["vol_ma"]).replace([np.inf, -np.inf], np.nan)
    out["trend_bias"] = np.select(
        [
            (out["ema_fast"] > out["ema_mid"]) & (out["ema_mid"] > out["ema_slow"]),
            (out["ema_fast"] < out["ema_mid"]) & (out["ema_mid"] < out["ema_slow"]),
        ],
        [1, -1],
        default=0,
    )
    return out

def cross_above(a_prev: float, a_now: float, b_prev: float, b_now: float) -> bool:
    return a_prev <= b_prev and a_now > b_now

def cross_below(a_prev: float, a_now: float, b_prev: float, b_now: float) -> bool:
    return a_prev >= b_prev and a_now < b_now



# ============================================================
# REGIME FILTER
# ============================================================

def attach_regime_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["eth_ret_lb"] = out["close"].pct_change(BTC_LOOKBACK_RETURN_BARS).fillna(0.0)
    out["eth_regime"] = np.select(
        [
            (out["trend_bias"] < 0) & (out["eth_ret_lb"] <= -0.01),
            (out["trend_bias"] > 0) & (out["eth_ret_lb"] >= 0.01),
        ],
        [-1, 1],
        default=0,
    )
    out["btc_trend_bias"] = 0
    out["btc_ret_lb"] = 0.0
    out["btc_regime"] = 0
    return out

def maybe_attach_btc_regime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not BTC_REGIME_FILTER:
        return out

    btc_path = BTC_REGIME_CSV.strip()
    try:
        if btc_path and os.path.exists(btc_path):
            btc_raw = load_ohlcv_csv(btc_path)
        elif BTC_AUTO_DOWNLOAD:
            if BTC_REGIME_DIR.strip():
                ensure_dir(BTC_REGIME_DIR)
                dl_path = os.path.join(BTC_REGIME_DIR, f"btcusdt_{ETH_TIMEFRAME}_{ETH_DOWNLOAD_DAYS}d_downloaded.csv")
            else:
                dl_path = os.path.join(ETH_OUTPUT_DIR, f"btcusdt_{ETH_TIMEFRAME}_{ETH_DOWNLOAD_DAYS}d_downloaded.csv")
            btc_raw = maybe_download_binance_ohlcv(BTC_SYMBOL, ETH_TIMEFRAME, ETH_DOWNLOAD_DAYS, dl_path)
        else:
            return out

        btc = add_indicators(btc_raw).dropna().reset_index(drop=True)
        btc["btc_ret_lb"] = btc["close"].pct_change(BTC_LOOKBACK_RETURN_BARS).fillna(0.0)
        btc["btc_regime"] = np.select(
            [
                (btc["trend_bias"] < 0) & (btc["btc_ret_lb"] <= -0.01),
                (btc["trend_bias"] > 0) & (btc["btc_ret_lb"] >= 0.01),
            ],
            [-1, 1],
            default=0,
        )
        keep = btc[["timestamp", "trend_bias", "btc_ret_lb", "btc_regime"]].rename(columns={"trend_bias": "btc_trend_bias"}).sort_values("timestamp")
        out = out.sort_values("timestamp")
        out = pd.merge_asof(out, keep, on="timestamp", direction="backward")
        out["btc_trend_bias"] = out["btc_trend_bias"].fillna(0).astype(int)
        out["btc_ret_lb"] = out["btc_ret_lb"].fillna(0.0)
        out["btc_regime"] = out["btc_regime"].fillna(0).astype(int)
        return out
    except Exception as e:
        print(f"BTC regime attach skipped: {type(e).__name__} {e}")
        return out

def short_regime_ok(row: pd.Series) -> bool:
    if not USE_REGIME_FILTER:
        return True
    eth_ok = (int(row.get("trend_bias", 0)) < 0) or (int(row.get("eth_regime", 0)) < 0)
    if ETH_REGIME_ONLY_SHORT_WHEN_BEARISH and not eth_ok:
        return False
    if BTC_REGIME_FILTER:
        btc_ok = (int(row.get("btc_trend_bias", 0)) <= 0) or (int(row.get("btc_regime", 0)) <= 0)
        if not btc_ok:
            return False
    return True

def long_regime_ok(row: pd.Series) -> bool:
    if not USE_REGIME_FILTER:
        return True
    eth_ok = (int(row.get("trend_bias", 0)) > 0) or (int(row.get("eth_regime", 0)) > 0)
    if not eth_ok:
        return False
    if BTC_REGIME_FILTER:
        btc_ok = (int(row.get("btc_trend_bias", 0)) >= 0) or (int(row.get("btc_regime", 0)) >= 0)
        if not btc_ok:
            return False
    return True

# ============================================================
# SIGNAL STATE
# ============================================================

@dataclass
class PendingSignal:
    engine: str
    direction: str
    impulse_idx: int
    impulse_ts: str
    ref_price: float
    invalidation: float
    created_at_idx: int
    expires_at_idx: int
    priority: int
    meta: Dict[str, float]

@dataclass
class Position:
    engine: str
    direction: str
    entry_idx: int
    entry_ts: str
    entry_price: float
    stop_price: float
    qty: float
    risk_r: float
    initial_risk_per_unit: float
    remaining_qty: float
    partial1_done: bool = False
    partial2_done: bool = False
    exit_idx: Optional[int] = None
    exit_ts: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    mfe_r: float = 0.0
    mae_r: float = 0.0
    realized_pnl: float = 0.0
    fee_paid: float = 0.0
    bars_held: int = 0
    notes: str = ""


# ============================================================
# SIGNAL DETECTION
# ============================================================

def detect_breakdown_impulse(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(STRUCTURE_LOOKBACK + 2, ATR_LEN + 2):
        return None
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0 and np.isfinite(row["vol_ma"]) and row["vol_ma"] > 0):
        return None

    bearish_structure = (row["ema_fast"] < row["ema_mid"]) or (row["close"] < row["ema_mid"] < row["ema_slow"])
    impulse = (row["open"] - row["close"]) >= BREAKDOWN_IMPULSE_ATR * row["atr"]
    vol_ok = row["volume"] >= BREAKDOWN_VOL_MULT * row["vol_ma"]
    broke_structure = row["close"] < row["ll_struct"]
    closed_weak = row["close_pos_in_bar"] <= 0.30
    crossed_fast = cross_below(prev["close"], row["close"], prev["ema_fast"], row["ema_fast"])

    if bearish_structure and impulse and vol_ok and broke_structure and (closed_weak or crossed_fast) and short_regime_ok(row):
        ref_price = min(row["ll_struct"], row["close"] + 0.5 * (row["open"] - row["close"]))
        invalidation = row["high"] + BREAKDOWN_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="BREAKDOWN_PULLBACK_SHORT",
            direction="SHORT",
            impulse_idx=i,
            impulse_ts=str(row["timestamp"]),
            ref_price=float(ref_price),
            invalidation=float(invalidation),
            created_at_idx=i,
            expires_at_idx=i + BREAKDOWN_PULLBACK_MAX_BARS,
            priority=2,
            meta={"impulse_low": float(row["low"]), "impulse_high": float(row["high"]), "atr": float(row["atr"]), "ll_struct": float(row["ll_struct"])},
        )
    return None

def _base_reject_short(row: pd.Series, prev: pd.Series) -> bool:
    upper_wick_ratio = row["upper_wick"] / max(row["range"], 1e-12)
    weak_reject = (row["close"] < row["open"]) and (upper_wick_ratio >= 0.25) and (row["close_pos_in_bar"] <= 0.45)
    lower_high = row["high"] <= max(prev["high"], row["ema_mid"] + 0.4 * row["atr"])
    not_breakout = row["close"] < row["hh_struct"]
    bearish_bias = (row["ema_fast"] < row["ema_mid"]) and (row["close"] < row["ema_mid"])
    return bearish_bias and weak_reject and lower_high and not_breakout and short_regime_ok(row)

def detect_ema_fast_reject_short(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(EMA_MID + 2, ATR_LEN + 2, STRUCTURE_LOOKBACK + 2):
        return None
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0 and np.isfinite(row["vol_ma"]) and row["vol_ma"] > 0):
        return None
    near_fast = abs(row["high"] - row["ema_fast"]) <= EMA_REJECT_ZONE_ATR * row["atr"]
    if near_fast and _base_reject_short(row, prev):
        invalidation = max(row["high"], row["ema_fast"]) + EMA_REJECT_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="EMA_FAST_REJECT_SHORT", direction="SHORT", impulse_idx=i, impulse_ts=str(row["timestamp"]),
            ref_price=float(min(row["low"], row["close"])), invalidation=float(invalidation), created_at_idx=i,
            expires_at_idx=i + EMA_REJECT_LOOKBACK, priority=3,
            meta={"atr": float(row["atr"]), "ema_fast": float(row["ema_fast"]), "ema_mid": float(row["ema_mid"])}
        )
    return None

def detect_ema_mid_reject_short(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(EMA_MID + 2, ATR_LEN + 2, STRUCTURE_LOOKBACK + 2):
        return None
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0 and np.isfinite(row["vol_ma"]) and row["vol_ma"] > 0):
        return None
    near_mid = abs(row["high"] - row["ema_mid"]) <= EMA_REJECT_ZONE_ATR * row["atr"]
    if near_mid and _base_reject_short(row, prev):
        invalidation = max(row["high"], row["ema_mid"]) + EMA_REJECT_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="EMA_MID_REJECT_SHORT", direction="SHORT", impulse_idx=i, impulse_ts=str(row["timestamp"]),
            ref_price=float(min(row["low"], row["close"])), invalidation=float(invalidation), created_at_idx=i,
            expires_at_idx=i + EMA_REJECT_LOOKBACK, priority=3,
            meta={"atr": float(row["atr"]), "ema_fast": float(row["ema_fast"]), "ema_mid": float(row["ema_mid"])}
        )
    return None

def detect_breakdown_retest_short(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(STRUCTURE_LOOKBACK + 3, ATR_LEN + 3):
        return None
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    prev2 = df.iloc[i - 2]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0):
        return None
    had_breakdown = (prev["close"] < prev["ll_struct"]) or (prev2["close"] < prev2["ll_struct"])
    retest = (row["high"] >= row["ll_struct"]) or (row["high"] >= row["ema_fast"])
    reject = (row["close"] < row["open"]) and (row["close"] < row["ema_fast"]) and (row["close_pos_in_bar"] <= 0.45)
    if had_breakdown and retest and reject and short_regime_ok(row):
        invalidation = max(row["high"], row["ll_struct"]) + EMA_REJECT_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="BREAKDOWN_RETEST_SHORT", direction="SHORT", impulse_idx=i, impulse_ts=str(row["timestamp"]),
            ref_price=float(min(row["low"], row["close"])), invalidation=float(invalidation), created_at_idx=i,
            expires_at_idx=i + EMA_REJECT_LOOKBACK, priority=3,
            meta={"atr": float(row["atr"]), "ll_struct": float(row["ll_struct"]), "ema_fast": float(row["ema_fast"])}
        )
    return None

def detect_breakdown_continuation_short(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(STRUCTURE_LOOKBACK + 3, ATR_LEN + 3):
        return None
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    prev2 = df.iloc[i - 2]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0):
        return None

    bearish_bias = (row["ema_fast"] < row["ema_mid"]) and (row["close"] < row["ema_fast"])
    previous_break = (prev["close"] < prev["ll_struct"]) or (prev2["close"] < prev2["ll_struct"])
    continuation = (row["close"] < prev["low"]) and (row["body_signed"] < 0) and (row["close_pos_in_bar"] <= 0.35)
    no_reclaim = row["high"] <= (prev["close"] + 0.35 * row["atr"])
    if bearish_bias and previous_break and continuation and no_reclaim and short_regime_ok(row):
        invalidation = row["high"] + CONTINUATION_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="BREAKDOWN_CONTINUATION_SHORT",
            direction="SHORT",
            impulse_idx=i,
            impulse_ts=str(row["timestamp"]),
            ref_price=float(row["close"]),
            invalidation=float(invalidation),
            created_at_idx=i,
            expires_at_idx=i + CONTINUATION_MAX_BARS,
            priority=4,
            meta={"atr": float(row["atr"]), "prev_low": float(prev["low"]), "ll_struct": float(row["ll_struct"])},
        )
    return None

def detect_capitulation_impulse(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(STRUCTURE_LOOKBACK + 2, ATR_LEN + 2):
        return None

    row = df.iloc[i]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0 and np.isfinite(row["vol_ma"]) and row["vol_ma"] > 0):
        return None

    drop_big = (row["open"] - row["close"]) >= CAPITULATION_DROP_ATR * row["atr"]
    oversold = row["rsi_fast"] <= CAPITULATION_RSI_MAX
    lower_wick_ratio = row["lower_wick"] / max(row["range"], 1e-12)
    wick_ok = lower_wick_ratio >= CAPITULATION_WICK_RATIO
    washed_out = row["low"] < row["ll_struct"]
    vol_ok = row["volume"] >= 1.05 * row["vol_ma"]

    if drop_big and oversold and washed_out and vol_ok and wick_ok and long_regime_ok(row):
        invalidation = row["low"] - CAPITULATION_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="CAPITULATION_RECLAIM_LONG",
            direction="LONG",
            impulse_idx=i,
            impulse_ts=str(row["timestamp"]),
            ref_price=float(max(row["close"], row["open"])),
            invalidation=float(invalidation),
            created_at_idx=i,
            expires_at_idx=i + CAPITULATION_CONFIRM_BARS,
            priority=5,
            meta={"impulse_low": float(row["low"]), "impulse_high": float(row["high"]), "atr": float(row["atr"])},
        )
    return None

def detect_breakout_impulse(df: pd.DataFrame, i: int) -> Optional[PendingSignal]:
    if i < max(STRUCTURE_LOOKBACK + 2, ATR_LEN + 2):
        return None

    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not (np.isfinite(row["atr"]) and row["atr"] > 0 and np.isfinite(row["vol_ma"]) and row["vol_ma"] > 0):
        return None

    bullish_structure = (row["ema_fast"] > row["ema_mid"]) or (row["close"] > row["ema_mid"] > row["ema_slow"])
    impulse = (row["close"] - row["open"]) >= BREAKOUT_IMPULSE_ATR * row["atr"]
    vol_ok = row["volume"] >= BREAKOUT_VOL_MULT * row["vol_ma"]
    broke_structure = row["close"] > row["hh_struct"]
    closed_strong = row["close_pos_in_bar"] >= 0.70
    crossed_fast = cross_above(prev["close"], row["close"], prev["ema_fast"], row["ema_fast"])

    if bullish_structure and impulse and vol_ok and broke_structure and (closed_strong or crossed_fast) and long_regime_ok(row):
        ref_price = max(row["hh_struct"], row["close"] - 0.5 * (row["close"] - row["open"]))
        invalidation = row["low"] - BREAKOUT_STOP_ATR * row["atr"]
        return PendingSignal(
            engine="BREAKOUT_FIRST_PULLBACK_LONG",
            direction="LONG",
            impulse_idx=i,
            impulse_ts=str(row["timestamp"]),
            ref_price=float(ref_price),
            invalidation=float(invalidation),
            created_at_idx=i,
            expires_at_idx=i + BREAKOUT_PULLBACK_MAX_BARS,
            priority=1,
            meta={"impulse_low": float(row["low"]), "impulse_high": float(row["high"]), "atr": float(row["atr"]), "hh_struct": float(row["hh_struct"])},
        )
    return None

def confirm_pending_entry(df: pd.DataFrame, i: int, sig: PendingSignal) -> Tuple[bool, Optional[float], Optional[float], str]:
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    atr_now = float(row["atr"]) if np.isfinite(row["atr"]) else 0.0

    if i <= sig.impulse_idx or i > sig.expires_at_idx:
        return False, None, None, "EXPIRED"

    if sig.engine == "BREAKDOWN_PULLBACK_SHORT":
        retest_zone = sig.ref_price + BREAKDOWN_RETEST_ZONE_ATR * atr_now
        pulled_back = row["high"] >= sig.ref_price
        not_too_deep = row["high"] <= retest_zone
        failed_close = row["close"] < row["open"] or row["close"] < row["ema_fast"]
        weak_high = row["close_pos_in_bar"] <= (0.50 if ENTRY_RELAXED_MODE else 0.45)
        broke_prev_low = row["low"] < prev["low"] or row["close"] < prev["close"]
        if pulled_back and not_too_deep and failed_close and weak_high and broke_prev_low:
            entry = min(prev["low"], row["close"])
            stop = max(sig.invalidation, row["high"] + BREAKDOWN_STOP_ATR * atr_now)
            return True, float(entry), float(stop), "PULLBACK_FAIL_SHORT"
        return False, None, None, "WAIT_SHORT"

    if sig.engine in {"EMA_FAST_REJECT_SHORT", "EMA_MID_REJECT_SHORT", "BREAKDOWN_RETEST_SHORT"}:
        weak_again = (row["close"] < row["open"]) or (row["close"] < row["ema_fast"])
        below_prev = row["low"] < prev["low"] or row["close"] < prev["close"]
        if weak_again and below_prev and short_regime_ok(row):
            entry = min(prev["low"], row["close"])
            stop = max(sig.invalidation, row["high"] + EMA_REJECT_STOP_ATR * atr_now)
            return True, float(entry), float(stop), sig.engine
        return False, None, None, f"WAIT_{sig.engine}"

    if sig.engine == "BREAKDOWN_CONTINUATION_SHORT":
        immediate_weak = row["close"] <= sig.ref_price or row["low"] < prev["low"]
        if immediate_weak:
            entry = min(row["close"], prev["low"])
            stop = max(sig.invalidation, row["high"] + CONTINUATION_STOP_ATR * atr_now)
            return True, float(entry), float(stop), "BREAKDOWN_CONTINUATION_SHORT"
        return False, None, None, "WAIT_CONTINUATION_SHORT"

    if sig.engine == "CAPITULATION_RECLAIM_LONG":
        impulse_low = sig.meta["impulse_low"]
        retest_failed = row["low"] > impulse_low
        reclaimed = (row["close"] > row["open"] and row["close"] > prev["high"]) or (ENTRY_RELAXED_MODE and row["close"] > row["ema_fast"])
        above_fast = row["close"] >= row["ema_fast"] or row["close"] >= sig.ref_price
        if retest_failed and reclaimed and above_fast:
            entry = max(prev["high"], row["close"])
            stop = min(sig.invalidation, row["low"] - CAPITULATION_STOP_ATR * atr_now)
            return True, float(entry), float(stop), "CAPITULATION_RECLAIM_LONG"
        return False, None, None, "WAIT_CAP_LONG"

    if sig.engine == "BREAKOUT_FIRST_PULLBACK_LONG":
        retest_zone = sig.ref_price - BREAKOUT_RETEST_ZONE_ATR * atr_now
        pulled_back = row["low"] <= sig.ref_price
        not_too_deep = row["low"] >= retest_zone
        bounced = row["close"] > row["open"] or row["close"] > row["ema_fast"]
        strong_close = row["close_pos_in_bar"] >= (0.50 if ENTRY_RELAXED_MODE else 0.55)
        broke_prev_high = row["high"] > prev["high"] or row["close"] > prev["close"]
        if pulled_back and not_too_deep and bounced and strong_close and broke_prev_high:
            entry = max(prev["high"], row["close"])
            stop = min(sig.invalidation, row["low"] - BREAKOUT_STOP_ATR * atr_now)
            return True, float(entry), float(stop), "FIRST_PULLBACK_LONG"
        return False, None, None, "WAIT_BO_LONG"

    return False, None, None, "UNKNOWN"


# ============================================================
# POSITION / BACKTEST
# ============================================================

def calc_qty_from_risk(equity: float, entry: float, stop: float) -> float:
    risk_cash = max(0.0, equity * RISK_PER_TRADE_PCT)
    per_unit_risk = abs(entry - stop)
    if risk_cash <= 0 or per_unit_risk <= 0:
        return 0.0
    return max(0.0, risk_cash / per_unit_risk)

def trade_pnl(direction: str, entry: float, exit_: float, qty: float) -> float:
    return (exit_ - entry) * qty if direction == "LONG" else (entry - exit_) * qty

def apply_execution_price(raw_price: float, direction: str, action: str) -> float:
    if action == "BUY":
        return raw_price * (1.0 + SLIPPAGE_RATE)
    return raw_price * (1.0 - SLIPPAGE_RATE)

def calc_fee(notional: float) -> float:
    return abs(notional) * FEE_RATE

def get_engine_time_stop(engine: str) -> int:
    mapping = {
        "BREAKOUT_FIRST_PULLBACK_LONG": 14,
        "CAPITULATION_RECLAIM_LONG": 8,
        "BREAKDOWN_PULLBACK_SHORT": 12,
        "EMA_FAST_REJECT_SHORT": 10,
        "EMA_MID_REJECT_SHORT": 12,
        "BREAKDOWN_RETEST_SHORT": 10,
        "BREAKDOWN_CONTINUATION_SHORT": 8,
    }
    return int(mapping.get(engine, DEFAULT_TIME_STOP_BARS))

def get_engine_max_hold(engine: str) -> int:
    mapping = {
        "BREAKOUT_FIRST_PULLBACK_LONG": 42,
        "CAPITULATION_RECLAIM_LONG": 20,
        "BREAKDOWN_PULLBACK_SHORT": 36,
        "EMA_FAST_REJECT_SHORT": 28,
        "EMA_MID_REJECT_SHORT": 32,
        "BREAKDOWN_RETEST_SHORT": 24,
        "BREAKDOWN_CONTINUATION_SHORT": 20,
    }
    return int(mapping.get(engine, DEFAULT_MAX_HOLD_BARS))

def detect_opposite_shock(df: pd.DataFrame, i: int, pos: Position) -> bool:
    if i < 1 or not OPPOSITE_SHOCK_EXIT:
        return False
    row = df.iloc[i]
    prev = df.iloc[i - 1]
    if not np.isfinite(row["atr"]) or row["atr"] <= 0:
        return False
    if pos.bars_held < OPPOSITE_SHOCK_MIN_BARS:
        return False
    if OPPOSITE_SHOCK_REQUIRE_PARTIAL and (not pos.partial1_done) and pos.mfe_r < OPPOSITE_SHOCK_MIN_MFE_R:
        return False

    bullish_shock = (row["close"] - row["open"]) >= 1.5 * row["atr"] and row["close"] > row["hh_struct"] and row["close_pos_in_bar"] >= 0.78
    bearish_shock = (row["open"] - row["close"]) >= 1.5 * row["atr"] and row["close"] < row["ll_struct"] and row["close_pos_in_bar"] <= 0.22
    hard_cross_up = cross_above(prev["close"], row["close"], prev["ema_mid"], row["ema_mid"])
    hard_cross_dn = cross_below(prev["close"], row["close"], prev["ema_mid"], row["ema_mid"])

    if pos.direction == "LONG":
        return bearish_shock or (hard_cross_dn and row["close"] < row["ema_fast"])
    return bullish_shock or (hard_cross_up and row["close"] > row["ema_fast"])

def update_position(df: pd.DataFrame, i: int, pos: Position) -> Tuple[Optional[Position], List[Dict]]:
    row = df.iloc[i]
    exits: List[Dict] = []
    pos.bars_held += 1

    if pos.direction == "LONG":
        favorable = row["high"] - pos.entry_price
        adverse = row["low"] - pos.entry_price
    else:
        favorable = pos.entry_price - row["low"]
        adverse = pos.entry_price - row["high"]

    pos.mfe_r = max(pos.mfe_r, favorable / max(pos.initial_risk_per_unit, 1e-12))
    pos.mae_r = min(pos.mae_r, adverse / max(pos.initial_risk_per_unit, 1e-12))

    stop_hit = False
    stop_exec_raw = pos.stop_price
    if pos.direction == "LONG" and row["low"] <= pos.stop_price:
        stop_hit = True
    elif pos.direction == "SHORT" and row["high"] >= pos.stop_price:
        stop_hit = True

    if stop_hit:
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(stop_exec_raw, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, pos.remaining_qty)
        fee = calc_fee(pos.remaining_qty * pos.entry_price) + calc_fee(pos.remaining_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.exit_idx = i
        pos.exit_ts = str(row["timestamp"])
        pos.exit_price = float(exit_px)
        pos.exit_reason = "STOP_HIT"
        exits.append({"kind": "FINAL_EXIT", "reason": "STOP_HIT", "price": float(exit_px), "qty": float(pos.remaining_qty)})
        pos.remaining_qty = 0.0
        return None, exits

    r_now = pos.mfe_r

    if (not pos.partial1_done) and r_now >= PARTIAL1_R and pos.remaining_qty > 0:
        close_qty = min(pos.qty * PARTIAL1_CLOSE_PCT, pos.remaining_qty)
        raw_px = row["close"]
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, close_qty)
        fee = calc_fee(close_qty * pos.entry_price) + calc_fee(close_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.remaining_qty -= close_qty
        pos.partial1_done = True
        exits.append({"kind": "PARTIAL1", "reason": "R1", "price": float(exit_px), "qty": float(close_qty)})
        if pos.direction == "LONG":
            pos.stop_price = max(pos.stop_price, pos.entry_price + 0.10 * pos.initial_risk_per_unit)
        else:
            pos.stop_price = min(pos.stop_price, pos.entry_price - 0.10 * pos.initial_risk_per_unit)

    if (not pos.partial2_done) and r_now >= PARTIAL2_R and pos.remaining_qty > 0:
        close_qty = min(pos.qty * PARTIAL2_CLOSE_PCT, pos.remaining_qty)
        raw_px = row["close"]
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, close_qty)
        fee = calc_fee(close_qty * pos.entry_price) + calc_fee(close_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.remaining_qty -= close_qty
        pos.partial2_done = True
        exits.append({"kind": "PARTIAL2", "reason": "R2", "price": float(exit_px), "qty": float(close_qty)})
        if pos.direction == "LONG":
            pos.stop_price = max(pos.stop_price, float(row["ema_fast"]))
        else:
            pos.stop_price = min(pos.stop_price, float(row["ema_fast"]))

    if pos.partial1_done and pos.remaining_qty > 0:
        if pos.direction == "LONG":
            trail = float(row["ema_fast"] - 0.20 * row["atr"])
            pos.stop_price = max(pos.stop_price, trail)
        else:
            trail = float(row["ema_fast"] + 0.20 * row["atr"])
            pos.stop_price = min(pos.stop_price, trail)

    if detect_opposite_shock(df, i, pos) and pos.remaining_qty > 0:
        raw_px = float(row["close"])
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, pos.remaining_qty)
        fee = calc_fee(pos.remaining_qty * pos.entry_price) + calc_fee(pos.remaining_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.exit_idx = i
        pos.exit_ts = str(row["timestamp"])
        pos.exit_price = float(exit_px)
        pos.exit_reason = "OPPOSITE_SHOCK_EXIT"
        exits.append({"kind": "FINAL_EXIT", "reason": "OPPOSITE_SHOCK_EXIT", "price": float(exit_px), "qty": float(pos.remaining_qty)})
        pos.remaining_qty = 0.0
        return None, exits

    time_stop_bars = get_engine_time_stop(pos.engine)
    if pos.remaining_qty > 0 and pos.bars_held >= time_stop_bars and pos.mfe_r < MIN_PROGRESS_R_BY_TIME_STOP:
        raw_px = float(row["close"])
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, pos.remaining_qty)
        fee = calc_fee(pos.remaining_qty * pos.entry_price) + calc_fee(pos.remaining_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.exit_idx = i
        pos.exit_ts = str(row["timestamp"])
        pos.exit_price = float(exit_px)
        pos.exit_reason = "TIME_STOP_NO_PROGRESS"
        exits.append({"kind": "FINAL_EXIT", "reason": "TIME_STOP_NO_PROGRESS", "price": float(exit_px), "qty": float(pos.remaining_qty)})
        pos.remaining_qty = 0.0
        return None, exits

    max_hold_bars = get_engine_max_hold(pos.engine)
    if pos.remaining_qty > 0 and pos.bars_held >= max_hold_bars:
        raw_px = float(row["close"])
        action = "SELL" if pos.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, pos.direction, action)
        pnl = trade_pnl(pos.direction, pos.entry_price, exit_px, pos.remaining_qty)
        fee = calc_fee(pos.remaining_qty * pos.entry_price) + calc_fee(pos.remaining_qty * exit_px)
        pos.realized_pnl += pnl - fee
        pos.fee_paid += fee
        pos.exit_idx = i
        pos.exit_ts = str(row["timestamp"])
        pos.exit_price = float(exit_px)
        pos.exit_reason = "MAX_HOLD_EXIT"
        exits.append({"kind": "FINAL_EXIT", "reason": "MAX_HOLD_EXIT", "price": float(exit_px), "qty": float(pos.remaining_qty)})
        pos.remaining_qty = 0.0
        return None, exits

    return pos, exits


# ============================================================
# BACKTEST
# ============================================================

def build_trade_snapshot(pos: Position) -> Dict:
    return {
        "engine": pos.engine,
        "direction": pos.direction,
        "entry_ts": pos.entry_ts,
        "exit_ts": pos.exit_ts,
        "entry_price": pos.entry_price,
        "exit_price": pos.exit_price,
        "qty": pos.qty,
        "bars_held": pos.bars_held,
        "mfe_r": pos.mfe_r,
        "mae_r": pos.mae_r,
        "realized_pnl": pos.realized_pnl,
        "fee_paid": pos.fee_paid,
        "exit_reason": pos.exit_reason,
        "notes": pos.notes,
    }

def backtest(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict, pd.DataFrame, Dict[str, pd.DataFrame]]:
    pending_signals: List[PendingSignal] = []
    position: Optional[Position] = None
    equity = INITIAL_CAPITAL
    equity_curve: List[Dict] = []
    trades: List[Dict] = []
    decisions: List[Dict] = []

    start_i = max(EMA_SLOW + 5, STRUCTURE_LOOKBACK + 5, ATR_LEN + 5, VOL_MA_LEN + 5)

    for i in range(start_i, len(df)):
        row = df.iloc[i]

        if position is not None:
            old = Position(**asdict(position))
            new_position, exit_events = update_position(df, i, position)
            for ev in exit_events:
                decisions.append(
                    {
                        "timestamp": row["timestamp"],
                        "type": "EXIT_EVENT",
                        "engine": old.engine,
                        "direction": old.direction,
                        "detail": ev,
                    }
                )
            if new_position is None:
                old.exit_idx = old.exit_idx if old.exit_idx is not None else i
                if old.exit_ts is None:
                    old.exit_ts = str(row["timestamp"])
                if old.exit_price is None and exit_events:
                    old.exit_price = float(exit_events[-1]["price"])
                if old.exit_reason is None and exit_events:
                    old.exit_reason = str(exit_events[-1]["reason"])
                trades.append(build_trade_snapshot(old))
                equity += old.realized_pnl
                position = None
            else:
                position = new_position

        # detect new impulses
        if ALLOW_SHORT:
            for fn in [detect_breakdown_impulse, detect_ema_fast_reject_short, detect_ema_mid_reject_short, detect_breakdown_retest_short, detect_breakdown_continuation_short]:
                sig = fn(df, i)
                if sig is not None:
                    pending_signals.append(sig)
                    decisions.append({"timestamp": row["timestamp"], "type": "SIGNAL_NEW", "engine": sig.engine, "direction": sig.direction, "meta": asdict(sig)})

        if ALLOW_LONG:
            for fn in [detect_capitulation_impulse, detect_breakout_impulse]:
                sig = fn(df, i)
                if sig is not None:
                    pending_signals.append(sig)
                    decisions.append({"timestamp": row["timestamp"], "type": "SIGNAL_NEW", "engine": sig.engine, "direction": sig.direction, "meta": asdict(sig)})

        # remove expired signals
        alive: List[PendingSignal] = []
        for sig in pending_signals:
            if i <= sig.expires_at_idx:
                alive.append(sig)
            else:
                decisions.append({"timestamp": row["timestamp"], "type": "SIGNAL_DROP", "engine": sig.engine, "direction": sig.direction, "reason": "EXPIRED"})
        pending_signals = alive

        # entry
        if position is None or (not ONE_POSITION_ONLY and len(pending_signals) > 0):
            chosen: Optional[Tuple[PendingSignal, float, float, str]] = None
            ordered = sorted(pending_signals, key=lambda x: (x.priority, x.created_at_idx))
            for sig in ordered:
                ok, entry_raw, stop_raw, reason = confirm_pending_entry(df, i, sig)
                decisions.append({"timestamp": row["timestamp"], "type": "SIGNAL_CHECK", "engine": sig.engine, "direction": sig.direction, "reason": reason})
                if ok and entry_raw is not None and stop_raw is not None:
                    chosen = (sig, entry_raw, stop_raw, reason)
                    break

            if chosen is not None and position is None:
                sig, entry_raw, stop_raw, reason = chosen
                action = "BUY" if sig.direction == "LONG" else "SELL"
                entry_px = apply_execution_price(entry_raw, sig.direction, action)

                qty = 0.0
                if sig.direction == "LONG" and stop_raw < entry_px:
                    qty = calc_qty_from_risk(equity, entry_px, stop_raw)
                elif sig.direction == "SHORT" and stop_raw > entry_px:
                    qty = calc_qty_from_risk(equity, entry_px, stop_raw)

                notional = qty * entry_px
                if qty <= 0 or notional <= 0:
                    decisions.append({"timestamp": row["timestamp"], "type": "ENTRY_SKIP", "engine": sig.engine, "direction": sig.direction, "reason": "INVALID_QTY_OR_STOP"})
                else:
                    entry_fee = calc_fee(notional)
                    risk_per_unit = abs(entry_px - stop_raw)
                    position = Position(
                        engine=sig.engine,
                        direction=sig.direction,
                        entry_idx=i,
                        entry_ts=str(row["timestamp"]),
                        entry_price=float(entry_px),
                        stop_price=float(stop_raw),
                        qty=float(qty),
                        risk_r=1.0,
                        initial_risk_per_unit=float(risk_per_unit),
                        remaining_qty=float(qty),
                        fee_paid=float(entry_fee),
                        realized_pnl=-float(entry_fee),
                        notes=reason,
                    )
                    decisions.append(
                        {
                            "timestamp": row["timestamp"],
                            "type": "ENTRY",
                            "engine": sig.engine,
                            "direction": sig.direction,
                            "entry_price": float(entry_px),
                            "stop_price": float(stop_raw),
                            "qty": float(qty),
                            "notional": float(notional),
                            "reason": reason,
                        }
                    )
                    pending_signals = [s for s in pending_signals if s is not sig]

        mtm_equity = equity
        if position is not None:
            raw_mtm = float(row["close"])
            unreal = trade_pnl(position.direction, position.entry_price, raw_mtm, position.remaining_qty)
            mtm_equity += position.realized_pnl + unreal
        equity_curve.append({"timestamp": row["timestamp"], "equity": mtm_equity, "position": 0 if position is None else 1})

    if position is not None:
        row = df.iloc[-1]
        raw_px = float(row["close"])
        action = "SELL" if position.direction == "LONG" else "BUY"
        exit_px = apply_execution_price(raw_px, position.direction, action)
        pnl = trade_pnl(position.direction, position.entry_price, exit_px, position.remaining_qty)
        fee = calc_fee(position.remaining_qty * position.entry_price) + calc_fee(position.remaining_qty * exit_px)
        position.realized_pnl += pnl - fee
        position.fee_paid += fee
        position.exit_idx = len(df) - 1
        position.exit_ts = str(row["timestamp"])
        position.exit_price = float(exit_px)
        position.exit_reason = "FORCED_FINAL_BAR_EXIT"
        position.remaining_qty = 0.0
        trades.append(build_trade_snapshot(position))
        equity += position.realized_pnl
        position = None

    trades_df = pd.DataFrame(trades)
    decisions_df = pd.DataFrame(decisions)
    equity_df = pd.DataFrame(equity_curve)

    reports = build_reports(trades_df)
    summary = build_summary(trades_df, equity_df, reports)
    return trades_df, decisions_df, summary, equity_df, reports


# ============================================================
# REPORT
# ============================================================

def _agg_basic(grp: pd.DataFrame) -> pd.Series:
    gp = grp.loc[grp["realized_pnl"] > 0, "realized_pnl"].sum()
    gl = grp.loc[grp["realized_pnl"] <= 0, "realized_pnl"].sum()
    pf = (gp / abs(gl)) if gl < 0 else np.nan
    return pd.Series(
        {
            "trades": int(len(grp)),
            "wins": int((grp["realized_pnl"] > 0).sum()),
            "losses": int((grp["realized_pnl"] <= 0).sum()),
            "win_rate_pct": float((grp["realized_pnl"] > 0).mean() * 100.0) if len(grp) else 0.0,
            "net_pnl": float(grp["realized_pnl"].sum()),
            "gross_profit": float(gp),
            "gross_loss": float(gl),
            "profit_factor": float(pf) if np.isfinite(pf) else np.nan,
            "avg_pnl": float(grp["realized_pnl"].mean()) if len(grp) else 0.0,
            "avg_bars_held": float(grp["bars_held"].mean()) if len(grp) else 0.0,
            "avg_mfe_r": float(grp["mfe_r"].mean()) if len(grp) else 0.0,
            "avg_mae_r": float(grp["mae_r"].mean()) if len(grp) else 0.0,
            "fee_total": float(grp["fee_paid"].sum()),
        }
    )

def build_reports(trades_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if trades_df.empty:
        empty = pd.DataFrame()
        return {
            "monthly_report": empty,
            "engine_report": empty,
            "direction_report": empty,
            "exit_reason_report": empty,
        }

    t = trades_df.copy()
    t["entry_ts"] = pd.to_datetime(t["entry_ts"], utc=True, errors="coerce")
    t["exit_ts"] = pd.to_datetime(t["exit_ts"], utc=True, errors="coerce")
    t["exit_month"] = t["exit_ts"].dt.to_period("M").astype(str)

    monthly_report = t.groupby("exit_month", dropna=False).apply(_agg_basic).reset_index()
    engine_report = t.groupby("engine", dropna=False).apply(_agg_basic).reset_index()
    direction_report = t.groupby("direction", dropna=False).apply(_agg_basic).reset_index()
    exit_reason_report = t.groupby("exit_reason", dropna=False).apply(_agg_basic).reset_index()

    return {
        "monthly_report": monthly_report,
        "engine_report": engine_report,
        "direction_report": direction_report,
        "exit_reason_report": exit_reason_report,
    }

def build_summary(trades_df: pd.DataFrame, equity_df: pd.DataFrame, reports: Dict[str, pd.DataFrame]) -> Dict:
    if equity_df.empty:
        return {
            "bot": BOT_NAME,
            "trades": 0,
            "initial_capital": INITIAL_CAPITAL,
            "final_equity": INITIAL_CAPITAL,
            "net_pnl": 0.0,
            "return_pct": 0.0,
        }

    final_equity = float(equity_df["equity"].iloc[-1])
    net_pnl = final_equity - INITIAL_CAPITAL
    ret_pct = (net_pnl / INITIAL_CAPITAL) * 100.0 if INITIAL_CAPITAL > 0 else 0.0

    summary = {
        "bot": BOT_NAME,
        "trades": int(len(trades_df)),
        "initial_capital": float(INITIAL_CAPITAL),
        "final_equity": final_equity,
        "net_pnl": net_pnl,
        "return_pct": ret_pct,
        "entry_relaxed_mode": bool(ENTRY_RELAXED_MODE),
        "risk_per_trade_pct": float(RISK_PER_TRADE_PCT * 100.0),
        "long_engine_enabled": bool(ALLOW_LONG),
        "use_regime_filter": bool(USE_REGIME_FILTER),
        "btc_regime_filter": bool(BTC_REGIME_FILTER),
    }

    if trades_df.empty:
        return summary

    wins = int((trades_df["realized_pnl"] > 0).sum())
    losses = int((trades_df["realized_pnl"] <= 0).sum())
    gross_profit = float(trades_df.loc[trades_df["realized_pnl"] > 0, "realized_pnl"].sum())
    gross_loss = float(trades_df.loc[trades_df["realized_pnl"] <= 0, "realized_pnl"].sum())
    profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else np.inf
    avg_pnl = float(trades_df["realized_pnl"].mean())
    avg_bars = float(trades_df["bars_held"].mean())
    avg_mfe_r = float(trades_df["mfe_r"].mean())
    avg_mae_r = float(trades_df["mae_r"].mean())
    fee_total = float(trades_df["fee_paid"].sum())

    eq = equity_df["equity"].astype(float)
    roll_max = eq.cummax()
    dd = (eq / roll_max - 1.0) * 100.0
    max_dd_pct = float(dd.min()) if len(dd) else 0.0

    by_engine = {}
    for _, row in reports["engine_report"].iterrows():
        by_engine[str(row["engine"])] = {
            "trades": int(row["trades"]),
            "win_rate_pct": float(row["win_rate_pct"]),
            "net_pnl": float(row["net_pnl"]),
            "avg_pnl": float(row["avg_pnl"]),
            "avg_bars_held": float(row["avg_bars_held"]),
            "profit_factor": None if pd.isna(row["profit_factor"]) else float(row["profit_factor"]),
        }

    by_direction = {}
    for _, row in reports["direction_report"].iterrows():
        by_direction[str(row["direction"])] = {
            "trades": int(row["trades"]),
            "win_rate_pct": float(row["win_rate_pct"]),
            "net_pnl": float(row["net_pnl"]),
            "profit_factor": None if pd.isna(row["profit_factor"]) else float(row["profit_factor"]),
        }

    by_exit_reason = {}
    for _, row in reports["exit_reason_report"].iterrows():
        by_exit_reason[str(row["exit_reason"])] = {
            "trades": int(row["trades"]),
            "net_pnl": float(row["net_pnl"]),
            "win_rate_pct": float(row["win_rate_pct"]),
        }

    summary.update(
        {
            "wins": wins,
            "losses": losses,
            "win_rate_pct": (wins / len(trades_df)) * 100.0 if len(trades_df) else 0.0,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": float(profit_factor) if np.isfinite(profit_factor) else None,
            "avg_pnl": avg_pnl,
            "avg_bars_held": avg_bars,
            "avg_mfe_r": avg_mfe_r,
            "avg_mae_r": avg_mae_r,
            "fee_total": fee_total,
            "max_drawdown_pct": max_dd_pct,
            "by_engine": by_engine,
            "by_direction": by_direction,
            "by_exit_reason": by_exit_reason,
        }
    )
    return summary

def print_summary(summary: Dict) -> None:
    print("\n[SUMMARY]")
    for k, v in summary.items():
        if k in {"by_engine", "by_direction", "by_exit_reason"}:
            print(f"{k}:")
            for ek, ev in v.items():
                print(f"  - {ek}: {ev}")
        else:
            print(f"{k}: {v}")

def maybe_plot(equity_df: pd.DataFrame, trades_df: pd.DataFrame, out_dir: str) -> None:
    if not ETH_PLOT:
        return
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        print("matplotlib 미설치 -> 플롯 스킵")
        return

    if not equity_df.empty:
        plt.figure(figsize=(12, 5))
        plt.plot(pd.to_datetime(equity_df["timestamp"]), equity_df["equity"])
        plt.title(f"{BOT_NAME} Equity Curve")
        plt.xlabel("Time")
        plt.ylabel("Equity")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "equity_curve.png"), dpi=160)
        plt.close()

    if not trades_df.empty:
        pnl = trades_df["realized_pnl"].astype(float).reset_index(drop=True)
        plt.figure(figsize=(12, 5))
        plt.plot(np.arange(len(pnl)), pnl.cumsum())
        plt.title(f"{BOT_NAME} Trade-by-Trade Cumulative PnL")
        plt.xlabel("Trade #")
        plt.ylabel("Cum PnL")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "trade_cum_pnl.png"), dpi=160)
        plt.close()


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ensure_dir(ETH_OUTPUT_DIR)

    try:
        csv_path = ETH_BACKTEST_CSV.strip()
        csv_exists = bool(csv_path) and os.path.exists(csv_path)
        should_fallback_download = ETH_AUTO_DOWNLOAD or ETH_FALLBACK_DOWNLOAD_IF_CSV_MISSING

        if csv_exists:
            print(f"Loading CSV: {csv_path}")
            raw_df = load_ohlcv_csv(csv_path)
        else:
            if csv_path:
                print(f"CSV not found -> fallback download mode | missing_path={csv_path}")
            else:
                print("ETH_BACKTEST_CSV 미지정 -> fallback download mode")

            if not should_fallback_download:
                raise FileNotFoundError(
                    "ETH_BACKTEST_CSV가 비어 있거나 파일이 없고, fallback download도 비활성화되어 있습니다. "
                    "ETH_BACKTEST_CSV 경로를 올바르게 지정하거나 ETH_FALLBACK_DOWNLOAD_IF_CSV_MISSING=1 로 실행하세요."
                )

            dl_path = os.path.join(ETH_OUTPUT_DIR, f"ethusdt_{ETH_TIMEFRAME}_{ETH_DOWNLOAD_DAYS}d_downloaded.csv")
            print(f"Downloading Binance OHLCV | symbol={ETH_SYMBOL} | timeframe={ETH_TIMEFRAME} | days={ETH_DOWNLOAD_DAYS}")
            raw_df = maybe_download_binance_ohlcv(ETH_SYMBOL, ETH_TIMEFRAME, ETH_DOWNLOAD_DAYS, dl_path)
            print(f"Downloaded rows={len(raw_df)} -> {dl_path}")

        df = add_indicators(raw_df).dropna().reset_index(drop=True)
        df = attach_regime_filters(df)
        df = maybe_attach_btc_regime(df)
        print(f"Prepared rows={len(df)} | {df['timestamp'].min()} -> {df['timestamp'].max()}")

        trades_df, decisions_df, summary, equity_df, reports = backtest(df)

        trades_path = os.path.join(ETH_OUTPUT_DIR, "trades.csv")
        decisions_path = os.path.join(ETH_OUTPUT_DIR, "decisions.csv")
        equity_path = os.path.join(ETH_OUTPUT_DIR, "equity_curve.csv")
        summary_path = os.path.join(ETH_OUTPUT_DIR, "summary.json")
        enriched_path = os.path.join(ETH_OUTPUT_DIR, "enriched_ohlcv.csv")
        monthly_path = os.path.join(ETH_OUTPUT_DIR, "monthly_report.csv")
        engine_path = os.path.join(ETH_OUTPUT_DIR, "engine_report.csv")
        direction_path = os.path.join(ETH_OUTPUT_DIR, "direction_report.csv")
        exit_reason_path = os.path.join(ETH_OUTPUT_DIR, "exit_reason_report.csv")

        trades_df.to_csv(trades_path, index=False)
        decisions_df.to_csv(decisions_path, index=False)
        equity_df.to_csv(equity_path, index=False)
        df.to_csv(enriched_path, index=False)

        if not reports["monthly_report"].empty:
            reports["monthly_report"].to_csv(monthly_path, index=False)
        else:
            pd.DataFrame().to_csv(monthly_path, index=False)

        if not reports["engine_report"].empty:
            reports["engine_report"].to_csv(engine_path, index=False)
        else:
            pd.DataFrame().to_csv(engine_path, index=False)

        if not reports["direction_report"].empty:
            reports["direction_report"].to_csv(direction_path, index=False)
        else:
            pd.DataFrame().to_csv(direction_path, index=False)

        if not reports["exit_reason_report"].empty:
            reports["exit_reason_report"].to_csv(exit_reason_path, index=False)
        else:
            pd.DataFrame().to_csv(exit_reason_path, index=False)

        safe_json_dump(summary, summary_path)
        maybe_plot(equity_df, trades_df, ETH_OUTPUT_DIR)

        print_summary(summary)
        print("\n[OUTPUT FILES]")
        for p in [trades_path, decisions_path, equity_path, summary_path, enriched_path, monthly_path, engine_path, direction_path, exit_reason_path]:
            print(p)

    except Exception as e:
        err = {"error_type": type(e).__name__, "message": str(e), "traceback": traceback.format_exc()}
        err_path = os.path.join(ETH_OUTPUT_DIR, "error.json")
        safe_json_dump(err, err_path)
        print("\n[ERROR]")
        print(type(e).__name__, str(e))
        print(traceback.format_exc())
        print(f"Saved error to: {err_path}")
        raise

if __name__ == "__main__":
    main()
