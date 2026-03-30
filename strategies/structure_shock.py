import pandas as pd
import numpy as np
import config
from .base_strategy import BaseStrategy
from typing import List, Dict, Optional

class StructureShockStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("StructureShock")
        # Strategy Parameters (Copied from v17.3)
        self.ATR_LEN = 14
        self.EMA_FAST = 10
        self.EMA_MID = 50
        self.EMA_SLOW = 99
        self.VOL_MA_LEN = 20
        self.RSI_FAST_LEN = 6
        self.RSI_LEN = 14
        self.STRUCTURE_LOOKBACK = 12
        self.RECENT_IMPULSE_LOOKBACK = 20
        self.BREAKDOWN_IMPULSE_ATR = 1.25
        self.BREAKDOWN_VOL_MULT = 1.20
        self.BREAKDOWN_STOP_ATR = 0.15

    def ema(self, series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False, min_periods=span).mean()

    def rsi(self, series: pd.Series, length: int) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
        avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs)).fillna(50.0)

    def atr(self, df: pd.DataFrame, length: int) -> pd.Series:
        prev_close = df["close"].shift(1)
        tr = pd.concat([
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["ema_fast"] = self.ema(out["close"], self.EMA_FAST)
        out["ema_mid"] = self.ema(out["close"], self.EMA_MID)
        out["ema_slow"] = self.ema(out["close"], self.EMA_SLOW)
        out["atr"] = self.atr(out, self.ATR_LEN)
        out["vol_ma"] = out["volume"].rolling(self.VOL_MA_LEN, min_periods=self.VOL_MA_LEN).mean()
        out["rsi_fast"] = self.rsi(out["close"], self.RSI_FAST_LEN)
        out["rsi"] = self.rsi(out["close"], self.RSI_LEN)
        out["body"] = (out["close"] - out["open"]).abs()
        out["range"] = (out["high"] - out["low"]).replace(0, np.nan)
        out["close_pos_in_bar"] = ((out["close"] - out["low"]) / out["range"]).fillna(0.5)
        out["hh_struct"] = out["high"].rolling(self.STRUCTURE_LOOKBACK).max().shift(1)
        out["ll_struct"] = out["low"].rolling(self.STRUCTURE_LOOKBACK).min().shift(1)
        
        # Trend Bias
        out["trend_bias"] = np.select(
            [(out["ema_fast"] > out["ema_mid"]) & (out["ema_mid"] > out["ema_slow"]),
             (out["ema_fast"] < out["ema_mid"]) & (out["ema_mid"] < out["ema_slow"])],
            [1, -1], default=0
        )
        return out

    def detect_signals(self, df: pd.DataFrame) -> list:
        """Detect entry signals from the latest bar"""
        signals = []
        if len(df) < self.EMA_SLOW + 5:
            return signals

        i = len(df) - 1
        row = df.iloc[i]

        # Safety: skip if essential indicators are NaN
        if pd.isna(row.get("ema_fast")) or pd.isna(row.get("atr")) or pd.isna(row.get("vol_ma")):
            return signals

        # 1. Bullish Impulse Breakout (LONG)
        if config.ALLOW_LONG:
            bullish_structure = (row["ema_fast"] > row["ema_mid"]) or (row["close"] > row["ema_mid"] > row["ema_slow"])
            impulse = (row["close"] - row["open"]) >= self.BREAKDOWN_IMPULSE_ATR * row["atr"]
            vol_ok = row["volume"] >= self.BREAKDOWN_VOL_MULT * row["vol_ma"]
            broke_structure = row["close"] > row["hh_struct"]
            closed_strong = row["close_pos_in_bar"] >= 0.70

            if bullish_structure and impulse and vol_ok and broke_structure and closed_strong:
                signals.append({
                    "side": "LONG",
                    "entry_price": float(row["close"]),
                    "stop_price": float(row["low"] - self.BREAKDOWN_STOP_ATR * row["atr"]),
                    "reason": "BREAKOUT_IMPULSE_LONG"
                })

        # 2. Breakdown Impulse Short (SHORT)
        if config.ALLOW_SHORT and not signals:
            bearish_structure = (row["ema_fast"] < row["ema_mid"]) or (row["close"] < row["ema_mid"] < row["ema_slow"])
            impulse = (row["open"] - row["close"]) >= self.BREAKDOWN_IMPULSE_ATR * row["atr"]
            vol_ok = row["volume"] >= self.BREAKDOWN_VOL_MULT * row["vol_ma"]
            broke_structure = row["close"] < row["ll_struct"]
            closed_weak = row["close_pos_in_bar"] <= 0.30

            if bearish_structure and impulse and vol_ok and broke_structure and closed_weak:
                signals.append({
                    "side": "SHORT",
                    "entry_price": float(row["close"]),
                    "stop_price": float(row["high"] + self.BREAKDOWN_STOP_ATR * row["atr"]),
                    "reason": "BREAKDOWN_IMPULSE_SHORT"
                })

        return signals
