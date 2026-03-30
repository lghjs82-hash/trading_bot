import pandas as pd
import numpy as np
import config
from .base_strategy import BaseStrategy
from typing import List, Dict, Optional

class IntradayTrendStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("IntradayTrend")
        # Indicators: 15m for setup, 5m for trigger (Simplified for single timeframe for now)
        self.EMA_FAST = 20
        self.EMA_SLOW = 50
        self.ATR_LEN = 14
        self.FVG_MIN_GAP_PCT = 0.10
        self.TP1_R = 1.4

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["ema20"] = out["close"].ewm(span=self.EMA_FAST, adjust=False).mean()
        out["ema50"] = out["close"].ewm(span=self.EMA_SLOW, adjust=False).mean()
        
        # ATR
        prev_close = out["close"].shift(1)
        tr = pd.concat([
            (out["high"] - out["low"]).abs(),
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ], axis=1).max(axis=1)
        out["atr14"] = tr.rolling(self.ATR_LEN).mean()
        
        # Bull FVG Detection
        out["bull_fvg"] = (
            (out["high"].shift(2) < out["low"]) & 
            (((out["low"] - out["high"].shift(2)) / out["high"].shift(2)) * 100 >= self.FVG_MIN_GAP_PCT)
        ).astype(int)
        
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        if len(df) < 50: return []
        
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        
        # 1. Bull FVG Revisit Long (Respect ALLOW_LONG)
        if config.ALLOW_LONG and row["bull_fvg"] == 1:
            signals.append({
                "side": "LONG",
                "entry_price": float(row["close"]),
                "stop_price": float(row["low"] - row["atr14"]),
                "reason": "BULL_FVG_DETECTED"
            })
            
        # 2. Bear FVG Revisit Short (Respect ALLOW_SHORT)
        # Adding Bear FVG detection logic here for symmetry
        bear_fvg = (df["low"].shift(2) > df["high"]) & \
                   (((df["low"].shift(2) - df["high"]) / df["high"]) * 100 >= self.FVG_MIN_GAP_PCT)
        
        if config.ALLOW_SHORT and not signals and bear_fvg.iloc[i]:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row["close"]),
                "stop_price": float(row["high"] + row["atr14"]),
                "reason": "BEAR_FVG_DETECTED"
            })
            
        return signals
