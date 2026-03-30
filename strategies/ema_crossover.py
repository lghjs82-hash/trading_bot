import pandas as pd
import config
from .base_strategy import BaseStrategy
from typing import List, Dict

class EMACrossoverStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("EMACrossover")

    def ema(self, series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False, min_periods=span).mean()

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["ema_fast"] = self.ema(out["close"], config.EMA_FAST_LEN)
        out["ema_slow"] = self.ema(out["close"], config.EMA_SLOW_LEN)
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        if len(df) < max(config.EMA_FAST_LEN, config.EMA_SLOW_LEN): 
            return []
            
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i-1]
        
        # Fast crosses over Slow -> LONG
        if prev["ema_fast"] <= prev["ema_slow"] and row["ema_fast"] > row["ema_slow"]:
            signals.append({
                "side": "LONG",
                "entry_price": float(row["close"]),
                "stop_price": float(row["low"] * 0.99), # Simple 1% stop
                "reason": "EMA_CROSS_LONG"
            })
            
        # Fast crosses under Slow -> SHORT
        elif prev["ema_fast"] >= prev["ema_slow"] and row["ema_fast"] < row["ema_slow"]:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row["close"]),
                "stop_price": float(row["high"] * 1.01), # Simple 1% stop
                "reason": "EMA_CROSS_SHORT"
            })
            
        return signals
