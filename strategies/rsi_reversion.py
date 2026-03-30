import pandas as pd
import numpy as np
import config
from .base_strategy import BaseStrategy
from typing import List, Dict

class RSIReversionStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("RSIReversion")

    def rsi(self, series: pd.Series, length: int) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
        avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs)).fillna(50.0)

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["rsi"] = self.rsi(out["close"], config.RSI_LEN)
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        if len(df) < config.RSI_LEN + 1: 
            return []
            
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i-1]
        
        # RSI crosses back above Oversold -> Reversion LONG
        if prev["rsi"] <= config.RSI_OVERSOLD and row["rsi"] > config.RSI_OVERSOLD:
            signals.append({
                "side": "LONG",
                "entry_price": float(row["close"]),
                "stop_price": float(row["low"] * 0.99),
                "reason": "RSI_OVERSOLD_REVERSION"
            })
            
        # RSI crosses back below Overbought -> Reversion SHORT
        elif prev["rsi"] >= config.RSI_OVERBOUGHT and row["rsi"] < config.RSI_OVERBOUGHT:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row["close"]),
                "stop_price": float(row["high"] * 1.01),
                "reason": "RSI_OVERBOUGHT_REVERSION"
            })
            
        return signals
