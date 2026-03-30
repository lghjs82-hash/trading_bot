import pandas as pd
import config
from .base_strategy import BaseStrategy
from typing import List, Dict

class CustomStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("CustomStrategy")

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # Example Custom Logic: Simple moving average using CUSTOM_PARAM_1
        ma_len = max(2, int(config.CUSTOM_PARAM_1))
        out["custom_ma"] = out["close"].rolling(window=ma_len).mean()
        
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        ma_len = max(2, int(config.CUSTOM_PARAM_1))
        if len(df) < ma_len:
            return []
            
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i-1]
        
        # Example Custom Condition using CUSTOM_PARAM_2 as a static threshold or multiplier
        threshold = config.CUSTOM_PARAM_2
        
        # Custom LONG: Price crosses above MA + Threshold padding
        if prev["close"] <= prev["custom_ma"] and row["close"] > row["custom_ma"] + threshold:
            signals.append({
                "side": "LONG",
                "entry_price": float(row["close"]),
                "stop_price": float(row["low"] * 0.99),
                "reason": "CUSTOM_MA_BREAKOUT_LONG"
            })
            
        # Custom SHORT: Price crosses below MA - Threshold padding
        elif prev["close"] >= prev["custom_ma"] and row["close"] < row["custom_ma"] - threshold:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row["close"]),
                "stop_price": float(row["high"] * 1.01),
                "reason": "CUSTOM_MA_BREAKOUT_SHORT"
            })
            
        return signals
