import pandas as pd
import config
from .base_strategy import BaseStrategy
from typing import List, Dict

class MACDTrendStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("MACDTrend")

    def ema(self, series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False, min_periods=span).mean()

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # Calculate MACD
        fast_ema = self.ema(out["close"], config.MACD_FAST)
        slow_ema = self.ema(out["close"], config.MACD_SLOW)
        out["macd_line"] = fast_ema - slow_ema
        out["macd_signal"] = self.ema(out["macd_line"], config.MACD_SIGNAL)
        out["macd_hist"] = out["macd_line"] - out["macd_signal"]
        
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        if len(df) < config.MACD_SLOW + config.MACD_SIGNAL:
            return []
            
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i-1]
        
        # MACD Line crosses above Signal Line -> LONG
        if prev["macd_line"] <= prev["macd_signal"] and row["macd_line"] > row["macd_signal"]:
            signals.append({
                "side": "LONG",
                "entry_price": float(row["close"]),
                "stop_price": float(row["low"] * 0.99),
                "reason": "MACD_BULLISH_CROSS"
            })
            
        # MACD Line crosses below Signal Line -> SHORT
        elif prev["macd_line"] >= prev["macd_signal"] and row["macd_line"] < row["macd_signal"]:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row["close"]),
                "stop_price": float(row["high"] * 1.01),
                "reason": "MACD_BEARISH_CROSS"
            })
            
        return signals
