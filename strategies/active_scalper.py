import pandas as pd
import numpy as np
import config
from .base_strategy import BaseStrategy
from typing import List, Dict

class ActiveScalperStrategy(BaseStrategy):
    """
    Highly active scalping strategy using Bollinger Bands and RSI.
    Detects fast mean-reversions at the extremes of the bands.
    """
    def __init__(self):
        super().__init__("ActiveScalper")
        self.BB_LEN = 20
        self.BB_STD = 2.0
        self.RSI_LEN = getattr(config, 'RSI_LEN', 14)
        self.RSI_OVERBOUGHT = getattr(config, 'RSI_OVERBOUGHT', 70)
        self.RSI_OVERSOLD = getattr(config, 'RSI_OVERSOLD', 30)
        self.ATR_LEN = 14
        self.STOP_LOSS_ATR_MULT = 1.5

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        
        # 1. Bollinger Bands
        out['bb_mid'] = out['close'].rolling(self.BB_LEN).mean()
        out['bb_std'] = out['close'].rolling(self.BB_LEN).std()
        out['bb_upper'] = out['bb_mid'] + (out['bb_std'] * self.BB_STD)
        out['bb_lower'] = out['bb_mid'] - (out['bb_std'] * self.BB_STD)
        
        # 2. RSI Calculation
        delta = out['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.RSI_LEN).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.RSI_LEN).mean()
        rs = gain / loss
        out['rsi'] = 100 - (100 / (1 + rs))
        
        # 3. ATR (Average True Range) for tight stop losses
        prev_close = out['close'].shift(1)
        tr = pd.concat([
            (out['high'] - out['low']).abs(),
            (out['high'] - prev_close).abs(),
            (out['low'] - prev_close).abs(),
        ], axis=1).max(axis=1)
        out['atr'] = tr.rolling(self.ATR_LEN).mean()
        
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        if len(df) < 50:
            return []
            
        signals = []
        i = len(df) - 1
        row = df.iloc[i]
        
        # Ensure values are not NaN before evaluating logic
        if pd.isna(row['bb_lower']) or pd.isna(row['rsi']):
            return []
            
        # Long condition: Price drops below lower BB AND RSI is deeply oversold
        long_condition = (row['close'] < row['bb_lower']) and (row['rsi'] < self.RSI_OVERSOLD)
        
        # Short condition: Price pumps above upper BB AND RSI is highly overbought
        short_condition = (row['close'] > row['bb_upper']) and (row['rsi'] > self.RSI_OVERBOUGHT)
        
        # Signal Generation
        if config.ALLOW_LONG and long_condition:
            signals.append({
                "side": "LONG",
                "entry_price": float(row['close']),
                "stop_price": float(row['close'] - (row['atr'] * self.STOP_LOSS_ATR_MULT)),
                "reason": f"BB_LOWER_BOUNCE (RSI: {row['rsi']:.1f})"
            })
            
        elif config.ALLOW_SHORT and short_condition:
            signals.append({
                "side": "SHORT",
                "entry_price": float(row['close']),
                "stop_price": float(row['close'] + (row['atr'] * self.STOP_LOSS_ATR_MULT)),
                "reason": f"BB_UPPER_REJECT (RSI: {row['rsi']:.1f})"
            })
            
        return signals

    def check_exit_condition(self, df: pd.DataFrame, current_position_side: str) -> bool:
        """
        ActiveScalper Exit: Close when price returns to the mean (Bollinger Band middle line).
        """
        if len(df) < 2:
            return False
            
        i = len(df) - 1
        row = df.iloc[i]
        
        if pd.isna(row['bb_mid']):
            return False
            
        if current_position_side == "LONG":
            # Exit if price hits or exceeds middle band
            return row['close'] >= row['bb_mid']
        elif current_position_side == "SHORT":
            # Exit if price hits or drops below middle band
            return row['close'] <= row['bb_mid']
            
        return False
