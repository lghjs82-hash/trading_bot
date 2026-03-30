from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, List, Dict

class BaseStrategy(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators required for the strategy"""
        pass

    @abstractmethod
    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        """Detect entry signals from the latest data"""
        pass

    def check_exit_condition(self, df: pd.DataFrame, current_position_side: str) -> bool:
        """
        Check if the current position should be closed based on technical indicators.
        Override this in subclasses for indicator-based exits (e.g., BB Middle cross).
        """
        return False
