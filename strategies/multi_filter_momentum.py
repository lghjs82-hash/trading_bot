"""
Multi-Filter Momentum Strategy (MFM)
====================================
거래 이력 분석 결과를 반영한 전략:

문제점 (분석 결과):
- 승률 39% (목표: 55% 이상)
- Profit Factor 0.69 (목표: 1.5 이상)
- 과도한 거래 (1일 33개 체결) -> 노이즈에 반응

해결책:
1. 진입 조건 강화: 3개 이상의 독립적인 지표가 동시에 합의해야만 진입 (AND 조건)
2. 추세 필터: 200 EMA 위/아래로 롱/숏 방향 제한
3. 변동성 필터: ATR이 충분히 클 때만 거래 (저변동성 노이즈 회피)
4. 볼륨 확인: 평균 거래량보다 높을 때만 진입 (기관 참여 확인)
5. 보수적 손절 위치: ATR 기반 2.0x 손절로 조기 중단 방지
6. 목표가/손절 비율(R:R): 최소 2:1 보장되는 경우만 진입
"""

import pandas as pd
import numpy as np
import config
from .base_strategy import BaseStrategy
from typing import List, Dict


class MultiFilterMomentumStrategy(BaseStrategy):
    """
    다중 필터 모멘텀 전략.
    낮은 승률 문제를 해결하기 위해 3개 이상의 기술적 조건이
    동시에 충족될 때만 진입하는 보수적인 전략.
    
    핵심 진입 조건 (LONG):
      - 200 EMA 위 (상승 추세)
      - EMA 10 > EMA 50 (단기 모멘텀)
      - RSI 45~65 사이 (과열/과매도 아님 - 추세 초기)
      - 현재 볼륨 > 1.5x 평균 (확인된 움직임)
      - 전 캔들 대비 양봉 마감 (방향성 확인)
      
    핵심 진입 조건 (SHORT):
      - 200 EMA 아래
      - EMA 10 < EMA 50
      - RSI 35~55 사이
      - 볼륨 > 1.5x 평균
      - 전 캔들 대비 음봉 마감
    
    손절/목표:
      - 손절: ATR * 1.5 (진입가 기준)
      - 목표거리 >= 손절거리 * 2.0 (R:R 2:1 강제)
    """

    def __init__(self):
        super().__init__("MultiFilterMomentum")
        self.EMA_FAST = 10
        self.EMA_MID = 50
        self.EMA_SLOW = 200
        self.RSI_LEN = 14
        self.ATR_LEN = 14
        self.VOL_MA_LEN = 20
        self.VOL_MA_LEN = 20
        self.ATR_STOP_MULT = 1.8      # 손절: ATR 1.8배 (노이즈 방지 강화)
        self.ATR_TP_MULT_LONG = 3.6   # 롱 목표: ATR 3.6배 (R:R 2:1)
        self.ATR_TP_MULT_SHORT = 2.8  # 숏 목표: ATR 2.8배 (빠른 익절)
        self.VOL_THRESHOLD = 1.2      # 볼륨 필터 완화 (1.5 -> 1.2)
        self.RSI_LONG_LOW = 50        # 롱: 모멘텀 시작 (45 -> 50)
        self.RSI_LONG_HIGH = 70       # 롱 상한 (68 -> 70)
        self.RSI_SHORT_LOW = 30       # 숏 하한 (32 -> 30)
        self.RSI_SHORT_HIGH = 50      # 숏: 하락 시작 (55 -> 50)

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
        out["rsi"] = self.rsi(out["close"], self.RSI_LEN)
        out["atr"] = self.atr(out, self.ATR_LEN)
        out["vol_ma"] = out["volume"].rolling(self.VOL_MA_LEN, min_periods=self.VOL_MA_LEN).mean()
        # 캔들 방향성
        out["bullish_candle"] = out["close"] > out["open"]
        out["bearish_candle"] = out["close"] < out["open"]
        return out

    def detect_signals(self, df: pd.DataFrame) -> List[Dict]:
        signals = []
        # 200 EMA를 쓰므로 최소 210개 이상의 캔들 필요
        min_bars = self.EMA_SLOW + 10
        if len(df) < min_bars:
            return signals

        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # NaN 안전 체크
        for col in ["ema_fast", "ema_mid", "ema_slow", "rsi", "atr", "vol_ma"]:
            if pd.isna(row.get(col)):
                return signals

        atr_val = float(row["atr"])
        vol_ok = row["volume"] >= self.VOL_THRESHOLD * row["vol_ma"]

        # ── LONG 진입 ──────────────────────────────────────────────
        if config.ALLOW_LONG:
            trend_up = (row["ema_fast"] > row["ema_mid"]) and (row["close"] > row["ema_slow"])
            rsi_ok   = self.RSI_LONG_LOW <= row["rsi"] <= self.RSI_LONG_HIGH
            candle_ok = bool(row["bullish_candle"])
            momentum_up = row["ema_fast"] > prev["ema_fast"]

            score = sum([trend_up, rsi_ok, candle_ok, vol_ok, momentum_up])

            if score >= 4:
                stop  = float(row["close"]) - atr_val * self.ATR_STOP_MULT
                tp    = float(row["close"]) + atr_val * self.ATR_TP_MULT_LONG
                signals.append({
                    "side": "LONG",
                    "entry_price": float(row["close"]),
                    "stop_price": stop,
                    "take_profit": tp,
                    "reason": f"MFM_LONG (score={score}/5: trend={trend_up}, rsi={row['rsi']:.1f}, vol={vol_ok}, mom={momentum_up})"
                })

        # ── SHORT 진입 ─────────────────────────────────────────────
        if config.ALLOW_SHORT and not signals:
            trend_dn  = (row["ema_fast"] < row["ema_mid"]) and (row["close"] < row["ema_slow"])
            rsi_ok    = self.RSI_SHORT_LOW <= row["rsi"] <= self.RSI_SHORT_HIGH
            candle_ok = bool(row["bearish_candle"])
            momentum_dn = row["ema_fast"] < prev["ema_fast"]

            score = sum([trend_dn, rsi_ok, candle_ok, vol_ok, momentum_dn])

            if score >= 4:
                stop = float(row["close"]) + atr_val * self.ATR_STOP_MULT
                tp   = float(row["close"]) - atr_val * self.ATR_TP_MULT_SHORT
                signals.append({
                    "side": "SHORT",
                    "entry_price": float(row["close"]),
                    "stop_price": stop,
                    "take_profit": tp,
                    "reason": f"MFM_SHORT (score={score}/5: trend={trend_dn}, rsi={row['rsi']:.1f}, vol={vol_ok}, mom={momentum_dn})"
                })

        return signals

    def check_exit_condition(self, df: pd.DataFrame, current_position_side: str) -> bool:
        """
        추세가 반전되면 조기 청산:
        - LONG: EMA Fast가 EMA Mid 아래로 교차하면 청산
        - SHORT: EMA Fast가 EMA Mid 위로 교차하면 청산
        """
        if len(df) < 2:
            return False

        i = len(df) - 1
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        for col in ["ema_fast", "ema_mid"]:
            if pd.isna(row.get(col)) or pd.isna(prev.get(col)):
                return False

        if current_position_side == "LONG":
            # Fast EMA 가 Mid EMA 아래로 교차 → 추세 반전 → 청산
            return (prev["ema_fast"] >= prev["ema_mid"]) and (row["ema_fast"] < row["ema_mid"])

        elif current_position_side == "SHORT":
            # Fast EMA 가 Mid EMA 위로 교차 → 청산
            return (prev["ema_fast"] <= prev["ema_mid"]) and (row["ema_fast"] > row["ema_mid"])

        return False
