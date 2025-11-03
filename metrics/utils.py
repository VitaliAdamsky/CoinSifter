# metrics/utils.py

import logging
import pandas as pd
import numpy as np
import pandas_ta_classic as ta
from scipy.stats import linregress, entropy, stats # (ИЗМЕНЕНИЕ) Добавлен 'stats'
from scipy.signal import argrelextrema

log = logging.getLogger(__name__)


def get_movement_efficiency(close_prices, window=100):
    """
    Calculate movement efficiency ratio.
    """
    if len(close_prices) < window:
        return np.nan
    
    try:
        series = close_prices.tail(window)
        
        net_change = series.iloc[-1] - series.iloc[0]
        sum_of_movements = np.abs(series.diff().dropna()).sum()
        
        if sum_of_movements == 0:
            return 0.0
        
        return net_change / sum_of_movements
    
    except Exception as e:
        log.warning(f"Error calculating movement efficiency: {e}")
        return np.nan


def get_fractal_dimension(close_prices):
    """
    Calculate fractal dimension of price series.
    """
    try:
        prices = np.log(close_prices.astype(float))
        n = len(prices)
        
        if n < 100:
            return np.nan
        
        lg = np.zeros(n)
        lg[0] = 0
        
        for k in range(1, n):
            price_diff = prices.iloc[k] - prices.iloc[k-1]
            lg[k] = lg[k-1] + price_diff
        
        L = np.abs(lg).mean()
        
        if L == 0:
            return np.nan
        
        fd = np.log(n) / (np.log(n) + np.log(1 / L))
        return fd
    
    except Exception as e:
        log.warning(f"Error calculating fractal dimension: {e}")
        return np.nan


def get_swing_r_squared(ohlc_df, window=5):
    """
    Calculate R² for swing quality.
    """
    try:
        highs_idx = argrelextrema(ohlc_df['high'].values, np.greater, order=window)[0]
        lows_idx = argrelextrema(ohlc_df['low'].values, np.less, order=window)[0]
        
        highs = ohlc_df['high'].iloc[highs_idx]
        lows = ohlc_df['low'].iloc[lows_idx]
        
        if len(highs) < 2 or len(lows) < 2:
            return np.nan
        
        y_h = highs.iloc[1:].values
        x_h = highs.iloc[:-1].values
        
        if len(y_h) < 2:
            return np.nan
        
        r2_high = linregress(x_h, y_h)[2]**2
        
        y_l = lows.iloc[1:].values
        x_l = lows.iloc[:-1].values
        
        if len(y_l) < 2:
            return np.nan
        
        r2_low = linregress(x_l, y_l)[2]**2
        
        return (r2_high + r2_low) / 2
    
    except Exception as e:
        log.warning(f"Error calculating swing R²: {e}")
        return np.nan


# ============================================================================
# === Метрики, перенесенные из calculator.py ===
# ============================================================================

def calculate_smoothness_index(close_prices, window=20):
    """
    (ПЕРЕНЕСЕНО ИЗ CALCULATOR.PY)
    Calculate smoothness index as 1 - (RMSD / SMA).
    """
    try:
        if len(close_prices) < window:
            return np.nan
        
        sma = close_prices.rolling(window=window, min_periods=window).mean()
        squared_deviations = (close_prices - sma) ** 2
        rmsd = np.sqrt(squared_deviations.rolling(window=window, min_periods=window).mean())
        smoothness_index = 1 - (rmsd / (sma + 1e-10))
        result = smoothness_index.iloc[-1]
        
        return float(result) if not pd.isna(result) else np.nan
    
    except Exception as e:
        log.debug(f"Error calculating smoothness index: {e}")
        return np.nan

def calculate_skewness_kurtosis(close_prices, window=50):
    """
    (ПЕРЕНЕСЕНО ИЗ CALCULATOR.PY)
    Calculate rolling skewness and kurtosis of log returns.
    """
    try:
        if len(close_prices) < window + 1:
            return {'skewness': np.nan, 'kurtosis': np.nan}
        
        log_returns = np.log(close_prices / close_prices.shift(1)).dropna()
        
        if len(log_returns) < window:
            return {'skewness': np.nan, 'kurtosis': np.nan}
        
        rolling_skew = log_returns.rolling(window=window, min_periods=window).apply(
            lambda x: stats.skew(x, bias=False), raw=True
        )
        skewness = rolling_skew.iloc[-1]
        
        rolling_kurt = log_returns.rolling(window=window, min_periods=window).apply(
            lambda x: stats.kurtosis(x, fisher=False, bias=False), raw=True
        )
        kurtosis = rolling_kurt.iloc[-1]
        
        return {
            'skewness': float(skewness) if not pd.isna(skewness) else np.nan,
            'kurtosis': float(kurtosis) if not pd.isna(kurtosis) else np.nan
        }
    
    except Exception as e:
        log.debug(f"Error calculating skewness/kurtosis: {e}")
        return {'skewness': np.nan, 'kurtosis': np.nan}

# ============================================================================
# === "Эталонные" версии метрик (ранее дублированные) ===
# ============================================================================

def calculate_candle_jagginess(ohlc_df, window=20):
    """
    (ЭТАЛОННАЯ ВЕРСИЯ)
    Calculate the average Candle Jagginess Index (CJI) over a window.
    """
    if not all(col in ohlc_df.columns for col in ['high', 'low', 'open', 'close']):
        return np.nan
    
    if len(ohlc_df) < window:
        return np.nan
    
    try:
        candle_range = ohlc_df['high'] - ohlc_df['low']
        candle_body = (ohlc_df['close'] - ohlc_df['open']).abs()
        total_wick = candle_range - candle_body
        jagginess_index = total_wick / (candle_range + 1e-10)
        
        avg_jagginess = jagginess_index.rolling(window=window).mean().iloc[-1]
        
        return float(avg_jagginess) if pd.notna(avg_jagginess) else np.nan
    
    except Exception as e:
        log.warning(f"Error calculating Candle Jagginess Index: {e}")
        return np.nan

def calculate_movement_intensity(ohlc_df, window=14):
    """
    (ЭТАЛОNНАЯ ВЕРСИЯ)
    Calculate movement intensity ratio.
    """
    try:
        # (ИСПРАВЛЕНИЕ) Версия в utils.py была сломана
        # (она использовала ohlc_df['close'] - ohlc_df['open']).
        # Мы используем версию из calculator.py (ewm(abs(returns))),
        # так как она была более продвинутой.
        
        if len(ohlc_df) < window + 1:
            return np.nan
        
        close_prices = ohlc_df.get('close')
        if close_prices is None or close_prices.empty:
            return np.nan
        
        abs_returns = abs(close_prices.diff().dropna())
        
        if len(abs_returns) < window:
            return np.nan
        
        ema_intensity = abs_returns.ewm(span=window, adjust=False, min_periods=window).mean()
        result = ema_intensity.iloc[-1]
        
        return float(result) if not pd.isna(result) else np.nan
    
    except Exception as e:
        log.warning(f"Error calculating movement intensity: {e}")
        return np.nan


def calculate_atr_stability(ohlc_df, atr_window=14, stability_window=14):
    """
    (ЭТАЛОННАЯ ВЕРСИЯ)
    Calculate ATR stability using coefficient of variation.
    """
    try:
        high = ohlc_df.get('high')
        low = ohlc_df.get('low')
        close = ohlc_df.get('close')
        
        if high is None or low is None or close is None:
            return np.nan
            
        atr = ta.atr(high, low, close, length=atr_window).dropna()
        
        if len(atr) < stability_window:
            return np.nan
        
        # (ИСПРАВЛЕНИЕ) Версия в utils.py была сломана
        # (она брала .mean() от rolling CV).
        # Мы используем версию из calculator.py (CV от recent_atr).
        
        recent_atr = atr.iloc[-stability_window:]
        std_atr = recent_atr.std()
        mean_atr = recent_atr.mean()
        
        if mean_atr == 0 or pd.isna(mean_atr) or pd.isna(std_atr):
            return np.nan
        
        cv_atr = std_atr / mean_atr
        return float(cv_atr)
    
    except Exception as e:
        log.warning(f"Error calculating ATR stability: {e}")
        return np.nan