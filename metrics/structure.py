# metrics/structure.py

import logging
import pandas as pd
import numpy as np
from hurst import compute_Hc
from scipy.stats import linregress, entropy
from .utils import get_movement_efficiency, get_fractal_dimension, get_swing_r_squared

log = logging.getLogger(__name__)

MIN_CANDLES_FOR_HURST = 100
MIN_CANDLES_FOR_ENTROPY = 50


def _clip_to_valid_range(value, lower_bound=0.01, upper_bound=0.99):
    """
    Clips a value or array to a valid range [lower_bound, upper_bound].
    Useful for metrics that should not exceed 1 or be less than 0.
    """
    return np.clip(value, lower_bound, upper_bound)


def _calculate_hurst_for_tf(series, min_candles, log_label="Hurst"):
    """
    Calculate Hurst exponent for a time series.
    Returns np.nan if insufficient data or calculation fails.
    """
    if series is None or series.empty or len(series) < min_candles:
        log.debug(f"[{log_label}] Skipped: insufficient data (need {min_candles}, got {len(series) if series is not None else 0})")
        return np.nan

    try:
        # CHANGE: Use kind='change' to analyze returns
        H, c, data = compute_Hc(series, kind='change', simplified=True)

        # CLIPPING: Limit H to range [0.01, 0.99] to remove invalid values > 1
        H_clipped = _clip_to_valid_range(H, lower_bound=0.01, upper_bound=0.99)
        return H_clipped
    except (ValueError, np.linalg.LinAlgError) as e:
        log.warning(f"[{log_label}] Calculation error: {e}")
        return np.nan


def calculate_entropy(series, window=None, min_candles=MIN_CANDLES_FOR_ENTROPY):
    """
    Calculate Shannon entropy of price returns.
    """
    try:
        if window:
            series = series.tail(window)

        if len(series) < min_candles:
            log.debug(f"[Entropy] Skipped: insufficient data (need {min_candles}, got {len(series)})")
            return np.nan

        returns = series.pct_change().dropna()
        if len(returns) < min_candles:
            return np.nan

        # Discretize returns into 10 bins
        hist, _ = np.histogram(returns, bins=10)
        return entropy(hist, base=2)

    except Exception as e:
        log.warning(f"Error calculating entropy: {e}")
        return np.nan


def calculate_trend_quality(close_prices, window=20):
    """
    Calculate trend quality using rolling R² from linear regression.
    """
    if len(close_prices) < window:
        return np.nan

    try:
        log_prices = np.log(close_prices.astype(float))
        x = np.arange(window)

        r_squared = log_prices.rolling(window=window).apply(
            lambda y: linregress(x, y)[2]**2 if not np.isnan(y).any() else np.nan,
            raw=True
        )

        return r_squared.mean()

    except Exception as e:
        log.warning(f"Error calculating trend quality: {e}")
        return np.nan


def calculate_mr_quality(close_prices, window=20):
    """
    Calculate mean reversion quality using detrended R².
    This function analyzes the relationship between consecutive detrended values.
    """
    if len(close_prices) < window:
        return np.nan

    try:
        # Detrend the series using a simple moving average
        sma = close_prices.rolling(window=window).mean()
        detrended = close_prices - sma
        detrended = detrended.dropna()

        # Check if we still have enough data after detrending and dropping NaNs
        if len(detrended) < 2:
            log.debug("MR Quality: Insufficient data after detrending.")
            return np.nan

        # Prepare x (previous detrended values) and y (current detrended values)
        y_values = detrended.iloc[1:].values
        x_values = detrended.iloc[:-1].values

        if len(y_values) < 2:
            log.debug("MR Quality: Not enough points for regression after slicing.")
            return np.nan

        # Check for zero variance (which makes correlation meaningless)
        if np.std(x_values) < 1e-10 or np.std(y_values) < 1e-10:
            log.debug("MR Quality: No variance in detrended series (perfect trend or flat), R² is undefined.")
            # Could return 0 or np.nan here. np.nan seems more appropriate if the metric is undefined.
            return np.nan

        slope, intercept, r_value, p_value, std_err = linregress(x_values, y_values)
        return r_value**2

    except Exception as e:
        log.warning(f"Error calculating MR quality: {e}")
        return np.nan


def calculate_swing_quality(ohlc_df, window=5):
    """
    Calculate swing quality based on high/low predictability.
    """
    if len(ohlc_df) < window * 2:
        return np.nan

    try:
        return get_swing_r_squared(ohlc_df, window=window)
    except Exception as e:
        log.warning(f"Error calculating swing quality: {e}")
        return np.nan


def calculate_movement_efficiency(close_prices, window=100):
    """
    Calculate movement efficiency (net movement / total movement).
    """
    if len(close_prices) < window:
        return np.nan

    try:
        return get_movement_efficiency(close_prices, window=window)
    except Exception as e:
        log.warning(f"Error calculating movement efficiency: {e}")
        return np.nan


def calculate_fractal_dimension(close_prices, window=None):
    """
    Calculate fractal dimension.
    """
    min_candles = MIN_CANDLES_FOR_HURST # Assuming FD calculation also needs this amount
    if len(close_prices) < min_candles:
        return np.nan

    try:
        if window:
            close_prices = close_prices.tail(window)

        fd = get_fractal_dimension(close_prices)

        # CLIPPING: Limit FD to range [0.01, 0.99] to remove invalid values > 1
        fd_clipped = _clip_to_valid_range(fd, lower_bound=0.01, upper_bound=0.99)
        return fd_clipped

    except Exception as e:
        log.warning(f"Error calculating fractal dimension: {e}")
        return np.nan


def calculate_hurst_metrics(ohlcv_data_map, timeframes_to_calc):
    """
    Calculate Hurst exponent for all specified timeframes.
    """
    results = {}

    for tf in timeframes_to_calc:
        key = f'hurst_{tf}'
        series = ohlcv_data_map.get(tf, pd.DataFrame()).get('close')

        results[key] = _calculate_hurst_for_tf(
            series,
            MIN_CANDLES_FOR_HURST,
            f"Hurst {tf}"
        )

    return results
