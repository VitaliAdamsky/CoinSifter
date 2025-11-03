# metrics/technical.py

import logging
import pandas as pd
import numpy as np
import pandas_ta_classic as ta

log = logging.getLogger(__name__)


def calculate_adx_metrics(ohlc_df, adx_period=14, analysis_window=90):
    """
    Calculate ADX-based metrics for trend strength analysis.
    
    Returns:
        Dictionary with two metrics:
        - adx_above_25_pct_90d: Percentage of bars where ADX > 25
        - di_plus_dominant_pct_90d: Percentage of bars where DI+ > DI-
    """
    results = {
        f'adx_above_25_pct_{analysis_window}d': np.nan,
        f'di_plus_dominant_pct_{analysis_window}d': np.nan
    }
    
    # Check for required columns
    if not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        log.warning("[ADX] Skipped: missing 'high', 'low', or 'close' columns")
        return results
    
    if len(ohlc_df) < adx_period + analysis_window:
        log.debug(f"[ADX] Skipped: insufficient data (need {adx_period + analysis_window}, got {len(ohlc_df)})")
        return results
    
    try:
        # Calculate ADX and Directional Indicators
        # Add small epsilon to prevent issues with flat candles (H=L)
        adx_data = ta.adx(
            ohlc_df['high'],
            ohlc_df['low'] + 1e-10,
            ohlc_df['close'],
            length=adx_period
        )
        
        if adx_data is None or adx_data.empty:
            log.warning("[ADX] Calculation returned None or empty DataFrame")
            return results
        
        # Extract metrics for analysis window
        adx_series = adx_data[f'ADX_{adx_period}'].tail(analysis_window)
        di_plus = adx_data[f'DMP_{adx_period}'].tail(analysis_window)
        di_minus = adx_data[f'DMN_{adx_period}'].tail(analysis_window)
        
        if adx_series.isnull().all() or len(adx_series) == 0:
            log.debug("[ADX] ADX series is empty or all NaN after slicing")
            return results
        
        # Calculate percentage of bars where ADX > 25
        adx_above_25 = (adx_series > 25).sum()
        results[f'adx_above_25_pct_{analysis_window}d'] = (adx_above_25 / len(adx_series)) * 100
        
        # Calculate percentage of bars where DI+ > DI-
        di_plus_dominant = (di_plus > di_minus).sum()
        results[f'di_plus_dominant_pct_{analysis_window}d'] = (di_plus_dominant / len(adx_series)) * 100
    
    except Exception as e:
        log.warning(f"Error calculating ADX metrics: {e}")
    
    return results