# metrics/market.py

import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)


def _align_and_get_returns(asset_close, btc_close, min_length):
    """
    Helper function to align indices and calculate log returns.
    
    Returns:
        Tuple of (asset_returns, btc_returns, length) or (None, None, 0) if insufficient data
    """
    # Align indices
    common_index = asset_close.index.intersection(btc_close.index)
    
    if len(common_index) < min_length:
        return None, None, 0
    
    asset_close = asset_close.loc[common_index]
    btc_close = btc_close.loc[common_index]
    
    # Calculate log returns
    asset_returns = np.log(asset_close / asset_close.shift(1)).dropna()
    btc_returns = np.log(btc_close / btc_close.shift(1)).dropna()
    
    # Align again after dropna
    common_returns_index = asset_returns.index.intersection(btc_returns.index)
    
    if len(common_returns_index) < min_length - 1:
        return None, None, 0
    
    return (
        asset_returns.loc[common_returns_index],
        btc_returns.loc[common_returns_index],
        len(common_returns_index)
    )


def calculate_btc_correlation(asset_close, btc_close, window=30):
    """
    Calculate rolling correlation between asset returns and Bitcoin returns.
    
    Returns the most recent correlation value.
    """
    try:
        asset_returns, btc_returns, length = _align_and_get_returns(
            asset_close, btc_close, window + 1
        )
        
        if asset_returns is None or length < window:
            return np.nan
        
        rolling_corr = asset_returns.rolling(
            window=window, min_periods=window
        ).corr(btc_returns)
        
        result = rolling_corr.iloc[-1]
        return result if pd.notna(result) else np.nan
    
    except Exception as e:
        log.debug(f"Error calculating BTC correlation: {e}")
        return np.nan


def calculate_btc_correlation_stability(asset_close, btc_close, window=30, stability_window=60):
    """
    Calculate stability of correlation with Bitcoin over time.
    
    Returns:
        Dictionary with three metrics:
        - btc_corr_stability_current_correlation: Most recent correlation value
        - btc_corr_stability_correlation_std: Standard deviation of correlation
        - btc_corr_stability_correlation_stability_score: Stability score (0-1, higher is more stable)
    """
    try:
        min_length = window + stability_window
        asset_returns, btc_returns, length = _align_and_get_returns(
            asset_close, btc_close, min_length
        )
        
        if asset_returns is None or length < min_length - 1:
            return {
                'btc_corr_stability_current_correlation': np.nan,
                'btc_corr_stability_correlation_std': np.nan,
                'btc_corr_stability_correlation_stability_score': np.nan
            }
        
        # Calculate rolling correlation
        rolling_corr = asset_returns.rolling(
            window=window, min_periods=window
        ).corr(btc_returns).dropna()
        
        if len(rolling_corr) < stability_window:
            return {
                'btc_corr_stability_current_correlation': np.nan,
                'btc_corr_stability_correlation_std': np.nan,
                'btc_corr_stability_correlation_stability_score': np.nan
            }
        
        # Take last N correlation values
        recent_corr = rolling_corr.iloc[-stability_window:]
        current_corr = recent_corr.iloc[-1]
        corr_std = recent_corr.std()
        
        # Stability score: lower std = higher stability
        stability_score = max(0, 1 - corr_std) if pd.notna(corr_std) else np.nan
        
        return {
            'btc_corr_stability_current_correlation': current_corr if pd.notna(current_corr) else np.nan,
            'btc_corr_stability_correlation_std': corr_std if pd.notna(corr_std) else np.nan,
            'btc_corr_stability_correlation_stability_score': stability_score
        }
    
    except Exception as e:
        log.debug(f"Error calculating correlation stability: {e}")
        return {
            'btc_corr_stability_current_correlation': np.nan,
            'btc_corr_stability_correlation_std': np.nan,
            'btc_corr_stability_correlation_stability_score': np.nan
        }