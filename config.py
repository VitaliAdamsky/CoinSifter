# config.py (FIXED VERSION)

import os
from dotenv import load_dotenv

# --- Exchange Configuration ---
EXCHANGES_TO_LOAD = ['binanceusdm', 'bybit']

EXCHANGE_VOLUME_KEYS = {
    'binanceusdm': 'quoteVolume',
    'bybit': 'quoteVolume'
}

# --- Timeframe Configuration ---
TIMEFRAMES_TO_LOAD = {
    '1h': 30,
    '2h': 60,
    '4h': 90,
    '12h': 55,
    '1d': 181
}
HISTORY_LOAD_DAYS = TIMEFRAMES_TO_LOAD

# --- Market Configuration ---
QUOTE_CURRENCIES = ['USDT']
BTC_SYMBOL = 'BTC/USDT:USDT'

# --- Analysis Thresholds ---
MIN_CANDLES_FOR_MATURITY = 180
MIN_CANDLES_FOR_HURST = 100
MIN_CANDLES_FOR_ENTROPY = 50
MIN_VOLUME_24H_USD = 3_000_000

# --- CCXT Configuration ---
CANDLE_LIMIT_DEFAULT = 1000
RETRY_ATTEMPTS = 5
RETRY_WAIT_MIN = 3
RETRY_WAIT_MAX = 30

# --- Worker Configuration ---
MAX_CONCURRENT_PER_EXCHANGE = 10
NUM_WORKERS = 10
QUEUE_MAX_SIZE = 1000
ANALYSIS_BATCH_SIZE = 50

# --- Database Schema ---
# Total metrics: 16 metrics × 5 timeframes = 80 metrics + 4 BTC metrics = 84 metrics
DATABASE_SCHEMA = {
    # Identification
    "symbol": "VARCHAR(50) NOT NULL",
    "full_symbol": "VARCHAR(100) NOT NULL UNIQUE",
    "exchanges": "TEXT[]",
    "logoUrl": "VARCHAR(255)",
    
    # Volume Data
    "volume_24h_usd": "DOUBLE PRECISION",
    
    # ⚠️ FIXED: Changed from INTEGER to SMALLINT (sufficient for 1-6 range)
    # Volume Category (1-6 ranking)
    "category": "SMALLINT",
    
    # Metadata
    "analyzed_at": "TIMESTAMP WITH TIME ZONE",
    
    # --- Character Metrics (Hurst) - 5 metrics ---
    "hurst_1h": "DOUBLE PRECISION",
    "hurst_2h": "DOUBLE PRECISION",
    "hurst_4h": "DOUBLE PRECISION",
    "hurst_12h": "DOUBLE PRECISION",
    "hurst_1d": "DOUBLE PRECISION",
    
    # --- Entropy - 5 metrics ---
    "entropy_1h": "DOUBLE PRECISION",
    "entropy_2h": "DOUBLE PRECISION",
    "entropy_4h": "DOUBLE PRECISION",
    "entropy_12h": "DOUBLE PRECISION",
    "entropy_1d": "DOUBLE PRECISION",
    
    # --- Trend Quality - 5 metrics ---
    "trend_quality_1h_w20": "DOUBLE PRECISION",
    "trend_quality_2h_w20": "DOUBLE PRECISION",
    "trend_quality_4h_w20": "DOUBLE PRECISION",
    "trend_quality_12h_w20": "DOUBLE PRECISION",
    "trend_quality_1d_w20": "DOUBLE PRECISION",
    
    # --- Mean Reversion Quality - 5 metrics ---
    "mr_quality_1h_w20": "DOUBLE PRECISION",
    "mr_quality_2h_w20": "DOUBLE PRECISION",
    "mr_quality_4h_w20": "DOUBLE PRECISION",
    "mr_quality_12h_w20": "DOUBLE PRECISION",
    "mr_quality_1d_w20": "DOUBLE PRECISION",
    
    # --- Swing Quality - 5 metrics ---
    "swing_quality_1h_w5": "DOUBLE PRECISION",
    "swing_quality_2h_w5": "DOUBLE PRECISION",
    "swing_quality_4h_w5": "DOUBLE PRECISION",
    "swing_quality_12h_w5": "DOUBLE PRECISION",
    "swing_quality_1d_w5": "DOUBLE PRECISION",
    
    # --- Movement Efficiency - 5 metrics ---
    "movement_efficiency_1h": "DOUBLE PRECISION",
    "movement_efficiency_2h": "DOUBLE PRECISION",
    "movement_efficiency_4h": "DOUBLE PRECISION",
    "movement_efficiency_12h": "DOUBLE PRECISION",
    "movement_efficiency_1d": "DOUBLE PRECISION",
    
    # --- Fractal Dimension - 5 metrics ---
    "fractal_dimension_1h": "DOUBLE PRECISION",
    "fractal_dimension_2h": "DOUBLE PRECISION",
    "fractal_dimension_4h": "DOUBLE PRECISION",
    "fractal_dimension_12h": "DOUBLE PRECISION",
    "fractal_dimension_1d": "DOUBLE PRECISION",
    
    # --- ADX Metrics - 10 metrics (2 per timeframe) ---
    "adx_above_25_pct_90d_1h": "DOUBLE PRECISION",
    "adx_above_25_pct_90d_2h": "DOUBLE PRECISION",
    "adx_above_25_pct_90d_4h": "DOUBLE PRECISION",
    "adx_above_25_pct_90d_12h": "DOUBLE PRECISION",
    "adx_above_25_pct_90d_1d": "DOUBLE PRECISION",
    "di_plus_dominant_pct_90d_1h": "DOUBLE PRECISION",
    "di_plus_dominant_pct_90d_2h": "DOUBLE PRECISION",
    "di_plus_dominant_pct_90d_4h": "DOUBLE PRECISION",
    "di_plus_dominant_pct_90d_12h": "DOUBLE PRECISION",
    "di_plus_dominant_pct_90d_1d": "DOUBLE PRECISION", 
    
    # --- Smoothness Index - 5 metrics ---
    "smoothness_index_1h_w20": "DOUBLE PRECISION",
    "smoothness_index_2h_w20": "DOUBLE PRECISION",
    "smoothness_index_4h_w20": "DOUBLE PRECISION",
    "smoothness_index_12h_w20": "DOUBLE PRECISION",
    "smoothness_index_1d_w20": "DOUBLE PRECISION",
    
    # --- Skewness - 5 metrics ---
    "skewness_1h_w50": "DOUBLE PRECISION",
    "skewness_2h_w50": "DOUBLE PRECISION",
    "skewness_4h_w50": "DOUBLE PRECISION",
    "skewness_12h_w50": "DOUBLE PRECISION",
    "skewness_1d_w50": "DOUBLE PRECISION",
    
    # --- Kurtosis - 5 metrics ---
    "kurtosis_1h_w50": "DOUBLE PRECISION",
    "kurtosis_2h_w50": "DOUBLE PRECISION",
    "kurtosis_4h_w50": "DOUBLE PRECISION",
    "kurtosis_12h_w50": "DOUBLE PRECISION",
    "kurtosis_1d_w50": "DOUBLE PRECISION",
    
    # --- Movement Intensity - 5 metrics ---
    "movement_intensity_1h_w14": "DOUBLE PRECISION",
    "movement_intensity_2h_w14": "DOUBLE PRECISION",
    "movement_intensity_4h_w14": "DOUBLE PRECISION",
    "movement_intensity_12h_w14": "DOUBLE PRECISION",
    "movement_intensity_1d_w14": "DOUBLE PRECISION",
    
    # --- ATR Stability - 5 metrics ---
    "atr_stability_1h_w14": "DOUBLE PRECISION",
    "atr_stability_2h_w14": "DOUBLE PRECISION",
    "atr_stability_4h_w14": "DOUBLE PRECISION",
    "atr_stability_12h_w14": "DOUBLE PRECISION",
    "atr_stability_1d_w14": "DOUBLE PRECISION",

    # --- Movement Character Index (MCI) - 5 metrics (НОВАЯ) ---
    "mci_1h": "DOUBLE PRECISION",
    "mci_2h": "DOUBLE PRECISION",
    "mci_4h": "DOUBLE PRECISION",
    "mci_12h": "DOUBLE PRECISION",
    "mci_1d": "DOUBLE PRECISION",

    # --- Candle Jagginess Index (Jagginess) - 5 metrics (НОВАЯ) ---
    "jagginess_1h_w20": "DOUBLE PRECISION",
    "jagginess_2h_w20": "DOUBLE PRECISION",
    "jagginess_4h_w20": "DOUBLE PRECISION",
    "jagginess_12h_w20": "DOUBLE PRECISION",
    "jagginess_1d_w20": "DOUBLE PRECISION",
    
    # --- BTC Correlation Metrics (1d only) - 4 metrics ---
    "btc_corr_1d_w30": "DOUBLE PRECISION",
    "btc_corr_stability_current_correlation": "DOUBLE PRECISION",
    "btc_corr_stability_correlation_std": "DOUBLE PRECISION",
    "btc_corr_stability_correlation_stability_score": "DOUBLE PRECISION"
}

# --- Security ---
SECRET_TOKEN = os.getenv('SECRET_TOKEN')