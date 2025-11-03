# metrics/calculator.py

import logging
import pandas as pd
import numpy as np
from scipy import stats 
import pandas_ta_classic as ta 

# --- (ИЗМЕНЕНИЕ №1) ИМПОРТЫ ---
# Импортируем ВСЕ метрики из их "эталонных" файлов

# 1. Из structure.py (Без изменений)
from .structure import (
    calculate_hurst_metrics, 
    calculate_entropy,
    calculate_trend_quality, 
    calculate_mr_quality,
    calculate_swing_quality, 
    calculate_movement_efficiency,
    calculate_fractal_dimension
)
# 2. Из technical.py (Без изменений)
from .technical import calculate_adx_metrics

# 3. (ИЗМЕНЕНИЕ) Из utils.py (включая Jagginess и др.)
from .utils import (
    calculate_smoothness_index,
    calculate_skewness_kurtosis,
    calculate_movement_intensity,
    calculate_atr_stability,
    calculate_candle_jagginess
)

# 4. Из character.py (MCI)
from .character import calculate_movement_character_index 

# 5. Из market.py (Без изменений)
from .market import (
    calculate_btc_correlation,
    calculate_btc_correlation_stability
)
# --- КОНЕЦ ИЗМЕНЕНИЯ ---


log = logging.getLogger(__name__)
TIMEFRAMES = ['1h', '2h', '4h', '12h', '1d']


# ============================================================================
# === (ИЗМЕНЕНИЕ №1) ФУНКЦИИ УДАЛЕНЫ ===
#
# Функции calculate_smoothness_index, calculate_skewness_kurtosis,
# calculate_movement_intensity, calculate_atr_stability,
# и calculate_candle_jagginess (5 функций) УДАЛЕНЫ ИЗ ЭТОГО ФАЙЛА.
#
# Они теперь импортируются из metrics/utils.py (или других модулей).
#
# ============================================================================


# ============================================================================
# === ОСНОВНАЯ ЛОГИКА ОРКЕСТРАТОРА ===
# ============================================================================

def calculate_all_metrics(ohlcv_data, btc_data_1d):
    """
    Calculate all metrics for all timeframes.
    """
    metrics = {}
    
    # 1. Структурные метрики (Hurst, R-Squared, и т.д.)
    try:
        metrics.update(calculate_hurst_metrics(ohlcv_data, TIMEFRAMES))
    except Exception as e:
        log.warning(f"Error calculating Hurst metrics: {e}")
    
    # 2. Расчеты по таймфреймам
    for tf in TIMEFRAMES:
        df_tf = ohlcv_data.get(tf, pd.DataFrame())
        
        if df_tf.empty:
            log.debug(f"Skipping timeframe {tf}: no data")
            continue
        
        df_close = df_tf.get('close', pd.Series(dtype=float))
        if df_close.empty:
            continue
        
        # 1. Структурные метрики (из structure.py)
        try:
            metrics[f'entropy_{tf}'] = calculate_entropy(df_close)
            metrics[f'trend_quality_{tf}_w20'] = calculate_trend_quality(df_close, window=20)
            metrics[f'mr_quality_{tf}_w20'] = calculate_mr_quality(df_close, window=20)
            metrics[f'swing_quality_{tf}_w5'] = calculate_swing_quality(df_tf, window=5)
            metrics[f'movement_efficiency_{tf}'] = calculate_movement_efficiency(df_close, window=100)
            metrics[f'fractal_dimension_{tf}'] = calculate_fractal_dimension(df_close)
        except Exception as e:
            log.warning(f"Error calculating structure metrics for {tf}: {e}")
            
        # 2. Метрики "Характера" (из utils.py)
        try:
            # Индекс Ершистости Свечи (CJI)
            metrics[f'jagginess_{tf}_w20'] = calculate_candle_jagginess(df_tf, window=20)
            
            # Сглаженность
            metrics[f'smoothness_index_{tf}_w20'] = calculate_smoothness_index(df_close, window=20)
            
            # Статистика
            skew_kurt = calculate_skewness_kurtosis(df_close, window=50)
            metrics[f'skewness_{tf}_w50'] = skew_kurt.get('skewness')
            metrics[f'kurtosis_{tf}_w50'] = skew_kurt.get('kurtosis')
            
            # Волатильность
            metrics[f'movement_intensity_{tf}_w14'] = calculate_movement_intensity(df_tf, window=14)
            metrics[f'atr_stability_{tf}_w14'] = calculate_atr_stability(df_tf, atr_window=14, stability_window=14)
            
        except Exception as e:
            log.warning(f"Error calculating character metrics for {tf}: {e}")

        # 3. Интегрированные метрики (MCI) (из character.py)
        try:
            hurst_val = metrics.get(f'hurst_{tf}')
            fd_val = metrics.get(f'fractal_dimension_{tf}')
            metrics[f'mci_{tf}'] = calculate_movement_character_index(hurst_val, fd_val)
        except Exception as e:
            log.warning(f"Error calculating integrated metrics for {tf}: {e}")

        # 4. Технические метрики (ADX) (из technical.py)
        try:
            adx_metrics = calculate_adx_metrics(df_tf, adx_period=14, analysis_window=90)
            for key, value in adx_metrics.items():
                metrics[f"{key}_{tf}"] = value
        except Exception as e:
            log.warning(f"Error calculating ADX metrics for {tf}: {e}")

    
    # 5. BTC correlation metrics (1d only) (из market.py)
    try:
        df_1d_close = ohlcv_data.get('1d', pd.DataFrame()).get('close', pd.Series(dtype=float))
        if not df_1d_close.empty and not btc_data_1d.empty:
            metrics['btc_corr_1d_w30'] = calculate_btc_correlation(
                df_1d_close, btc_data_1d['close'], window=30
            )
            metrics.update(calculate_btc_correlation_stability(
                df_1d_close, btc_data_1d['close'], window=30
            ))
    except Exception as e:
        log.warning(f"Error calculating BTC correlation metrics: {e}")
    
    # Clean up metrics
    final_metrics = {}
    for key, value in metrics.items():
        if value is not None and np.isfinite(value) and not isinstance(value, (np.ndarray, pd.Series)):
            final_metrics[key] = value
    
    return final_metrics