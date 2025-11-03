# tests/test_metrics.py

import pytest
import pandas as pd
import numpy as np
import logging
from unittest.mock import MagicMock, ANY # (ИЗМЕНЕНИЕ) Добавлено для моков БД

# --- (Импорты из structure.py) ---
from metrics.structure import (
    calculate_hurst_metrics,
    _calculate_hurst_for_tf,
    calculate_entropy,
    calculate_trend_quality,
    calculate_mr_quality,
    calculate_swing_quality,
    calculate_movement_efficiency,
    calculate_fractal_dimension,
    MIN_CANDLES_FOR_HURST,
    MIN_CANDLES_FOR_ENTROPY
)

# --- (Импорты из utils.py) ---
from metrics.utils import (
    calculate_candle_jagginess,
    calculate_atr_stability
)

# --- (Импорты из technical.py и market.py) ---
from metrics.technical import calculate_adx_metrics
from metrics.market import (
    calculate_btc_correlation, 
    calculate_btc_correlation_stability
)

# --- (ИЗМЕНЕНИЕ) Импорты из character.py и ranking.py ---
from metrics.character import (
    calculate_movement_character_index,
    H_MAX_DEVIATION,
    FD_MAX_DEVIATION
)
from metrics.ranking import update_volume_categories


# Отключаем логгирование DEBUG от метрик во время тестов
logging.getLogger("metrics.structure").setLevel(logging.WARNING)
logging.getLogger("metrics.utils").setLevel(logging.WARNING)
logging.getLogger("metrics.technical").setLevel(logging.WARNING)
logging.getLogger("metrics.market").setLevel(logging.WARNING)
logging.getLogger("metrics.character").setLevel(logging.WARNING) # (Новое)
logging.getLogger("metrics.ranking").setLevel(logging.WARNING)   # (Новое)


# ============================================================================
# === (ШАГ 1) "ЗОЛОТЫЕ" ДАННЫЕ (ФИКСТУРЫ) ===
# ============================================================================

def _create_ohlcv_data(series):
    """Хелпер для создания DataFrame из 'close'."""
    df = pd.DataFrame()
    df['close'] = series
    df['open'] = series.shift(1).fillna(series.iloc[0])
    df['high'] = df[['open', 'close']].max(axis=1) + 0.1
    df['low'] = df[['open', 'close']].min(axis=1) - 0.1
    return {'1h': df}

@pytest.fixture(scope="module")
def trending_price_data():
    """ (Hurst < 0.5) """
    count = MIN_CANDLES_FOR_HURST + 5
    prices = np.linspace(100, 150, count)
    series = pd.Series(prices)
    return _create_ohlcv_data(series)

@pytest.fixture(scope="module")
def returns_with_persistence():
    """ (Hurst > 0.5) """
    count = MIN_CANDLES_FOR_HURST + 5
    returns_pattern = np.concatenate([
        np.full(10, 0.02),   # 10 положительных
        np.full(10, -0.02),  # 10 отрицательных
    ])
    returns = np.tile(returns_pattern, (count // 20) + 1)[:count]
    returns += np.random.normal(0, 0.001, count)
    prices = 100 * np.exp(np.cumsum(returns))
    series = pd.Series(prices)
    return _create_ohlcv_data(series)


@pytest.fixture(scope="module")
def flat_ohlcv_data():
    """ "Мертвые" данные (O=H=L=C = 100) """
    count = MIN_CANDLES_FOR_HURST + 5
    series = pd.Series(np.full(count, 100.0))
    df = pd.DataFrame()
    df['close'] = series
    df['open'] = series
    df['high'] = series
    df['low'] = series
    return {'1h': df}

@pytest.fixture(scope="module")
def btc_data_fixture():
    """
    (Ваша версия фикстуры)
    """
    count = 100
    base_returns = np.random.normal(0.01, 0.005, count)
    btc_up_prices = 50 * np.exp(np.cumsum(base_returns))
    btc_down_prices = 100 * np.exp(np.cumsum(-base_returns))
    btc_flat_prices = np.full(count, 50.0)
    
    return {
        'base_returns': base_returns,
        'btc_up': pd.Series(btc_up_prices),
        'btc_down': pd.Series(btc_down_prices),
        'btc_flat': pd.Series(btc_flat_prices)
    }


# ============================================================================
# === (ШАГ 2) ТЕСТЫ METRICS/STRUCTURE.PY ===
# ============================================================================

def test_calculate_hurst_metrics(trending_price_data, returns_with_persistence):
    """ (Прошел) """
    # 1. Тест на линейный тренд (mean-reverting returns)
    trend_results = calculate_hurst_metrics(trending_price_data, ['1h'])
    h_trend = trend_results['hurst_1h']
    assert h_trend is not np.nan
    assert 0 < h_trend < 1

    # 2. Тест на блочные returns (persistent returns)
    persist_results = calculate_hurst_metrics(returns_with_persistence, ['1h'])
    h_persist = persist_results['hurst_1h']
    assert h_persist is not np.nan
    assert 0 < h_persist < 1
    
    # 3. ГЛАВНАЯ ПРОВЕРКА (относительное сравнение)
    assert h_persist > h_trend

def test_calculate_entropy(trending_price_data):
    """ (Прошел) """
    series = trending_price_data['1h']['close']
    entropy = calculate_entropy(series)
    assert entropy is not np.nan
    assert entropy > 0 

def test_metrics_insufficient_data(trending_price_data):
    """ (Прошел) """
    short_df = trending_price_data['1h'].head(10)
    short_series = short_df['close']
    short_map = {'1h': short_df}
    
    assert _calculate_hurst_for_tf(short_series, MIN_CANDLES_FOR_HURST) is np.nan
    assert calculate_hurst_metrics(short_map, ['1h'])['hurst_1h'] is np.nan
    assert calculate_entropy(short_series, min_candles=MIN_CANDLES_FOR_ENTROPY) is np.nan
    assert calculate_trend_quality(short_series, window=20) is np.nan
    assert calculate_mr_quality(short_series, window=20) is np.nan
    assert calculate_swing_quality(short_df, window=5) is np.nan
    assert calculate_movement_efficiency(short_series, window=100) is np.nan
    assert calculate_fractal_dimension(short_series) is np.nan

# ============================================================================
# === (ШАГ 3) ТЕСТЫ METRICS/UTILS.PY ===
# ============================================================================

def test_calculate_candle_jagginess(trending_price_data):
    """ (Прошел) """
    df = trending_price_data['1h'].copy()
    jagginess_normal = calculate_candle_jagginess(df, window=20)
    assert jagginess_normal is not np.nan
    assert jagginess_normal > 0

    df['high'] = df[['open', 'close']].max(axis=1)
    df['low'] = df[['open', 'close']].min(axis=1)
    jagginess_zero = calculate_candle_jagginess(df, window=20)
    assert jagginess_zero == 0.0

def test_jagginess_edge_cases(flat_ohlcv_data):
    """ (Прошел) """
    df = flat_ohlcv_data['1h']
    jagginess = calculate_candle_jagginess(df, window=20)
    assert jagginess == 0.0

def test_atr_stability_edge_cases(flat_ohlcv_data):
    """ (Прошел) """
    df = flat_ohlcv_data['1h']
    stability = calculate_atr_stability(df)
    assert stability is np.nan

# ============================================================================
# === (ШАГ 4) ТЕСТЫ METRICS/TECHNICAL.PY ===
# ============================================================================

def test_adx_metrics_edge_cases(flat_ohlcv_data):
    """ (Прошел) """
    df = flat_ohlcv_data['1h']
    adx_metrics = calculate_adx_metrics(df)
    assert adx_metrics['adx_above_25_pct_90d'] is np.nan
    assert adx_metrics['di_plus_dominant_pct_90d'] is np.nan

# ============================================================================
# === (ШАГ 5) ТЕСТЫ METRICS/MARKET.PY ===
# ============================================================================

def test_btc_correlation(btc_data_fixture):
    """ (Прошел) """
    window = 30
    
    # 1. Идеальная корреляция (1.0)
    base_returns = btc_data_fixture['base_returns']
    asset_up = pd.Series(100 * np.exp(np.cumsum(base_returns)))
    btc_up = btc_data_fixture['btc_up']
    corr_pos = calculate_btc_correlation(asset_up, btc_up, window)
    assert corr_pos == pytest.approx(1.0, abs=1e-3)

    # 2. Идеальная обратная корреляция (-1.0)
    btc_down = btc_data_fixture['btc_down']
    corr_neg = calculate_btc_correlation(asset_up, btc_down, window)
    assert corr_neg == pytest.approx(-1.0, abs=1e-3)

    # 3. Отсутствие корреляции (BTC "мертвый")
    btc_flat = btc_data_fixture['btc_flat']
    corr_nan = calculate_btc_correlation(asset_up, btc_flat, window)
    assert corr_nan is np.nan

def test_btc_correlation_stability(btc_data_fixture):
    """ (Прошел) """
    window = 30
    stability_window = 60
    
    # 1. Идеально стабильный тренд (1.0)
    base_returns = btc_data_fixture['base_returns']
    asset_up = pd.Series(100 * np.exp(np.cumsum(base_returns)))
    btc_up = btc_data_fixture['btc_up']
    
    metrics_pos = calculate_btc_correlation_stability(
        asset_up, btc_up, window, stability_window
    )
    assert metrics_pos['btc_corr_stability_current_correlation'] == pytest.approx(1.0, abs=1e-3)
    assert metrics_pos['btc_corr_stability_correlation_std'] == pytest.approx(0.0, abs=1e-3)
    assert metrics_pos['btc_corr_stability_correlation_stability_score'] == pytest.approx(1.0, abs=1e-3)

    # 2. "Мертвый" BTC (NaN)
    btc_flat = btc_data_fixture['btc_flat']
    metrics_nan = calculate_btc_correlation_stability(
        asset_up, btc_flat, window, stability_window
    )
    assert metrics_nan['btc_corr_stability_current_correlation'] is np.nan
    assert metrics_nan['btc_corr_stability_correlation_std'] is np.nan
    assert metrics_nan['btc_corr_stability_correlation_stability_score'] is np.nan

# ============================================================================
# === (ШАГ 6) ТЕСТЫ METRICS/CHARACTER.PY (НОВЫЕ) ===
# ============================================================================

def test_mci_logic():
    """
    Тестирует математику MCI на 3 сценариях.
    """
    # 1. "Идеальный" Хаос (Random Walk)
    # H = 0.5 (Центр)
    # FD = 0.79 (Центр)
    mci_random = calculate_movement_character_index(0.5, 0.79)
    assert mci_random == pytest.approx(0.0)

    # 2. "Идеальный" Тренд (Макс H, Мин FD)
    # H = 0.99 (Макс Клиппинг)
    # FD = 0.01 (Мин Клиппинг)
    norm_H = (0.99 - 0.5) / H_MAX_DEVIATION  # -> 1.0
    norm_FD = (0.79 - 0.01) / FD_MAX_DEVIATION # -> 1.0
    mci_trend = calculate_movement_character_index(0.99, 0.01)
    # 0.5 * 1.0 + 0.5 * 1.0 = 1.0
    assert mci_trend == pytest.approx(1.0)

    # 3. "Идеальный" Шум (Мин H, Макс FD)
    # H = 0.01 (Мин Клиппинг)
    # FD = 0.99 (Макс Клиппинг)
    norm_H = (0.01 - 0.5) / H_MAX_DEVIATION  # -> -1.0
    norm_FD = (0.79 - 0.99) / FD_MAX_DEVIATION # -> -0.256...
    mci_noise = calculate_movement_character_index(0.01, 0.99)
    # 0.5 * -1.0 + 0.5 * -0.256... = -0.628...
    assert mci_noise == pytest.approx(-0.6282, abs=1e-3)

def test_mci_nan_handling():
    """
    Тестирует, что MCI возвращает NaN, если вход NaN.
    """
    assert np.isnan(calculate_movement_character_index(np.nan, 0.5))
    assert np.isnan(calculate_movement_character_index(0.5, np.nan))
    assert np.isnan(calculate_movement_character_index(np.nan, np.nan))

# ============================================================================
# === (ШАГ 6) ТЕСТЫ METRICS/RANKING.PY (НОВЫЕ) ===
# ============================================================================

def test_volume_categories(mocker):
    """
    Тестирует расчет категорий объема.
    Использует mocker для имитации ответа БД.
    """
    # 1. Создаем 12 "монет" для 6 категорий (по 2 монеты на ранг)
    mock_db_data = [
        {'full_symbol': 'A', 'volume_24h_usd': 10},
        {'full_symbol': 'B', 'volume_24h_usd': 11},
        {'full_symbol': 'C', 'volume_24h_usd': 20},
        {'full_symbol': 'D', 'volume_24h_usd': 21},
        {'full_symbol': 'E', 'volume_24h_usd': 30},
        {'full_symbol': 'F', 'volume_24h_usd': 31},
        {'full_symbol': 'G', 'volume_24h_usd': 40},
        {'full_symbol': 'H', 'volume_24h_usd': 41},
        {'full_symbol': 'I', 'volume_24h_usd': 50},
        {'full_symbol': 'J', 'volume_24h_usd': 51},
        {'full_symbol': 'K', 'volume_24h_usd': 60},
        {'full_symbol': 'L', 'volume_24h_usd': 61},
    ]
    
    # 2. Мокаем (имитируем) иерархию БД
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Мокаем get_db_connection(), чтобы он вернул наш мок
    mocker.patch("metrics.ranking.get_db_connection", return_value=mock_conn)
    mock_conn.cursor.return_value = mock_cursor
    
    # Мокаем .fetchall(), чтобы он вернул наши 12 монет
    mock_cursor.fetchall.return_value = mock_db_data
    
    # Мокаем psycopg2.extras.execute_values, чтобы мы могли проверить, что в него ушло
    mock_execute_values = mocker.patch("metrics.ranking.execute_values")

    # 3. Вызываем функцию
    update_volume_categories(table_name="test_table")

    # 4. Проверяем, что execute_values был вызван с ПРАВИЛЬНЫМИ данными
    
    # execute_values(cursor, query, data) -> args[2] это 'data'
    call_args = mock_execute_values.call_args[0]
    update_data = call_args[2] # list( (rank, symbol), ... )
    
    # Превращаем [(1, 'A'), (1, 'B'), ...] в {'A': 1, 'B': 1, ...}
    result_ranks = {symbol: rank for rank, symbol in update_data}
    
    assert len(result_ranks) == 12
    
    # Ранг 1 (Низший)
    assert result_ranks['A'] == 1
    assert result_ranks['B'] == 1
    # Ранг 2
    assert result_ranks['C'] == 2
    assert result_ranks['D'] == 2
    # Ранг 3
    assert result_ranks['E'] == 3
    assert result_ranks['F'] == 3
    # Ранг 4
    assert result_ranks['G'] == 4
    assert result_ranks['H'] == 4
    # Ранг 5
    assert result_ranks['I'] == 5
    assert result_ranks['J'] == 5
    # Ранг 6 (Высший)
    assert result_ranks['K'] == 6
    assert result_ranks['L'] == 6