# tests/test_service_data_fetcher.py

import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

# Импортируем тестируемые функции
from services.data_fetcher import (
    fetch_all_coins_data, 
    fetch_all_ohlcv_data,
    _extract_base_symbol # Тестируем утилиту напрямую
)

# --- Данные для моков ---

MOCK_MARKETS = {
    'BTC/USDT': {'symbol': 'BTC/USDT', 'active': True, 'type': 'swap', 'quote': 'USDT', 'name': 'BTC/USDT'},
    'SOL/USDT': {'symbol': 'SOL/USDT', 'active': True, 'type': 'swap', 'quote': 'USDT', 'name': 'SOL/USDT'},
    'ADA/USDT': {'symbol': 'ADA/USDT', 'active': True, 'type': 'swap', 'quote': 'USDT', 'name': 'ADA/USDT'},
}

# Объем > MIN_VOLUME
MOCK_TICKER_BTC = {
    'symbol': 'BTC/USDT',
    'quoteVolumeCurrency': 'USDT',
    'last': 60000.0,
    'quoteVolume': 500_000_000.0, # (config.EXCHANGE_VOLUME_KEYS)
    'volume': 8000.0,
    'percentage': 1.5
}
# Объем > MIN_VOLUME
MOCK_TICKER_SOL = {
    'symbol': 'SOL/USDT',
    'quoteVolumeCurrency': 'USDT',
    'last': 150.0,
    'quoteVolume': 200_000_000.0,
    'volume': 1_000_000.0,
    'percentage': -2.0
}
# Объем < MIN_VOLUME
MOCK_TICKER_ADA_LOW_VOL = {
    'symbol': 'ADA/USDT',
    'quoteVolumeCurrency': 'USDT',
    'last': 0.5,
    'quoteVolume': 100.0, # <-- Слишком низкий объем
    'volume': 200.0,
    'percentage': 0.1
}

MOCK_OHLCV_DATA = [
    [1678886400000, 100, 110, 90, 105, 1000],
    [1678886500000, 105, 115, 100, 110, 1200],
]

# --- Фикстуры (Настройка тестов) ---

@pytest.fixture
def mock_config(mocker):
    """Мокает 'config', особенно 'MIN_VOLUME_24H_USD'."""
    mocker.patch('config.MIN_VOLUME_24H_USD', 1_000_000.0)
    mocker.patch('config.EXCHANGE_VOLUME_KEYS', {'binanceusdm': 'quoteVolume', 'bybit': 'quoteVolume'})

@pytest.fixture
def mock_exchange(mocker):
    """Создает мок 'exchange' и 'initialize_exchange'."""
    mock_ex = MagicMock()
    mock_ex.id = 'binanceusdm'
    
    # Мокаем 'initialize_exchange'
    mocker.patch(
        "services.data_fetcher.initialize_exchange",
        new_callable=AsyncMock,
        return_value=mock_ex
    )
    return mock_ex

@pytest.fixture
def mock_api_calls(mocker):
    """Мокает 'fetch_markets' и 'fetch_tickers'."""
    mock_markets = mocker.patch(
        "services.data_fetcher.fetch_markets",
        new_callable=AsyncMock,
        return_value=MOCK_MARKETS
    )
    mock_tickers = mocker.patch(
        "services.data_fetcher.fetch_tickers",
        new_callable=AsyncMock,
        return_value={} # По умолчанию пустой
    )
    return mock_markets, mock_tickers


# --- Тесты для _extract_base_symbol ---

def test_extract_base_symbol():
    """Тестирует утилиту извлечения базового символа."""
    assert _extract_base_symbol("SOL/USDT") == "SOL"
    assert _extract_base_symbol("ETH/BTC") == "ETH"
    assert _extract_base_symbol("1000PEPE/USDT") == "1000PEPE"
    assert _extract_base_symbol("") == ""

# --- Тесты для fetch_all_coins_data ---

@pytest.mark.asyncio
async def test_fetch_all_coins_blacklist_filtering(mock_config, mock_exchange, mock_api_calls):
    """
    (ГЛАВНЫЙ ТЕСТ)
    Проверяет, что монета (SOL) отфильтрована по базовому символу
    из черного списка (SOL).
    """
    mock_markets, mock_tickers = mock_api_calls
    
    # API вернет BTC и SOL
    mock_tickers.return_value = {
        'BTC/USDT': MOCK_TICKER_BTC,
        'SOL/USDT': MOCK_TICKER_SOL
    }
    
    # Черный список содержит БАЗОВЫЙ символ 'SOL'
    blacklist = {"SOL"}
    
    final_coin_list, _, _, skipped_coins = await fetch_all_coins_data(
        exchange_ids=['binanceusdm'],
        quote_currencies=['USDT'],
        blacklist=blacklist,
        log_prefix="[Test]"
    )
    
    # Проверка:
    # 1. В итоговом списке ТОЛЬКО BTC
    assert len(final_coin_list) == 1
    assert final_coin_list[0]['symbol'] == 'BTC/USDT'
    
    # 2. 'SOL' попал в 'skipped_coins'
    assert 'Blacklist' in skipped_coins
    assert 'Volume' not in skipped_coins # Убедимся, что не отфильтрован по объему
    
    # 3. 'skipped_coins' содержит ПОЛНЫЙ СИМВОЛ (full_symbol)
    assert skipped_coins['Blacklist'] == {"SOL/USDT:USDT"}

@pytest.mark.asyncio
async def test_fetch_all_coins_volume_filtering(mock_config, mock_exchange, mock_api_calls):
    """
    Проверяет, что монета (ADA) отфильтрована по 'MIN_VOLUME_24H_USD'.
    """
    mock_markets, mock_tickers = mock_api_calls
    
    # API вернет ADA (низкий объем) и BTC (высокий объем)
    mock_tickers.return_value = {
        'BTC/USDT': MOCK_TICKER_BTC,
        'ADA/USDT': MOCK_TICKER_ADA_LOW_VOL 
    }
    
    blacklist = set()
    
    final_coin_list, _, _, skipped_coins = await fetch_all_coins_data(
        exchange_ids=['binanceusdm'],
        quote_currencies=['USDT'],
        blacklist=blacklist,
        log_prefix="[Test]"
    )
    
    # Проверка:
    # 1. В итоговом списке ТОЛЬКО BTC
    assert len(final_coin_list) == 1
    assert final_coin_list[0]['symbol'] == 'BTC/USDT'
    
    # 2. 'ADA' попала в 'skipped_coins'
    assert 'Volume' in skipped_coins
    assert 'Blacklist' not in skipped_coins
    
    # 3. 'skipped_coins' содержит ПОЛНЫЙ СИМВОЛ (full_symbol)
    assert skipped_coins['Volume'] == {"ADA/USDT:USDT"}

@pytest.mark.asyncio
async def test_fetch_all_coins_aggregation(mocker, mock_config, mock_api_calls):
    """
    Тестирует агрегацию монеты (SOL) с двух бирж (Binance и Bybit).
    Проверяет, что выбран НАИБОЛЬШИЙ объем.
    """
    # --- Настройка Моков ---
    
    # Мок 1: Binance
    mock_ex_binance = MagicMock()
    mock_ex_binance.id = 'binanceusdm'
    
    # Мок 2: Bybit
    mock_ex_bybit = MagicMock()
    mock_ex_bybit.id = 'bybit'
    
    # Мокаем 'initialize_exchange', чтобы он вернул оба
    mocker.patch(
        "services.data_fetcher.initialize_exchange",
        new_callable=AsyncMock,
        side_effect=[mock_ex_binance, mock_ex_bybit]
    )
    
    # Мокаем 'fetch_markets'
    mock_markets = mocker.patch(
        "services.data_fetcher.fetch_markets",
        new_callable=AsyncMock,
        return_value=MOCK_MARKETS # Обе биржи вернут одинаковые рынки
    )
    
    # Мокаем 'fetch_tickers'
    # Binance (SOL, Объем 200M)
    tickers_binance = {'SOL/USDT': MOCK_TICKER_SOL} 
    # Bybit (SOL, Объем 300M - БОЛЬШЕ)
    ticker_sol_bybit = MOCK_TICKER_SOL.copy()
    ticker_sol_bybit['quoteVolume'] = 300_000_000.0
    tickers_bybit = {'SOL/USDT': ticker_sol_bybit}
    
    mocker.patch(
        "services.data_fetcher.fetch_tickers",
        new_callable=AsyncMock,
        side_effect=[tickers_binance, tickers_bybit]
    )

    # --- Вызов ---
    
    final_coin_list, _, _, skipped_coins = await fetch_all_coins_data(
        exchange_ids=['binanceusdm', 'bybit'],
        quote_currencies=['USDT'],
        blacklist=set(),
        log_prefix="[Test]"
    )
    
    # --- Проверки ---
    
    # 1. В списке только 1 монета (SOL)
    assert len(final_coin_list) == 1
    assert final_coin_list[0]['symbol'] == 'SOL/USDT'
    
    # 2. 'exchanges' содержит ОБЕ биржи
    assert sorted(final_coin_list[0]['exchanges']) == ['binanceusdm', 'bybit']
    
    # 3. (Главная проверка) 'volume_24h_usd' равен 300M (максимальный)
    assert final_coin_list[0]['volume_24h_usd'] == 300_000_000.0

# --- Тесты для fetch_all_ohlcv_data ---

@pytest.mark.asyncio
async def test_fetch_all_ohlcv_data(mocker):
    """
    Тестирует параллельную загрузку нескольких таймфреймов (1h, 4h).
    """
    mock_exchange = MagicMock()
    mock_exchange.parse8601.return_value = 1678886400000
    
    # Мокаем 'fetch_ohlcv'
    mock_api_call = mocker.patch(
        "services.data_fetcher.fetch_ohlcv",
        new_callable=AsyncMock,
        return_value=MOCK_OHLCV_DATA
    )
    
    tf_config = {'1h': 7, '4h': 30}
    
    result_map = await fetch_all_ohlcv_data(
        mock_exchange, 
        "BTC/USDT", 
        tf_config,
        "[Test]"
    )
    
    # Проверки:
    # 1. 'fetch_ohlcv' был вызван 2 раза (для 1h и 4h)
    assert mock_api_call.call_count == 2
    
    # 2. Результат - это словарь с 2 ключами
    assert isinstance(result_map, dict)
    assert len(result_map) == 2
    assert '1h' in result_map
    assert '4h' in result_map
    
    # 3. Значения - это pd.DataFrame
    assert isinstance(result_map['1h'], pd.DataFrame)
    assert len(result_map['1h']) == len(MOCK_OHLCV_DATA)
    assert result_map['1h'].index.name == 'timestamp'