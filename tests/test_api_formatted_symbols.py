# tests/test_api_formatted_symbols.py

import pytest
import pytest_asyncio
import httpx 
from httpx import ASGITransport 
import asyncio
from unittest.mock import MagicMock, AsyncMock
import os

# --- (Импорты для настройки) ---
from api.router import app
from api.security import verify_token

# --- (Импорты для МОДУЛЬНЫХ тестов) ---
from api.endpoints.formatted_symbols import (
    _format_tv_symbol,
    _format_tv_exchange
)

# --- Фикстуры (Настройка тестов API) ---

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

def mock_verify_token():
    return True

@pytest_asyncio.fixture(scope="function")
async def async_client(mocker):
    app.dependency_overrides[verify_token] = mock_verify_token
    mocker.patch('psycopg2.connect', return_value=MagicMock())
    mocker.patch('services.mongo_service.MongoClient', return_value=MagicMock())
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), 
        base_url="http://test"
    ) as client:
        yield client
    
    del app.dependency_overrides[verify_token]

# ============================================================================
# === (ЗАДАЧА 1) ТЩАТЕЛЬНЫЕ ТЕСТЫ ХЕЛПЕРОВ ===
# ============================================================================

def test_format_tv_symbol_helper():
    """
    (ИЗМЕНЕНИЕ №2)
    Тестирует хелпер '_format_tv_symbol' (убрать :... и /)
    """
    # Главный случай (как вы указали)
    assert _format_tv_symbol("BTC/USDT:USDT") == "BTCUSDT"
    
    # Другие случаи
    assert _format_tv_symbol("SOL/USDT:USDT") == "SOLUSDT"
    assert _format_tv_symbol("ETH/BTC:BTC") == "ETHBTC"
    assert _format_tv_symbol("1000PEPE/USDT:USDT") == "1000PEPEUSDT"
    
    # Случаи без :
    assert _format_tv_symbol("DOGE/USDT") == "DOGEUSDT"
    assert _format_tv_symbol("INVALID") == "INVALID"
    assert _format_tv_symbol("") == ""

def test_format_tv_exchange_helper():
    """
    Тестирует хелпер '_format_tv_exchange' (убрать usdm)
    (Этот тест не меняется)
    """
    assert _format_tv_exchange("binanceusdm") == "binance"
    assert _format_tv_exchange("bybit") == "bybit"

# ============================================================================
# === (ЗАДАЧА 2) ТЕСТЫ ЭНДПОИНТА (API) ===
# ============================================================================

# (ИЗМЕНЕНИЕ №2) Обновляем MOCK_CACHE_DATA, чтобы поле 'symbol' 
# соответствовало вашим данным
MOCK_CACHE_DATA = [
    # Случай 1 (Ваш пример: SOL)
    {
        "symbol": "SOL/USDT:USDT",
        "exchanges": ["binanceusdm", "bybit"]
    },
    # Случай 2 (Ваш пример: ATOM)
    {
        "symbol": "ATOM/USDT:USDT",
        "exchanges": ["bybit"]
    },
    # Случай 3 (Ваш пример: ALGO)
    {
        "symbol": "ALGO/USDT:USDT",
        "exchanges": ["binanceusdm"]
    },
    # Случай 4 (Без :USDT)
    {
        "symbol": "DOGE/USDT",
        "exchanges": []
    }
]

@pytest.mark.asyncio
async def test_get_formatted_symbols_success(async_client, mocker):
    """
    (ГЛАВНЫЙ ТЕСТ API)
    Проверяет GET /coins/formatted-symbols.
    """
    
    # 1. Мокаем 'get_cached_coins_data'
    mocker.patch(
        "api.endpoints.formatted_symbols.services.get_cached_coins_data",
        new_callable=AsyncMock,
        return_value=MOCK_CACHE_DATA
    )
    
    # 2. Вызываем API
    response = await async_client.get("/api/v1/coins/formatted-symbols")
    
    # 3. Проверки
    assert response.status_code == 200
    data = response.json()
    
    assert data["count"] == 4
    
    # --- (ИЗМЕНЕНИЕ №2) Проверяем ИСПРАВЛЕННЫЙ ожидаемый результат ---
    expected_data = [
        {"symbol": "SOLUSDT", "exchanges": ["binance", "bybit"]},
        {"symbol": "ATOMUSDT", "exchanges": ["bybit"]},
        {"symbol": "ALGOUSDT", "exchanges": ["binance"]},
        {"symbol": "DOGEUSDT", "exchanges": []},
    ]
    
    assert data["symbols"] == expected_data

@pytest.mark.asyncio
async def test_get_formatted_symbols_empty_cache(async_client, mocker):
    """
    Тестирует GET /coins/formatted-symbols, если кэш пуст.
    (Этот тест не меняется)
    """
    
    mocker.patch(
        "api.endpoints.formatted_symbols.services.get_cached_coins_data",
        new_callable=AsyncMock,
        return_value=[]
    )
    
    response = await async_client.get("/api/v1/coins/formatted-symbols")
    
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["symbols"] == []