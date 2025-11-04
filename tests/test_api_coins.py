# tests/test_api_coins.py

import pytest
import pytest_asyncio
import httpx 
from httpx import ASGITransport 
import asyncio
from unittest.mock import MagicMock, AsyncMock
import os # <-- (ИЗМЕНЕНИЕ №1)

# --- (Импорты из 'api.router' и 'security' для настройки) ---
from api.router import app
from api.security import verify_token

# --- (Импорты из тестируемого файла) ---
# Импортируем утилиту для прямого теста
from api.endpoints.coins import _extract_base_symbol_from_full 

# --- Данные для моков ---
COIN_SOL = {
    "full_symbol": "SOL/USDT:USDT", 
    "symbol": "SOL/USDT", 
    "hurst_1h": 0.4
}
COIN_BTC = {
    "full_symbol": "BTC/USDT:USDT", 
    "symbol": "BTC/USDT", 
    "hurst_1h": 0.6
}
MOCK_ALL_COINS = [COIN_SOL, COIN_BTC]
MOCK_BLACKLIST = {"SOL"} # Черный список (базовый символ)

# --- (Копия фикстур для изоляции теста) ---

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

def mock_verify_token():
    return True

@pytest_asyncio.fixture(scope="function")
async def async_client(mocker):
    """
    Создает async-клиент и отключает безопасность 
    (Depends(verify_token))
    """
    app.dependency_overrides[verify_token] = mock_verify_token
    
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ 'Connection Refused' ---
    mocker.patch('psycopg2.connect', return_value=MagicMock())
    mocker.patch('services.mongo_service.MongoClient', return_value=MagicMock())
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    # Мокаем зависимости, которые используют ВСЕ тесты в этом файле
    
    # 1. Мокаем кэш
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=MOCK_ALL_COINS
    )
    
    # 2. Мокаем блэклист
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        new_callable=AsyncMock,
        return_value=MOCK_BLACKLIST
    )
    
    # 3. Мокаем config.DATABASE_SCHEMA (для CSV)
    mocker.patch(
        "config.DATABASE_SCHEMA", 
        {"full_symbol": "TEXT", "symbol": "TEXT", "hurst_1h": "FLOAT"}
    )

    # (Исправлено) Используем 'transport=ASGITransport(app=app)'
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), 
        base_url="http://test"
    ) as client:
        yield client
    
    del app.dependency_overrides[verify_token]


# --- Тесты ---

def test_extract_base_symbol_from_full_util():
    """
    (ГЛАВНЫЙ ТЕСТ №1)
    Прямое тестирование утилиты '_extract_base_symbol_from_full'.
    """
    assert _extract_base_symbol_from_full("SOL/USDT:USDT") == "SOL"
    assert _extract_base_symbol_from_full("1000PEPE/USDT:USDT") == "1000PEPE"

@pytest.mark.asyncio
async def test_get_filtered_coins_json_blacklist_works(async_client, mocker):
    """
    (ГЛАВНЫЙ ТЕСТ №2)
    Проверяет GET /coins/filtered (JSON).
    Убеждается, что 'SOL/USDT:USDT' отфильтрован 'SOL' из блэклиста.
    """
    # Моки уже установлены в 'async_client'
    
    response = await async_client.get("/coins/filtered")
    
    assert response.status_code == 200
    
    data = response.json()
    
    # Проверяем, что 'SOL' был отфильтрован, а 'BTC' остался
    assert data["count"] == 1
    assert len(data["coins"]) == 1
    assert data["coins"][0]["full_symbol"] == "BTC/USDT:USDT"

@pytest.mark.asyncio
async def test_get_filtered_coins_csv_blacklist_works(async_client, mocker):
    """
    (ГЛАВНЫЙ ТЕСТ №3)
    Проверяет GET /coins/filtered/csv (CSV).
    Убеждается, что 'SOL/USDT:USDT' отфильтрован.
    """
    # Моки уже установлены в 'async_client'
    
    response = await async_client.get("/coins/filtered/csv")
    
    assert response.status_code == 200
    
    # --- (ИЗМЕНЕНИЕ №2) ИСПРАВЛЕНИЕ KeyError: 'media-type' ---
    assert "text/csv" in response.headers["content-type"]
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    csv_content = response.text
    assert "full_symbol,symbol,hurst_1h" in csv_content
    assert "BTC/USDT:USDT,BTC/USDT,0.6" in csv_content
    assert "SOL/USDT:USDT" not in csv_content

@pytest.mark.asyncio
async def test_get_filtered_coins_csv_all_filtered_404(async_client, mocker):
    """
    (ГЛАВНЫЙ ТЕСТ №4)
    Проверяет GET /coins/filtered/csv (CSV).
    Убеждается, что вернется 404, если ВСЕ монеты отфильтрованы.
    """
    # Переопределяем моки для этого теста
    
    # 1. Кэш вернет ТОЛЬКО SOL
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=[COIN_SOL]
    )
    
    # 2. Блэклист вернет {"SOL"}
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        new_callable=AsyncMock,
        return_value={"SOL"}
    )
    
    response = await async_client.get("/coins/filtered/csv")
    
    # (Ожидаем 404, так как мы исправили coins.py)
    assert response.status_code == 404
    assert "No data found after filtering" in response.text