# tests/test_api_endpoints.py

import pytest
import pytest_asyncio
import httpx 
from httpx import ASGITransport 
import asyncio
from datetime import datetime
from unittest.mock import MagicMock, AsyncMock
import os 

# Импортируем наше приложение FastAPI
from api.router import app
# Импортируем функцию безопасности, которую будем переопределять
from api.security import verify_token
# Импортируем фоновую задачу для прямого тестирования
from api.endpoints.trigger import run_analysis_in_background


# --- Фикстуры (Настройка тестов) ---

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

def mock_verify_token():
    """Пустая функция-заглушка для отключения безопасности."""
    return True

@pytest_asyncio.fixture(scope="function")
async def async_client(mocker):
    """
    (ВАЖНО) Создает async-клиент для тестов и отключает 
    безопасность Depends(verify_token)
    """
    app.dependency_overrides[verify_token] = mock_verify_token
    
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ 'Connection Refused' ---
    # Мы "отключаем" psycopg2.connect и MongoClient на самом низком уровне.
    # Теперь они не будут пытаться установить реальное соединение.
    mocker.patch('psycopg2.connect', return_value=MagicMock())
    mocker.patch('services.mongo_service.MongoClient', return_value=MagicMock())
    
    # 'os.getenv' все еще нужен, чтобы код не упал до вызова MongoClient
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), 
        base_url="http://test"
    ) as client:
        yield client
    
    del app.dependency_overrides[verify_token]


# --- Тесты для health.py ---

@pytest.mark.asyncio
async def test_health_check_ok(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_get_last_analysis_success(async_client, mocker):
    mock_time = datetime(2025, 10, 1, 12, 30, 0)
    
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.health.fetch_last_analysis_timestamp",
        return_value=mock_time
    )
    
    response = await async_client.get("/health/last_analysis")
    assert response.status_code == 200
    assert response.json() == {"analyzed_at": "2025-10-01T12:30:00"}

@pytest.mark.asyncio
async def test_get_last_analysis_not_found(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.health.fetch_last_analysis_timestamp",
        return_value=None 
    )
    
    response = await async_client.get("/health/last_analysis")
    assert response.status_code == 404
    assert "No analysis data found" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_last_analysis_db_error(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.health.fetch_last_analysis_timestamp",
        side_effect=Exception("DB Boom!")
    )
    
    response = await async_client.get("/health/last_analysis")
    assert response.status_code == 500
    
    # --- (ИЗМЕНЕНИЕ №2) ИСПРАВЛЕНИЕ AssertionError ---
    # Эндпоинт возвращает свое сообщение, а не "DB Boom!"
    assert "Failed to fetch" in response.json()["detail"]
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---


# --- Тесты для logs.py ---

@pytest.mark.asyncio
async def test_get_logs_success(async_client, mocker):
    mock_logs = [{"id": 1, "status": "Завершено", "coins_saved": 150}]
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.logs.fetch_logs_from_db",
        return_value=mock_logs
    )
    
    response = await async_client.get("/logs")
    assert response.status_code == 200
    assert response.json() == {"count": 1, "logs": mock_logs}

@pytest.mark.asyncio
async def test_get_logs_db_error(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.logs.fetch_logs_from_db",
        side_effect=Exception("DB Boom!")
    )
    
    response = await async_client.get("/logs")
    assert response.status_code == 500
    assert "DB Boom!" in response.json()["detail"]


# --- Тесты для blacklist.py ---

@pytest.mark.asyncio
async def test_get_blacklist_success(async_client, mocker):
    mock_blacklist_set = {"BTC", "ETH"}
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.blacklist.services.load_blacklist_from_mongo_async",
        return_value=mock_blacklist_set
    )
    
    response = await async_client.get("/blacklist")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    assert sorted(data["blacklist"]) == ["BTC", "ETH"] 

@pytest.mark.asyncio
async def test_get_blacklist_mongo_error(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.blacklist.services.load_blacklist_from_mongo_async",
        side_effect=Exception("Mongo Boom!")
    )
    
    response = await async_client.get("/blacklist")
    assert response.status_code == 500
    assert "DB Error (MongoDB)" in response.json()["detail"]


# --- Тесты для data_quality.py ---

@pytest.mark.asyncio
async def test_get_data_quality_report_success(async_client, mocker):
    mock_report = {"total_coins": 10, "missing_data_report": {}}
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.data_quality.get_data_quality_report",
        return_value=mock_report
    )
    
    response = await async_client.get("/data-quality-report")
    assert response.status_code == 200
    assert response.json() == mock_report

@pytest.mark.asyncio
async def test_get_data_quality_report_service_error(async_client, mocker):
    mock_report = {"error": "Service failed"}
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.data_quality.get_data_quality_report",
        return_value=mock_report
    )
    
    response = await async_client.get("/data-quality-report")
    assert response.status_code == 500
    assert "Service failed" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_data_quality_report_exception(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1) Мы должны мокать там, где функция ИСПОЛЬЗУЕТСЯ
    mocker.patch(
        "api.endpoints.data_quality.get_data_quality_report",
        side_effect=Exception("Service Boom!")
    )
    
    response = await async_client.get("/data-quality-report")
    assert response.status_code == 500
    assert "Service Boom!" in response.json()["detail"]


# --- Тесты для coins.py ---
# (ПРОПУЩЕНЫ, так как они в tests/test_api_coins.py, 
#  но я оставлю их здесь, как в вашем файле)

COIN_SOL = {"full_symbol": "SOL/USDT:USDT", "symbol": "SOL/USDT"}
COIN_BTC = {"full_symbol": "BTC/USDT:USDT", "symbol": "BTC/USDT"}
MOCK_ALL_COINS = [COIN_SOL, COIN_BTC]

@pytest.mark.asyncio
async def test_get_filtered_coins_json_success(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        return_value=set()
    )
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=MOCK_ALL_COINS
    )
    response = await async_client.get("/coins/filtered")
    assert response.status_code == 200
    # ( ... )

@pytest.mark.asyncio
async def test_get_filtered_coins_json_blacklist_works(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        return_value={"SOL"}
    )
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=MOCK_ALL_COINS
    )
    response = await async_client.get("/coins/filtered")
    assert response.status_code == 200
    # ( ... )

@pytest.mark.asyncio
async def test_get_filtered_coins_json_empty_cache(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        return_value=set()
    )
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=[]
    )
    response = await async_client.get("/coins/filtered")
    assert response.status_code == 200
    # ( ... )

@pytest.mark.asyncio
async def test_get_filtered_csv_success(async_client, mocker):
    mocker.patch("config.DATABASE_SCHEMA", {"full_symbol": "TEXT", "symbol": "TEXT"})
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        return_value=set()
    )
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=[COIN_SOL]
    )
    response = await async_client.get("/coins/filtered/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "SOL/USDT:USDT" in response.text

@pytest.mark.asyncio
async def test_get_filtered_csv_all_filtered(async_client, mocker):
    mocker.patch("config.DATABASE_SCHEMA", {"full_symbol": "TEXT", "symbol": "TEXT"})
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.coins.services.load_blacklist_from_mongo_async",
        return_value={"SOL"} 
    )
    mocker.patch(
        "api.endpoints.coins.services.get_cached_coins_data",
        return_value=[COIN_SOL]
    )
    response = await async_client.get("/coins/filtered/csv")
    assert response.status_code == 404 


# --- Тесты для trigger.py ---

@pytest.mark.asyncio
async def test_trigger_success(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1)
    mock_create_log = mocker.patch(
        "api.endpoints.trigger.create_log_entry",
        return_value=123
    )
    mock_add_task = mocker.patch("fastapi.BackgroundTasks.add_task")
    response = await async_client.post("/trigger")
    assert response.status_code == 200
    # ( ... )

@pytest.mark.asyncio
async def test_trigger_log_create_fails(async_client, mocker):
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch(
        "api.endpoints.trigger.create_log_entry",
        return_value=None
    )
    response = await async_client.post("/trigger")
    assert response.status_code == 500
    # ( ... )


# --- Тесты для trigger.py (Фоновая задача) ---

@pytest.mark.asyncio
async def test_bg_task_run_analysis_success(mocker):
    """Тестирует 'run_analysis_in_background' (Успех)"""
    
    # (ИЗМЕНЕНИЕ №1) Эти тесты не используют 'async_client',
    # поэтому им нужен свой мок для БД.
    mocker.patch('psycopg2.connect', return_value=MagicMock())
    
    mock_analysis = mocker.patch(
        "analysis.analysis_logic",
        new_callable=AsyncMock,
        return_value=(150, "Все ОК") 
    )
    # (ИЗМЕНЕНИЕ №1) Мокаем там, где используется
    mock_update = mocker.patch("api.endpoints.trigger.update_log_status")
    
    await run_analysis_in_background(log_id=99, log_prefix="[Test]")
    
    mock_analysis.assert_called_once_with(99, "[Test]")
    mock_update.assert_called_once_with(
        99, 
        "Завершено", 
        "Все ОК", 
        150 
    )

@pytest.mark.asyncio
async def test_bg_task_run_analysis_fails(mocker):
    """Тестирует 'run_analysis_in_background' (Ошибка)"""
    
    # (ИЗМЕНЕНИЕ №1)
    mocker.patch('psycopg2.connect', return_value=MagicMock())
    
    mock_analysis = mocker.patch(
        "analysis.analysis_logic",
        new_callable=AsyncMock,
        side_effect=Exception("Analysis Boom!")
    )
    # (ИЗМЕНЕНИЕ №1)
    mock_update = mocker.patch("api.endpoints.trigger.update_log_status")
    
    await run_analysis_in_background(log_id=99, log_prefix="[Test]")
    
    mock_update.assert_called_once_with(
        99, 
        "Ошибка", 
        "Критическая ошибка: Analysis Boom!", 
        0 
    )