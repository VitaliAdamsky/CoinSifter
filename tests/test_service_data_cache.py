# tests/test_service_data_cache.py

import pytest
import asyncio
import time
from unittest.mock import MagicMock, AsyncMock, patch

# Импортируем тестируемую функцию
from services.data_cache_service import get_cached_coins_data, DEFAULT_CACHE_TTL

# Импортируем "внутренности" модуля, чтобы сбросить кэш
from services import data_cache_service

# --- Данные для моков ---
MOCK_COIN_DATA_1 = [{"symbol": "BTC"}, {"symbol": "ETH"}]
MOCK_COIN_DATA_2 = [{"symbol": "SOL"}, {"symbol": "ADA"}]

# --- Фикстуры (Настройка тестов) ---

@pytest.fixture(autouse=True)
def reset_cache_state():
    """
    Сбрасывает глобальный кэш перед каждым тестом 
    для полной изоляции.
    """
    data_cache_service._cache = []
    data_cache_service._last_load_time = 0.0
    yield


@pytest.fixture
def mock_db_call(mocker):
    """
    Мокает (подменяет) функцию 'fetch_all_coins_from_db',
    которая вызывается кэшем.
    """
    # Мы используем patch('...fetch_all_coins_from_db'), 
    # потому что data_cache_service импортирует ее под этим именем.
    mock_func = mocker.patch(
        "services.data_cache_service.fetch_all_coins_from_db",
        return_value=MOCK_COIN_DATA_1
    )
    return mock_func

# --- Тесты ---

@pytest.mark.asyncio
async def test_cache_loads_once_on_first_call(mock_db_call):
    """
    Тест 1: Кэш загружается при первом вызове.
    Тест 2: Кэш НЕ загружается при втором вызове (данные свежие).
    """
    # 1. Первый вызов (кэш пуст)
    data1 = await get_cached_coins_data()
    
    # 2. Второй вызов (кэш "свежий")
    data2 = await get_cached_coins_data()
    
    # Проверки
    assert data1 == MOCK_COIN_DATA_1
    assert data2 == MOCK_COIN_DATA_1
    
    # Главная проверка: БД была вызвана ТОЛЬКО ОДИН РАЗ
    mock_db_call.assert_called_once()

@pytest.mark.asyncio
async def test_cache_race_condition_lock(mock_db_call):
    """
    Тестирует 'asyncio.Lock' (гонку потоков).
    Два одновременных запроса должны вызвать БД только ОДИН раз.
    """
    
    # Запускаем два вызова "одновременно"
    results = await asyncio.gather(
        get_cached_coins_data(),
        get_cached_coins_data()
    )
    
    # Проверки
    assert results[0] == MOCK_COIN_DATA_1
    assert results[1] == MOCK_COIN_DATA_1
    
    # Главная проверка: БД была вызвана ТОЛЬКО ОДИН РАЗ
    mock_db_call.assert_called_once()

@pytest.mark.asyncio
async def test_cache_ttl_expiration(mock_db_call, mocker):
    """
    Тестирует "протухание" кэша (TTL).
    Второй вызов (после TTL) должен снова вызвать БД.
    """
    
    start_time = time.time()
    
    # 1. Мокаем 'time.time', чтобы контролировать время
    mock_time = mocker.patch('time.time')
    
    # 2. Вызов 1 (Загрузка кэша)
    mock_time.return_value = start_time
    data1 = await get_cached_coins_data()
    
    assert data1 == MOCK_COIN_DATA_1
    mock_db_call.assert_called_once() # БД вызвана 1 раз

    # 3. Вызов 2 (Кэш еще "свежий")
    mock_time.return_value = start_time + (DEFAULT_CACHE_TTL - 10)
    data2 = await get_cached_coins_data()
    
    assert data2 == MOCK_COIN_DATA_1
    mock_db_call.assert_called_once() # БД НЕ была вызвана (всего 1 раз)

    # 4. Вызов 3 (Кэш "протух")
    # Перематываем время
    mock_time.return_value = start_time + DEFAULT_CACHE_TTL + 5
    
    # Меняем ответ от БД, чтобы убедиться, что данные обновились
    mock_db_call.return_value = MOCK_COIN_DATA_2
    
    data3 = await get_cached_coins_data()
    
    # Проверки
    assert data3 == MOCK_COIN_DATA_2 # Вернулись НОВЫЕ данные
    
    # Главная проверка: БД была вызвана ВТОРОЙ РАЗ
    assert mock_db_call.call_count == 2

@pytest.mark.asyncio
async def test_cache_force_reload(mock_db_call):
    """
    Тестирует 'force_reload=True'.
    БД должна быть вызвана, даже если кэш "свежий".
    """
    # 1. Вызов 1 (Загрузка кэша)
    data1 = await get_cached_coins_data()
    assert data1 == MOCK_COIN_DATA_1
    mock_db_call.assert_called_once() # БД вызвана 1 раз

    # 2. Вызов 2 (Принудительная перезагрузка)
    # Меняем ответ от БД
    mock_db_call.return_value = MOCK_COIN_DATA_2
    
    data2 = await get_cached_coins_data(force_reload=True)
    
    # Проверки
    assert data2 == MOCK_COIN_DATA_2 # Вернулись НОВЫЕ данные
    
    # Главная проверка: БД была вызвана ВТОРОЙ РАЗ
    assert mock_db_call.call_count == 2

@pytest.mark.asyncio
async def test_cache_returns_stale_on_error(mock_db_call, mocker):
    """
    Тестирует, что кэш вернет "старые" данные, если обновление 
    БД провалилось (упало с ошибкой).
    """
    start_time = time.time()
    mock_time = mocker.patch('time.time')
    
    # 1. Вызов 1 (Успешная загрузка)
    mock_time.return_value = start_time
    data1 = await get_cached_coins_data()
    assert data1 == MOCK_COIN_DATA_1
    mock_db_call.assert_called_once()

    # 2. Настраиваем "протухание" кэша и ошибку БД
    mock_time.return_value = start_time + DEFAULT_CACHE_TTL + 5
    mock_db_call.side_effect = Exception("DB Boom!")
    
    # 3. Вызов 2 (Обновление падает)
    data2 = await get_cached_coins_data()
    
    # Проверки
    # Главная проверка: Функция НЕ упала, а вернула СТАРЫЕ данные
    assert data2 == MOCK_COIN_DATA_1 
    
    # БД была вызвана 2 раза (первый успех, вторая ошибка)
    assert mock_db_call.call_count == 2