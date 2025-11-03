# tests/test_service_mongo.py

import pytest
import asyncio
from unittest.mock import MagicMock, patch

# Импортируем тестируемую функцию
from services.mongo_service import load_blacklist_from_mongo_async, get_mongo_client, close_mongo_client

# --- Фикстуры (Настройка тестов) ---

@pytest.fixture(autouse=True)
def reset_mongo_client():
    """
    Автоматически сбрасывает (закрывает) синглтон _mongo_client 
    после каждого теста, чтобы тесты были изолированы.
    """
    yield
    # Выполняем очистку после теста
    close_mongo_client("[Test Cleanup]")

# --- Тесты ---

@pytest.mark.asyncio
async def test_load_blacklist_success(mocker):
    """
    Тестирует успешную загрузку черного списка.
    Проверяет, что функция возвращает 'set'.
    """
    # 1. Создаем полный мок клиента MongoDB
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()
    
    # 2. Настраиваем, что вернет .find()
    # (Mongo возвращает список словарей)
    mock_blacklist_docs = [
        {"symbol": "BTC"},
        {"symbol": "SOL"},
        {} # Пустой документ для проверки устойчивости
    ]
    mock_collection.find.return_value = mock_blacklist_docs
    
    # 3. Связываем моки
    mock_db.blacklist = mock_collection
    mock_client.general = mock_db
    
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ AssertionError ---
    # Мокаем 'os.getenv', чтобы он вернул фейковый URL.
    # Это необходимо, чтобы 'get_mongo_client' не вернул 'None'
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    # 4. Мокаем 'MongoClient' из pymongo, чтобы он вернул наш мок
    mocker.patch("services.mongo_service.MongoClient", return_value=mock_client)

    # 5. Вызываем тестируемую функцию
    blacklist = await load_blacklist_from_mongo_async(log_prefix="[Test]")
    
    # 6. Проверки
    # Проверяем, что 'find' был вызван правильно
    mock_collection.find.assert_called_once_with({}, {'_id': 0, 'symbol': 1})
    
    # Главная проверка: функция должна вернуть 'set'
    assert isinstance(blacklist, set)
    
    # Проверяем содержимое
    assert len(blacklist) == 2
    assert "SOL" in blacklist
    assert "BTC" in blacklist

@pytest.mark.asyncio
async def test_load_blacklist_connection_error(mocker):
    """
    Тестирует случай, когда MongoClient не может подключиться.
    Проверяет, что функция возвращает пустой 'set'.
    """
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ AssertionError ---
    # Мокаем 'os.getenv', чтобы он вернул фейковый URL.
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    # 1. Мокаем 'MongoClient', чтобы он вызвал ошибку при создании
    mocker.patch(
        "services.mongo_service.MongoClient", 
        side_effect=Exception("Connection Failed")
    )
    
    # 2. Вызываем тестируемую функцию
    blacklist = await load_blacklist_from_mongo_async(log_prefix="[Test]")
    
    # 3. Проверки
    # Главная проверка: функция должна вернуть ПУСТОЙ 'set'
    assert isinstance(blacklist, set)
    assert len(blacklist) == 0

@pytest.mark.asyncio
async def test_mongo_client_singleton(mocker):
    """
    Тестирует, что 'get_mongo_client' создает клиент (пул) только один раз.
    """
    mock_client_instance = MagicMock()
    
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ AssertionError ---
    mocker.patch('os.getenv', return_value='mongodb://fake-url-for-test')
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    mock_mongo_client_class = mocker.patch(
        "services.mongo_service.MongoClient", 
        return_value=mock_client_instance
    )
    
    # Вызываем 3 раза подряд
    client1 = get_mongo_client("[Test 1]")
    client2 = get_mongo_client("[Test 2]")
    client3 = get_mongo_client("[Test 3]")
    
    # Проверяем, что все вернули ОДИН И ТОТ ЖЕ экземпляр
    assert client1 is mock_client_instance
    assert client2 is mock_client_instance
    assert client3 is mock_client_instance
    
    # Главная проверка: MongoClient (класс) был вызван только 1 раз
    mock_mongo_client_class.assert_called_once()