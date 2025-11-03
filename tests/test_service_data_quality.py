# tests/test_service_data_quality.py

import pytest
import pandas as pd
from unittest.mock import AsyncMock

# Импортируем тестируемую функцию
from services.data_quality_service import get_data_quality_report

# --- Данные для моков ---

# Данные с "дыркой" (пропуском)
MOCK_DATA_WITH_NAN = [
    {
        "symbol": "BTC",
        "hurst_1h": 0.5,
        "entropy_1h": 1.2
    },
    {
        "symbol": "SOL",
        "hurst_1h": None,  # <-- Вот пропуск
        "entropy_1h": 1.4
    }
]

# "Чистые" данные
MOCK_DATA_CLEAN = [
    {
        "symbol": "BTC",
        "hurst_1h": 0.5,
        "entropy_1h": 1.2
    },
    {
        "symbol": "ETH",
        "hurst_1h": 0.6,
        "entropy_1h": 1.3
    }
]


# --- Фикстуры (Настройка тестов) ---

@pytest.fixture
def mock_cache_call(mocker):
    """
    Мокает (подменяет) функцию 'get_cached_coins_data',
    которая является зависимостью.
    """
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ KeyError ---
    # Мы должны использовать 'new_callable=AsyncMock', чтобы 'await'
    # на 'get_cached_coins_data' вернул 'return_value' (список),
    # а не сам мок-объект.
    mock_func = mocker.patch(
        "services.data_quality_service.get_cached_coins_data",
        new_callable=AsyncMock
    )
    # Устанавливаем 'return_value' по умолчанию
    mock_func.return_value = MOCK_DATA_WITH_NAN
    
    # Возвращаем сам мок (patch object), чтобы тесты 
    # могли его изменять
    return mock_func
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---


# --- Тесты ---

@pytest.mark.asyncio
async def test_data_quality_success(mock_cache_call):
    """
    Тестирует, что 'missing_data_report' ПРАВИЛЬНО находит
    пропущенные значения (None/NaN).
    """
    # Мок уже настроен на возврат MOCK_DATA_WITH_NAN
    # (через mock_cache_call.return_value = MOCK_DATA_WITH_NAN)
    
    report = await get_data_quality_report()
    
    # Проверяем общие данные
    assert report["total_coins"] == 2
    assert "error" not in report
    
    # Главная проверка:
    missing_report = report["missing_data_report"]
    
    # 'hurst_1h' должен содержать 'SOL'
    assert "hurst_1h" in missing_report
    assert missing_report["hurst_1h"] == ["SOL"]
    
    # 'entropy_1h' не должен содержать пропусков
    assert "entropy_1h" in missing_report
    assert missing_report["entropy_1h"] == []
    
    # 'symbol' не должен содержать пропусков
    assert "symbol" in missing_report
    assert missing_report["symbol"] == []

@pytest.mark.asyncio
async def test_data_quality_no_missing(mock_cache_call):
    """
    Тестирует, что 'missing_data_report' возвращает пустые
    списки, если пропусков нет.
    """
    # Меняем мок, чтобы он вернул "чистые" данные
    mock_cache_call.return_value = MOCK_DATA_CLEAN
    
    report = await get_data_quality_report()
    
    assert report["total_coins"] == 2
    assert "error" not in report
    
    # Главная проверка:
    missing_report = report["missing_data_report"]
    assert missing_report["hurst_1h"] == []
    assert missing_report["entropy_1h"] == []
    assert missing_report["symbol"] == []

@pytest.mark.asyncio
async def test_data_quality_empty_cache(mock_cache_call):
    """
    Тестирует, что функция возвращает ошибку, если кэш пуст (None или []).
    """
    mock_cache_call.return_value = [] # Кэш пуст
    
    report = await get_data_quality_report()
    
    # Функция должна вернуть 'error' (из блока 'if not data_list:')
    assert "error" in report
    assert "Не удалось загрузить данные" in report["error"]
    assert report["total_coins"] == 0

@pytest.mark.asyncio
async def test_data_quality_service_exception(mock_cache_call):
    """
    Тестирует, что функция возвращает ошибку, если 'get_cached_coins_data'
    падает с исключением.
    """
    mock_cache_call.side_effect = Exception("Cache Boom!")
    
    report = await get_data_quality_report()
    
    # Функция должна перехватить ошибку и вернуть 'error'
    assert "error" in report
    assert "Критическая ошибка: Cache Boom!" in report["error"]
    assert report["total_coins"] == 0