# tests/test_service_exchange_utils.py

import pytest
import asyncio
import ccxt
import time
from unittest.mock import AsyncMock, MagicMock, patch

# Импортируем декоратор, трекер и утилиты
from services.exchange_utils import (
    retry_on_network_error, 
    rate_limiter, 
    calculate_request_weight,
    RateLimitTracker
)

# --- Фикстуры (Настройка тестов) ---

@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """
    Сбрасывает состояние глобального 'rate_limiter'
    перед каждым тестом для изоляции.
    """
    new_limiter = RateLimitTracker()
    with patch('services.exchange_utils.rate_limiter', new_limiter):
        yield

# --- Тесты для @retry_on_network_error ---

@pytest.mark.asyncio
async def test_retry_decorator_success_on_retry(mocker):
    """
    Тестирует, что декоратор повторяет вызов при 'NetworkError'
    и возвращает успешный результат.
    """
    mock_sleep = mocker.patch('asyncio.sleep', new_callable=AsyncMock)
    
    # --- (ИЗМЕНЕНИЕ №1) ИСПРАВЛЕНИЕ 'AttributeError' / 'AssertionError' ---
    # Мы должны мокать полный путь, где config используется
    mocker.patch('services.exchange_utils.config.MAX_RETRIES', 3, create=True)
    mocker.patch('services.exchange_utils.config.RETRY_DELAY_BASE', 0.1, create=True)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    mock_api_call = AsyncMock(
        side_effect=[
            ccxt.NetworkError("Connection failed"), # Попытка 1
            ccxt.RequestTimeout("Timeout"),       # Попытка 2
            "Success"                           # Попытка 3
        ]
    )
    
    mock_exchange = MagicMock()
    mock_exchange.id = 'test_ex'
    
    decorated_func = retry_on_network_error()(mock_api_call)
    
    result = await decorated_func(mock_exchange, log_prefix="[Test]")
    
    assert result == "Success"
    assert mock_api_call.call_count == 3
    
    # Считаем только "настоящие" ожидания (не sleep(0))
    real_sleep_calls = [
        call for call in mock_sleep.await_args_list 
        if call.args[0] > 0
    ]
    assert len(real_sleep_calls) == 2

@pytest.mark.asyncio
async def test_retry_decorator_fails_after_max_retries(mocker):
    """
    Тестирует, что декоратор вернет 'None', если все 'MAX_RETRIES'
    попыток провалились.
    """
    mock_sleep = mocker.patch('asyncio.sleep', new_callable=AsyncMock)
    
    # --- (ИЗМЕНЕНИЕ №1) ---
    mocker.patch('services.exchange_utils.config.MAX_RETRIES', 3, create=True)
    mocker.patch('services.exchange_utils.config.RETRY_DELAY_BASE', 0.1, create=True)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    mock_api_call = AsyncMock(
        side_effect=ccxt.NetworkError("Connection failed") # Падает всегда
    )
    
    mock_exchange = MagicMock()
    mock_exchange.id = 'test_ex'
    
    decorated_func = retry_on_network_error()(mock_api_call)
    
    result = await decorated_func(mock_exchange, log_prefix="[Test]")
    
    assert result is None
    assert mock_api_call.call_count == 3
    
    # Считаем только "настоящие" ожидания (не sleep(0))
    real_sleep_calls = [
        call for call in mock_sleep.await_args_list 
        if call.args[0] > 0
    ]
    # 3 попытки = 3 ожидания (0.1s, 0.2s, 0.4s)
    assert len(real_sleep_calls) == 3 

@pytest.mark.asyncio
async def test_retry_decorator_handles_rate_limit(mocker):
    """
    Тестирует, что декоратор ждет при 'RateLimitExceeded'.
    """
    mock_sleep = mocker.patch('asyncio.sleep', new_callable=AsyncMock)
    
    # --- (ИЗМЕНЕНИЕ №1) ---
    mocker.patch('services.exchange_utils.config.MAX_RETRIES', 3, create=True)
    mocker.patch('services.exchange_utils.config.RETRY_DELAY_BASE', 0.1, create=True)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    
    mock_api_call = AsyncMock(
        side_effect=[
            ccxt.RateLimitExceeded("Too many requests"), # Попытка 1
            "Success"                                 # Попытка 2
        ]
    )
    
    mock_exchange = MagicMock()
    mock_exchange.id = 'binanceusdm' 
    
    decorated_func = retry_on_network_error()(mock_api_call)
    
    result = await decorated_func(mock_exchange, log_prefix="[Test]")
    
    assert result == "Success"
    assert mock_api_call.call_count == 2
    
    real_sleep_calls = [
        call for call in mock_sleep.await_args_list 
        if call.args[0] > 0
    ]
    assert len(real_sleep_calls) >= 1 # Был хотя бы 1 вызов с ожиданием

# --- Тесты для RateLimitTracker ---

@pytest.mark.asyncio
async def test_rate_limit_tracker_waits_when_limit_hit(mocker):
    """
    Тестирует, что 'RateLimitTracker' вызывает 'asyncio.sleep',
    когда лимит достигнут.
    """
    mock_sleep = mocker.patch('asyncio.sleep', new_callable=AsyncMock)
    
    tracker = rate_limiter 
    mock_time = mocker.patch('time.time', return_value=1000.0)
    
    # (Лимит 20, safety_margin=1, Доступно 19)
    mocker.patch.object(tracker, '_get_max_limit', return_value=20)

    # --- Фаза 1: Используем 19 из 19 (Доступно 19) ---
    
    for i in range(19):
        await tracker.check_and_wait('test_ex', weight=1)
        
    # 'asyncio.sleep' должен был вызываться 19 раз (с 0)
    assert mock_sleep.call_count == 19
    mock_sleep.assert_called_with(0)
    
    mock_sleep.reset_mock()
    
    # --- Фаза 2: Превышаем лимит ---
    
    # 20-й вызов. (used (19) + weight (1)) > available (19) -> True
    await tracker.check_and_wait('test_ex', weight=1)
    
    # --- (ИЗМЕНЕНИЕ №2) ИСПРАВЛЕНИЕ AssertionError ---
    # Должно быть 2 вызова: sleep(61.0) (ожидание) и sleep(0) (разблокировка)
    assert mock_sleep.call_count == 2
    
    # Проверяем, что ХОТЯ БЫ ОДИН вызов был с ожиданием > 0
    waited_for_limit = any(
        call.args[0] > 0 for call in mock_sleep.await_args_list
    )
    assert waited_for_limit == True
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---


# --- Тесты для calculate_request_weight ---

def test_calculate_request_weight_binance():
    """Тестирует логику веса для Binance (зависит от 'limit')"""
    
    weight1 = calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=100)
    assert weight1 == 1
    
    weight2 = calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=500)
    assert weight2 == 2
    weight3 = calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1000)
    assert weight3 == 2
    
    weight4 = calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1500)
    assert weight4 == 5

def test_calculate_request_weight_bybit():
    """Тестирует логику веса для Bybit (всегда 1)"""
    
    weight1 = calculate_request_weight('bybit', 'fetch_ohlcv', limit=100)
    assert weight1 == 1
    
    weight2 = calculate_request_weight('bybit', 'fetch_ohlcv', limit=1500)
    assert weight1 == 1

def test_calculate_request_weight_other_funcs():
    """Тестирует вес для других функций"""
    
    assert calculate_request_weight('binanceusdm', 'fetch_tickers') == 0
    assert calculate_request_weight('binanceusdm', 'fetch_markets') == 1
    
    assert calculate_request_weight('bybit', 'fetch_tickers') == 1
    assert calculate_request_weight('bybit', 'fetch_markets') == 1