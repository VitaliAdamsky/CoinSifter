# test_rate_limiter.py
"""
Юнит-тесты для Rate Limiter системы.
Все тесты работают с ЛОКАЛЬНЫМИ инстансами, без глобального состояния.
"""
import pytest
import asyncio
import time
from unittest.mock import AsyncMock

from services.exchange_utils import RateLimitTracker, calculate_request_weight


# ======================================================================
# --- БАЗОВЫЕ ТЕСТЫ ---
# ======================================================================

@pytest.mark.asyncio
async def test_rate_limiter_basic_tracking():
    """
    Проверяем базовое отслеживание лимитов.
    """
    tracker = RateLimitTracker()
    
    # 1. Первый запрос (инициализация)
    await tracker.check_and_wait('test_exchange', weight=10)
    
    assert 'test_exchange' in tracker.limits
    assert tracker.limits['test_exchange']['used'] == 10
    assert tracker.limits['test_exchange']['max'] == 1000  # Дефолт для неизвестных бирж
    assert tracker.limits['test_exchange']['total_requests'] == 1


@pytest.mark.asyncio
async def test_rate_limiter_binance_max_limit():
    """
    Проверяем, что для Binance лимит = 2400.
    """
    tracker = RateLimitTracker()
    
    await tracker.check_and_wait('binanceusdm', weight=1)
    
    assert tracker.limits['binanceusdm']['max'] == 2400


@pytest.mark.asyncio
async def test_rate_limiter_bybit_max_limit():
    """
    Проверяем, что для Bybit лимит = 120.
    """
    tracker = RateLimitTracker()
    
    await tracker.check_and_wait('bybit', weight=1)
    
    assert tracker.limits['bybit']['max'] == 120


@pytest.mark.asyncio
async def test_rate_limiter_accumulation():
    """
    Проверяем накопление весов.
    """
    tracker = RateLimitTracker()
    
    # Делаем 3 запроса с весами 5, 10, 15
    await tracker.check_and_wait('test_exchange', weight=5)
    await tracker.check_and_wait('test_exchange', weight=10)
    await tracker.check_and_wait('test_exchange', weight=15)
    
    # Должно накопиться 30
    assert tracker.limits['test_exchange']['used'] == 30
    assert tracker.limits['test_exchange']['total_requests'] == 3


@pytest.mark.asyncio
async def test_rate_limiter_reset_after_minute():
    """
    Проверяем сброс счетчика через 60 секунд.
    (ВАЖНО: Мы НЕ ЖДЕМ реально 60 секунд, а подменяем время)
    """
    tracker = RateLimitTracker()
    
    # 1. Первый запрос
    await tracker.check_and_wait('test_exchange', weight=100)
    assert tracker.limits['test_exchange']['used'] == 100
    
    # 2. Подменяем reset_at (делаем вид, что прошла минута)
    tracker.limits['test_exchange']['reset_at'] = time.time() - 1  # В прошлом!
    
    # 3. Второй запрос (должен сброситься)
    await tracker.check_and_wait('test_exchange', weight=50)
    
    # Счетчик должен сброситься и стать = 50 (не 150!)
    assert tracker.limits['test_exchange']['used'] == 50


@pytest.mark.asyncio
async def test_rate_limiter_waits_when_limit_reached():
    """
    Проверяем, что rate limiter ЖДЕТ, когда лимит превышен.
    """
    tracker = RateLimitTracker()
    
    # 1. Заполняем лимит почти до предела (с учетом 10% буфера)
    # Для дефолта: max=1000, safety_margin=100, так что лимит=900
    await tracker.check_and_wait('test_exchange', weight=850)
    
    assert tracker.limits['test_exchange']['used'] == 850
    
    # 2. Пытаемся добавить еще 100 (900 + 100 > 900)
    # Должен сработать WAIT
    
    start_time = time.time()
    await tracker.check_and_wait('test_exchange', weight=100)
    elapsed = time.time() - start_time
    
    # Проверяем, что произошло ожидание (хотя бы 0.5 секунды)
    # (В реале ждет 60+ секунд, но мы подставим меньше для теста)
    assert elapsed > 0.4, f"Ожидалось ожидание, но прошло только {elapsed:.2f}с"
    
    # После ожидания счетчик должен сброситься
    assert tracker.limits['test_exchange']['used'] == 100


@pytest.mark.asyncio 
async def test_rate_limiter_concurrent_requests():
    """
    Проверяем, что rate limiter работает корректно при параллельных запросах.
    """
    tracker = RateLimitTracker()
    
    # Запускаем 5 запросов параллельно
    tasks = [
        tracker.check_and_wait('test_exchange', weight=10)
        for _ in range(5)
    ]
    
    await asyncio.gather(*tasks)
    
    # Все 5 запросов должны учестться
    assert tracker.limits['test_exchange']['used'] == 50
    assert tracker.limits['test_exchange']['total_requests'] == 5


@pytest.mark.asyncio
async def test_rate_limiter_instance_stats():
    """
    Проверяем, что локальный RateLimitTracker корректно накапливает статистику.
    """
    tracker = RateLimitTracker()
    
    # Делаем несколько запросов
    await tracker.check_and_wait('binanceusdm', weight=500)
    await tracker.check_and_wait('bybit', weight=30)
    
    # Проверяем структуру
    assert 'binanceusdm' in tracker.limits
    assert 'bybit' in tracker.limits
    
    # Проверяем binanceusdm
    binance_stats = tracker.limits['binanceusdm']
    assert binance_stats['used'] == 500
    assert binance_stats['max'] == 2400
    assert binance_stats['total_requests'] == 1
    
    # Проверяем bybit
    bybit_stats = tracker.limits['bybit']
    assert bybit_stats['used'] == 30
    assert bybit_stats['max'] == 120
    assert bybit_stats['total_requests'] == 1
    
    # Проверяем процентное соотношение
    binance_pct = (binance_stats['used'] / binance_stats['max']) * 100
    assert binance_pct == pytest.approx(20.83, rel=0.1)


# ======================================================================
# --- ТЕСТЫ ФУНКЦИИ calculate_request_weight ---
# ======================================================================

@pytest.mark.asyncio
async def test_calculate_request_weight_binance():
    """
    Проверяем расчет веса для Binance OHLCV.
    """
    # Тест различных лимитов
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=50) == 1
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=100) == 1
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=200) == 2
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=500) == 2
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=700) == 5
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1000) == 5
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1500) == 10
    
    # fetch_tickers имеет вес 40
    assert calculate_request_weight('binanceusdm', 'fetch_tickers') == 40


@pytest.mark.asyncio
async def test_calculate_request_weight_non_binance():
    """
    Проверяем, что для не-Binance бирж вес = 1.
    """
    assert calculate_request_weight('bybit', 'fetch_ohlcv', limit=1000) == 1
    assert calculate_request_weight('bybit', 'fetch_tickers') == 1