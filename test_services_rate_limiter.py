# test_rate_limiter.py (ИСПРАВЛЕНО под Rate Limiter v2)
"""
Юнит-тесты для Rate Limiter системы v2.
Все тесты работают с ЛОКАЛЬНЫМИ инстансами, без глобального состояния.

ИЗМЕНЕНИЯ v2:
- Буфер безопасности: 10% → 5%
- Веса fetch_ohlcv: изменены пороги
- fetch_tickers: вес 40 → 0
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
    (ИСПРАВЛЕНО v2) Проверяем, что rate limiter ЖДЕТ, когда лимит превышен.
    
    ИЗМЕНЕНИЯ:
    - Буфер безопасности теперь 5% (было 10%)
    - Для max=1000: лимит = 950 (было 900)
    """
    tracker = RateLimitTracker()
    
    # === ИСПРАВЛЕНИЕ: Новый буфер 5% ===
    # Для дефолта: max=1000, safety_margin=50, так что лимит=950
    await tracker.check_and_wait('test_exchange', weight=900)
    
    assert tracker.limits['test_exchange']['used'] == 900
    
    # 2. Пытаемся добавить еще 100 (950 + 100 > 950)
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
# --- ТЕСТЫ ФУНКЦИИ calculate_request_weight (ИСПРАВЛЕНО v2) ---
# ======================================================================

@pytest.mark.asyncio
async def test_calculate_request_weight_binance():
    """
    (ИСПРАВЛЕНО v2) Проверяем расчет веса для Binance OHLCV.
    
    НОВЫЕ ПОРОГИ:
    - limit <= 200: вес 1  (было <= 100)
    - limit <= 1000: вес 2  (было <= 500: 2, <= 1000: 5)
    - limit > 1000: вес 5  (было 10)
    """
    # === ИСПРАВЛЕННЫЕ ТЕСТЫ ===
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=50) == 1
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=100) == 1
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=200) == 1   # ← ИЗМЕНЕНО (было 2)
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=500) == 2   # ← ОК
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=700) == 2   # ← ИЗМЕНЕНО (было 5)
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1000) == 2  # ← ИЗМЕНЕНО (было 5)
    assert calculate_request_weight('binanceusdm', 'fetch_ohlcv', limit=1500) == 5  # ← ИЗМЕНЕНО (было 10)
    
    # === НОВОЕ: fetch_tickers теперь вес 0 (было 40) ===
    assert calculate_request_weight('binanceusdm', 'fetch_tickers') == 0


@pytest.mark.asyncio
async def test_calculate_request_weight_non_binance():
    """
    Проверяем, что для не-Binance бирж вес = 1.
    """
    assert calculate_request_weight('bybit', 'fetch_ohlcv', limit=1000) == 1
    assert calculate_request_weight('bybit', 'fetch_tickers') == 1  # Для не-Binance всегда 1


# ======================================================================
# --- НОВЫЙ ТЕСТ: Проверка буфера безопасности 5% ---
# ======================================================================

@pytest.mark.asyncio
async def test_rate_limiter_safety_margin_5_percent():
    """
    (НОВОЕ v2) Проверяем, что буфер безопасности = 5% (не 10%).
    """
    tracker = RateLimitTracker()
    
    # Для binanceusdm: max=2400, буфер=5% (120), лимит=2280
    await tracker.check_and_wait('binanceusdm', weight=2200)
    
    # Должно пройти без ожидания (2200 < 2280)
    assert tracker.limits['binanceusdm']['used'] == 2200
    
    # Пытаемся добавить ещё 100 (2200 + 100 > 2280) - должен подождать
    start_time = time.time()
    await tracker.check_and_wait('binanceusdm', weight=100)
    elapsed = time.time() - start_time
    
    assert elapsed > 0.4, f"Ожидалось ожидание при превышении 95%, но прошло {elapsed:.2f}с"
    
    # После ожидания счётчик сброшен
    assert tracker.limits['binanceusdm']['used'] == 100