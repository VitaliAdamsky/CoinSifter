# services/exchange_utils.py 

import ccxt.pro as ccxt_pro
import ccxt
import logging
import asyncio
import functools
import time
from collections import defaultdict
import config

# --- Настройка ---
log = logging.getLogger(__name__)

EXCHANGE_CLASS_MAP = {
    'binanceusdm': 'binanceusdm',
    'bybit': 'bybit',
}

# ============================================================================
# === Rate Limit Tracker (С ФИКСОМ asyncio.sleep(0)) ===
# ============================================================================

class RateLimitTracker:
    """
    Отслеживает использование rate limits для бирж в реальном времени.
    """
    
    def __init__(self):
        self.limits = {}  
        self.lock = asyncio.Lock()
        self.last_log_time = {}  
    
    def _get_max_limit(self, exchange_id):
        """Возвращает максимальный лимит для биржи."""
        if 'binance' in exchange_id.lower():
            return 2400  
        elif 'bybit' in exchange_id.lower():
            return 120   
        else:
            return 1000  
    
    async def check_and_wait(self, exchange_id, weight=1):
        """
        Проверяет rate limit и ждет, если нужно.
        """
        async with self.lock:
            now = time.time()
            
            if exchange_id not in self.limits:
                self.limits[exchange_id] = {
                    'used': 0,
                    'max': self._get_max_limit(exchange_id),
                    'reset_at': now + 60,
                    'total_requests': 0
                }
                self.last_log_time[exchange_id] = now
            
            limit_info = self.limits[exchange_id]
            
            if now >= limit_info['reset_at']:
                old_used = limit_info['used']
                limit_info['used'] = 0
                limit_info['reset_at'] = now + 60
                
                percentage = (old_used / limit_info['max']) * 100 if limit_info['max'] > 0 else 0
                log.info(
                    f"[RateLimit] 🔄 {exchange_id}: Сброс счетчика. "
                    f"Использовано: {old_used}/{limit_info['max']} "
                    f"({percentage:.1f}%)"
                )
            
            safety_margin = int(limit_info['max'] * 0.05)
            available_limit = limit_info['max'] - safety_margin
            
            if limit_info['used'] + weight > available_limit:
                wait_time = limit_info['reset_at'] - now + 1.0  
                
                log.warning(
                    f"[RateLimit] 🚦 {exchange_id}: Лимит {available_limit} достигнут (запрос +{weight}). "
                    f"Ожидание {wait_time:.1f}с до сброса..."
                )
                
                await asyncio.sleep(wait_time)
                
                limit_info['used'] = 0
                limit_info['reset_at'] = time.time() + 60
                
                log.info(f"[RateLimit] ✅ {exchange_id}: Счетчик сброшен после ожидания.")
            
            limit_info['used'] += weight
            limit_info['total_requests'] += 1
            
            if now - self.last_log_time.get(exchange_id, 0) > 20:
                percentage = (limit_info['used'] / limit_info['max']) * 100
                log.info(
                    f"[RateLimit] 📊 {exchange_id}: "
                    f"{limit_info['used']}/{limit_info['max']} "
                    f"({percentage:.1f}%) | "
                    f"Запросов: {limit_info['total_requests']}"
                )
                self.last_log_time[exchange_id] = now

        # КРИТИЧЕСКИЙ ФИКС: Асинхронная разблокировка
        await asyncio.sleep(0) 

# Глобальный экземпляр (singleton)
rate_limiter = RateLimitTracker()


# ============================================================================
# === calculate_request_weight ===
# ============================================================================

def calculate_request_weight(exchange_id, func_name, **kwargs):
    """
    Рассчитывает вес запроса для конкретной биржи.
    """
    if 'binance' not in exchange_id.lower():
        return 1
    
    if func_name == 'fetch_ohlcv':
        limit = kwargs.get('limit', 500)
        
        if limit <= 200:
            return 1 
        elif limit <= 1000:
            return 2 
        else:
            return 5
    
    elif func_name == 'fetch_tickers':
        return 0
    
    elif func_name == 'fetch_markets':
        return 1
    
    else:
        return 1


# ============================================================================
# === retry_on_network_error ===
# ============================================================================

def retry_on_network_error(max_attempts_func=None):
    """
    Декоратор, который:
    1. Проверяет rate limit ДО запроса
    2. Повторяет запрос при сетевых ошибках
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            
            log_prefix = kwargs.get('log_prefix', '')
            if not log_prefix and args:
                for arg in reversed(args):
                    if isinstance(arg, str) and ('[' in arg or 'Этап' in arg):
                        log_prefix = arg
                        break
            if not log_prefix:
                log_prefix = f"[{func.__name__}]"

            max_attempts = config.MAX_RETRIES if hasattr(config, 'MAX_RETRIES') else 3 
            attempts = 0
            
            exchange = args[0] if args else None
            exchange_id = exchange.id if (exchange and hasattr(exchange, 'id')) else 'unknown'

            while attempts < max_attempts:
                try:
                    if exchange and hasattr(exchange, 'id'):
                        weight = calculate_request_weight(
                            exchange_id, 
                            func.__name__, 
                            **kwargs
                        )
                        
                        if weight > 0:
                            await rate_limiter.check_and_wait(exchange_id, weight)
                    
                    return await func(*args, **kwargs)
                
                except ccxt.ExchangeNotAvailable as e:
                    log.error(
                        f"{log_prefix} ❌ (ExchangeNotAvailable): {e}. "
                        f"Биржа {exchange_id} недоступна, прерываем."
                    )
                    if func.__name__ in ['fetch_tickers', 'fetch_markets']:
                        return {}
                    return None
                
                except ccxt.RateLimitExceeded as e:
                    attempts += 1
                    
                    if 'binance' in exchange_id.lower():
                        reset_at = rate_limiter.limits.get(exchange_id, {}).get('reset_at', time.time() + 60)
                        wait_time = max(1, reset_at - time.time() + 1)
                    else:
                        wait_time = 10
                    
                    log.warning(
                        f"{log_prefix} ⚠️ (RateLimitExceeded): {e}. "
                        f"Попытка {attempts}/{max_attempts}. "
                        f"Ожидание {wait_time:.1f}с..."
                    )
                    
                    await asyncio.sleep(wait_time)
                    
                    if exchange_id in rate_limiter.limits:
                        rate_limiter.limits[exchange_id]['used'] = 0
                        rate_limiter.limits[exchange_id]['reset_at'] = time.time() + 60
                
                except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                    attempts += 1
                    
                    retry_wait_min = config.RETRY_DELAY_BASE if hasattr(config, 'RETRY_DELAY_BASE') else 2.0
                    retry_wait_max = 30 
                    
                    wait_time = min(retry_wait_max, retry_wait_min * (2 ** attempts))
                    
                    error_type = type(e).__name__
                    log.warning(
                        f"{log_prefix} ⚠️ ({error_type}): {e}. "
                        f"Попытка {attempts}/{max_attempts}. "
                        f"Ожидание {wait_time:.1f}с..."
                    )
                    
                    await asyncio.sleep(wait_time)
                
                except ccxt.BaseError as e:
                    attempts += 1
                    error_type = type(e).__name__
                    log.warning(
                        f"{log_prefix} ⚠️ ({error_type}): {e}. "
                        f"Попытка {attempts}/{max_attempts}."
                    )
                    await asyncio.sleep(1)

            context = ""
            try:
                if func.__name__ == 'fetch_ohlcv':
                    symbol = args[1] if len(args) > 1 else '?'
                    timeframe = args[2] if len(args) > 2 else '?'
                    context = f"{symbol} {timeframe}"
                elif func.__name__ == 'fetch_tickers':
                    context = f"tickers для {exchange_id}"
                elif func.__name__ == 'fetch_markets':
                    context = f"markets для {exchange_id}"
                else:
                    context = f"{func.__name__}"
            except Exception:
                context = f"{func.__name__}"

            log.error(
                f"{log_prefix} ❌ Не удалось загрузить {context} "
                f"после {max_attempts} попыток."
            )
            
            if func.__name__ in ['fetch_tickers', 'fetch_markets']:
                return {}
            return None
            
        return wrapper
    return decorator


# ============================================================================
# === initialize_exchange ===
# ============================================================================

async def initialize_exchange(exchange_id, log_prefix=""):
    """
    Инициализирует асинхронный экземпляр биржи CCXT с правильными настройками.
    """
    log.info(f"{log_prefix} 🔄 Инициализация биржи {exchange_id}...")

    class_name = EXCHANGE_CLASS_MAP.get(exchange_id, exchange_id)

    try:
        if not hasattr(ccxt_pro, class_name):
            log.error(
                f"{log_prefix} ❌ Биржа '{class_name}' "
                f"(из ID '{exchange_id}') не поддерживается ccxt.pro."
            )
            return None

        exchange_class = getattr(ccxt_pro, class_name)

        exchange_options = {
            'enableRateLimit': True,  
            'rateLimit': 500,  
            'timeout': 30000,
            'verbose': False, 
        }
        
        if 'binance' in exchange_id.lower():
            exchange_options.update({
                'options': {
                    'defaultType': 'future',  
                    'adjustForTimeDifference': True,  
                    'recvWindow': 10000,  
                },
                'rateLimit': 250, 
            })
            log.debug(f"{log_prefix} 📝 Binance настроен на USDⓈ-M Futures")
        
        elif 'bybit' in exchange_id.lower():
            exchange_options.update({
                'options': {
                    'defaultType': 'future', 
                },
                'rateLimit': 500, 
            })
            log.debug(f"{log_prefix} 📝 Bybit настроен на Futures")

        exchange = exchange_class(exchange_options)
        
        log.info(
            f"{log_prefix} ✅ Биржа {exchange_id} успешно инициализирована "
            f"(rateLimit: {exchange_options['rateLimit']}ms)"
        )
        
        return exchange

    except Exception as e:
        log.error(
            f"{log_prefix} ❌ Не удалось инициализировать биржу {exchange_id}: {e}", 
            exc_info=True
        )
        return None


# ============================================================================
# === Утилиты для мониторинга ===
# ============================================================================

def get_rate_limit_stats():
    """Возвращает статистику использования rate limits."""
    stats = {}
    for exchange_id, info in rate_limiter.limits.items():
        percentage = (info['used'] / info['max']) * 100 if info['max'] > 0 else 0
        stats[exchange_id] = {
            'used': info['used'],
            'max': info['max'],
            'percentage': round(percentage, 2),
            'total_requests': info['total_requests'],
            'reset_in': round(info['reset_at'] - time.time(), 1) if info['reset_at'] > time.time() else 0
        }
    return stats


def log_rate_limit_summary(log_prefix=""):
    """Логирует итоговую статистику rate limits."""
    stats = get_rate_limit_stats()
    
    if not stats:
        log.info(f"{log_prefix} [RateLimit] 📊 Статистика пуста (не было запросов)")
        return
    
    log.info(f"{log_prefix} [RateLimit] 📊 ===== ИТОГОВАЯ СТАТИСТИКА =====")
    for exchange_id, data in stats.items():
        log.info(
            f"{log_prefix} [RateLimit] 📊 {exchange_id}: "
            f"{data['used']}/{data['max']} ({data['percentage']}%) | "
            f"Всего запросов: {data['total_requests']} | "
            f"Сброс через: {data['reset_in']}с"
        )