# analysis/stage_3_wave_bybit.py

import logging
import asyncio
import time
from datetime import datetime

import config
from services import data_fetcher
from metrics.calculator import calculate_all_metrics

from .constants import (
    FETCH_ANALYSIS_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY_BASE,
    LOG_PROGRESS_EVERY_N_COINS,
    AVG_TIME_PER_COIN_ESTIMATE
)

log = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _format_time(seconds):
    """Форматирует секунды в MM:SS"""
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}:{secs:02d}"


def _log_progress(current, total, success_count, failed_count, retry_count, elapsed_seconds, log_prefix):
    """Логирует прогресс (Render-safe)"""
    if total == 0:
        return
    
    percent = (current / total) * 100
    avg_time_per_coin = elapsed_seconds / current if current > 0 else 5.0
    eta_seconds = avg_time_per_coin * (total - current)
    
    elapsed_str = _format_time(elapsed_seconds)
    eta_str = _format_time(eta_seconds)
    
    log.info(
        f"{log_prefix} Обработано: {current}/{total} ({percent:.1f}%) | "
        f"✅ {success_count} | ❌ {failed_count} | 🔄 {retry_count} | "
        f"⏱️  {elapsed_str} / ~{eta_str}"
    )


# ============================================================================
# CORE ANALYSIS FUNCTIONS
# ============================================================================

async def _analyze_coin_metrics_task(coin_data, exchange, btc_cache_1d, df_1d, log_prefix=""):
    """
    Загружает и анализирует одну монету.
    """
    symbol = coin_data['symbol']
    
    try:
        ohlcv_data_map = await asyncio.wait_for(
            data_fetcher.fetch_all_ohlcv_data(
                exchange, 
                symbol,
                config.TIMEFRAMES_TO_LOAD,
                log_prefix
            ),
            timeout=FETCH_ANALYSIS_TIMEOUT
        )
        
        if not ohlcv_data_map:
            return None, "Analysis (Missing TFs)"

        ohlcv_data_map['1d'] = df_1d
        
        metrics = calculate_all_metrics(ohlcv_data_map, btc_cache_1d)
        
        if not metrics:
            return None, "Analysis (Calc Error)"
            
        final_data = {
            'symbol': symbol,
            'full_symbol': coin_data['full_symbol'],
            'name': coin_data['name'],
            'quoteCurrency': coin_data['quoteCurrency'],
            'usdPrice': coin_data['usdPrice'],
            'volume_24h_usd': coin_data['volume_24h_usd'],
            'volume24h': (
                coin_data['volume_24h_usd'] / coin_data['usdPrice']
                if coin_data['usdPrice'] > 0 
                else 0.0
            ),
            'change24h': coin_data['change24h'],
            'exchanges': coin_data['exchanges'],
            'logoUrl': coin_data['logoUrl'],
            'analyzed_at': datetime.now()
        }
        
        final_data.update(metrics)
        
        return final_data, None

    except asyncio.TimeoutError:
        return None, "Analysis (Timeout)"
    except Exception as e:
        log.warning(f"{log_prefix} Ошибка: {e}")
        return None, f"Analysis (Error: {type(e).__name__})"


async def _analyze_with_retry(coin_data, exchange, btc_cache_1d, df_1d, log_prefix=""):
    """
    Retry механизм (до MAX_RETRIES попыток).
    """
    symbol = coin_data['symbol']
    exchanges_list = coin_data.get('exchanges', []) 
    task_log_prefix = f"{log_prefix} [{symbol}]"
    
    log.debug(f"{task_log_prefix} ➡️ Начинаю анализ. Источник бирж: {exchanges_list}. Использую: {exchange.id}")
    
    retry_count = 0
    
    for attempt in range(1, MAX_RETRIES + 1):
        final_data, error_reason = await _analyze_coin_metrics_task(
            coin_data,
            exchange,
            btc_cache_1d,
            df_1d,
            task_log_prefix
        )
        
        if final_data and not error_reason:
            if attempt > 1:
                log.debug(f"{task_log_prefix} ✅ Успешно с попытки {attempt}/{MAX_RETRIES}")
            return final_data, None, retry_count
        
        if attempt < MAX_RETRIES:
            retry_count += 1
            wait_time = RETRY_DELAY_BASE ** attempt
            log.debug(f"{task_log_prefix} 🔄 Попытка {attempt}/{MAX_RETRIES} провалена ({error_reason}), жду {wait_time:.1f}с...")
            await asyncio.sleep(wait_time)
        else:
            log.warning(f"{task_log_prefix} ❌ Все {MAX_RETRIES} попытки провалены: {error_reason}")
    
    return None, error_reason, retry_count


# ============================================================================
# BYBIT WAVE (Батчинг с детализацией логов)
# ============================================================================

async def run_bybit_wave(
    coins_to_process,  # {symbol: (coin_data, df_1d)}
    exchange,
    btc_cache_1d,
    log_prefix
):
    """
    Асинхронная пакетная загрузка монет с Bybit.
    """
    all_symbols = list(coins_to_process.keys())
    total_coins = len(all_symbols)
    results_list = []
    
    success_count = 0
    failed_count = 0
    total_retry_count = 0
    
    start_time = time.time()
    batch_size = config.ANALYSIS_BATCH_SIZE
    
    log.info(f"{log_prefix} 🌊 Начало загрузки {total_coins} монет с Bybit (Батч: {batch_size})")
    
    estimated_total_time = total_coins * AVG_TIME_PER_COIN_ESTIMATE / 2 
    log.info(f"{log_prefix} ⏱️  Ожидаемое время (консервативно): ~{_format_time(estimated_total_time)}")
    
    log_interval = LOG_PROGRESS_EVERY_N_COINS
    
    for i in range(0, total_coins, batch_size):
        batch_symbols = all_symbols[i:i + batch_size]
        batch_tasks = []
        
        for symbol in batch_symbols:
            coin_data, df_1d = coins_to_process[symbol]
            batch_tasks.append(
                _analyze_with_retry(
                    coin_data,
                    exchange,
                    btc_cache_1d,
                    df_1d,
                    log_prefix
                )
            )
        
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        batch_success_symbols = [] 

        for result in results:
            if isinstance(result, Exception):
                log.error(f"{log_prefix} ❌ Крит. ошибка в пакете: {result}")
                continue
                
            final_data, error_reason, retry_count = result
            
            total_retry_count += retry_count
            
            if final_data:
                results_list.append(final_data)
                success_count += 1
                batch_success_symbols.append(final_data['symbol']) 
            else:
                failed_count += 1 

        current_processed = i + len(batch_tasks)
        
        # --- ДЕТАЛИЗАЦИЯ ЛОГА ---
        log.debug(
            f"{log_prefix} [Пакет {i//batch_size + 1}] Обработано {len(batch_tasks)} монет. "
            f"✅ Успех: {len(batch_success_symbols)}. "
            f"Успешные монеты: {', '.join(batch_success_symbols[:5])}..."
        )
        # --- КОНЕЦ ДЕТАЛИЗАЦИИ ЛОГА ---

        if current_processed % log_interval == 0 or current_processed == total_coins:
            elapsed = time.time() - start_time
            _log_progress(
                current=current_processed,
                total=total_coins,
                success_count=success_count,
                failed_count=failed_count,
                retry_count=total_retry_count,
                elapsed_seconds=elapsed,
                log_prefix=log_prefix
            )
    
    processed_symbols = {r['symbol'] for r in results_list}
    skipped_set = set(all_symbols) - processed_symbols
    
    final_success_count = len(results_list)

    total_time = time.time() - start_time
    success_rate = (final_success_count / total_coins * 100) if total_coins > 0 else 0
    
    log.info(
        f"{log_prefix} ✅ Волна Bybit завершена: {final_success_count}/{total_coins} ({success_rate:.1f}%) "
        f"за {_format_time(total_time)} | Повторов: {total_retry_count} | Пропущено: {len(skipped_set)}"
    )
    
    return results_list, skipped_set