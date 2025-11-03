# analysis/stage_2_maturity.py (ФИНАЛЬНАЯ ВЕРСИЯ: СНИЖЕНИЕ ШУМА)

import logging
import asyncio
import pandas as pd
import statistics
from collections import defaultdict
from typing import Optional, Tuple, Any

import config
from services import data_fetcher

from .constants import FETCH_MATURITY_TIMEOUT

log = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTION: Проверка одной биржи
# ============================================================================

async def _maturity_check_single_exchange(
    coin_data: dict, 
    exchange_obj: Any, 
    exchange_id: str, 
    log_prefix_task: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Выполняет проверку зрелости на ОДНОЙ бирже.
    """
    symbol = coin_data['symbol']
    
    try:
        tf = '1d'
        days = config.HISTORY_LOAD_DAYS.get(tf, 180)
        
        ohlcv_data_map = await asyncio.wait_for(
            data_fetcher.fetch_all_ohlcv_data(
                exchange_obj,
                symbol,
                {tf: days}, 
                log_prefix_task
            ),
            timeout=FETCH_MATURITY_TIMEOUT
        )

        if not ohlcv_data_map or tf not in ohlcv_data_map:
            return None, f"Maturity (1d fetch failed on {exchange_id})"
            
        df_1d = ohlcv_data_map[tf]
        
        min_candles = config.MIN_CANDLES_FOR_MATURITY
        actual_candles = len(df_1d)
        
        if actual_candles < min_candles:
            # --- ИЗМЕНЕНИЕ: СНИЖЕНИЕ УРОВНЯ ЛОГА ДО DEBUG (УМЕНЬШЕНИЕ ШУМА) ---
            log.debug( 
                f"{log_prefix_task} ⏭️  Недостаточно свечей на {exchange_id}: "
                f"{actual_candles}/{min_candles}."
            )
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---
            return None, f"Maturity (Need {min_candles}, Got {actual_candles} on {exchange_id})"
            
        log.debug(f"{log_prefix_task} ✅ Зрелая монета на {exchange_id}: {actual_candles} свечей")
        return df_1d, None

    except asyncio.TimeoutError:
        log.warning(f"{log_prefix_task} ❌ Таймаут {FETCH_MATURITY_TIMEOUT}с на {exchange_id}.")
        return None, f"Maturity (Timeout on {exchange_id})"
    except Exception as e:
        log.warning(f"{log_prefix_task} ⏭️  Ошибка на {exchange_id}: {e}")
        return None, f"Maturity (Error: {type(e).__name__} on {exchange_id})"


# ============================================================================
# CORE TASK: Проверка с параллельным Fallback
# ============================================================================

async def _check_coin_maturity_task(coin_data, exchanges, btc_cache_1d, log_prefix=""):
    """
    Запускает проверку зрелости на всех доступных биржах одновременно
    и возвращает результат от первой успешной биржи.
    """
    symbol = coin_data['symbol']
    exchanges_list = coin_data.get('exchanges', [])
    log_prefix_task = f"{log_prefix} [{symbol}]"
    
    tasks = {} 
    
    has_binance = 'binanceusdm' in exchanges_list and exchanges.get('binanceusdm') is not None
    has_bybit = 'bybit' in exchanges_list and exchanges.get('bybit') is not None
    
    if not (has_binance or has_bybit):
        log.debug(f"{log_prefix_task} ❌ Нет поддерживаемой биржи: {exchanges_list}")
        return coin_data, None, "Maturity (No supported exchange)", None

    exchanges_to_check = []
    if has_binance:
        exchanges_to_check.append(('binanceusdm', exchanges['binanceusdm']))
    if has_bybit:
        exchanges_to_check.append(('bybit', exchanges['bybit']))

    for ex_id, ex_obj in exchanges_to_check:
        tasks[ex_id] = asyncio.create_task(
            _maturity_check_single_exchange(coin_data, ex_obj, ex_id, log_prefix_task)
        )

    done, pending = await asyncio.wait(
        tasks.values(),
        return_when=asyncio.FIRST_COMPLETED 
    )

    for task in done:
        try:
            df_1d, skip_reason = task.result()
            
            if df_1d is not None:
                exchange_id_success = next(ex_id for ex_id, t in tasks.items() if t == task)
                
                for p_task in pending:
                    p_task.cancel()
                    
                return coin_data, df_1d, None, exchange_id_success
        
        except asyncio.CancelledError:
            pass 
        except Exception as e:
            log.error(f"{log_prefix_task} ❌ Крит. ошибка в параллельной задаче: {e}", exc_info=True)
            
    if pending:
        log.debug(f"{log_prefix_task} 💡 Первый запрос провален, ждем оставшиеся ({len(pending)} задач)...")
        
        results_from_pending = await asyncio.gather(*pending, return_exceptions=True)
        
        last_error = "Maturity (All attempts failed)"
        
        for result in results_from_pending:
            if isinstance(result, Exception):
                 last_error = f"Maturity (Critical: {type(result).__name__})"
                 continue
                 
            df_1d, skip_reason = result
            if df_1d is not None:
                exchange_id_success = next(ex_id for ex_id, t in tasks.items() if t.done() and t.result()[0] is not None)
                return coin_data, df_1d, None, exchange_id_success
            
            if skip_reason:
                last_error = skip_reason 
                
        log.debug(f"{log_prefix_task} ❌ Все доступные биржи провалились. Последняя ошибка: {last_error}")
        return coin_data, None, last_error, None
        
    last_error = "Maturity (All attempts failed after first check)"
    for task in done:
        try:
            _, skip_reason = task.result()
            if skip_reason:
                last_error = skip_reason
        except:
             pass

    log.debug(f"{log_prefix_task} ❌ Все доступные биржи провалились. Последняя ошибка: {last_error}")
    return coin_data, None, last_error, None


# ============================================================================
# ORCHESTRATOR: Запуск батчинга
# ============================================================================

async def run_maturity_stage(coins_to_check, exchanges, btc_cache_1d, log_prefix=""):
    """
    Запускает проверку "зрелости" (Этап 2) пачками (батчами).
    """
    log_prefix = f"{log_prefix}[Этап 2]"
    log.info(f"{log_prefix} Проверка 'зрелости' {len(coins_to_check)} монет...")
    
    mature_coins_map = {}
    skipped_coins = defaultdict(list)
    
    candle_counts = []
    exchange_usage = defaultdict(int)
    fallback_success = 0 
    
    total_to_check = len(coins_to_check)
    
    if total_to_check == 0:
        log.info(f"{log_prefix} (Этап 2) Проверка пропущена: нет монет для анализа.")
        return mature_coins_map, skipped_coins
        
    import config 
    from .constants import FETCH_MATURITY_TIMEOUT 
    
    batch_size = config.ANALYSIS_BATCH_SIZE
    
    for i in range(0, total_to_check, batch_size):
        batch_coins = coins_to_check[i : i + batch_size]
        
        tasks = []
        for coin_data in batch_coins:
            tasks.append(
                _check_coin_maturity_task(
                    coin_data, 
                    exchanges,
                    btc_cache_1d, 
                    log_prefix
                )
            )
            
        batch_timeout = FETCH_MATURITY_TIMEOUT + 5.0 
        
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=batch_timeout
            )
        except asyncio.TimeoutError:
            log.error(f"{log_prefix} ⌛ (ПАЧКА {i//batch_size+1}) Таймаут {batch_timeout}с. Пропуск {len(batch_coins)} монет.")
            for coin in batch_coins:
                skipped_coins["Maturity (Batch Timeout)"].append(coin['symbol'])
            continue

        for result in results:
            if isinstance(result, Exception):
                log.error(f"{log_prefix} (ПАЧКА) Необработанная ошибка: {result}", exc_info=True)
                continue
                
            coin_data, ohlcv_1d, skip_reason, exchange_used = result
            symbol = coin_data['symbol']
            
            if skip_reason:
                skipped_coins[skip_reason].append(symbol)
                
                if "Got " in skip_reason:
                    try:
                        actual = int(skip_reason.split("Got ")[1].split(" on")[0])
                        candle_counts.append(actual)
                    except:
                        pass
                        
            elif ohlcv_1d is not None:
                mature_coins_map[symbol] = (coin_data, ohlcv_1d)
                candle_counts.append(len(ohlcv_1d))
                
                if exchange_used:
                    exchange_usage[exchange_used] += 1
                    
                    if exchange_used == 'bybit' and 'binanceusdm' in coin_data.get('exchanges', []):
                        fallback_success += 1
            else:
                skipped_coins["Maturity (Unknown)"].append(symbol)
        
        processed_count = min(i + batch_size, total_to_check)
        
        # --- ПРОГРЕСС В КОНСОЛИ ---
        print(f"{log_prefix} Обработано {processed_count}/{total_to_check}...\r", end="", flush=True)
    
    print()  
        
    total_mature = len(mature_coins_map)
    
    if candle_counts:
        avg_candles = statistics.mean(candle_counts)
        median_candles = statistics.median(candle_counts)
        min_candles_setting = config.MIN_CANDLES_FOR_MATURITY
        
        log.info(f"{log_prefix} " + "=" * 60)
        log.info(f"{log_prefix} 📊 ДИАГНОСТИКА ЗРЕЛОСТИ:")
        log.info(f"{log_prefix} ├─ Порог зрелости (config):     {min_candles_setting} свечей")
        log.info(f"{log_prefix} ├─ Среднее кол-во свечей:       {avg_candles:.1f}")
        log.info(f"{log_prefix} ├─ Медиана свечей:              {median_candles:.0f}")
        log.info(f"{log_prefix} ├─ Мин/Макс свечей:             {min(candle_counts)}/{max(candle_counts)}")
        log.info(f"{log_prefix} └─ Зрелых монет:                {total_mature} ({total_mature/total_to_check*100:.1f}%)")
        
        if exchange_usage:
            log.info(f"{log_prefix} ")
            log.info(f"{log_prefix} 📊 ИСПОЛЬЗОВАНИЕ БИРЖ ДЛЯ ПРОВЕРКИ:")
            for exchange_id, count in sorted(exchange_usage.items()):
                log.info(f"{log_prefix} ├─ {exchange_id}: {count} монет ({count/total_mature*100:.1f}%)")
            
            if fallback_success > 0:
                log.info(f"{log_prefix} └─ Fallback (Bybit вместо Binance): {fallback_success} монет 🔄")
        
        log.info(f"{log_prefix} " + "=" * 60)
    
    if skipped_coins:
        log.info(f"{log_prefix} 📋 ПРИЧИНЫ ПРОПУСКОВ:")
        for reason, symbols in sorted(skipped_coins.items(), key=lambda x: len(x[1]), reverse=True):
            log.info(f"{log_prefix} ├─ {reason}: {len(symbols)} монет")
    
    log.info(f"{log_prefix} (Этап 2) ✅ Найдено {total_mature} 'зрелых' монет.")
        
    return mature_coins_map, skipped_coins