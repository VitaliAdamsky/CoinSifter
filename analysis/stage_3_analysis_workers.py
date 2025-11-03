# analysis/stage_3_analysis_workers.py

import logging
from collections import defaultdict
import time
import asyncio
import gc

from .stage_3_wave_binance import run_binance_wave
from .stage_3_wave_bybit import run_bybit_wave

log = logging.getLogger(__name__)


async def run_analysis_stage_workers(
    mature_coins_map,  # {symbol: (coin_data, df_1d)}
    active_exchanges,
    markets_map,
    btc_cache_1d,
    log_prefix=""
):
    """
    Разделяет "зрелые" монеты на две волны (Binance Wave и Bybit Wave) и запускает анализ.
    """
    
    log_prefix = f"{log_prefix}[Этап 3]"
    total_mature = len(mature_coins_map)
    log.info(f"{log_prefix} Начало полного анализа {total_mature} 'зрелых' монет...")

    final_data_to_save = []
    skipped_analysis_set = set()
    
    # ========================================================================
    # 1. РАЗДЕЛЕНИЕ МОНЕТ ПО БИРЖАМ (ПРИОРИТЕТ BINANCE)
    # ========================================================================
    
    binance_coins = {}
    bybit_only_coins = {}
    skipped_no_exchange = set()
    
    binance_exclusive = 0
    binance_and_bybit = 0
    bybit_exclusive = 0
    
    for symbol, (coin_data, df_1d) in mature_coins_map.items():
        exchanges_list = coin_data.get('exchanges', [])
        
        if not exchanges_list:
            skipped_no_exchange.add(symbol)
            continue
        
        has_binance = 'binanceusdm' in exchanges_list
        has_bybit = 'bybit' in exchanges_list
        
        if has_binance:
            binance_coins[symbol] = (coin_data, df_1d)
            if has_bybit:
                binance_and_bybit += 1
            else:
                binance_exclusive += 1
                
        elif has_bybit:
            bybit_only_coins[symbol] = (coin_data, df_1d)
            bybit_exclusive += 1
        else:
            skipped_no_exchange.add(symbol)
            
    log.info(f"{log_prefix} 📊 РАЗДЕЛЕНИЕ МОНЕТ ПО БИРЖАМ:")
    log.info(f"{log_prefix} ├─ Binance Wave (использует Binance API): {len(binance_coins)} монет")
    log.info(f"{log_prefix} │  ├─ Только Binance:        {binance_exclusive} монет")
    log.info(f"{log_prefix} │  └─ Binance + Bybit:       {binance_and_bybit} монет")
    log.info(f"{log_prefix} └─ Bybit Wave (использует Bybit API): {len(bybit_only_coins)} монет")

    
    # ========================================================================
    # 2. ВОЛНА BINANCE
    # ========================================================================
    
    if binance_coins and 'binanceusdm' in active_exchanges:
        log.info(f"{log_prefix} 🌊 Запуск Волна 1: Binance ({len(binance_coins)} монет)")
        
        binance_results, skipped_binance = await run_binance_wave(
            coins_to_process=binance_coins,
            exchange=active_exchanges['binanceusdm'],
            btc_cache_1d=btc_cache_1d,
            log_prefix=f"{log_prefix}[Binance Wave]"
        )
        final_data_to_save.extend(binance_results)
        skipped_analysis_set.update(skipped_binance)
        
        del binance_results, skipped_binance, binance_coins
        gc.collect()
    else:
        log.warning(f"{log_prefix} ⚠️ Волна 1: Binance пропущена (нет монет или биржа недоступна).")


    # ========================================================================
    # 3. ВОЛНА BYBIT
    # ========================================================================
    
    if bybit_only_coins and 'bybit' in active_exchanges:
        log.info(f"{log_prefix} 🌊 Запуск Волна 2: Bybit ({len(bybit_only_coins)} монет)")
        
        bybit_results, skipped_bybit = await run_bybit_wave(
            coins_to_process=bybit_only_coins,
            exchange=active_exchanges['bybit'],
            btc_cache_1d=btc_cache_1d,
            log_prefix=f"{log_prefix}[Bybit Wave]"
        )
        final_data_to_save.extend(bybit_results)
        skipped_analysis_set.update(skipped_bybit)
        
        del bybit_results, skipped_bybit, bybit_only_coins
        gc.collect()
    else:
        log.warning(f"{log_prefix} ⚠️ Волна 2: Bybit пропущена (нет монет или биржа недоступна).")


    # ========================================================================
    # 4. ФИНАЛИЗАЦИЯ
    # ========================================================================
    
    total_successful = len(final_data_to_save)
    log.info(f"{log_prefix} ✅ Полный анализ завершен. Успешно обработано: {total_successful} монет.")
    
    return final_data_to_save, skipped_analysis_set