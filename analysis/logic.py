# analysis/logic.py

import logging
import asyncio
import time
from datetime import datetime
import pandas as pd
import numpy as np
import math 
from collections import defaultdict
import gc

# Импортируем модули проекта
import config
from services import data_fetcher
from services import mongo_service  # <-- Используем Mongo-сервис

# (ИЗМЕНЕНИЕ) Импортируем 'calculate_volume_categories'
from metrics.ranking import calculate_volume_categories 

# Импортируем модули Этапов
from .stage_0_prereqs import load_btc_and_blacklist
from .stage_2_maturity import run_maturity_stage
from .stage_3_analysis_workers import run_analysis_stage_workers

# --- Настройка ---
log = logging.getLogger(__name__)


# --- ГЛАВНАЯ ЛОГИКА ---

async def analysis_logic(run_id, log_prefix=""):
    """
    Главная "дирижерская" функция анализа.
    """
    start_time = time.time()
    log.info(f"{log_prefix} --- НАЧАЛО АНАЛИЗА (Run ID: {run_id}) ---")

    btc_cache_1d = None
    active_exchanges = {}
    markets_map = {}
    
    total_found = 0
    total_mature = 0
    total_successful = 0
    total_skipped = 0
    saved_count = 0
    
    skipped_coins = defaultdict(set) 

    try:
        # --- ЭТАП 0: КЭШ BTC И ЧЕРНЫЙ СПИСОК ---
        
        btc_cache_1d, blacklist = await load_btc_and_blacklist(log_prefix)
        
        if btc_cache_1d is None:
            log.warning(f"{log_prefix} ⛔ Не удалось загрузить BTC. Анализ невозможен.")
            return 0, "Критическая ошибка: Не удалось загрузить кэш BTC"
        
        # --- ЭТАП 1: ЗАГРУЗКА ДАННЫХ ---
        log_prefix_1 = f"{log_prefix}[Этап 1]"
        log.info(f"{log_prefix_1} Загрузка всех монет...")
        
        all_coins_data, active_exchanges, markets_map, skipped_fetch = \
            await data_fetcher.fetch_all_coins_data(
                config.EXCHANGES_TO_LOAD,
                config.QUOTE_CURRENCIES,
                blacklist,
                log_prefix_1
            )
        
        for reason, symbols in skipped_fetch.items():
            skipped_coins[reason].update(symbols)

        total_found = len(all_coins_data)
        if total_found == 0:
            log.warning(f"{log_prefix_1} ⛔ (Этап 1) Не найдено ни одной монеты.")
            return 0, "Не найдено монет (Этап 1)"
            
        log.info(f"{log_prefix_1} ✅ Найдено {total_found} монет.")

        # --- ЭТАП 2: ПРОВЕРКА "ЗРЕЛОСТИ" ---
        
        mature_coins_map, skipped_maturity = await run_maturity_stage(
            all_coins_data,
            active_exchanges,  
            btc_cache_1d,
            log_prefix
        )
        
        for reason, symbols in skipped_maturity.items():
            skipped_coins[reason].update(symbols)
            
        total_mature = len(mature_coins_map)
        if total_mature == 0:
            log.warning(f"{log_prefix} ⛔ (Этап 2) Не найдено 'зрелых' монет.")
            return 0, "Не найдено 'зрелых' монет (Этап 2)"
            
        del all_coins_data
        gc.collect()

        # --- ЭТАП 3: ПОЛНЫЙ АНАЛИЗ ---
        
        final_data_to_save, skipped_analysis_set = await run_analysis_stage_workers(
            mature_coins_map,
            active_exchanges,
            markets_map,
            btc_cache_1d,
            log_prefix
        )
        
        if skipped_analysis_set:
            skipped_coins["Analysis (Error/Timeout)"].update(skipped_analysis_set)
            
        total_successful = len(final_data_to_save)
        
        del mature_coins_map
        gc.collect()

        if total_successful == 0:
            log.warning(f"{log_prefix} ⛔ (Этап 3) Не удалось проанализировать ни одной монеты.")
            return 0, "Не удалось проанализировать монеты (Этап 3)"
            
        log.info(f"{log_prefix} (Этап 3) ✅ Успешно проанализировано {total_successful} монет.")

        # --- (ИЗМЕНЕНИЕ) ЭТАП 5 (РАНГИ) ПЕРЕМЕЩЕН ПЕРЕД ЭТАПОМ 4 ---
        log_prefix_5 = f"{log_prefix}[Этап 5]"
        try:
            log.info(f"{log_prefix_5} Расчет категорий (рангов) объема...")
            # 1. Вызываем новую in-memory функцию
            rank_map = calculate_volume_categories(final_data_to_save, log_prefix_5)
            
            # 2. Добавляем 'category' к данным ПЕРЕД сохранением
            if rank_map:
                for coin in final_data_to_save:
                    rank = rank_map.get(coin['full_symbol'])
                    if rank:
                        coin['category'] = int(rank)
            log.info(f"{log_prefix_5} ✅ Категории успешно рассчитаны и добавлены.")
            
        except Exception as e:
            log.error(f"{log_prefix_5} ❌ Ошибка при расчете Категорий (Рангов): {e}", exc_info=True)
        # --- КОНЕЦ ИЗМЕНЕНИЯ ЭТАПА 5 ---

        # --- ЭТАП 4: СОХРАНЕНИЕ В БД (MONGODB) ---
        log_prefix_4 = f"{log_prefix}[Этап 4]"
        log.info(f"{log_prefix_4} Сохранение {total_successful} монет в MongoDB...")
        try:
            # (Логика clear_existing_data() теперь внутри save_coins_to_mongo)
            saved_count = await mongo_service.save_coins_to_mongo(final_data_to_save, log_prefix_4)
            log.info(f"{log_prefix_4} ✅ Успешно сохранено {saved_count} монет в MongoDB.")
        
        except Exception as e:
            log.error(f"{log_prefix_4} ❌ Ошибка при сохранении в MongoDB: {e}", exc_info=True)
            
        del final_data_to_save
        gc.collect()
            
        # (ИЗМЕНЕНИЕ) ЭТАП 5 УДАЛЕН ОТСЮДА
        
        # --- ЗАВЕРШЕНИЕ ---
        total_time_seconds = time.time() - start_time
        total_skipped = sum(len(s) for s in skipped_coins.values())
        
        # --- СВОДКА ПРОПУСКОВ ---
        log.info(f"{log_prefix} " + "=" * 62)
        log.info(f"{log_prefix} 📋 ДЕТАЛИЗАЦИЯ ПРОПУЩЕННЫХ МОНЕТ (ВСЕ ЭТАПЫ):")
        
        sorted_skipped = sorted(skipped_coins.items(), key=lambda x: len(x[1]), reverse=True)
        
        for reason, symbols in sorted_skipped:
            if symbols:
                log.info(f"{log_prefix} ├─ {reason}: {len(symbols)} монет")
                 
        log.info(f"{log_prefix} └─ Общее кол-во пропусков: {total_skipped} монет")
        log.info(f"{log_prefix} " + "=" * 62)

        # --- ФИНАЛЬНАЯ СВОДКА ---
        log.info(f"{log_prefix} ╔{'═' * 60}╗")
        log.info(f"{log_prefix} ║{'АНАЛИЗ ЗАВЕРШЕН':^60}║")
        log.info(f"{log_prefix} ╠{'═' * 60}╣")
        log.info(f"{log_prefix} ║ Время выполнения: {total_time_seconds: >42.2f} сек ║")
        log.info(f"{log_prefix} ║ Найдено (Объем): {total_found:>44} ║")
        log.info(f"{log_prefix} ║ Найдено ('Зрелых'): {total_mature:>40} ║")
        log.info(f"{log_prefix} ║ Успешно проанализировано: {total_successful:>33} ║")
        log.info(f"{log_prefix} ║ Ошибок (всего): {total_skipped:>41} ║") 
        # (ИЗМЕНЕНИЕ) Обновлен текст
        log.info(f"{log_prefix} ║ Сохранено в MongoDB: {saved_count:>38} ║")
        log.info(f"{log_prefix} ╚{'═' * 60}╝")
        
        return saved_count, f"Анализ завершен. Сохранено {saved_count} из {total_successful} 'зрелых' монет."

    except Exception as e:
        log.error(f"{log_prefix} КРИТИЧЕСКАЯ ОШИБКА в analysis_logic: {e}", exc_info=True)
        return 0, f"Критическая ошибка: {e}"

    finally:
        try:
            del btc_cache_1d
            gc.collect()
            log.info(f"{log_prefix} Очистка кэша и памяти завершена.")
        except Exception:
            pass

        try:
            mongo_service.close_mongo_client(log_prefix)
        except Exception as e:
            log.error(f"{log_prefix} Ошибка при закрытии MongoDB в finally: {e}")

        if active_exchanges:
            log.info(f"{log_prefix} Закрытие {len(active_exchanges)} активных соединений...")
            
            close_tasks = []
            for ex in active_exchanges.values():
                if ex and hasattr(ex, 'close'):
                    close_tasks.append(ex.close())
            
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            
            log.info(f"{log_prefix} Все соединения закрыты.")