import logging
import pandas as pd

# (ИЗМЕНЕНО) Исправляем циклический импорт.
# Импортируем НЕ из 'services', а НАПРЯМУЮ из "соседа".
from .data_cache_service import get_cached_coins_data

log = logging.getLogger(__name__)

# (ИЗМЕНЕНО) Функция стала асинхронной
async def get_data_quality_report(log_prefix="[DataQuality]"):
    """
    (РЕФАКТОРИНГ) Анализирует DataFrame (из кэша) на предмет качества данных
    и возвращает подробный отчет.
    """
    log.info(f"{log_prefix} Запуск анализа качества данных (из кэша)...")

    try:
        # 1. (ИЗМЕНЕНО) Получаем данные из централизованного кэша
        data_list = await get_cached_coins_data(
            log_prefix=f"{log_prefix} [Cache]"
        )

        if not data_list:
            log.warning(f"{log_prefix} Кэш пуст. Анализ невозможен.")
            return {
                "error": "Не удалось загрузить данные из кэша.",
                "total_coins": 0,
                "analysis_date": None,
                "all_coins_sorted": [],
                "missing_data_report": {}
            }
        
        # 2. (ИЗМЕНЕНО) Преобразуем list[dict] в DataFrame
        df = pd.DataFrame(data_list)
        
        log.info(f"{log_prefix} Данные из кэша загружены. Обнаружено {len(df)} монет. Начинаем анализ пропусков...")

        # 3. Инициализация отчета (БЕЗ ИЗМЕНЕНИЙ)
        missing_data_report = {}
        
        # 4. Анализ пропусков по колонкам (БЕЗ ИЗМЕНЕНИЙ)
        for column in df.columns:
            nan_mask = df[column].isna()
            
            if nan_mask.any():
                missing_symbols = df[nan_mask]['symbol'].tolist()
                missing_data_report[column] = sorted(list(set(missing_symbols)))
            else:
                missing_data_report[column] = []

        # 5. Сбор общей информации (БЕЗ ИЗМЕНЕНИЙ)
        total_coins = len(df)
        all_coins_sorted = sorted(df['symbol'].unique())
        
        analysis_date = None
        if 'analyzed_at' in df.columns and not df.empty:
            try:
                first_valid_date = df['analyzed_at'].dropna().iloc[0]
                if pd.notna(first_valid_date):
                    analysis_date = str(first_valid_date)
            except IndexError:
                log.warning(f"{log_prefix} Колонка 'analyzed_at' пуста, не удалось получить дату.")
                analysis_date = "N/A"
        
        log.info(f"{log_prefix} Анализ качества данных завершен.")

        # 6. Формирование итогового JSON (БЕЗ ИЗМЕНЕНИЙ)
        return {
            "total_coins": total_coins,
            "analysis_date": analysis_date,
            "all_coins_sorted": all_coins_sorted,
            "missing_data_report": missing_data_report
        }

    except Exception as e:
        log.error(f"{log_prefix} Критическая ошибка при анализе качества данных: {e}", exc_info=True)
        return {
            "error": f"Критическая ошибка: {e}",
            "total_coins": 0,
            "analysis_date": None,
            "all_coins_sorted": [],
            "missing_data_report": {}
        }