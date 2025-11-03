# db_schema_validator.py

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

import database # Для get_db_connection
import config   # Для DATABASE_SCHEMA

# --- Настройка ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
log = logging.getLogger(__name__)

# ANSI-коды для цветного вывода
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Карта типов: как тип в config.py называется в information_schema PostgreSQL
# (Это самая важная часть)
TYPE_MAP = {
    "VARCHAR(50)": "character varying",
    "VARCHAR(100)": "character varying",
    "VARCHAR(255)": "character varying",
    "TEXT[]": "text[]",
    "DOUBLE PRECISION": "double precision",
    "INTEGER": "integer",
    "TIMESTAMP WITH TIME ZONE": "timestamp with time zone"
}


def validate_schema():
    """
    Подключается к БД и сравнивает схему из config.py 
    с реальной схемой в PostgreSQL.
    """
    log.info("--- Запуск Валидатора Схемы БД ---")
    conn = None
    errors_found = 0
    
    try:
        # 1. Получаем ожидаемую схему из config.py
        expected_schema = config.DATABASE_SCHEMA
        expected_columns = set(expected_schema.keys())
        
        # 2. Получаем реальную схему из PostgreSQL
        log.info("Подключение к PostgreSQL...")
        conn = database.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        table_name = 'monthly_coin_selection'
        
        log.info(f"Запрос фактической схемы для таблицы '{table_name}'...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s;
        """, (table_name,))
        
        actual_rows = cursor.fetchall()
        
        if not actual_rows:
            log.error(f"{bcolors.FAIL}КРИТИЧЕСКАЯ ОШИБКА: Таблица '{table_name}' не найдена в базе данных.{bcolors.ENDC}")
            log.error("Пожалуйста, запустите 'rebuild_db.py' для создания таблиц.")
            return

        actual_types = {}
        for row in actual_rows:
            actual_types[row['column_name']] = row['data_type']
            
        actual_columns = set(actual_types.keys())
        
        log.info("--- Сравнение схем... ---")

        # 3. Проверка: Типы колонок
        for col_name, config_type in expected_schema.items():
            
            # Находим, как Postgres называет этот тип
            expected_pg_type = TYPE_MAP.get(config_type)
            
            # Отдельная обработка для VARCHAR, так как длина не важна
            if 'VARCHAR' in config_type:
                expected_pg_type = 'character varying'

            if not expected_pg_type:
                log.warning(f"Неизвестный тип в config.py: {config_type} (колонка {col_name}). Пропуск.")
                continue

            if col_name not in actual_types:
                log.error(f"{bcolors.FAIL}[ОШИБКА] Ожидаемая колонка '{col_name}' отсутствует в БД.{bcolors.ENDC}")
                errors_found += 1
                continue
                
            actual_type = actual_types.get(col_name)
            
            # Главная проверка
            if actual_type != expected_pg_type:
                log.error(f"{bcolors.FAIL}{bcolors.BOLD}--- НЕСООТВЕТСТВИЕ ТИПА ---{bcolors.ENDC}")
                log.error(f"  Колонка:   {bcolors.BOLD}{col_name}{bcolors.ENDC}")
                log.error(f"  Ожидалось (из config.py): {config_type} (т.е. '{expected_pg_type}')")
                log.error(f"  Найдено (в БД):          {bcolors.FAIL}{actual_type}{bcolors.ENDC}")
                
                # Подсказка
                if col_name == 'volume_24h_usd' and actual_type == 'integer':
                    log.warning(f"{bcolors.WARNING}ПОДСКАЗКА: Это и есть причина ошибки 'integer out of range'!{bcolors.ENDC}")
                
                errors_found += 1
            else:
                log.info(f"[OK] Колонка '{col_name}' (Тип: {actual_type})")

        # 4. Проверка: Лишние колонки в БД
        extra_columns = actual_columns - expected_columns - {'id'} # Игнорируем 'id'
        if extra_columns:
            log.warning(f"{bcolors.WARNING}ВНИМАНИЕ: В БД найдены 'лишние' колонки: {extra_columns}{bcolors.ENDC}")

        # 5. Итог
        log.info("--- Валидация завершена ---")
        if errors_found > 0:
            log.error(f"{bcolors.FAIL}РЕЗУЛЬТАТ: Найдено {errors_found} критических несоответствий!{bcolors.ENDC}")
            log.error(f"{bcolors.FAIL}Ваша схема БД не синхронизирована с 'config.py'.{bcolors.ENDC}")
            log.error("РЕШЕНИЕ: 1. Остановите приложение. 2. Запустите 'python rebuild_db.py'.")
        else:
            log.info(f"{bcolors.OKGREEN}РЕЗУЛЬТАТ: Схема БД полностью синхронизирована с 'config.py'.{bcolors.ENDC}")

    except psycopg2.OperationalError as e:
        log.error(f"Ошибка подключения к БД: {e}")
        log.error("Убедитесь, что 'DATABASE_URL' в .env файле указан верно.")
    except Exception as e:
        log.error(f"Неизвестная ошибка: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            log.info("Соединение с БД закрыто.")


if __name__ == "__main__":
    load_dotenv()
    validate_schema()