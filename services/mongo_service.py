# services/mongo_service.py
"""
(МИГРАЦИЯ НА MONGO)
Этот модуль управляет ВСЕМИ соединениями и операциями с MongoDB.
Он заменяет функции из database/coins.py и database/logs.py.

- База данных: 'general'
- Коллекции:
    - 'coin-sifter': (Новая) Хранит результаты анализа (90+ метрик).
    - 'blacklist': (Существующая) Хранит черный список.
    - 'script_run_logs': (Новая) Хранит логи выполнения.
"""

import logging
import asyncio
import os
from pymongo import MongoClient, UpdateOne
from pymongo.results import InsertManyResult, DeleteResult
# (ИЗМЕНЕНИЕ) Импортируем 'timezone'
from datetime import datetime, timezone
from typing import List, Dict, Any, Set, Optional

# --- Настройка ---
log = logging.getLogger(__name__)

# --- Константы коллекций ---
DB_NAME = "general"
COINS_COLLECTION = "coin-sifter"
BLACKLIST_COLLECTION = "blacklist"
LOGS_COLLECTION = "script_run_logs"


# --- Пул соединений MongoDB ---
_mongo_client: Optional[MongoClient] = None

def get_mongo_client(log_prefix=""):
    """
    Создает и возвращает ЕДИНЫЙ клиент MongoDB с пулом соединений.
    """
    global _mongo_client
    if _mongo_client is None:
        mongo_url = os.getenv('MONGO_DB_URL')

        if not mongo_url:
            log.error("MONGO_DB_URL не установлен в .env файле.")
            return None

        try:
            log.info(f"{log_prefix} Создание нового пула соединений MongoDB...")
            _mongo_client = MongoClient(
                mongo_url,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=10,
                minPoolSize=2
            )
            # Проверяем соединение
            _mongo_client.server_info()
            log.info("Пул MongoDB успешно создан.")
        except Exception as e:
            log.error(f"Не удалось подключиться к MongoDB: {e}")
            _mongo_client = None # Сбрасываем, чтобы попробовать снова
            return None

    return _mongo_client

def close_mongo_client(log_prefix=""):
    """
    Закрывает пул соединений MongoDB.
    """
    global _mongo_client
    if _mongo_client is not None:
        try:
            _mongo_client.close()
            _mongo_client = None
            log.info(f"{log_prefix} Пул соединений MongoDB закрыт.")
        except Exception as e:
            log.error(f"{log_prefix} Ошибка при закрытии MongoDB: {e}")


# ============================================================================
# === ЧЕРНЫЙ СПИСОК (Blacklist) ===
# ============================================================================

def _load_blacklist_from_mongo_sync(log_prefix="") -> Set[str]:
    """
    (Sync) Загружает черный список монет из 'general.blacklist'.
    """
    client = get_mongo_client(log_prefix)
    if client is None:
        return set()

    try:
        db = client[DB_NAME]
        collection = db[BLACKLIST_COLLECTION]

        blacklist_docs = list(collection.find({}, {'_id': 0, 'symbol': 1}))
        
        blacklist = set()
        for doc in blacklist_docs:
            if 'symbol' in doc:
                blacklist.add(doc['symbol'])

        if blacklist:
            log.info(f"{log_prefix} Загружено {len(blacklist)} монет в черном списке.")
        else:
            log.warning(f"{log_prefix} Черный список в MongoDB пуст или не найден.")

        return blacklist

    except Exception as e:
        log.error(f"{log_prefix} Ошибка при загрузке черного списка из MongoDB: {e}")
        return set()

async def load_blacklist_from_mongo_async(log_prefix="") -> Set[str]:
    """
    (Async) Асинхронная обертка для _load_blacklist_from_mongo_sync.
    Используется в router.py и analysis, чтобы не блокировать event loop.
    """
    return await asyncio.to_thread(_load_blacklist_from_mongo_sync, log_prefix)


# ============================================================================
# === РЕЗУЛЬТАТЫ АНАЛИЗА (Coins) ===
# (Замена database/coins.py)
# ============================================================================

def _save_coins_to_mongo_sync(coins: List[Dict[str, Any]], log_prefix="[DB.Mongo.Save]") -> int:
    """
    (Sync) Полностью перезаписывает коллекцию 'coin-sifter' новыми данными.
    """
    if not coins:
        log.warning(f"{log_prefix} Нет данных для сохранения.")
        return 0
        
    client = get_mongo_client(log_prefix)
    if client is None:
        log.error(f"{log_prefix} ❌ Не удалось подключиться к Mongo. Сохранение отменено.")
        return 0
        
    try:
        db = client[DB_NAME]
        collection = db[COINS_COLLECTION]
        
        # --- 1. Очистка ---
        log.info(f"{log_prefix} Очистка коллекции '{COINS_COLLECTION}'...")
        delete_result: DeleteResult = collection.delete_many({})
        log.info(f"{log_prefix} ✅ Удалено {delete_result.deleted_count} старых документов.")
        
        # --- 2. Вставка ---
        log.info(f"{log_prefix} Вставка {len(coins)} новых документов...")
        # (Примечание: убираем '_id', если он случайно попал, 
        # чтобы Mongo сгенерировала свои)
        for coin in coins:
            coin.pop('_id', None)
            
        insert_result: InsertManyResult = collection.insert_many(coins, ordered=False)
        saved_count = len(insert_result.inserted_ids)
        
        log.info(f"{log_prefix} ✅ Успешно вставлено {saved_count} монет.")
        
        return saved_count

    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при сохранении в MongoDB: {e}", exc_info=True)
        return 0

async def save_coins_to_mongo(coins: List[Dict[str, Any]], log_prefix=""):
    """
    (Async) Асинхронная обертка для _save_coins_to_mongo_sync.
    """
    return await asyncio.to_thread(_save_coins_to_mongo_sync, coins, log_prefix)


def _get_all_coins_from_mongo_sync(log_prefix="[DB.Mongo.FetchAll]") -> List[Dict[str, Any]]:
    """
    (Sync) Загружает ВСЕ монеты из 'coin-sifter'.
    """
    client = get_mongo_client(log_prefix)
    if client is None:
        log.error(f"{log_prefix} ❌ Не удалось подключиться к Mongo. Загрузка отменена.")
        return []
        
    try:
        db = client[DB_NAME]
        collection = db[COINS_COLLECTION]
        
        log.info(f"{log_prefix} Загрузка ВСЕХ монет из '{COINS_COLLECTION}'...")
        
        # Убираем '_id' из проекции, так как он не сериализуется в JSON
        data = list(collection.find({}, {'_id': 0}))
        
        log.info(f"{log_prefix} ✅ Загружено {len(data)} монет.")
        return data

    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при чтении из MongoDB: {e}", exc_info=True)
        return []

async def get_all_coins_from_mongo_async(log_prefix=""):
    """
    (Async) Асинхронная обертка для _get_all_coins_from_mongo_sync.
    Используется 'data_cache_service'.
    """
    return await asyncio.to_thread(_get_all_coins_from_mongo_sync, log_prefix)


# ============================================================================
# === ЛОГИ ВЫПОЛНЕНИЯ (Logs) ===
# (Замена database/logs.py)
# ============================================================================

def _create_mongo_log_entry_sync(status: str, details: str = "") -> Optional[str]:
    """
    (Sync) Создает новую запись в 'script_run_logs' и возвращает ее _id (как строку).
    """
    client = get_mongo_client("[DB.Mongo.Log]")
    if client is None: return None
    
    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        log_doc = {
            # (ИЗМЕНЕНИЕ) Используем 'timezone.utc'
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "status": status,
            "details": details,
            "coins_saved": 0
        }
        
        result = collection.insert_one(log_doc)
        log_id_str = str(result.inserted_id)
        log.debug(f"Created log entry with ID: {log_id_str}")
        return log_id_str
        
    except Exception as e:
        log.error(f"❌ Error creating log entry in Mongo: {e}", exc_info=True)
        return None

async def create_mongo_log_entry(status: str, details: str = "") -> Optional[str]:
    """
    (Async) Асинхронная обертка для _create_mongo_log_entry_sync.
    """
    return await asyncio.to_thread(_create_mongo_log_entry_sync, status, details)


def _update_mongo_log_status_sync(log_id_str: str, status: str, details: str = "", coins_saved: int = None):
    """
    (Sync) Обновляет статус лога в 'script_run_logs' по _id (строке).
    """
    client = get_mongo_client("[DB.Mongo.LogUpdate]")
    if client is None: return
    
    try:
        from bson.objectid import ObjectId
        log_oid = ObjectId(log_id_str)
    except Exception:
        log.error(f"❌ Некорректный Mongo _id: {log_id_str}")
        return

    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        update_fields = {
            "status": status,
            "details": details
        }
        
        if status in ("Завершено", "Completed", "Критическая ошибка", "Ошибка", "Error"):
            # (ИЗМЕНЕНИЕ) Используем 'timezone.utc'
            update_fields["end_time"] = datetime.now(timezone.utc)
        
        if coins_saved is not None:
            update_fields["coins_saved"] = coins_saved
            
        collection.update_one(
            {"_id": log_oid},
            {"$set": update_fields}
        )
        log.debug(f"Updated log {log_id_str}: status='{status}', coins_saved={coins_saved}")
        
    except Exception as e:
        log.error(f"❌ Error updating log {log_id_str} in Mongo: {e}")

async def update_mongo_log_status(log_id_str: str, status: str, details: str = "", coins_saved: int = None):
    """
    (Async) Асинхронная обертка для _update_mongo_log_status_sync.
    """
    await asyncio.to_thread(_update_mongo_log_status_sync, log_id_str, status, details, coins_saved)


def _get_mongo_logs_sync(limit: int = 50) -> List[Dict[str, Any]]:
    """
    (Sync) Загружает последние N логов из 'script_run_logs'.
    """
    client = get_mongo_client("[DB.Mongo.FetchLogs]")
    if client is None: return []

    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        logs_cursor = collection.find(
            {}, 
            {'_id': 1, 'start_time': 1, 'end_time': 1, 'status': 1, 'details': 1, 'coins_saved': 1}
        ).sort("start_time", -1).limit(limit)
        
        logs = []
        for doc in logs_cursor:
            doc['id'] = str(doc.pop('_id')) # Преобразуем _id в id (как было в PG)
            logs.append(doc)
            
        return logs
        
    except Exception as e:
        log.error(f"❌ Error fetching logs from Mongo: {e}")
        return []

async def get_mongo_logs(limit: int = 50) -> List[Dict[str, Any]]:
    """
    (Async) Асинхронная обертка для _get_mongo_logs_sync.
    """
    return await asyncio.to_thread(_get_mongo_logs_sync, limit)