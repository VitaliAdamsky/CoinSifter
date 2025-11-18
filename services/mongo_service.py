# services/mongo_service.py

import logging
import asyncio
import os
from pymongo import MongoClient, UpdateOne
from pymongo.results import InsertManyResult, DeleteResult
from bson import ObjectId
from datetime import datetime, timezone
from typing import List, Dict, Any, Set, Optional

# --- Настройка ---
log = logging.getLogger(__name__)

# --- Константы коллекций ---
DB_NAME = "general"
COINS_COLLECTION = "coin-sifter"
BLACKLIST_COLLECTION = "blacklist"
LOGS_COLLECTION = "script_run_logs"

# --- (НОВАЯ КОНСТАНТА) ---
# Автоматически удалять логи из 'script_run_logs' старше X дней
LOGS_TTL_DAYS = 60
# --- (КОНЕЦ НОВОЙ КОНСТАНТЫ) ---


# --- Пул соединений MongoDB ---
_mongo_client: Optional[MongoClient] = None

def get_mongo_client(log_prefix=""):
    """
    Создает и возвращает ЕДИНЫЙ клиент MongoDB с пулом соединений.
    (ИЗМЕНЕНО) Также гарантирует наличие индексов (включая TTL).
    """
    global _mongo_client
    
    if _mongo_client is None:
        log.info(f"{log_prefix} [Mongo.Init] _mongo_client is None. Создание нового клиента...")
        db_url = os.getenv('MONGO_DB_URL')
        if not db_url:
            log.error(f"{log_prefix} [Mongo.Init] ❌ MONGO_DB_URL не установлен.")
            raise ValueError("MONGO_DB_URL must be set")

        try:
            _mongo_client = MongoClient(
                db_url, 
                maxPoolSize=20, 
                minPoolSize=5,
                connectTimeoutMS=10000,
                serverSelectionTimeoutMS=15000
            )
            # Проверка соединения
            _mongo_client.admin.command('ping')
            log.info(f"{log_prefix} [Mongo.Init] ✅ Пинг MongoDB успешен.")

            # --- (НОВЫЙ БЛОК) Гарантируем наличие TTL индекса ---
            log.info(f"{log_prefix} [Mongo.Init] Гарантируем наличие TTL индекса для '{LOGS_COLLECTION}'...")
            db = _mongo_client[DB_NAME]
            logs_collection = db[LOGS_COLLECTION]
            
            # Устанавливаем TTL (время жизни)
            ttl_seconds = LOGS_TTL_DAYS * 24 * 60 * 60
            logs_collection.create_index(
                "start_time", 
                expireAfterSeconds=ttl_seconds
            )
            log.info(f"{log_prefix} [Mongo.Init] ✅ TTL индекс для логов установлен на {LOGS_TTL_DAYS} дней.")
            # --- (КОНЕЦ НОВОГО БЛОКА) ---
            
        except Exception as e:
            log.error(f"{log_prefix} [Mongo.Init] ❌ Не удалось подключиться или настроить MongoDB: {e}", exc_info=True)
            _mongo_client = None
            raise
    
    return _mongo_client

def close_mongo_client(log_prefix=""):
    """
    Закрывает соединение с MongoDB (если оно было открыто).
    """
    global _mongo_client
    if _mongo_client:
        log.info(f"{log_prefix} [Mongo.Close] Закрытие клиента MongoDB...")
        try:
            _mongo_client.close()
            _mongo_client = None
            log.info(f"{log_prefix} [Mongo.Close] ✅ Клиент MongoDB успешно закрыт.")
        except Exception as e:
            log.error(f"{log_prefix} [Mongo.Close] ❌ Ошибка при закрытии клиента MongoDB: {e}", exc_info=True)

# ============================================================================
# BLACKLIST OPERATIONS
# ============================================================================

def _load_blacklist_from_mongo_sync(log_prefix="") -> Set[str]:
    """
    (Sync) Загружает черный список из MongoDB.
    """
    client = get_mongo_client(f"{log_prefix} [DB.Mongo.Blacklist]")
    if client is None: return set()
    
    log.info(f"{log_prefix} Загрузка Blacklist из MongoDB ('{BLACKLIST_COLLECTION}')...")
    
    try:
        db = client[DB_NAME]
        collection = db[BLACKLIST_COLLECTION]
        
        # Загружаем все документы, извлекаем поле 'symbol'
        blacklist_docs = collection.find({}, {'symbol': 1, '_id': 0})
        
        # Используем set comprehension для эффективности
        blacklist_set = {
            doc['symbol'] 
            for doc in blacklist_docs 
            if doc and 'symbol' in doc and doc['symbol']
        }
        
        log.info(f"{log_prefix} ✅ Blacklist загружен. Найдено {len(blacklist_set)} уникальных символов.")
        return blacklist_set
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при загрузке Blacklist из Mongo: {e}", exc_info=True)
        return set()

async def load_blacklist_from_mongo_async(log_prefix="") -> Set[str]:
    """
    (Async) Асинхронная обертка для _load_blacklist_from_mongo_sync.
    """
    return await asyncio.to_thread(_load_blacklist_from_mongo_sync, log_prefix)


# ============================================================================
# COIN OPERATIONS (V3)
# ============================================================================

def _save_coins_to_mongo_v3_sync(data_to_save: List[Dict[str, Any]], log_prefix: str = "") -> int:
    """
    (Sync) Сохраняет данные (V3) в MongoDB, используя 'full_symbol' как ключ.
    """
    client = get_mongo_client(f"{log_prefix} [DB.Mongo.SaveV3]")
    if client is None or not data_to_save:
        if not data_to_save:
            log.warning(f"{log_prefix} Нет данных для сохранения.")
        return 0

    log.info(f"{log_prefix} Подготовка {len(data_to_save)} монет для 'bulk_write'...")
    
    try:
        db = client[DB_NAME]
        collection = db[COINS_COLLECTION]
        
        # 1. Очищаем ВСЮ коллекцию перед записью
        log.info(f"{log_prefix} Очистка коллекции '{COINS_COLLECTION}' перед записью...")
        clear_result = collection.delete_many({})
        log.info(f"{log_prefix} ✅ Коллекция очищена. Удалено {clear_result.deleted_count} старых документов.")

        # 2. Выполняем 'insert_many' (быстрее, чем bulk update, если мы все равно чистим)
        # (Удаляем '_id' на всякий случай, если он остался от предыдущих операций)
        for item in data_to_save:
            item.pop('_id', None) 
            
        result: InsertManyResult = collection.insert_many(data_to_save, ordered=False)
        saved_count = len(result.inserted_ids)
        
        log.info(f"{log_prefix} ✅ 'insert_many' завершен. Вставлено {saved_count} новых документов.")
        return saved_count
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка во время bulk_write (V3) в Mongo: {e}", exc_info=True)
        return 0

async def save_coins_to_mongo_v3(data_to_save: List[Dict[str, Any]], log_prefix: str = "") -> int:
    """
    (Async) Асинхронная обертка для _save_coins_to_mongo_v3_sync.
    """
    return await asyncio.to_thread(_save_coins_to_mongo_v3_sync, data_to_save, log_prefix)


def _get_all_coins_from_mongo_sync(log_prefix: str = "") -> List[Dict[str, Any]]:
    """
    (Sync) Загружает ВСЕ монеты из коллекции 'coin-sifter'.
    """
    client = get_mongo_client(f"{log_prefix} [DB.Mongo.FetchAll]")
    if client is None: return []

    log.info(f"{log_prefix} Загрузка ВСЕХ монет из MongoDB ('{COINS_COLLECTION}')...")
    
    try:
        db = client[DB_NAME]
        collection = db[COINS_COLLECTION]
        
        # Загружаем все документы
        coins_cursor = collection.find()
        
        # Конвертируем _id в str (для JSON-сериализации) и убираем его
        coins_list = []
        for doc in coins_cursor:
            doc.pop('_id', None) # Убираем _id из словаря
            coins_list.append(doc)
            
        log.info(f"{log_prefix} ✅ Успешно загружено {len(coins_list)} монет из MongoDB.")
        return coins_list
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при загрузке монет из Mongo: {e}", exc_info=True)
        return []

async def get_all_coins_from_mongo_async(log_prefix: str = "") -> List[Dict[str, Any]]:
    """
    (Async) Асинхронная обертка для _get_all_coins_from_mongo_sync.
    """
    return await asyncio.to_thread(_get_all_coins_from_mongo_sync, log_prefix)

# ============================================================================
# LOG OPERATIONS
# ============================================================================

def _create_mongo_log_entry_sync(status: str, details: str = "") -> Optional[str]:
    """
    (Sync) Создает новую запись лога в 'script_run_logs' и возвращает ее _id (как str).
    """
    client = get_mongo_client("[DB.Mongo.CreateLog]")
    if client is None: return None

    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        log_entry = {
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "status": status,
            "details": details,
            "coins_saved": 0
        }
        
        result = collection.insert_one(log_entry)
        log_id_str = str(result.inserted_id)
        
        log.info(f"[DB.Mongo.CreateLog] ✅ Создана запись в логе. Run ID: {log_id_str}")
        return log_id_str
        
    except Exception as e:
        log.error(f"❌ Error creating log entry in Mongo: {e}")
        return None

async def create_mongo_log_entry(status: str, details: str = "") -> Optional[str]:
    """
    (Async) Асинхронная обертка для _create_mongo_log_entry_sync.
    """
    return await asyncio.to_thread(_create_mongo_log_entry_sync, status, details)


def _update_mongo_log_status_sync(log_id_str: str, status: str, details: str = "", coins_saved: int = None):
    """
    (Sync) Обновляет существующую запись лога в 'script_run_logs'.
    """
    if not log_id_str:
        log.warning("[DB.Mongo.UpdateLog] Пропущен вызов (log_id_str is None).")
        return

    client = get_mongo_client(f"[DB.Mongo.UpdateLog ID: {log_id_str}]")
    if client is None: return

    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        # Конвертируем str обратно в ObjectId
        try:
            obj_id = ObjectId(log_id_str)
        except Exception:
            log.error(f"❌ Неверный формат log_id: '{log_id_str}'. Невозможно обновить лог.")
            return

        update_fields = {
            "status": status,
            "details": details,
            "end_time": datetime.now(timezone.utc)
        }
        
        if coins_saved is not None:
            update_fields["coins_saved"] = coins_saved
            
        collection.update_one(
            {"_id": obj_id},
            {"$set": update_fields}
        )
        
        log.info(f"✅ Лог {log_id_str} обновлен: Status='{status}', Coins={coins_saved}")
        
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
            doc['id'] = str(doc.pop('_id')) # Конвертируем _id в 'id' (str)
            logs.append(doc)
            
        log.info(f"[DB.Mongo.FetchLogs] ✅ Загружено {len(logs)} логов.")
        return logs
        
    except Exception as e:
        log.error(f"❌ Ошибка при загрузке логов из Mongo: {e}", exc_info=True)
        return []

async def get_mongo_logs(limit: int = 50) -> List[Dict[str, Any]]:
    """
    (Async) Асинхронная обертка для _get_mongo_logs_sync.
    """
    return await asyncio.to_thread(_get_mongo_logs_sync, limit)


# --- (НОВЫЙ БЛОК) Ручная очистка логов ---

def _clear_all_mongo_logs_sync(log_prefix: str = "") -> int:
    """
    (Sync) Полностью очищает коллекцию 'script_run_logs'.
    """
    client = get_mongo_client(f"{log_prefix} [DB.Mongo.ClearLogs]")
    if client is None: return 0

    log.warning(f"{log_prefix} ⚠️ Запрос на ПОЛНУЮ ОЧИСТКУ коллекции '{LOGS_COLLECTION}'...")
    
    try:
        db = client[DB_NAME]
        collection = db[LOGS_COLLECTION]
        
        # Полная очистка
        result: DeleteResult = collection.delete_many({})
        deleted_count = result.deleted_count
        
        log.info(f"{log_prefix} ✅ Коллекция '{LOGS_COLLECTION}' очищена. Удалено документов: {deleted_count}")
        return deleted_count
        
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при очистке логов из Mongo: {e}", exc_info=True)
        return 0

async def clear_all_mongo_logs(log_prefix: str = "") -> int:
    """
    (Async) Асинхронная обертка для _clear_all_mongo_logs_sync.
    """
    return await asyncio.to_thread(_clear_all_mongo_logs_sync, log_prefix)

# --- (КОНЕЦ НОВОГО БЛОКА) ---