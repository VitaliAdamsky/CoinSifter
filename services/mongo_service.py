import logging
import asyncio
import os
from pymongo import MongoClient
from datetime import datetime

# --- Настройка ---
log = logging.getLogger(__name__)

# --- Пул соединений MongoDB ---
_mongo_client = None

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
    (V3) (Проблема #1) Закрывает пул соединений MongoDB.
    """
    global _mongo_client
    if _mongo_client is not None:
        try:
            _mongo_client.close()
            _mongo_client = None
            log.info(f"{log_prefix} Пул соединений MongoDB закрыт.")
        except Exception as e:
            log.error(f"{log_prefix} Ошибка при закрытии MongoDB: {e}")


def load_blacklist_from_mongo(log_prefix=""):
    """
    (V3) Загружает черный список монет из коллекции 'general.blacklist' в MongoDB.
    (Использует пул соединений)
    """
    client = get_mongo_client(log_prefix)
    if client is None:
        return set() # (ИЗМЕНЕНИЕ №1) Возвращаем пустой set

    try:
        db = client.general
        collection = db.blacklist

        # (V3) analysis.py V3 ожидает СПИСОК СИМВОЛОВ (e.g., 'BTC/USDT:USDT')
        blacklist_docs = list(collection.find({}, {'_id': 0, 'symbol': 1}))
        
        # (ИЗМЕНЕНИЕ №1) Используем set для O(1) поиска
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
        return set() # (ИЗМЕНЕНИЕ №1) Возвращаем пустой set

# --- (ИЗМЕНЕНИЕ №2) АСИНХРОННЫЕ ФУНКЦИИ ДЛЯ ROUTER.PY ---

async def load_blacklist_from_mongo_async(log_prefix=""):
    """
    (ДОБАВЛЕНО) Асинхронная обертка для load_blacklist_from_mongo.
    Используется в router.py, чтобы не блокировать event loop.
    """
    return await asyncio.to_thread(load_blacklist_from_mongo, log_prefix)

# --- (ИЗМЕНЕНИЕ №1) Функции add/remove УДАЛЕНЫ по требованию ---
