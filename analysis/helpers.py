# analysis/helpers.py

import logging
from contextlib import asynccontextmanager
# --- (ИЗМЕНЕНИЕ №1) ---
# (УДАЛЕНО) import services
from services import exchange_utils
# --- (КОНЕЦ ИЗМЕНЕНИЯ) ---

log = logging.getLogger(__name__)

@asynccontextmanager
async def exchange_manager(exchange_id, log_prefix=""):
    """
    Асинхронный менеджер контекста. (РЕШЕНИЕ №9 - Безопасное закрытие)
    """
    exchange = None
    try:
        # --- (ИЗМЕНЕНИЕ №2) ---
        exchange = await exchange_utils.initialize_exchange(exchange_id, log_prefix)
        # --- (КОНЕЦ ИЗМЕНЕНИЯ) ---
        yield exchange
    except Exception as e:
        log.error(f"{log_prefix} Ошибка при инициализации {exchange_id}: {e}", exc_info=True)
        yield None # Явно возвращаем None при ошибке
    finally:
        # (РЕШЕНИЕ №9) Безопасное закрытие
        if exchange is not None and hasattr(exchange, 'close'):
            try:
                await exchange.close()
                log.debug(f"{log_prefix} Соединение с {exchange_id} закрыто.")
            except Exception as e:
                log.warning(f"{log_prefix} Ошибка при закрытии {exchange_id}: {e}")