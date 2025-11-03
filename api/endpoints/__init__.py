# api/endpoints/__init__.py

"""Инициализация всех роутеров API."""

from .health import health_router
from .logs import logs_router
from .blacklist import blacklist_router
from .data_quality import data_quality_router
from .trigger import trigger_router
from .coins import coins_router
# (ИЗМЕНЕНИЕ №1) Добавляем новый роутер
from .formatted_symbols import formatted_symbols_router

__all__ = [
    "health_router",
    "logs_router",
    "blacklist_router",
    "data_quality_router",
    "trigger_router",
    "coins_router",
    "formatted_symbols_router", # (ИЗМЕНЕНИЕ №1)
]