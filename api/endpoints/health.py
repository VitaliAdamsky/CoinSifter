# api/endpoints/health.py

from fastapi import APIRouter, Depends, HTTPException
# (ИЗМЕНЕНИЕ №1) Исправлен неверный импорт
from ..security import verify_token
from database import fetch_last_analysis_timestamp
import logging

log = logging.getLogger(__name__)

# --- Setup ---
health_router = APIRouter()

@health_router.get("/health")
@health_router.head("/health")  # ✅ ИСПРАВЛЕНИЕ
def health_check():
    """Server health check."""
    return {"status": "ok"}


@health_router.get("/health/last_analysis")
def get_last_analysis(token: str = Depends(verify_token)):
    """
    Возвращает timestamp последнего анализа из БД.
    Защищено токеном.
    """
    log_prefix = "[API.Health.LastAnalysis]"
    log.info(f"{log_prefix} Запрос времени последнего анализа...")
    
    try:
        timestamp = fetch_last_analysis_timestamp(log_prefix=log_prefix)
        
        if timestamp is None:
            log.warning(f"{log_prefix} Таблица пуста - данных нет.")
            raise HTTPException(
                status_code=404,
                detail="No analysis data found. Table is empty."
            )
        
        log.info(f"{log_prefix} ✅ Возврат timestamp: {timestamp}")
        return {"analyzed_at": timestamp}
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"{log_prefix} ❌ Ошибка при получении timestamp: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch last analysis timestamp"
        )

# (ИЗМЕНЕНИЕ №2) Лишняя скобка '}' была удалена отсюда