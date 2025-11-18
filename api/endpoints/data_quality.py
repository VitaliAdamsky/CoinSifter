# api/endpoints/data_quality.py

import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

# --- (ИСПРАВЛЕНИЕ РЕФАКТОРИНГА) ---
# (БЫЛО) from services import get_data_quality_report
# (СТАЛО) Импортируем НАПРЯМУЮ из файла, а не из __init__.py
from services.data_quality_service import get_data_quality_report
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# Импортируем правильную зависимость
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)
data_quality_router = APIRouter()

# Определяем зависимость API ключа на уровне модуля
_API_KEY_DEP = Depends(verify_token)

# --- Эндпоинт ---

@data_quality_router.get(
    "/data-quality-report", 
    tags=["Data Quality", "Health"],
    summary="Получить отчет о качестве данных (пропуски в БД)",
    dependencies=[_API_KEY_DEP] 
)
async def get_data_quality_report_endpoint():
    """
    Анализирует таблицу 'filtered_coins' (теперь в Mongo) на предмет 
    пропущенных значений (NaN) и возвращает полный отчет.
    """
    log.info("API: Запрошен отчет о качестве данных...")
    try:
        # (ИЗМЕНЕНИЕ) 'get_data_quality_report' импортирован напрямую
        report = await get_data_quality_report(log_prefix="[API.DataQuality]")
        
        if "error" in report:
            log.error(f"API: Ошибка при создании отчета о качестве: {report['error']}")
            raise HTTPException(status_code=500, detail=report['error'])
            
        return JSONResponse(content=report)
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"API: Необработанная ошибка в get_data_quality_report_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")