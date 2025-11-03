import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

# Импортируем сервисную функцию
from services import get_data_quality_report

# (ИЗМЕНЕНО) Импортируем правильную зависимость
from ..security import verify_token

# --- Настройка ---
log = logging.getLogger(__name__)
data_quality_router = APIRouter()

# --- (ИСПРАВЛЕНИЕ) Определяем зависимость API ключа на уровне модуля ---
# (ИЗМЕНЕНО) Используем правильную функцию
_API_KEY_DEP = Depends(verify_token)

# --- Эндпоинт ---

@data_quality_router.get(
    "/data-quality-report", 
    tags=["Data Quality", "Health"],
    summary="Получить отчет о качестве данных (пропуски в БД)",
    # (ИСПРАВЛЕНИЕ) Используем _API_KEY_DEP 
    dependencies=[_API_KEY_DEP] 
)
async def get_data_quality_report_endpoint():
    """
    Анализирует таблицу 'filtered_coins' в PostgreSQL на предмет 
    пропущенных значений (NaN) и возвращает полный отчет.
    
    Отчет включает:
    - total_coins: Общее количество монет.
    - analysis_date: Дата последнего анализа (из файла).
    - all_coins_sorted: Список всех монет (алфавитный порядок).
    - missing_data_report: Словарь, где ключ - имя колонки, 
      а значение - список 'symbol' монет, у которых в этой колонке 
      пропущено значение.
    """
    log.info("API: Запрошен отчет о качестве данных...")
    try:
        # (ИЗМЕНЕНО) Добавлен await, т.к. сервис стал асинхронным
        report = await get_data_quality_report(log_prefix="[API.DataQuality]")
        
        if "error" in report:
            log.error(f"API: Ошибка при создании отчета о качестве: {report['error']}")
            raise HTTPException(status_code=500, detail=report['error'])
            
        return JSONResponse(status_code=200, content=report)
        
    except Exception as e:
        log.error(f"API: Необработанная ошибка в /data-quality-report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")