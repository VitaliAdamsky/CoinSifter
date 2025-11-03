# api/endpoints/blacklist.py

import logging
import re
from fastapi import APIRouter, HTTPException, Depends

# Import project modules
import services

# Import our security module
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
# (ИЗМЕНЕНО) Переименовано для соответствия __init__.py
blacklist_router = APIRouter()

# (Problem #6) Symbol validation
SYMBOL_REGEX = re.compile(r"^[A-Z0-9/:-]{1,50}$")

# --- API Endpoints (Blacklist) ---

# (УДАЛЕНО) Эндпоинт add_to_blacklist (POST)
# (УДАЛЕНО) Эндпоинт remove_from_blacklist (DELETE)

# (ИЗМЕНЕНО) Используем новое имя переменной
@blacklist_router.get("/blacklist", dependencies=[Depends(verify_token)])
async def get_blacklist():
    """(V3) Gets the ENTIRE Blacklist (MongoDB)."""
    try:
        # --- (V3) Switched from DB to Services ---
        blacklist = await services.load_blacklist_from_mongo_async(log_prefix="[API /blacklist GET]")
        return {"count": len(blacklist), "blacklist": list(blacklist)} # (Convert set to list for JSON)
    except Exception as e:
        log.error(f"[API /blacklist GET] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"DB Error (MongoDB): {e}")