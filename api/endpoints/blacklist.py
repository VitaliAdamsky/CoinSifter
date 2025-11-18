# api/endpoints/blacklist.py

import logging
import re
from fastapi import APIRouter, HTTPException, Depends

# --- (ИСПРАВЛЕНИЕ) ---
# (БЫЛО) import services
# (СТАЛО) Импортируем НАПРЯМУЮ
from services.mongo_service import load_blacklist_from_mongo_async
# --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---

# Import our security module
from api.security import verify_token

# --- Setup ---
log = logging.getLogger(__name__)
blacklist_router = APIRouter()

# Symbol validation
SYMBOL_REGEX = re.compile(r"^[A-Z0-9/:-]{1,50}$")

# --- API Endpoints (Blacklist) ---

@blacklist_router.get("/blacklist", dependencies=[Depends(verify_token)])
async def get_blacklist():
    """(V3) Gets the ENTIRE Blacklist (MongoDB)."""
    try:
        # --- (ИСПРАВЛЕНИЕ) ---
        # (БЫЛО) blacklist = await services.load_blacklist_from_mongo_async(...)
        # (СТАЛО) Вызываем напрямую
        blacklist = await load_blacklist_from_mongo_async(
            log_prefix="[API /blacklist GET]"
        )
        # --- (КОНЕЦ ИСПРАВЛЕНИЯ) ---
        
        return {"count": len(blacklist), "blacklist": list(blacklist)}
    except Exception as e:
        log.error(f"[API /blacklist GET] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")