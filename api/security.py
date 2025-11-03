# api/security.py

import logging
from fastapi import HTTPException, Depends, Request
from fastapi.security import OAuth2PasswordBearer
import config

# --- Настройка ---
log = logging.getLogger(__name__)

# --- Безопасность (Токен) ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def verify_token(request: Request):
    """
    (V3) Проверяет X-Auth-Token.
    Вызывается через Depends() в эндпоинтах.
    """
    # (V3) Используем 'X-Auth-Token' (исправлено)
    token = request.headers.get("X-Auth-Token")
    
    # (ИЗМЕНЕНИЕ №1) (AttributeError) Исправлено: AUTH_TOKEN -> SECRET_TOKEN
    # Эта строка (config.SECRET_TOKEN) теперь "хирургически" синхронизирована с config.py
    if not token or token != config.SECRET_TOKEN:
        log.warning("Доступ запрещен: Неверный X-Auth-Token.")
        raise HTTPException(status_code=401, detail="Неверный X-Auth-Token")
    return True