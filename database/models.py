from pydantic import BaseModel, Field
from typing import List, Dict, Any

class CoinQueryParams(BaseModel):
    """
    Pydantic модель для структурирования параметров запроса к БД.
    (Используется в database/coins.py)
    """
    page: int = 1
    limit: int = 100
    sort_by: str = 'volume_24h_usd'
    sort_order: str = 'desc'
    filters: Dict[str, Any] = Field(default_factory=dict)

class CoinsFilteredResponse(BaseModel):
    """
    Pydantic модель для структурирования ответа от БД.
    (Используется в database/coins.py)
    """
    total: int
    data: List[Dict[str, Any]]