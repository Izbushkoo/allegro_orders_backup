"""
@file: app/models/offer.py
@description: Pydantic-модели для работы с офферами
"""

from pydantic import BaseModel

class ExternalStockUpdateRequest(BaseModel):
    external_id: str
    stock: int