"""
@file: app/api/v1/api.py
@description: Основной API роутер v1 для Allegro Orders Backup
@dependencies: fastapi
"""

from fastapi import APIRouter

from app.api.v1 import tokens, orders, sync, offers

# Основной роутер для API v1
api_router = APIRouter()

# Подключение всех роутеров
api_router.include_router(
    tokens.router,
    prefix="/tokens",
    tags=["tokens"],
)

api_router.include_router(
    orders.router,
    prefix="/orders",
    tags=["orders"],
)

api_router.include_router(
    sync.router,
    prefix="/sync",
    tags=["sync"],
)

api_router.include_router(
    offers.router,
    prefix="/offers",
    tags=["offers"],
)