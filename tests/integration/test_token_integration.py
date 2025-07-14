"""
@file: test_token_integration.py
@description: Интеграционные тесты для работы с токенами через сервисы (реальная БД)
@dependencies: pytest, SQLModel, asyncio
"""

import pytest
from sqlmodel.ext.asyncio.session import AsyncSession
from app.services.token_service import TokenService
from app.models.user_token import UserToken
from uuid import uuid4
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_token_crud_integration(async_session: AsyncSession):
    service = TokenService(async_session)
    user_id = f"test_user_{uuid4()}"
    allegro_token = "allegro_token"
    refresh_token = "refresh_token"
    expires_at = datetime.utcnow() + timedelta(days=30)

    # Создание токена
    token = await service.create_token(
        user_id=user_id,
        allegro_token=allegro_token,
        refresh_token=refresh_token,
        expires_at=expires_at
    )
    assert token.user_id == user_id
    assert token.allegro_token == allegro_token

    # Получение токенов
    tokens, total = await service.get_tokens(user_id=user_id, page=1, per_page=10, active_only=True)
    assert total == 1
    assert tokens[0].id == token.id

    # Обновление токена
    updated_token = await service.update_token(
        token_id=token.id,
        user_id=user_id,
        allegro_token="updated_token",
        refresh_token=None,
        expires_at=None,
        is_active=None
    )
    assert updated_token.allegro_token == "updated_token"

    # Удаление токена
    deleted = await service.delete_token(token_id=token.id, user_id=user_id)
    assert deleted is True
    tokens, total = await service.get_tokens(user_id=user_id, page=1, per_page=10, active_only=True)
    assert total == 0 