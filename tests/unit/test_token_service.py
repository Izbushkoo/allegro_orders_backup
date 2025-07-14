"""
@file: test_token_service.py
@description: Unit-тесты для TokenService (app/services/token_service.py)
@dependencies: pytest, unittest.mock, SQLModel
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from datetime import datetime, timedelta

from app.services.token_service import TokenService
from app.models.user_token import UserToken

@pytest.fixture
def mock_db_session():
    return AsyncMock()

@pytest.fixture
def token_data():
    return {
        "user_id": "test_user",
        "allegro_token": "allegro_token",
        "refresh_token": "refresh_token",
        "expires_at": datetime.utcnow() + timedelta(days=30)
    }

@pytest.mark.asyncio
async def test_create_token_success(mock_db_session, token_data):
    service = TokenService(mock_db_session)
    # Мокаем возврат UserToken
    service.create_token = AsyncMock(return_value=UserToken(id=uuid4(), **token_data, is_active=True, created_at=datetime.utcnow()))
    token = await service.create_token(**token_data)
    assert token.user_id == token_data["user_id"]
    assert token.allegro_token == token_data["allegro_token"]

@pytest.mark.asyncio
async def test_get_tokens_empty(mock_db_session):
    service = TokenService(mock_db_session)
    service.get_tokens = AsyncMock(return_value=([], 0))
    tokens, total = await service.get_tokens(user_id="test_user", page=1, per_page=10, active_only=True)
    assert tokens == []
    assert total == 0

@pytest.mark.asyncio
async def test_update_token_not_found(mock_db_session):
    service = TokenService(mock_db_session)
    service.update_token = AsyncMock(return_value=None)
    result = await service.update_token(token_id=uuid4(), user_id="test_user", allegro_token="new", refresh_token=None, expires_at=None, is_active=None)
    assert result is None

@pytest.mark.asyncio
async def test_delete_token_success(mock_db_session):
    service = TokenService(mock_db_session)
    service.delete_token = AsyncMock(return_value=True)
    result = await service.delete_token(token_id=uuid4(), user_id="test_user")
    assert result is True

@pytest.mark.asyncio
async def test_delete_token_not_found(mock_db_session):
    service = TokenService(mock_db_session)
    service.delete_token = AsyncMock(return_value=False)
    result = await service.delete_token(token_id=uuid4(), user_id="test_user")
    assert result is False 