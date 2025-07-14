"""
@file: conftest.py
@description: Фикстуры для тестов API (создание тестового пользователя и JWT-токена)
@dependencies: pytest, fastapi, httpx
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.models.user_token import UserToken
from app.api.v1.tokens import TokenNotFoundHTTPException
from datetime import datetime
from unittest.mock import patch, MagicMock
try:
    from unittest.mock import AsyncMock
except ImportError:
    # Для Python <3.8
    from asynctest import CoroutineMock as AsyncMock

client = TestClient(app)

TEST_TOKEN_ID = "11111111-1111-1111-1111-111111111111"

@pytest.fixture(scope="session")
def test_jwt_token():
    """
    Фикстура для получения тестового JWT-токена через /test-jwt/create (POST).
    """
    params = {"user_id": "test_user_123", "username": "test_user"}
    response = client.post("/test-jwt/create", params=params)
    assert response.status_code == 200
    token = response.json()["access_token"]
    return token

@pytest.fixture(autouse=True)
def mock_external_services():
    """
    Автоматический мокинг Allegro API и Celery для всех тестов.
    """
    user_token = UserToken(
        id=TEST_TOKEN_ID,
        user_id="test_user_123",
        allegro_token="allegro_test_token",
        refresh_token="allegro_refresh_token",
        expires_at=datetime(2099, 12, 31, 23, 59, 59),
        is_active=True,
        created_at=datetime(2024, 1, 1, 0, 0, 0),
        updated_at=None
    )
    async def get_user_token_by_id(token_id, user_id):
        if str(token_id) == TEST_TOKEN_ID:
            return user_token
        raise TokenNotFoundHTTPException()
    async def update_token(token_id, user_id, **kwargs):
        if str(token_id) == TEST_TOKEN_ID:
            return user_token
        raise TokenNotFoundHTTPException()
    with patch("app.services.allegro_auth_service.AllegroAuthService.initialize_device_flow", MagicMock(return_value={
        "device_code": "dev123",
        "user_code": "user123",
        "verification_uri": "https://allegro.pl/auth",
        "expires_in": 600,
        "interval": 5
    })) as _mock_device_code, \
         patch("app.services.allegro_auth_service.AllegroAuthService.check_auth_status", MagicMock(return_value={
        "status": "pending",
        "message": "Ожидание подтверждения"
    })) as _mock_status, \
         patch("app.services.token_service.TokenService.create_token", AsyncMock(return_value=user_token)) as _mock_create_token, \
         patch("app.services.token_service.TokenService.get_tokens", AsyncMock(return_value=([user_token], 1))) as _mock_get_tokens, \
         patch("app.services.token_service.TokenService.get_user_token_by_id", AsyncMock(side_effect=get_user_token_by_id)) as _mock_get_token, \
         patch("app.services.token_service.TokenService.update_token", AsyncMock(side_effect=update_token)) as _mock_update_token:
        yield 