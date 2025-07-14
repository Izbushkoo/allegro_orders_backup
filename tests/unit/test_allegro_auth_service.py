"""
@file: test_allegro_auth_service.py
@description: Unit-тесты для AllegroAuthService (app/services/allegro_auth_service.py)
@dependencies: pytest, unittest.mock
"""

import pytest
from unittest.mock import AsyncMock, patch
from app.services.allegro_auth_service import AllegroAuthService

@pytest.fixture
def mock_db_session():
    return AsyncMock()

@pytest.mark.asyncio
async def test_initialize_device_flow_success(mock_db_session):
    service = AllegroAuthService(mock_db_session)
    with patch.object(service, 'initialize_device_flow', AsyncMock(return_value={
        "device_code": "dev123",
        "user_code": "user123",
        "verification_uri": "https://allegro.pl/auth",
        "expires_in": 600,
        "interval": 5
    })):
        result = await service.initialize_device_flow(user_id="test_user")
        assert result["device_code"] == "dev123"
        assert result["user_code"] == "user123"

@pytest.mark.asyncio
async def test_check_auth_status_pending(mock_db_session):
    service = AllegroAuthService(mock_db_session)
    with patch.object(service, 'check_auth_status', AsyncMock(return_value={
        "status": "pending",
        "message": "Ожидание подтверждения"
    })):
        result = await service.check_auth_status(device_code="dev123", user_id="test_user")
        assert result["status"] == "pending"

@pytest.mark.asyncio
async def test_check_auth_status_completed(mock_db_session):
    service = AllegroAuthService(mock_db_session)
    with patch.object(service, 'check_auth_status', AsyncMock(return_value={
        "status": "completed",
        "message": "Авторизация успешна"
    })):
        result = await service.check_auth_status(device_code="dev123", user_id="test_user")
        assert result["status"] == "completed"

@pytest.mark.asyncio
async def test_check_auth_status_failed(mock_db_session):
    service = AllegroAuthService(mock_db_session)
    with patch.object(service, 'check_auth_status', AsyncMock(return_value={
        "status": "failed",
        "message": "Ошибка авторизации"
    })):
        result = await service.check_auth_status(device_code="dev123", user_id="test_user")
        assert result["status"] == "failed" 