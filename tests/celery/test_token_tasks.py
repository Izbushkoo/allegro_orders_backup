"""
@file: test_token_tasks.py
@description: Unit-тесты для celery-задач, связанных с токенами (app/tasks/token_tasks.py)
@dependencies: pytest, unittest.mock, celery
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from app.tasks import token_tasks

@pytest.mark.asyncio
async def test_poll_authorization_status_completed():
    with patch("app.tasks.token_tasks.AllegroAuthService") as MockService:
        mock_service = MockService.return_value
        mock_service.check_auth_status = AsyncMock(return_value={
            "status": "completed",
            "message": "Авторизация успешна"
        })
        # Мокаем update_token и другие методы, если вызываются
        mock_service.update_token = AsyncMock(return_value=True)
        # Запускаем задачу
        result = await token_tasks.poll_authorization_status(
            device_code="dev123",
            user_id="test_user",
            db_session=MagicMock()
        )
        assert result["status"] == "completed"
        assert result["message"] == "Авторизация успешна"

@pytest.mark.asyncio
async def test_poll_authorization_status_pending():
    with patch("app.tasks.token_tasks.AllegroAuthService") as MockService:
        mock_service = MockService.return_value
        mock_service.check_auth_status = AsyncMock(return_value={
            "status": "pending",
            "message": "Ожидание подтверждения"
        })
        result = await token_tasks.poll_authorization_status(
            device_code="dev123",
            user_id="test_user",
            db_session=MagicMock()
        )
        assert result["status"] == "pending"

@pytest.mark.asyncio
async def test_poll_authorization_status_failed():
    with patch("app.tasks.token_tasks.AllegroAuthService") as MockService:
        mock_service = MockService.return_value
        mock_service.check_auth_status = AsyncMock(return_value={
            "status": "failed",
            "message": "Ошибка авторизации"
        })
        result = await token_tasks.poll_authorization_status(
            device_code="dev123",
            user_id="test_user",
            db_session=MagicMock()
        )
        assert result["status"] == "failed"
        assert result["message"] == "Ошибка авторизации" 