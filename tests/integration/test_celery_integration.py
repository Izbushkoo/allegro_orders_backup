"""
@file: test_celery_integration.py
@description: Интеграционные тесты для celery-задач, связанных с токенами (EAGER-режим)
@dependencies: pytest, celery, app.tasks.token_tasks
"""

import pytest
from app.tasks import token_tasks
from unittest.mock import MagicMock, AsyncMock, patch

@pytest.mark.asyncio
async def test_poll_authorization_status_integration():
    # Мокаем сервис внутри задачи
    with patch("app.tasks.token_tasks.AllegroAuthService") as MockService:
        mock_service = MockService.return_value
        mock_service.check_device_code_status = AsyncMock(return_value={
            "status": "completed",
            "message": "Авторизация успешна"
        })
        mock_service.update_token = AsyncMock(return_value=True)
        result = token_tasks.poll_authorization_status.run(
            device_code="dev123",
            user_id="test_user",
            expires_at_iso="2099-12-31T23:59:59",
            interval_seconds=1
        )
        assert result["status"] == "completed"
        assert result["message"] == "Авторизация успешна" 