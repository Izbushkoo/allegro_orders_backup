"""
@file: app/tasks/token_tasks.py
@description: Celery задачи для управления токенами Allegro, включая массовое обновление с историей
@dependencies: celery, TaskHistoryService, TokenService, sqlmodel
"""

import asyncio
from datetime import datetime, timedelta

from celery.exceptions import Retry
from app.celery_app import celery_app
from app.core.logging import get_logger
from app.core.database import get_sync_session
from app.services.allegro_auth_service import AllegroAuthService
from app.core.database import get_sync_db_session_direct
from app.models.user_token import UserToken
from app.services.token_service import TokenService
from sqlmodel import Session
from app.services.task_history_service import TaskHistoryService
from uuid import UUID

logger = get_logger(__name__)


@celery_app.task(bind=True, max_retries=30, rate_limit='10/m')
def poll_authorization_status(self, device_code: str, user_id: str, account_name: str, expires_at_iso: str, interval_seconds: int = 5):
    """
    Задача для отслеживания статуса авторизации Device Code Flow (sync).
    Args:
        device_code: Код устройства для проверки
        user_id: ID пользователя
        account_name: Название аккаунта Allegro
        expires_at_iso: Время истечения в ISO формате
        interval_seconds: Интервал проверки в секундах
    """
    logger.debug(f"[DEBUG] poll_authorization_status task started (sync)")
    logger.debug(f"[DEBUG] Parameters: device_code={device_code[:10]}..., user_id={user_id}, expires_at_iso={expires_at_iso}, interval_seconds={interval_seconds}")
    logger.debug(f"[DEBUG] Current retry attempt: {self.request.retries + 1}/{self.max_retries + 1}")

    from app.core.database import get_sync_db_session_direct
    from app.services.allegro_auth_service import AllegroAuthService
    from celery.exceptions import Retry
    from datetime import datetime

    try:
        logger.debug(f"[DEBUG] Parsing expires_at_iso: {expires_at_iso}")
        expires_at = datetime.fromisoformat(expires_at_iso)
        current_time = datetime.utcnow()
        logger.debug(f"[DEBUG] Current time: {current_time}, expires_at: {expires_at}")

        # Проверяем не истекло ли время авторизации
        if current_time > expires_at:
            logger.warning(f"Authorization expired for user {user_id}, device_code: {device_code}")
            logger.debug(f"[DEBUG] Authorization expired: current_time={current_time} > expires_at={expires_at}")
            result = {
                "status": "expired",
                "user_id": user_id,
                "device_code": device_code,
                "message": "Authorization time expired"
            }
            logger.debug(f"[DEBUG] poll_authorization_status completed with expired status: {result}")
            return result

        logger.info(f"Polling authorization status for user {user_id}, attempt {self.request.retries + 1}/30 (sync)")
        logger.debug(f"[DEBUG] Creating sync DB session and AllegroAuthService instance")

        sync_session = get_sync_db_session_direct()
        try:
            auth_service = AllegroAuthService(sync_session)
            logger.debug(f"[DEBUG] Calling auth_service.check_auth_status_sync")
            result = auth_service.check_auth_status_sync(device_code, user_id, account_name)
            logger.debug(f"[DEBUG] Authorization check result: {result}")
        finally:
            logger.debug(f"[DEBUG] Closing sync DB session")
            sync_session.close()

        if result["status"] == "completed":
            logger.info(f"Authorization completed successfully for user {user_id}")
            final_result = {
                "status": "completed",
                "user_id": user_id,
                "device_code": device_code,
                "message": "Token saved successfully"
            }
            logger.debug(f"[DEBUG] poll_authorization_status completed successfully: {final_result}")
            return final_result
        elif result["status"] == "failed":
            logger.error(f"Authorization failed for user {user_id}: {result.get('message')}")
            final_result = {
                "status": "failed",
                "user_id": user_id,
                "device_code": device_code,
                "message": result.get("message", "Authorization failed")
            }
            logger.debug(f"[DEBUG] poll_authorization_status completed with failure: {final_result}")
            return final_result
        elif result["status"] == "pending":
            # Проверяем не истекло ли время
            expires_at = datetime.fromisoformat(expires_at_iso)
            current_time = datetime.utcnow()
            logger.debug(f"[DEBUG] Pending status, checking time again: current_time={current_time}, expires_at={expires_at}")

            if current_time >= expires_at:
                logger.warning(f"Authorization expired for user {user_id}")
                logger.debug(f"[DEBUG] Authorization expired during pending check")
                final_result = {
                    "status": "failed",
                    "user_id": user_id,
                    "device_code": device_code,
                    "message": "Authorization timeout expired"
                }
                logger.debug(f"[DEBUG] poll_authorization_status completed with timeout: {final_result}")
                return final_result

            # Продолжаем polling с задержкой
            logger.debug(f"Authorization still pending for user {user_id}, retrying in {interval_seconds}s")
            logger.debug(f"[DEBUG] Raising retry exception with countdown={interval_seconds}")
            raise self.retry(countdown=interval_seconds, max_retries=self.max_retries)
        else:
            logger.error(f"Unknown authorization status for user {user_id}: {result['status']}")
            logger.debug(f"[DEBUG] Unknown status received: {result}")
            final_result = {
                "status": "failed",
                "user_id": user_id,
                "device_code": device_code,
                "message": f"Unknown status: {result['status']}"
            }
            logger.debug(f"[DEBUG] poll_authorization_status completed with unknown status: {final_result}")
            return final_result

    except Retry:
        logger.debug(f"[DEBUG] Retry exception raised, will be handled by Celery")
        raise
    except Exception as exc:
        logger.error(f"Error polling authorization status for user {user_id}: {str(exc)}")
        logger.debug(f"[DEBUG] Exception in poll_authorization_status: {type(exc)} - {str(exc)}", exc_info=True)

        if self.request.retries >= self.max_retries:
            logger.debug(f"[DEBUG] Max retries reached: {self.request.retries}/{self.max_retries}")
            final_result = {
                "status": "failed",
                "user_id": user_id,
                "device_code": device_code,
                "message": f"Max retries exceeded: {str(exc)}"
            }
            logger.debug(f"[DEBUG] poll_authorization_status completed with max retries exceeded: {final_result}")
            return final_result

        delay = min(300, (2 ** self.request.retries) * interval_seconds)  # Максимум 5 минут
        logger.info(f"Retrying authorization polling for user {user_id} in {delay}s")
        logger.debug(f"[DEBUG] Exponential backoff delay calculated: {delay}s")
        logger.debug(f"[DEBUG] Raising retry exception with countdown={delay}")
        raise self.retry(countdown=delay, exc=exc, max_retries=self.max_retries)


@celery_app.task(bind=True)
def refresh_all_tokens(self):
    """
    Массовое обновление всех токенов с фиксацией истории выполнения в TaskHistory.
    """
    logger.info("[HISTORY] Starting refresh_all_tokens_with_history task")
    sync_session = get_sync_db_session_direct()
    task_history_service = TaskHistoryService(sync_session)
    task_id = self.request.id or "manual-call"
    user_id = None  # Глобальная задача, не привязана к конкретному пользователю
    params = {}
    description = "Массовое обновление всех токенов с историей"
    # Создаем запись в TaskHistory
    task_history = task_history_service.create_task(
        task_id=task_id,
        user_id=UUID("00000000-0000-0000-0000-000000000000"),
        task_type="refresh_all_tokens_with_history",
        params=params,
        description=description
    )
    tokens_refreshed = 0
    tokens_failed = 0
    errors = []
    auth_service = AllegroAuthService(sync_session)
    try:
        tokens = sync_session.query(UserToken).all()
        for token in tokens:
            try:
                auth_service.refresh_token_sync(token)
                tokens_refreshed += 1
            except Exception as e:
                logger.error(f"[HISTORY] Failed to refresh token {token.id}: {e}")
                tokens_failed += 1
                errors.append({"token_id": token.id, "error": str(e)})
        sync_session.commit()
        result = {
            "status": "completed",
            "tokens_refreshed": tokens_refreshed,
            "tokens_failed": tokens_failed,
            "errors": errors
        }
        task_history_service.update_task(
            task_id=task_id,
            status="COMPLETED",
            result=result,
            updated_at=datetime.utcnow()
        )
        logger.info(f"[HISTORY] refresh_all_tokens_with_history completed: {result}")
        return result
    except Exception as e:
        sync_session.rollback()
        error_result = {
            "status": "failed",
            "error": str(e),
            "tokens_refreshed": tokens_refreshed,
            "tokens_failed": tokens_failed,
            "errors": errors
        }
        task_history_service.update_task(
            task_id=task_id,
            status="FAILED",
            error=str(e),
            result=error_result,
            updated_at=datetime.utcnow()
        )
        logger.error(f"[HISTORY] refresh_all_tokens_with_history failed: {e}")
        return error_result
    finally:
        sync_session.close() 