"""
@file: app/tasks/token_tasks.py
@description: Celery задачи для управления токенами Allegro
@dependencies: celery
"""

import asyncio
from datetime import datetime, timedelta

from celery.exceptions import Retry
from app.celery_app import celery_app
from app.core.logging import get_logger
from app.core.database import get_async_session
from app.services.allegro_auth_service import AllegroAuthService

logger = get_logger(__name__)


@celery_app.task
def refresh_all_tokens():
    """Обновление всех истекающих токенов"""
    logger.debug(f"[DEBUG] refresh_all_tokens task started")
    logger.info("Starting token refresh task")
    # TODO: Реализовать обновление токенов
    logger.info("Token refresh task completed")
    result = {"status": "completed", "tokens_refreshed": 0}
    logger.debug(f"[DEBUG] refresh_all_tokens task completed: {result}")
    return result


@celery_app.task
def refresh_user_token(user_id: str):
    """Обновление токена конкретного пользователя"""
    logger.debug(f"[DEBUG] refresh_user_token task started with user_id: {user_id}")
    logger.info(f"Refreshing token for user: {user_id}")
    # TODO: Реализовать обновление токена пользователя
    logger.info(f"Token refresh completed for user: {user_id}")
    result = {"status": "completed", "user_id": user_id}
    logger.debug(f"[DEBUG] refresh_user_token task completed: {result}")
    return result


@celery_app.task(bind=True, max_retries=30, rate_limit='10/m')
def poll_authorization_status(self, device_code: str, user_id: str, expires_at_iso: str, interval_seconds: int = 5):
    """
    Задача для отслеживания статуса авторизации Device Code Flow.
    
    Args:
        device_code: Код устройства для проверки
        user_id: ID пользователя
        expires_at_iso: Время истечения в ISO формате
        interval_seconds: Интервал проверки в секундах
    """
    logger.debug(f"[DEBUG] poll_authorization_status task started")
    logger.debug(f"[DEBUG] Parameters: device_code={device_code[:10]}..., user_id={user_id}, expires_at_iso={expires_at_iso}, interval_seconds={interval_seconds}")
    logger.debug(f"[DEBUG] Current retry attempt: {self.request.retries + 1}/{self.max_retries + 1}")
    
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
        
        logger.info(f"Polling authorization status for user {user_id}, attempt {self.request.retries + 1}/30")
        logger.debug(f"[DEBUG] Calling _check_authorization_status_async")
        
        # Выполняем асинхронную проверку в синхронном контексте
        result = asyncio.run(_check_authorization_status_async(device_code, user_id))
        
        logger.debug(f"[DEBUG] Authorization check result: {result}")
        
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
        # Это исключение retry, просто пропускаем его - Celery обработает автоматически
        logger.debug(f"[DEBUG] Retry exception raised, will be handled by Celery")
        raise
    except Exception as exc:
        logger.error(f"Error polling authorization status for user {user_id}: {str(exc)}")
        logger.debug(f"[DEBUG] Exception in poll_authorization_status: {type(exc)} - {str(exc)}", exc_info=True)
        
        # Если достигли максимального количества попыток
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
        
        # Повторяем с экспоненциальной задержкой
        delay = min(300, (2 ** self.request.retries) * interval_seconds)  # Максимум 5 минут
        logger.info(f"Retrying authorization polling for user {user_id} in {delay}s")
        logger.debug(f"[DEBUG] Exponential backoff delay calculated: {delay}s")
        logger.debug(f"[DEBUG] Raising retry exception with countdown={delay}")
        raise self.retry(countdown=delay, exc=exc, max_retries=self.max_retries)


async def _check_authorization_status_async(device_code: str, user_id: str) -> dict:
    """
    Вспомогательная асинхронная функция для проверки статуса авторизации.
    
    Args:
        device_code: Код устройства
        user_id: ID пользователя
        
    Returns:
        dict: Результат проверки статуса
    """
    logger.debug(f"[DEBUG] _check_authorization_status_async called with device_code={device_code[:10]}..., user_id={user_id}")
    
    async for session in get_async_session():
        try:
            logger.debug(f"[DEBUG] Creating AllegroAuthService instance")
            auth_service = AllegroAuthService(session)
            
            logger.debug(f"[DEBUG] Calling auth_service.check_auth_status")
            result = await auth_service.check_auth_status(device_code, user_id)
            
            logger.debug(f"[DEBUG] Auth service result: {result}")
            logger.debug(f"[DEBUG] _check_authorization_status_async completed successfully")
            return result
        except Exception as e:
            logger.error(f"Error in async authorization check: {str(e)}")
            logger.debug(f"[DEBUG] Exception in _check_authorization_status_async: {type(e)} - {str(e)}", exc_info=True)
            result = {"status": "failed", "message": str(e)}
            logger.debug(f"[DEBUG] _check_authorization_status_async completed with error: {result}")
            return result
        finally:
            logger.debug(f"[DEBUG] Closing database session")
            await session.close() 