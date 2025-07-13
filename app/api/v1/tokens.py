"""
@file: app/api/v1/tokens.py
@description: API эндпоинты для работы с токенами Allegro
@dependencies: fastapi, pydantic
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.dependencies import DatabaseSession
from app.services.token_service import TokenService
from app.services.allegro_auth_service import AllegroAuthService
from app.exceptions import NotFoundError, ValidationError, TokenNotFoundHTTPException, ValidationHTTPException, InternalServerErrorHTTPException
from app.core.logging import get_logger
from app.tasks.token_tasks import poll_authorization_status

logger = get_logger(__name__)

router = APIRouter()

# Pydantic модели для API

class TokenCreate(BaseModel):
    """Модель для создания токена"""
    user_id: str = Field(..., description="Уникальный ID пользователя")
    allegro_token: str = Field(..., description="Токен доступа Allegro")
    refresh_token: str = Field(..., description="Токен для обновления")
    expires_at: datetime = Field(..., description="Дата истечения токена")

class TokenResponse(BaseModel):
    """Модель ответа с токеном"""
    id: UUID
    user_id: str
    expires_at: datetime
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime]

class TokenUpdate(BaseModel):
    """Модель для обновления токена"""
    allegro_token: Optional[str] = Field(None, description="Новый токен доступа")
    refresh_token: Optional[str] = Field(None, description="Новый токен для обновления")
    expires_at: Optional[datetime] = Field(None, description="Новая дата истечения")
    is_active: Optional[bool] = Field(None, description="Статус активности")

class TokenList(BaseModel):
    """Модель списка токенов"""
    tokens: List[TokenResponse]
    total: int
    page: int
    per_page: int

# Модели для Device Code Flow авторизации

class AuthInitializeRequest(BaseModel):
    """Запрос на инициализацию авторизации"""
    user_id: str = Field(..., description="ID пользователя для которого создается токен")

class AuthInitializeResponse(BaseModel):
    """Ответ с данными для авторизации"""
    device_code: str = Field(..., description="Код устройства для проверки статуса")
    user_code: str = Field(..., description="Код для ввода пользователем")
    verification_uri: str = Field(..., description="URL для авторизации")
    verification_uri_complete: Optional[str] = Field(None, description="Полный URL с кодом")
    expires_in: int = Field(..., description="Время жизни кодов в секундах")
    interval: int = Field(..., description="Интервал проверки статуса в секундах")
    task_id: str = Field(..., description="ID задачи Celery для отслеживания прогресса")

class AuthStatusResponse(BaseModel):
    """Ответ со статусом авторизации"""
    status: str = Field(..., description="Статус авторизации: pending, completed, failed")
    message: Optional[str] = Field(None, description="Дополнительное сообщение")

class TaskStatusResponse(BaseModel):
    """Ответ со статусом задачи Celery"""
    task_id: str = Field(..., description="ID задачи")
    status: str = Field(..., description="Статус задачи: PENDING, SUCCESS, FAILURE")
    result: Optional[dict] = Field(None, description="Результат выполнения задачи")
    progress: Optional[dict] = Field(None, description="Информация о прогрессе")

# API эндпоинты

@router.post("/", response_model=TokenResponse, summary="Создать токен")
async def create_token(
    token_data: TokenCreate,
    db_session: AsyncSession = DatabaseSession
):
    """
    Создать новый токен для пользователя.
    
    - **user_id**: Уникальный ID пользователя
    - **allegro_token**: Токен доступа от Allegro
    - **refresh_token**: Токен для обновления
    - **expires_at**: Дата истечения токена
    """
    try:
        token_service = TokenService(db_session)
        
        token = await token_service.create_token(
            user_id=token_data.user_id,
            allegro_token=token_data.allegro_token,
            refresh_token=token_data.refresh_token,
            expires_at=token_data.expires_at
        )
        
        return TokenResponse(
            id=token.id,
            user_id=token.user_id,
            expires_at=token.expires_at,
            is_active=token.is_active,
            created_at=token.created_at,
            updated_at=token.updated_at
        )
        
    except ValidationError as e:
        logger.error(f"Validation error creating token: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating token: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to create token")

@router.get("/", response_model=TokenList, summary="Получить список токенов")
async def get_tokens(
    page: int = 1,
    per_page: int = 10,
    user_id: Optional[str] = None,
    active_only: bool = True,
    db_session: AsyncSession = DatabaseSession
):
    """
    Получить список токенов с фильтрацией.
    
    - **page**: Номер страницы (по умолчанию 1)
    - **per_page**: Количество элементов на странице (по умолчанию 10)
    - **user_id**: Фильтр по ID пользователя
    - **active_only**: Только активные токены
    """
    try:
        token_service = TokenService(db_session)
        
        tokens, total = await token_service.get_tokens(
            page=page,
            per_page=per_page,
            user_id=user_id,
            active_only=active_only
        )
        
        token_responses = []
        for token in tokens:
            token_responses.append(TokenResponse(
                id=token.id,
                user_id=token.user_id,
                expires_at=token.expires_at,
                is_active=token.is_active,
                created_at=token.created_at,
                updated_at=token.updated_at
            ))
        
        return TokenList(
            tokens=token_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Error getting tokens: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to get tokens")

@router.get("/{token_id}", response_model=TokenResponse, summary="Получить токен")
async def get_token(
    token_id: UUID,
    db_session: AsyncSession = DatabaseSession
):
    """
    Получить токен по ID.
    
    - **token_id**: UUID токена
    """
    try:
        token_service = TokenService(db_session)
        
        token = await token_service.get_token(token_id)
        
        if not token:
            raise TokenNotFoundHTTPException()
        
        return TokenResponse(
            id=token.id,
            user_id=token.user_id,
            expires_at=token.expires_at,
            is_active=token.is_active,
            created_at=token.created_at,
            updated_at=token.updated_at
        )
        
    except TokenNotFoundHTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting token {token_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to get token")

@router.put("/{token_id}", response_model=TokenResponse, summary="Обновить токен")
async def update_token(
    token_id: UUID,
    token_update: TokenUpdate,
    db_session: AsyncSession = DatabaseSession
):
    """
    Обновить токен пользователя.
    
    - **token_id**: UUID токена
    - **allegro_token**: Новый токен доступа (необязательно)
    - **refresh_token**: Новый токен для обновления (необязательно)
    - **expires_at**: Новая дата истечения (необязательно)
    - **is_active**: Статус активности (необязательно)
    """
    try:
        token_service = TokenService(db_session)
        
        # Подготавливаем данные для обновления
        update_data = {}
        if token_update.allegro_token is not None:
            update_data["allegro_token"] = token_update.allegro_token
        if token_update.refresh_token is not None:
            update_data["refresh_token"] = token_update.refresh_token
        if token_update.expires_at is not None:
            update_data["expires_at"] = token_update.expires_at
        if token_update.is_active is not None:
            update_data["is_active"] = token_update.is_active
        
        token = await token_service.update_token(token_id, update_data)
        
        if not token:
            raise TokenNotFoundHTTPException()
        
        return TokenResponse(
            id=token.id,
            user_id=token.user_id,
            expires_at=token.expires_at,
            is_active=token.is_active,
            created_at=token.created_at,
            updated_at=token.updated_at
        )
        
    except TokenNotFoundHTTPException:
        raise
    except ValidationError as e:
        logger.error(f"Validation error updating token: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Error updating token {token_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to update token")

@router.delete("/{token_id}", summary="Удалить токен")
async def delete_token(
    token_id: UUID,
    db_session: AsyncSession = DatabaseSession
):
    """
    Удалить токен (деактивировать).
    
    - **token_id**: UUID токена
    """
    try:
        token_service = TokenService(db_session)
        
        await token_service.deactivate_token(token_id)
        
        return {"message": "Token deleted successfully"}
        
    except NotFoundError as e:
        logger.error(f"Token not found for deletion: {str(e)}")
        raise TokenNotFoundHTTPException()
    except Exception as e:
        logger.error(f"Error deleting token {token_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to delete token")

@router.post("/{token_id}/refresh", response_model=TokenResponse, summary="Обновить токен через refresh token")
async def refresh_token(
    token_id: UUID,
    db_session: AsyncSession = DatabaseSession
):
    """
    Обновить токен используя refresh token через Allegro API.
    
    - **token_id**: UUID токена для обновления
    """
    try:
        token_service = TokenService(db_session)
        
        token = await token_service.get_token(token_id)
        
        if not token:
            raise TokenNotFoundHTTPException()
        
        auth_service = AllegroAuthService(db_session)
        refreshed_token = await auth_service.refresh_token(token)
        
        if not refreshed_token:
            logger.error(f"Failed to refresh token: {token_id}")
            raise ValidationHTTPException(detail="Failed to refresh token")
        
        return TokenResponse(
            id=refreshed_token.id,
            user_id=refreshed_token.user_id,
            expires_at=refreshed_token.expires_at,
            is_active=refreshed_token.is_active,
            created_at=refreshed_token.created_at,
            updated_at=refreshed_token.updated_at
        )
        
    except TokenNotFoundHTTPException:
        raise
    except ValidationError as e:
        logger.error(f"Validation error refreshing token: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Error refreshing token {token_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to refresh token")

@router.get("/user/{user_id}", response_model=List[TokenResponse], summary="Получить токены пользователя")
async def get_user_tokens(
    user_id: str,
    active_only: bool = True,
    db_session: AsyncSession = DatabaseSession
):
    """
    Получить все токены пользователя.
    
    - **user_id**: ID пользователя
    - **active_only**: Только активные токены
    """
    try:
        token_service = TokenService(db_session)
        
        tokens = await token_service.get_user_tokens(
            user_id=user_id,
            active_only=active_only
        )
        
        result = []
        for token in tokens:
            result.append(TokenResponse(
                id=token.id,
                user_id=token.user_id,
                expires_at=token.expires_at,
                is_active=token.is_active,
                created_at=token.created_at,
                updated_at=token.updated_at
            ))
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting user tokens for {user_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to get user tokens")

# Device Code Flow эндпоинты

@router.post("/auth/initialize", response_model=AuthInitializeResponse, summary="Инициализация авторизации")
async def initialize_auth(
    request: AuthInitializeRequest,
    db_session: AsyncSession = DatabaseSession
):
    """
    Инициализирует процесс авторизации через Device Code Flow для Allegro API.
    
    Возвращает коды для авторизации пользователя:
    - **device_code**: Используется для проверки статуса авторизации
    - **user_code**: Код который пользователь должен ввести на странице авторизации
    - **verification_uri**: URL страницы авторизации Allegro
    - **expires_in**: Время жизни кодов в секундах
    - **interval**: Рекомендуемый интервал проверки статуса в секундах
    """
    logger.debug(f"[DEBUG] initialize_auth called with user_id: {request.user_id}")
    
    try:
        logger.debug(f"[DEBUG] Creating AllegroAuthService instance")
        auth_service = AllegroAuthService(db_session)
        
        logger.debug(f"[DEBUG] Calling auth_service.initialize_device_flow")
        auth_data = await auth_service.initialize_device_flow(request.user_id)
        
        logger.debug(f"[DEBUG] Auth data received: device_code={auth_data['device_code'][:10]}..., expires_in={auth_data['expires_in']}")
        
        # Запускаем Celery задачу для автоматического polling авторизации
        from datetime import datetime, timedelta
        expires_at = datetime.utcnow() + timedelta(seconds=auth_data["expires_in"])
        
        # Используем уникальный task_id на основе device_code для предотвращения дублирования
        import hashlib
        unique_task_id = f"auth_{hashlib.md5(auth_data['device_code'].encode()).hexdigest()}"
        
        logger.debug(f"[DEBUG] Starting Celery task with ID: {unique_task_id}")
        
        task = poll_authorization_status.apply_async(
            args=[
                auth_data["device_code"],
                request.user_id,
                expires_at.isoformat(),
                auth_data["interval"]
            ],
            task_id=unique_task_id
        )
        
        logger.debug(f"[DEBUG] Task started successfully with ID: {task.id}")
        
        return AuthInitializeResponse(
            device_code=auth_data["device_code"],
            user_code=auth_data["user_code"],
            verification_uri=auth_data["verification_uri"],
            verification_uri_complete=auth_data.get("verification_uri_complete"),
            expires_in=auth_data["expires_in"],
            interval=auth_data["interval"],
            task_id=task.id
        )
        
    except ValidationError as e:
        logger.error(f"Validation error during auth initialization: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during auth initialization: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to initialize authorization")

@router.get("/auth/status/{device_code}", response_model=AuthStatusResponse, summary="Проверка статуса авторизации")
async def check_auth_status(
    device_code: str,
    user_id: str,
    db_session: AsyncSession = DatabaseSession
):
    """
    Проверяет статус авторизации для указанного device_code.
    
    Возвращает один из статусов:
    - **pending**: Авторизация ещё не завершена пользователем
    - **completed**: Авторизация успешно завершена, токен сохранён
    - **failed**: Авторизация отклонена или произошла ошибка
    
    - **device_code**: Код устройства полученный от /auth/initialize
    - **user_id**: ID пользователя для которого проверяется авторизация
    """
    logger.debug(f"[DEBUG] check_auth_status called with device_code: {device_code[:10]}..., user_id: {user_id}")
    
    try:
        logger.debug(f"[DEBUG] Creating AllegroAuthService instance")
        auth_service = AllegroAuthService(db_session)
        
        logger.debug(f"[DEBUG] Calling auth_service.check_auth_status")
        status_data = await auth_service.check_auth_status(device_code, user_id)
        
        logger.debug(f"[DEBUG] Auth status received: {status_data}")
        
        return AuthStatusResponse(
            status=status_data["status"],
            message=status_data.get("message")
        )
        
    except ValidationError as e:
        logger.error(f"Validation error during auth status check: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during auth status check: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to check authorization status")

@router.post("/{token_id}/validate", response_model=TokenResponse, summary="Проверить и обновить токен")
async def validate_and_refresh_token(
    token_id: UUID,
    db_session: AsyncSession = DatabaseSession
):
    """
    Проверяет токен и при необходимости обновляет его.
    
    Выполняет следующие действия:
    1. Проверяет срок действия токена
    2. Валидирует токен через Allegro API
    3. Автоматически обновляет токен если он истёк или недействителен
    
    - **token_id**: UUID токена для проверки
    """
    logger.debug(f"[DEBUG] validate_and_refresh_token called with token_id: {token_id}")
    
    try:
        logger.debug(f"[DEBUG] Creating TokenService instance")
        token_service = TokenService(db_session)
        
        logger.debug(f"[DEBUG] Getting token from database")
        token = await token_service.get_token(token_id)
        
        if not token:
            raise TokenNotFoundHTTPException()
        
        logger.debug(f"[DEBUG] Token found, creating AllegroAuthService")
        auth_service = AllegroAuthService(db_session)
        logger.debug(f"[DEBUG] Calling auth_service.check_and_refresh_token")
        
        updated_token = await auth_service.check_and_refresh_token(token)
        
        if not updated_token:
            logger.error(f"Failed to validate or refresh token: {token_id}")
            raise ValidationHTTPException(detail="Failed to validate or refresh token")
        
        return TokenResponse(
            id=updated_token.id,
            user_id=updated_token.user_id,
            expires_at=updated_token.expires_at,
            is_active=updated_token.is_active,
            created_at=updated_token.created_at,
            updated_at=updated_token.updated_at
        )
        
    except TokenNotFoundHTTPException:
        raise
    except ValidationError as e:
        logger.error(f"Validation error during token validation: {str(e)}")
        raise ValidationHTTPException(detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during token validation: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to validate token")

@router.get("/auth/task/{task_id}", response_model=TaskStatusResponse, summary="Проверка статуса задачи авторизации")
async def get_auth_task_status(task_id: str):
    """
    Проверяет статус выполнения задачи авторизации.
    
    Возвращает информацию о прогрессе автоматического polling'а авторизации:
    - **PENDING**: Задача ещё выполняется
    - **SUCCESS**: Авторизация завершена (успешно или с ошибкой)
    - **FAILURE**: Критическая ошибка в задаче
    
    - **task_id**: ID задачи полученный от /auth/initialize
    """
    logger.debug(f"[DEBUG] get_auth_task_status called with task_id: {task_id}")
    
    try:
        from app.celery_app import celery_app
        
        logger.debug(f"[DEBUG] Getting task result from Celery")
        task_result = celery_app.AsyncResult(task_id)
        
        logger.debug(f"[DEBUG] Task state: {task_result.state}")
        
        if task_result.state == 'PENDING':
            response = {
                "task_id": task_id,
                "status": "PENDING",
                "progress": {
                    "message": "Authorization polling in progress...",
                    "current_step": "Waiting for user authorization"
                }
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result
            logger.debug(f"[DEBUG] Task result: {result}")
            response = {
                "task_id": task_id,
                "status": "SUCCESS",
                "result": result,
                "progress": {
                    "message": f"Authorization {result.get('status', 'completed')}",
                    "current_step": "Finished"
                }
            }
        elif task_result.state == 'FAILURE':
            logger.debug(f"[DEBUG] Task failed with error: {task_result.info}")
            response = {
                "task_id": task_id,
                "status": "FAILURE",
                "result": {
                    "status": "failed",
                    "message": str(task_result.info)
                },
                "progress": {
                    "message": "Task failed with error",
                    "current_step": "Error"
                }
            }
        else:
            # Другие состояния (RETRY, REVOKED, etc.)
            logger.debug(f"[DEBUG] Task in other state: {task_result.state}")
            response = {
                "task_id": task_id,
                "status": task_result.state,
                "progress": {
                    "message": f"Task in state: {task_result.state}",
                    "current_step": "Processing"
                }
            }
        
        return TaskStatusResponse(**response)
        
    except Exception as e:
        logger.error(f"Error getting task status for {task_id}: {str(e)}")
        raise InternalServerErrorHTTPException(detail="Failed to get task status")