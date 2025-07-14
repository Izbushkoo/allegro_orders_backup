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

from app.api.dependencies import DatabaseSession, CurrentUserDep
from app.core.auth import CurrentUser
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

class AuthInitializeRequest(BaseModel):
    """Запрос на инициализацию авторизации"""
    # user_id теперь берется из JWT токена
    pass

class AuthInitializeResponse(BaseModel):
    """Ответ с данными для авторизации"""
    device_code: str = Field(..., description="Код устройства для проверки статуса")
    user_code: str = Field(..., description="Код для ввода пользователем")
    verification_uri: str = Field(..., description="URL для авторизации")
    verification_uri_complete: Optional[str] = Field(None, description="Полный URL с кодом")
    expires_in: int = Field(..., description="Время жизни кодов в секундах")
    interval: int = Field(..., description="Интервал проверки статуса в секундах")
    task_id: str = Field(..., description="ID задачи Celery для отслеживания прогресса")

class AuthStatusRequest(BaseModel):
    """Запрос на проверку статуса авторизации"""
    device_code: str = Field(..., description="Код устройства")

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

# Endpoints

@router.post("/", response_model=TokenResponse, summary="Создать токен")
async def create_token(
    token_data: TokenCreate,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Создать новый токен Allegro для текущего пользователя.
    
    **Требует аутентификации через JWT токен.**
    
    - **allegro_token**: Access token Allegro
    - **refresh_token**: Refresh token Allegro  
    - **expires_at**: Дата истечения токена
    """
    try:
        logger.info(f"Создание токена для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        token = await token_service.create_token(
            user_id=current_user.user_id,
            allegro_token=token_data.allegro_token,
            refresh_token=token_data.refresh_token,
            expires_at=token_data.expires_at
        )
        
        logger.info(f"Токен успешно создан для пользователя {current_user.user_id}")
        return TokenResponse(
            id=token.id,
            user_id=token.user_id,
            expires_at=token.expires_at,
            is_active=token.is_active,
            created_at=token.created_at,
            updated_at=token.updated_at
        )
        
    except ValidationError as e:
        logger.error(f"Ошибка валидации при создании токена: {e}")
        raise ValidationHTTPException(detail=f"Validation error: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании токена: {e}")
        raise InternalServerErrorHTTPException(detail="Internal server error")


@router.get("/", response_model=TokenList, summary="Получить токены пользователя")
async def get_tokens(
    page: int = 1,
    per_page: int = 10,
    active_only: bool = True,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Получить список токенов текущего пользователя с пагинацией.
    
    **Требует аутентификации через JWT токен.**
    
    - **page**: Номер страницы (по умолчанию 1)
    - **per_page**: Количество элементов на странице (по умолчанию 10, максимум 100)
    - **active_only**: Показывать только активные токены (по умолчанию true)
    """
    try:
        # Валидация параметров
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 10
        if per_page > 100:
            per_page = 100
            
        logger.info(f"Получение токенов пользователя {current_user.user_id}, страница {page}")
        
        token_service = TokenService(db_session)
        tokens, total = await token_service.get_tokens(
            user_id=current_user.user_id,
            page=page,
            per_page=per_page,
            active_only=active_only
        )
        
        token_responses = [
            TokenResponse(
                id=token.id,
                user_id=token.user_id,
                expires_at=token.expires_at,
                is_active=token.is_active,
                created_at=token.created_at,
                updated_at=token.updated_at
            ) for token in tokens
        ]
        
        return TokenList(
            tokens=token_responses,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении токенов: {e}")
        raise InternalServerErrorHTTPException(detail="Internal server error")


@router.get("/{token_id}", response_model=TokenResponse, summary="Получить токен")
async def get_token(
    token_id: UUID,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Получить конкретный токен по ID.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может получить только свои токены.**
    
    - **token_id**: Уникальный идентификатор токена
    """
    try:
        logger.info(f"Получение токена {token_id} для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
        
        if not token:
            logger.warning(f"Токен {token_id} не найден для пользователя {current_user.user_id}")
            raise TokenNotFoundHTTPException()
            
        return TokenResponse(
            id=token.id,
            user_id=token.user_id,
            expires_at=token.expires_at,
            is_active=token.is_active,
            created_at=token.created_at,
            updated_at=token.updated_at
        )
        
    except NotFoundError:
        logger.warning(f"Токен {token_id} не найден")
        raise TokenNotFoundHTTPException()
    except Exception as e:
        logger.error(f"Ошибка при получении токена: {e}")
        raise InternalServerErrorHTTPException(detail="Internal server error")


@router.put("/{token_id}", response_model=TokenResponse, summary="Обновить токен")
async def update_token(
    token_id: UUID,
    token_update: TokenUpdate,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Обновить существующий токен.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может обновить только свои токены.**
    
    - **token_id**: Уникальный идентификатор токена
    - **allegro_token**: Новый access token (опционально)
    - **refresh_token**: Новый refresh token (опционально)
    - **expires_at**: Новая дата истечения (опционально)
    - **is_active**: Новый статус активности (опционально)
    """
    try:
        logger.info(f"Обновление токена {token_id} для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        
        # Проверяем, что токен принадлежит пользователю
        existing_token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
        if not existing_token:
            logger.warning(f"Токен {token_id} не найден для пользователя {current_user.user_id}")
            raise TokenNotFoundHTTPException()
        
        # Обновляем токен
        updated_token = await token_service.update_user_token(
            token_id=token_id,
            allegro_token=token_update.allegro_token,
            refresh_token=token_update.refresh_token,
            expires_at=token_update.expires_at,
            is_active=token_update.is_active
        )
        
        if not updated_token:
            logger.error(f"Не удалось обновить токен {token_id}")
            raise InternalServerErrorHTTPException(detail="Failed to update token")
            
        logger.info(f"Токен {token_id} успешно обновлен")
        return TokenResponse(
            id=updated_token.id,
            user_id=updated_token.user_id,
            expires_at=updated_token.expires_at,
            is_active=updated_token.is_active,
            created_at=updated_token.created_at,
            updated_at=updated_token.updated_at
        )
        
    except NotFoundError:
        logger.warning(f"Токен {token_id} не найден")
        raise TokenNotFoundHTTPException()
    except ValidationError as e:
        logger.error(f"Ошибка валидации при обновлении токена: {e}")
        raise ValidationHTTPException(detail=f"Validation error: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении токена: {e}")
        raise InternalServerErrorHTTPException(detail="Internal server error")


@router.delete("/{token_id}", summary="Удалить токен")
async def delete_token(
    token_id: UUID,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Удалить (деактивировать) токен.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может удалить только свои токены.**
    
    - **token_id**: Уникальный идентификатор токена
    """
    try:
        logger.info(f"Удаление токена {token_id} для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        
        # Проверяем, что токен принадлежит пользователю
        existing_token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
        if not existing_token:
            logger.warning(f"Токен {token_id} не найден для пользователя {current_user.user_id}")
            raise TokenNotFoundHTTPException()
        
        success = await token_service.delete_user_token(token_id)
        
        if not success:
            logger.error(f"Не удалось удалить токен {token_id}")
            raise InternalServerErrorHTTPException(detail="Failed to delete token")
            
        logger.info(f"Токен {token_id} успешно удален")
        return {"message": "Token successfully deleted"}
        
    except NotFoundError:
        logger.warning(f"Токен {token_id} не найден")
        raise TokenNotFoundHTTPException()
    except Exception as e:
        logger.error(f"Ошибка при удалении токена: {e}")
        raise InternalServerErrorHTTPException(detail="Internal server error")

@router.post("/{token_id}/refresh", response_model=TokenResponse, summary="Обновить токен через refresh token")
async def refresh_token(
    token_id: UUID,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Обновить access token используя refresh token.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может обновить только свои токены.**
    
    - **token_id**: Уникальный идентификатор токена
    """
    try:
        logger.info(f"Обновление токена {token_id} через refresh token для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        
        # Проверяем, что токен принадлежит пользователю
        existing_token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
        if not existing_token:
            logger.warning(f"Токен {token_id} не найден для пользователя {current_user.user_id}")
            raise TokenNotFoundHTTPException()
        
        # Обновляем токен через сервис авторизации
        auth_service = AllegroAuthService(db_session)
        updated_token = await auth_service.refresh_token(existing_token)
        
        if not updated_token:
            logger.error(f"Не удалось обновить токен {token_id}")
            raise InternalServerErrorHTTPException(detail="Failed to refresh token")
            
        logger.info(f"Токен {token_id} успешно обновлен через refresh token")
        return TokenResponse(
            id=updated_token.id,
            user_id=updated_token.user_id,
            expires_at=updated_token.expires_at,
            is_active=updated_token.is_active,
            created_at=updated_token.created_at,
            updated_at=updated_token.updated_at
        )
        
    except NotFoundError:
        logger.warning(f"Токен {token_id} не найден")
        raise TokenNotFoundHTTPException()
    except Exception as e:
        logger.error(f"Ошибка при обновлении токена через refresh: {e}")
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
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Инициализировать процесс авторизации Device Code Flow для получения токенов Allegro.
    
    **Требует аутентификации через JWT токен.**
    
    Возвращает коды и URL для авторизации пользователя в Allegro.
    Запускает фоновую задачу для polling статуса авторизации.
    """
    try:
        logger.info(f"Инициализация авторизации для пользователя {current_user.user_id}")
        
        auth_service = AllegroAuthService(db_session)
        
        # Инициализируем Device Code Flow
        device_flow_data = await auth_service.initialize_device_flow(current_user.user_id)
        
        # Запускаем задачу polling
        from datetime import timedelta
        expires_at = datetime.utcnow() + timedelta(seconds=device_flow_data["expires_in"])
        
        task = poll_authorization_status.delay(
            device_code=device_flow_data["device_code"],
            user_id=current_user.user_id,
            expires_at_iso=expires_at.isoformat(),
            interval_seconds=device_flow_data["interval"]
        )
        
        logger.info(f"Авторизация инициализирована для пользователя {current_user.user_id}, task_id: {task.id}")
        
        return AuthInitializeResponse(
            device_code=device_flow_data["device_code"],
            user_code=device_flow_data["user_code"],
            verification_uri=device_flow_data["verification_uri"],
            verification_uri_complete=device_flow_data.get("verification_uri_complete"),
            expires_in=device_flow_data["expires_in"],
            interval=device_flow_data["interval"],
            task_id=task.id
        )
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации авторизации: {e}")
        raise InternalServerErrorHTTPException(detail="Failed to initialize authorization")


@router.post("/auth/status", response_model=AuthStatusResponse, summary="Проверка статуса авторизации")
async def check_auth_status(
    request: AuthStatusRequest,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Проверить статус авторизации Device Code Flow.
    
    **Требует аутентификации через JWT токен.**
    
    - **device_code**: Код устройства полученный при инициализации
    """
    try:
        logger.info(f"Проверка статуса авторизации для пользователя {current_user.user_id}")
        
        auth_service = AllegroAuthService(db_session)
        status_data = await auth_service.check_auth_status(
            device_code=request.device_code,
            user_id=current_user.user_id
        )
        
        return AuthStatusResponse(
            status=status_data["status"],
            message=status_data.get("message")
        )
        
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса авторизации: {e}")
        raise InternalServerErrorHTTPException(detail="Failed to check authorization status")


@router.post("/{token_id}/validate", response_model=TokenResponse, summary="Проверить и обновить токен")
async def validate_and_refresh_token(
    token_id: UUID,
    current_user: CurrentUser = CurrentUserDep,
    db_session: AsyncSession = DatabaseSession
):
    """
    Проверить валидность токена и обновить его при необходимости.
    
    **Требует аутентификации через JWT токен.**
    **Пользователь может проверить только свои токены.**
    
    - **token_id**: Уникальный идентификатор токена
    """
    try:
        logger.info(f"Валидация токена {token_id} для пользователя {current_user.user_id}")
        
        token_service = TokenService(db_session)
        
        # Проверяем, что токен принадлежит пользователю
        existing_token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
        if not existing_token:
            logger.warning(f"Токен {token_id} не найден для пользователя {current_user.user_id}")
            raise TokenNotFoundHTTPException()
        
        # Проверяем и обновляем токен
        validated_token = await token_service.validate_and_refresh_token(token_id)
        
        if not validated_token:
            logger.error(f"Не удалось валидировать токен {token_id}")
            raise InternalServerErrorHTTPException(detail="Failed to validate token")
            
        logger.info(f"Токен {token_id} успешно валидирован")
        return TokenResponse(
            id=validated_token.id,
            user_id=validated_token.user_id,
            expires_at=validated_token.expires_at,
            is_active=validated_token.is_active,
            created_at=validated_token.created_at,
            updated_at=validated_token.updated_at
        )
        
    except NotFoundError:
        logger.warning(f"Токен {token_id} не найден")
        raise TokenNotFoundHTTPException()
    except Exception as e:
        logger.error(f"Ошибка при валидации токена: {e}")
        raise InternalServerErrorHTTPException(detail="Failed to validate token")


@router.get("/auth/task/{task_id}", response_model=TaskStatusResponse, summary="Проверка статуса задачи авторизации")
async def get_auth_task_status(
    task_id: str,
    current_user: CurrentUser = CurrentUserDep
):
    """
    Получить статус задачи авторизации Celery.
    
    **Требует аутентификации через JWT токен.**
    
    - **task_id**: Идентификатор задачи полученный при инициализации авторизации
    """
    try:
        logger.info(f"Проверка статуса задачи {task_id} для пользователя {current_user.user_id}")
        
        from celery.result import AsyncResult
        from app.celery_app import celery_app
        
        task_result = AsyncResult(task_id, app=celery_app)
        
        return TaskStatusResponse(
            task_id=task_id,
            status=task_result.status,
            result=task_result.result if task_result.successful() else None,
            progress=task_result.info if task_result.status == "PROGRESS" else None
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении статуса задачи: {e}")
        raise InternalServerErrorHTTPException(detail="Failed to get task status")