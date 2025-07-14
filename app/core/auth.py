"""
@file: app/core/auth.py
@description: JWT аутентификация и модель текущего пользователя
@dependencies: jose, fastapi, pydantic
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel

from app.core.settings import settings
from app.core.logging import get_logger

# Security scheme для Bearer токенов
security = HTTPBearer()

logger = get_logger(__name__)


class CurrentUser(BaseModel):
    """Модель текущего пользователя из JWT токена"""
    user_id: str
    username: Optional[str] = None
    is_active: bool = True
    
    class Config:
        from_attributes = True


class TokenPayload(BaseModel):
    """Модель payload JWT токена"""
    user_id: str
    username: Optional[str] = None
    exp: Optional[datetime] = None


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Создает JWT access token
    
    Args:
        data: Данные для включения в токен
        expires_delta: Время жизни токена
        
    Returns:
        JWT токен в виде строки
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.api.jwt_access_token_expire_minutes
        )
    
    to_encode.update({"exp": expire})
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.api.jwt_secret_key,
        algorithm=settings.api.jwt_algorithm
    )
    
    return encoded_jwt


def verify_token(token: str) -> TokenPayload:
    """
    Проверяет и декодирует JWT токен
    
    Args:
        token: JWT токен
        
    Returns:
        Payload токена
        
    Raises:
        HTTPException: При невалидном токене
    """
    try:
        payload = jwt.decode(
            token,
            settings.api.jwt_secret_key,
            algorithms=[settings.api.jwt_algorithm]
        )
        
        user_id: str = payload.get("user_id")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        token_data = TokenPayload(**payload)
        return token_data
        
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> CurrentUser:
    """
    Dependency для получения текущего пользователя из JWT токена
    
    Args:
        credentials: HTTP Authorization credentials
        
    Returns:
        Текущий пользователь
        
    Raises:
        HTTPException: При невалидном токене
    """
    try:
        logger.info(f"Декодирование JWT токена для аутентификации")
        token_payload = verify_token(credentials.credentials)
        
        logger.info(f"JWT токен успешно декодирован: user_id={token_payload.user_id}, username={token_payload.username}")
        
        current_user = CurrentUser(
            user_id=token_payload.user_id,
            username=token_payload.username,
            is_active=True
        )
        
        logger.info(f"Создан объект CurrentUser: user_id={current_user.user_id}")
        return current_user
        
    except HTTPException:
        logger.warning("JWT токен невалиден")
        raise
    except Exception as e:
        logger.error(f"Ошибка при валидации JWT токена: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_active_user(
    current_user: CurrentUser = Depends(get_current_user)
) -> CurrentUser:
    """
    Dependency для получения активного пользователя
    
    Args:
        current_user: Текущий пользователь
        
    Returns:
        Активный пользователь
        
    Raises:
        HTTPException: Если пользователь неактивен
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user 