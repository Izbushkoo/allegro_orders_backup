"""
@file: app/services/allegro_auth_service.py
@description: Сервис для работы с авторизацией Allegro API через Device Code Flow
@dependencies: aiohttp, requests, base64
"""

import base64
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.settings import settings
from app.core.logging import get_logger
from app.models.user_token import UserToken, UserTokenCreate
from app.services.token_service import TokenService
from app.exceptions import ValidationError, InternalServerErrorHTTPException

logger = get_logger(__name__)


class AllegroAuthService:
    """Сервис для работы с авторизацией Allegro API"""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.token_service = TokenService(db_session)
        self.allegro_settings = settings.allegro
        self.auth_url = f"{self.allegro_settings.auth_url}/device"
        self.token_url = f"{self.allegro_settings.auth_url}/token"
        self.api_url = self.allegro_settings.api_url
    
    async def initialize_device_flow(self, user_id: str) -> Dict[str, Any]:
        """
        Инициализирует процесс Device Code Flow авторизации для Allegro.
        
        Args:
            user_id: ID пользователя для которого создается токен
        
        Returns:
            Dict с данными авторизации (device_code, user_code, verification_uri, etc.)
        
        Raises:
            ValidationError: Если не удалось инициализировать авторизацию
        """
        logger.debug(f"[DEBUG] initialize_device_flow called with user_id: {user_id}")
        
        try:
            logger.info(f"Initializing Device Code Flow for user: {user_id}")
            
            logger.debug(f"[DEBUG] Creating authorization header")
            auth_str = f'{self.allegro_settings.client_id}:{self.allegro_settings.client_secret}'
            b64_auth_str = base64.b64encode(auth_str.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {b64_auth_str}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'client_id': self.allegro_settings.client_id
            }
            
            logger.debug(f"[DEBUG] Sending POST request to {self.auth_url}")
            logger.debug(f"[DEBUG] Request data: {data}")
            
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    self.auth_url,
                    headers=headers,
                    data=data
                )
                
                logger.debug(f"[DEBUG] Response status: {response.status_code}")
                
                if response.status_code == 200:
                    auth_data = response.json()
                    logger.debug(f"[DEBUG] Response data: {auth_data}")
                    
                    # Добавляем дополнительные поля для удобства
                    auth_data["user_id"] = user_id
                    
                    # Добавляем время истечения в ISO формате для Celery
                    expires_at = datetime.utcnow() + timedelta(seconds=auth_data.get("expires_in", 600))
                    auth_data["expires_at_iso"] = expires_at.isoformat()
                    
                    logger.info(f"Device Code Flow initialized successfully for user: {user_id}")
                    logger.debug(f"[DEBUG] initialize_device_flow completed successfully")
                    return auth_data
                else:
                    error_text = response.text
                    logger.error(f"Failed to initialize Device Code Flow: {response.status_code} - {error_text}")
                    logger.debug(f"[DEBUG] Error response text: {error_text}")
                    raise ValidationError(f"Failed to initialize authorization: {response.status_code}")
                        
        except httpx.RequestError as e:
            logger.error(f"Network error during Device Code Flow initialization: {str(e)}")
            logger.debug(f"[DEBUG] RequestError: {type(e)} - {str(e)}")
            raise ValidationError(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Device Code Flow initialization: {str(e)}")
            logger.debug(f"[DEBUG] Exception type: {type(e)}, details: {str(e)}", exc_info=True)
            raise InternalServerErrorHTTPException("Failed to initialize authorization")
    
    async def check_auth_status(self, device_code: str, user_id: str) -> Dict[str, str]:
        """
        Проверяет статус авторизации для данного device_code.
        
        Args:
            device_code: Код устройства, полученный от initialize_device_flow
            user_id: ID пользователя для которого проверяется авторизация
        
        Returns:
            Dict со статусом: {'status': 'pending'|'completed'|'failed'}
        
        Raises:
            ValidationError: Если произошла ошибка при проверке
        """
        logger.debug(f"[DEBUG] check_auth_status called with device_code: {device_code[:10]}..., user_id: {user_id}")
        
        try:
            logger.info(f"Checking auth status for user: {user_id}")
            
            logger.debug(f"[DEBUG] Creating authorization header")
            auth_str = f'{self.allegro_settings.client_id}:{self.allegro_settings.client_secret}'
            b64_auth_str = base64.b64encode(auth_str.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {b64_auth_str}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                'device_code': device_code
            }
            
            logger.debug(f"[DEBUG] Sending POST request to {self.token_url}")
            logger.debug(f"[DEBUG] Request data: {data}")
            
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    self.token_url,
                    headers=headers,
                    data=data
                )
                
                logger.debug(f"[DEBUG] Response status: {response.status_code}")
                
                if response.status_code == 400:
                    error_data = response.json()
                    error = error_data.get("error")
                    logger.debug(f"[DEBUG] Error response: {error_data}")
                    
                    if error == "authorization_pending":
                        logger.info(f"Authorization still pending for user: {user_id}")
                        logger.debug(f"[DEBUG] check_auth_status completed: pending")
                        return {"status": "pending"}
                    elif error == "slow_down":
                        logger.info(f"Rate limited, slowing down for user: {user_id}")
                        logger.debug(f"[DEBUG] check_auth_status completed: rate limited")
                        return {"status": "pending", "message": "Rate limited, please wait"}
                    elif error == "access_denied":
                        logger.warning(f"Authorization denied for user: {user_id}")
                        logger.debug(f"[DEBUG] check_auth_status completed: access denied")
                        return {"status": "failed", "message": "Authorization denied by user"}
                    elif error == "expired_token":
                        logger.warning(f"Device code expired for user: {user_id}")
                        logger.debug(f"[DEBUG] check_auth_status completed: expired token")
                        return {"status": "failed", "message": "Device code expired"}
                    else:
                        logger.error(f"Unknown authorization error for user {user_id}: {error}")
                        logger.debug(f"[DEBUG] check_auth_status completed: unknown error")
                        return {"status": "failed", "message": f"Authorization error: {error}"}
                
                elif response.status_code == 200:
                    token_data = response.json()
                    logger.debug(f"[DEBUG] Token response received, creating token in database")
                    
                    # Создаем токен в базе данных
                    expires_in = token_data.get("expires_in", 3600)  # По умолчанию 1 час
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    
                    logger.debug(f"[DEBUG] Token expires_in: {expires_in}, expires_at: {expires_at}")
                    
                    await self.token_service.create_token(
                        user_id=user_id,
                        allegro_token=token_data["access_token"],
                        refresh_token=token_data["refresh_token"],
                        expires_at=expires_at
                    )
                    
                    logger.info(f"Authorization completed and token saved for user: {user_id}")
                    logger.debug(f"[DEBUG] check_auth_status completed: authorization successful")
                    return {"status": "completed"}
                
                else:
                    error_text = response.text
                    logger.error(f"Unexpected response during auth check: {response.status_code} - {error_text}")
                    logger.debug(f"[DEBUG] Unexpected response: {error_text}")
                    return {"status": "failed", "message": f"Unexpected error: {response.status_code}"}
                        
        except httpx.RequestError as e:
            logger.error(f"Network error during auth status check: {str(e)}")
            logger.debug(f"[DEBUG] RequestError: {type(e)} - {str(e)}")
            raise ValidationError(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during auth status check: {str(e)}")
            logger.debug(f"[DEBUG] Exception type: {type(e)}, details: {str(e)}", exc_info=True)
            raise InternalServerErrorHTTPException("Failed to check authorization status")
    
    async def refresh_token(self, token: UserToken) -> Optional[UserToken]:
        """
        Обновляет токен доступа асинхронно.
        
        Args:
            token: Токен для обновления
            
        Returns:
            UserToken: Обновленный токен или None в случае ошибки
            
        Raises:
            ValidationError: Если не удалось обновить токен
        """
        try:
            logger.info(f"Refreshing token for user: {token.user_id}")
            
            auth_str = f'{self.allegro_settings.client_id}:{self.allegro_settings.client_secret}'
            b64_auth_str = base64.b64encode(auth_str.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {b64_auth_str}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': token.refresh_token
            }
            
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    self.token_url,
                    headers=headers,
                    data=data
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    
                    # Обновляем токен в базе данных
                    expires_in = token_data.get("expires_in", 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                    
                    update_data = {
                        "allegro_token": token_data["access_token"],
                        "refresh_token": token_data["refresh_token"],
                        "expires_at": expires_at
                    }
                    
                    updated_token = await self.token_service.update_token(token.id, update_data)
                    logger.info(f"Token refreshed successfully for user: {token.user_id}")
                    return updated_token
                
                else:
                    error_text = response.text
                    logger.error(f"Failed to refresh token: {response.status_code} - {error_text}")
                    raise ValidationError(f"Failed to refresh token: {response.status_code}")
                        
        except httpx.RequestError as e:
            logger.error(f"Network error during token refresh: {str(e)}")
            raise ValidationError(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during token refresh: {str(e)}")
            raise InternalServerErrorHTTPException("Failed to refresh token")
    
    async def validate_token(self, token: UserToken) -> bool:
        """
        Проверяет действительность токена через API запрос.
        
        Args:
            token: Токен для проверки
            
        Returns:
            bool: True если токен действителен, False в противном случае
        """
        try:
            headers = {
                'Authorization': f'Bearer {token.allegro_token}',
                'Accept': 'application/vnd.allegro.public.v1+json'
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f'{self.api_url}/me',
                    headers=headers
                )
                
                if response.status_code == 200:
                    logger.info(f"Token is valid for user: {token.user_id}")
                    return True
                elif response.status_code == 401:
                    logger.info(f"Token is expired for user: {token.user_id}")
                    return False
                else:
                    logger.warning(f"Unexpected response during token validation: {response.status_code}")
                    return False
                        
        except Exception as e:
            logger.error(f"Error during token validation: {str(e)}")
            return False
    
    async def check_and_refresh_token(self, token: UserToken) -> Optional[UserToken]:
        """
        Проверяет токен и при необходимости обновляет его.
        
        Args:
            token: Токен для проверки
            
        Returns:
            UserToken: Действующий токен или None в случае ошибки
        """
        try:
            # Сначала проверяем срок действия
            current_time = datetime.utcnow()
            time_threshold = current_time + timedelta(minutes=5)
            
            if token.expires_at <= time_threshold:
                logger.info(f"Token expires soon, refreshing for user: {token.user_id}")
                return await self.refresh_token(token)
            
            # Затем проверяем через API
            if await self.validate_token(token):
                return token
            else:
                logger.info(f"Token validation failed, refreshing for user: {token.user_id}")
                return await self.refresh_token(token)
                
        except Exception as e:
            logger.error(f"Error during token check and refresh: {str(e)}")
            return None 