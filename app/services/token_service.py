"""
@file: app/services/token_service.py
@description: Сервис для работы с токенами пользователей
@dependencies: sqlmodel, uuid
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime

from sqlmodel import select, and_, or_
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.user_token import UserToken
from app.exceptions import NotFoundError, ValidationError
from app.core.logging import get_logger
from app.models.user_token import UserTokenUpdate
from app.services.active_sync_schedule_service import ActiveSyncScheduleService
from app.services.periodic_task_service import PeriodicTaskService
from app.core.database import get_sync_db_session_direct, get_alchemy_session

logger = get_logger(__name__)


class TokenService:
    """Сервис для работы с токенами пользователей"""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
    
    async def validate_account_name_uniqueness(self, user_id: str, account_name: str) -> bool:
        """
        Проверить уникальность комбинации user_id + account_name.
        
        Args:
            user_id: ID пользователя
            account_name: Название аккаунта
            
        Returns:
            bool: True если комбинация уникальна, False если уже существует
        """
        try:
            query = select(UserToken).where(
                and_(
                    UserToken.user_id == user_id,
                    UserToken.account_name == account_name,
                    UserToken.is_active == True
                )
            )
            result = await self.db_session.exec(query)
            existing_token = result.first()
            
            return existing_token is None
            
        except Exception as e:
            logger.error(f"Failed to validate account name uniqueness: {str(e)}")
            return False
    
    async def create_token(
        self,
        user_id: str,
        account_name: str,
        allegro_token: str,
        refresh_token: str,
        expires_at: datetime
    ) -> UserToken:
        """
        Создать новый токен пользователя.
        
        Args:
            user_id: ID пользователя
            account_name: Название аккаунта Allegro
            allegro_token: Токен доступа Allegro
            refresh_token: Refresh токен
            expires_at: Дата истечения токена
            
        Returns:
            UserToken: Созданный токен
            
        Raises:
            ValidationError: Если данные невалидны или комбинация user_id + account_name не уникальна
        """
        try:
            # Проверяем уникальность комбинации user_id + account_name
            if not await self.validate_account_name_uniqueness(user_id, account_name):
                raise ValidationError(f"Аккаунт '{account_name}' уже существует для пользователя '{user_id}'")
            
            # Деактивируем старые токены пользователя для этого аккаунта
            old_tokens_query = select(UserToken).where(
                and_(
                    UserToken.user_id == user_id,
                    UserToken.account_name == account_name,
                    UserToken.is_active == True
                )
            )
            old_tokens_result = await self.db_session.exec(old_tokens_query)
            old_tokens = old_tokens_result.all()
            
            for old_token in old_tokens:
                old_token.is_active = False
            
            # Создаем новый токен
            token = UserToken(
                user_id=user_id,
                account_name=account_name,
                allegro_token=allegro_token,
                refresh_token=refresh_token,
                expires_at=expires_at,
                is_active=True
            )
            
            self.db_session.add(token)
            await self.db_session.commit()
            await self.db_session.refresh(token)
            
            logger.info(f"Token created for user {user_id}")
            return token
            
        except Exception as e:
            await self.db_session.rollback()
            logger.error(f"Failed to create token for user {user_id}: {str(e)}")
            raise ValidationError(f"Failed to create token: {str(e)}")
    
    async def get_token(self, token_id: UUID) -> Optional[UserToken]:
        """
        Получить токен по ID.
        
        Args:
            token_id: ID токена
            
        Returns:
            UserToken или None если не найден
        """
        try:
            query = select(UserToken).where(UserToken.id == token_id)
            result = await self.db_session.exec(query)
            return result.first()
        except Exception as e:
            logger.error(f"Failed to get token {token_id}: {str(e)}")
            return None
    
    async def get_token_or_raise(self, token_id: UUID) -> UserToken:
        """
        Получить токен по ID или выбросить исключение.
        
        Args:
            token_id: ID токена
            
        Returns:
            UserToken
            
        Raises:
            NotFoundError: Если токен не найден
        """
        token = await self.get_token(token_id)
        if not token:
            raise NotFoundError(f"Token with id {token_id} not found")
        return token
    
    async def get_tokens(
        self,
        page: int = 1,
        per_page: int = 10,
        user_id: Optional[str] = None,
        active_only: bool = True
    ) -> tuple[List[UserToken], int]:
        """
        Получить список токенов с пагинацией.
        
        Args:
            page: Номер страницы (начинается с 1)
            per_page: Количество элементов на странице
            user_id: Фильтр по ID пользователя (опционально)
            active_only: Показывать только активные токены
            
        Returns:
            Tuple[List[UserToken], int]: Список токенов и общее количество
        """
        try:
            # Строим базовый запрос
            query = select(UserToken)
            count_query = select(UserToken)
            
            # Применяем фильтры
            conditions = []
            if user_id:
                conditions.append(UserToken.user_id == user_id)
            if active_only:
                conditions.append(UserToken.is_active == True)
                
            if conditions:
                combined_condition = and_(*conditions)
                query = query.where(combined_condition)
                count_query = count_query.where(combined_condition)
            
            # Подсчитываем общее количество
            count_result = await self.db_session.exec(count_query)
            total = len(count_result.all())
            
            # Применяем пагинацию
            offset = (page - 1) * per_page
            query = query.offset(offset).limit(per_page)
            
            # Выполняем запрос
            result = await self.db_session.exec(query)
            tokens = result.all()
            
            return tokens, total
            
        except Exception as e:
            logger.error(f"Failed to get tokens: {str(e)}")
            return [], 0
    
    async def get_user_tokens(
        self,
        user_id: str,
        active_only: bool = True
    ) -> List[UserToken]:
        """
        Получить все токены пользователя.
        
        Args:
            user_id: ID пользователя
            active_only: Показывать только активные токены
            
        Returns:
            List[UserToken]: Список токенов пользователя
        """
        try:
            query = select(UserToken).where(UserToken.user_id == user_id)
            
            if active_only:
                query = query.where(UserToken.is_active == True)
            
            result = await self.db_session.exec(query)
            return result.all()
            
        except Exception as e:
            logger.error(f"Failed to get tokens for user {user_id}: {str(e)}")
            return []
    
    async def update_token(
        self, 
        token_id: UUID, 
        update_data: dict
    ) -> Optional[UserToken]:
        """
        Обновить токен.
        
        Args:
            token_id: ID токена
            update_data: Словарь с данными для обновления
            
        Returns:
            UserToken или None если не найден
        """
        try:
            # Используем SQL UPDATE для избежания проблем с Pydantic v2
            from sqlmodel import text
            
            # Строим SET часть запроса
            set_parts = []
            values = {"token_id": token_id}
            
            for key, value in update_data.items():
                if value is not None:
                    set_parts.append(f"{key} = :{key}")
                    values[key] = value
            
            if not set_parts:
                # Если нет данных для обновления, просто возвращаем токен
                return await self.get_token(token_id)
            
            # Добавляем updated_at
            set_parts.append("updated_at = :updated_at")
            values["updated_at"] = datetime.utcnow()
            
            sql = f"""
                UPDATE user_tokens 
                SET {', '.join(set_parts)}
                WHERE id = :token_id
            """
            
            await self.db_session.exec(text(sql).bindparams(**values))
            await self.db_session.commit()
            
            # Возвращаем обновленный токен
            return await self.get_token(token_id)
            
        except Exception as e:
            await self.db_session.rollback()
            logger.error(f"Failed to update token {token_id}: {str(e)}")
            return None
    
    async def deactivate_token(self, token_id: UUID) -> UserToken:
        """
        Деактивировать токен (мягкое удаление) и удалить связанные периодические задачи.
        
        Args:
            token_id: ID токена
            
        Returns:
            UserToken: Деактивированный токен
            
        Raises:
            NotFoundError: Если токен не найден
        """
        try:
            # Сначала получаем информацию о токене для поиска связанных задач
            token_to_deactivate = await self.get_token(token_id)
            if not token_to_deactivate:
                raise NotFoundError(f"Token with id {token_id} not found")
            
            # Удаляем связанные периодические задачи синхронизации
            await self._remove_periodic_sync_tasks(token_to_deactivate.user_id, str(token_id))
            
            # Используем SQL UPDATE для деактивации токена
            from sqlmodel import text
            
            sql = """
                UPDATE user_tokens 
                SET is_active = false, updated_at = :updated_at
                WHERE id = :token_id
            """
            
            result = await self.db_session.exec(
                text(sql).bindparams(
                    token_id=token_id, 
                    updated_at=datetime.utcnow()
                )
            )
            await self.db_session.commit()
            
            # Проверяем что токен был найден и обновлен
            updated_token = await self.get_token(token_id)
            if not updated_token:
                raise NotFoundError(f"Token with id {token_id} not found")
            
            logger.info(f"Token {token_id} deactivated and periodic tasks removed")
            return updated_token
            
        except NotFoundError:
            raise
        except Exception as e:
            await self.db_session.rollback()
            logger.error(f"Failed to deactivate token {token_id}: {str(e)}")
            raise NotFoundError(f"Failed to deactivate token: {str(e)}")
    
    async def is_token_expired(self, token_id: UUID) -> bool:
        """
        Проверить истек ли токен.
        
        Args:
            token_id: ID токена
            
        Returns:
            bool: True если токен истек
        """
        try:
            token = await self.get_token(token_id)
            if not token:
                return True
            
            return datetime.utcnow() > token.expires_at
        except Exception as e:
            logger.error(f"Failed to check if token {token_id} is expired: {str(e)}")
            return True
    
    async def get_expired_tokens(self) -> List[UserToken]:
        """
        Получить список истекших токенов.
        
        Returns:
            List[UserToken]: Список истекших токенов
        """
        try:
            current_time = datetime.utcnow()
            query = select(UserToken).where(
                and_(
                    UserToken.expires_at < current_time,
                    UserToken.is_active == True
                )
            )
            result = await self.db_session.exec(query)
            return result.all()
        except Exception as e:
            logger.error(f"Failed to get expired tokens: {str(e)}")
            return []
    
    async def get_user_token_by_id(self, token_id: UUID, user_id: str) -> Optional[UserToken]:
        """
        Получить токен по ID только если он принадлежит пользователю.
        
        Args:
            token_id: ID токена
            user_id: ID пользователя
            
        Returns:
            UserToken или None если не найден или не принадлежит пользователю
        """
        try:
            query = select(UserToken).where(
                and_(
                    UserToken.id == token_id,
                    UserToken.user_id == user_id
                )
            )
            result = await self.db_session.exec(query)
            return result.first()
        except Exception as e:
            logger.error(f"Failed to get user token {token_id} for user {user_id}: {str(e)}")
            return None
    
    async def update_user_token(
        self,
        token_id: UUID,
        account_name: Optional[str] = None,
        allegro_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        expires_at: Optional[datetime] = None,
        is_active: Optional[bool] = None
    ) -> Optional[UserToken]:
        """
        Обновить токен пользователя.
        
        Args:
            token_id: ID токена
            account_name: Новое название аккаунта (опционально)
            allegro_token: Новый access token (опционально)
            refresh_token: Новый refresh token (опционально)
            expires_at: Новая дата истечения (опционально)
            is_active: Новый статус активности (опционально)
            
        Returns:
            UserToken или None если не найден
        """
        try:
            update_data = {}
            if account_name is not None:
                update_data["account_name"] = account_name
            if allegro_token is not None:
                update_data["allegro_token"] = allegro_token
            if refresh_token is not None:
                update_data["refresh_token"] = refresh_token
            if expires_at is not None:
                update_data["expires_at"] = expires_at
            if is_active is not None:
                update_data["is_active"] = is_active
            
            return await self.update_token(token_id, update_data)
            
        except Exception as e:
            logger.error(f"Failed to update user token {token_id}: {str(e)}")
            return None
    
    async def delete_user_token(self, token_id: UUID) -> bool:
        """
        Удалить (деактивировать) токен пользователя.
        
        Args:
            token_id: ID токена
            
        Returns:
            bool: True если успешно удален
        """
        try:
            token = await self.deactivate_token(token_id)
            return token is not None
        except Exception as e:
            logger.error(f"Failed to delete user token {token_id}: {str(e)}")
            return False
    
    async def validate_and_refresh_token(self, token_id: UUID) -> Optional[UserToken]:
        """
        Проверить валидность токена и обновить его при необходимости.
        
        Args:
            token_id: ID токена
            
        Returns:
            UserToken или None если не удалось валидировать
        """
        try:
            token = await self.get_token(token_id)
            if not token:
                logger.warning(f"Token {token_id} not found for validation")
                return None
            
            # Проверяем истечение токена
            if datetime.utcnow() > token.expires_at:
                logger.info(f"Token {token_id} is expired, attempting refresh")
                
                # Пытаемся обновить через refresh token
                from app.services.allegro_auth_service import AllegroAuthService
                auth_service = AllegroAuthService(self.db_session)
                
                try:
                    refreshed_token = await auth_service.refresh_token(token)
                    
                    if refreshed_token:
                        logger.info(f"Token {token_id} successfully refreshed")
                        return refreshed_token
                    else:
                        logger.error(f"Failed to refresh token {token_id}")
                        # Деактивируем токен если не удалось обновить
                        await self.deactivate_token(token_id)
                        return None
                    
                except Exception as refresh_error:
                    logger.error(f"Failed to refresh token {token_id}: {refresh_error}")
                    # Деактивируем токен если не удалось обновить
                    await self.deactivate_token(token_id)
                    return None
            
            # Токен еще валиден
            logger.info(f"Token {token_id} is valid")
            return token
            
        except Exception as e:
            logger.error(f"Failed to validate token {token_id}: {str(e)}")
            return None 

    def create_token_sync(self, user_id: str, account_name: str, allegro_token: str, refresh_token: str, expires_at) -> 'UserToken':
        """
        Синхронное создание токена пользователя.
        Args:
            user_id: ID пользователя
            account_name: Название аккаунта Allegro
            allegro_token: Access token
            refresh_token: Refresh token
            expires_at: Время истечения (datetime)
        Returns:
            UserToken: созданный токен
        """
        from app.models.user_token import UserToken
        from sqlmodel import Session
        import uuid
        logger.info(f"[SYNC] Creating token for user: {user_id}")
        token = UserToken(
            id=uuid.uuid4(),
            user_id=user_id,
            account_name=account_name,
            allegro_token=allegro_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            is_active=True
        )
        self.db_session.add(token)
        self.db_session.commit()
        self.db_session.refresh(token)
        logger.info(f"[SYNC] Token created for user: {user_id}")
        return token

    def update_token_sync(self, token_id, update_data: dict) -> 'UserToken':
        """
        Синхронное обновление токена пользователя.
        Args:
            token_id: ID токена
            update_data: dict с обновляемыми полями
        Returns:
            UserToken: обновленный токен
        """
        from app.models.user_token import UserToken
        from sqlmodel import select
        logger.info(f"[SYNC] Updating token {token_id}")
        query = select(UserToken).where(UserToken.id == token_id)
        token = self.db_session.exec(query).first()
        if not token:
            logger.error(f"[SYNC] Token {token_id} not found for update")
            raise ValueError(f"Token {token_id} not found")
        for k, v in update_data.items():
            setattr(token, k, v)
        self.db_session.add(token)
        self.db_session.commit()
        self.db_session.refresh(token)
        logger.info(f"[SYNC] Token {token_id} updated")
        return token 

    async def _remove_periodic_sync_tasks(self, user_id: str, token_id: str):
        """
        Удалить все периодические задачи синхронизации для данного токена.
        
        Args:
            user_id: ID пользователя
            token_id: ID токена
        """
        try:
            logger.info(f"Removing periodic sync tasks for token {token_id} of user {user_id}")
            
            # Получаем синхронную сессию для работы с ActiveSyncSchedule и PeriodicTask
            sync_db = get_sync_db_session_direct()
            alchemy_db = get_alchemy_session()
            
            try:
                # Инициализируем сервисы
                schedule_service = ActiveSyncScheduleService(sync_db)
                periodic_service = PeriodicTaskService(alchemy_db)
                
                # Ищем активные расписания для данного токена
                schedule = schedule_service.get_by_token(user_id, token_id)
                
                if schedule:
                    logger.info(f"Found active sync schedule for token {token_id}: {schedule.task_name}")
                    
                    # Удаляем задачу из Celery Beat
                    removed = periodic_service.remove_periodic_sync_task(schedule.task_name)
                    if removed:
                        logger.info(f"Removed periodic task from Celery Beat: {schedule.task_name}")
                    else:
                        logger.warning(f"Periodic task not found in Celery Beat: {schedule.task_name}")
                    
                    # Деактивируем запись в ActiveSyncSchedule
                    deactivated = schedule_service.delete(user_id, token_id)
                    if deactivated:
                        logger.info(f"Deactivated sync schedule for token {token_id}")
                    else:
                        logger.warning(f"Failed to deactivate sync schedule for token {token_id}")
                        
                else:
                    logger.info(f"No active sync schedule found for token {token_id}")
                    
            finally:
                # Закрываем сессии
                sync_db.close()
                alchemy_db.close()
                
        except Exception as e:
            logger.error(f"Error removing periodic sync tasks for token {token_id}: {str(e)}")
            # Не прерываем процесс удаления токена из-за ошибки удаления периодических задач
            # Логируем ошибку и продолжаем 