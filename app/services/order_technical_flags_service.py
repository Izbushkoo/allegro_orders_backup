"""
@file: order_technical_flags_service.py
@description: Сервис для работы с техническими флагами заказов (стокировка, инвойсы)
@dependencies: OrderTechnicalFlags, SQLModel Session
"""

import logging
from typing import Optional, Dict, Any
from uuid import UUID
from sqlmodel import Session, select
from sqlalchemy.exc import IntegrityError

from app.models.order_technical_flags import (
    OrderTechnicalFlags, 
    OrderTechnicalFlagsCreate,
    OrderTechnicalFlagsUpdate,
    StockStatusUpdate,
    InvoiceStatusUpdate
)
from app.core.database import get_sync_db_session_direct

logger = logging.getLogger(__name__)

class OrderTechnicalFlagsService:
    """
    Сервис для работы с техническими флагами заказов.
    
    Возможности:
    - Автоматическое создание записи флагов при первом обращении
    - Обновление флагов списания стока
    - Обновление флагов создания инвойсов
    - Получение текущих флагов заказа
    - Валидация прав доступа пользователей
    """
    
    def __init__(self, user_id: str, token_id: UUID):
        """
        Инициализация сервиса для работы с техническими флагами.
        
        Args:
            user_id: ID пользователя
            token_id: ID конкретного токена (обязательный параметр для изоляции)
        """
        self.user_id = user_id
        self.token_id = token_id
        self.db = get_sync_db_session_direct()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                self.db.rollback()
            else:
                # Убеждаемся что все транзакции закоммичены
                self.db.commit()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии сессии OrderTechnicalFlagsService: {e}")
        finally:
            self.db.close()
    
    def get_or_create_flags(self, allegro_order_id: str) -> OrderTechnicalFlags:
        """
        Получить или создать техническую запись флагов для заказа.
        
        Args:
            allegro_order_id: ID заказа в Allegro
            
        Returns:
            OrderTechnicalFlags: Запись технических флагов
            
        Raises:
            Exception: При ошибках работы с БД
        """
        try:
            # Сначала пытаемся найти существующую запись
            query = select(OrderTechnicalFlags).where(
                OrderTechnicalFlags.token_id == self.token_id,
                OrderTechnicalFlags.allegro_order_id == allegro_order_id
            )
            
            existing_flags = self.db.exec(query).first()
            
            if existing_flags:
                logger.debug(f"Найдены существующие флаги для заказа {allegro_order_id}")
                return existing_flags
            
            # Если записи нет, создаем новую с дефолтными значениями
            new_flags = OrderTechnicalFlags(
                token_id=self.token_id,
                allegro_order_id=allegro_order_id,
                is_stock_updated=False,
                has_invoice_created=False,
                invoice_id=None
            )
            
            self.db.add(new_flags)
            self.db.commit()
            self.db.refresh(new_flags)
            
            logger.info(f"Созданы новые технические флаги для заказа {allegro_order_id}")
            return new_flags
            
        except IntegrityError as e:
            self.db.rollback()
            logger.error(f"Ошибка целостности при создании флагов: {e}")
            # Возможно, запись была создана другим процессом, пытаемся получить её
            query = select(OrderTechnicalFlags).where(
                OrderTechnicalFlags.token_id == self.token_id,
                OrderTechnicalFlags.allegro_order_id == allegro_order_id
            )
            existing_flags = self.db.exec(query).first()
            if existing_flags:
                return existing_flags
            raise
        except Exception as e:
            self.db.rollback()
            logger.error(f"Ошибка при получении/создании флагов для заказа {allegro_order_id}: {e}")
            raise
    
    def update_stock_status(self, allegro_order_id: str, is_stock_updated: bool) -> OrderTechnicalFlags:
        """
        Обновить статус списания стока для заказа.
        
        Args:
            allegro_order_id: ID заказа в Allegro
            is_stock_updated: Новый статус списания стока
            
        Returns:
            OrderTechnicalFlags: Обновленная запись флагов
            
        Raises:
            Exception: При ошибках работы с БД
        """
        try:
            flags = self.get_or_create_flags(allegro_order_id)
            
            old_status = flags.is_stock_updated
            flags.is_stock_updated = is_stock_updated
            
            self.db.add(flags)
            self.db.commit()
            self.db.refresh(flags)
            
            logger.info(
                f"Обновлен статус стока для заказа {allegro_order_id}: "
                f"{old_status} -> {is_stock_updated}"
            )
            
            return flags
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Ошибка при обновлении статуса стока для заказа {allegro_order_id}: {e}")
            raise
    
    def update_invoice_status(
        self, 
        allegro_order_id: str, 
        has_invoice_created: bool, 
        invoice_id: Optional[str] = None
    ) -> OrderTechnicalFlags:
        """
        Обновить статус создания инвойса для заказа.
        
        Args:
            allegro_order_id: ID заказа в Allegro
            has_invoice_created: Новый статус создания инвойса
            invoice_id: ID созданного инвойса (если есть)
            
        Returns:
            OrderTechnicalFlags: Обновленная запись флагов
            
        Raises:
            Exception: При ошибках работы с БД
        """
        try:
            flags = self.get_or_create_flags(allegro_order_id)
            
            old_status = flags.has_invoice_created
            old_invoice_id = flags.invoice_id
            
            flags.has_invoice_created = has_invoice_created
            flags.invoice_id = invoice_id
            
            self.db.add(flags)
            self.db.commit()
            self.db.refresh(flags)
            
            logger.info(
                f"Обновлен статус инвойса для заказа {allegro_order_id}: "
                f"status {old_status} -> {has_invoice_created}, "
                f"id {old_invoice_id} -> {invoice_id}"
            )
            
            return flags
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Ошибка при обновлении статуса инвойса для заказа {allegro_order_id}: {e}")
            raise
    
    def get_flags(self, allegro_order_id: str) -> Optional[OrderTechnicalFlags]:
        """
        Получить технические флаги заказа без автосоздания.
        
        Args:
            allegro_order_id: ID заказа в Allegro
            
        Returns:
            Optional[OrderTechnicalFlags]: Запись флагов или None, если не найдена
        """
        try:
            query = select(OrderTechnicalFlags).where(
                OrderTechnicalFlags.token_id == self.token_id,
                OrderTechnicalFlags.allegro_order_id == allegro_order_id
            )
            
            return self.db.exec(query).first()
            
        except Exception as e:
            logger.error(f"Ошибка при получении флагов для заказа {allegro_order_id}: {e}")
            raise
    
    def get_multiple_flags(self, order_ids: list[str]) -> Dict[str, Dict[str, Any]]:
        """
        Получить технические флаги для множества заказов одним запросом.
        
        Args:
            order_ids: Список ID заказов в Allegro
            
        Returns:
            Dict[str, Dict[str, Any]]: Словарь флагов по ID заказов как обычные Python данные
        """
        try:
            if not order_ids:
                return {}
                
            query = select(OrderTechnicalFlags).where(
                OrderTechnicalFlags.token_id == self.token_id,
                OrderTechnicalFlags.allegro_order_id.in_(order_ids)
            )
            
            flags_records = self.db.exec(query).all()
            
            # Создаем словарь с обычными Python данными (не SQLAlchemy объектами)
            flags_dict = {}
            for flags in flags_records:
                flags_dict[flags.allegro_order_id] = {
                    "is_stock_updated": flags.is_stock_updated,
                    "has_invoice_created": flags.has_invoice_created,
                    "invoice_id": flags.invoice_id
                }
            
            # Создаем записи для заказов, у которых нет флагов
            missing_order_ids = set(order_ids) - set(flags_dict.keys())
            
            # Создаем все недостающие записи в одной транзакции
            if missing_order_ids:
                new_flags_list = []
                try:
                    for order_id in missing_order_ids:
                        new_flags = OrderTechnicalFlags(
                            token_id=self.token_id,
                            allegro_order_id=order_id,
                            is_stock_updated=False,
                            has_invoice_created=False,
                            invoice_id=None
                        )
                        self.db.add(new_flags)
                        new_flags_list.append((order_id, new_flags))
                    
                    # Commit всех записей за раз
                    self.db.commit()
                    
                    # Refresh всех объектов после commit и конвертируем в Python данные
                    for order_id, flags in new_flags_list:
                        self.db.refresh(flags)
                        flags_dict[order_id] = {
                            "is_stock_updated": flags.is_stock_updated,
                            "has_invoice_created": flags.has_invoice_created,
                            "invoice_id": flags.invoice_id
                        }
                    
                except IntegrityError:
                    # Откат и повторный запрос для всех недостающих
                    self.db.rollback()
                    for order_id in missing_order_ids:
                        query = select(OrderTechnicalFlags).where(
                            OrderTechnicalFlags.token_id == self.token_id,
                            OrderTechnicalFlags.allegro_order_id == order_id
                        )
                        existing_flags = self.db.exec(query).first()
                        if existing_flags:
                            flags_dict[order_id] = {
                                "is_stock_updated": existing_flags.is_stock_updated,
                                "has_invoice_created": existing_flags.has_invoice_created,
                                "invoice_id": existing_flags.invoice_id
                            }
            
            logger.debug(f"Получены флаги для {len(flags_dict)} заказов")
            return flags_dict
            
        except Exception as e:
            logger.error(f"Ошибка при получении множественных флагов: {e}")
            raise
    
    def get_flags_summary(self) -> Dict[str, Any]:
        """
        Получить сводную статистику по техническим флагам токена.
        
        Returns:
            Dict[str, Any]: Статистика флагов
        """
        try:
            query = select(OrderTechnicalFlags).where(
                OrderTechnicalFlags.token_id == self.token_id
            )
            
            all_flags = self.db.exec(query).all()
            
            total_orders = len(all_flags)
            stock_updated_count = sum(1 for f in all_flags if f.is_stock_updated)
            invoices_created_count = sum(1 for f in all_flags if f.has_invoice_created)
            
            return {
                "total_orders_with_flags": total_orders,
                "stock_updated_orders": stock_updated_count,
                "invoices_created_orders": invoices_created_count,
                "stock_updated_percentage": (stock_updated_count / total_orders * 100) if total_orders > 0 else 0,
                "invoices_created_percentage": (invoices_created_count / total_orders * 100) if total_orders > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении сводки флагов: {e}")
            raise 