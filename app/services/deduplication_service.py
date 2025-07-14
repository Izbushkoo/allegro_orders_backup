"""
@file: deduplication_service.py
@description: Сервис дедупликации данных для предотвращения дублирования при использовании нескольких токенов
@dependencies: Order, OrderEvent, UserToken models
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import UUID
from sqlmodel import Session, select, func
from app.models.order import Order
from app.models.order_event import OrderEvent
from app.models.user_token import UserToken

logger = logging.getLogger(__name__)

class DeduplicationService:
    """
    Сервис дедупликации данных для токенов.
    
    Принципы работы:
    1. Дедупликация происходит только в рамках одного token_id
    2. События/заказы от разных токенов не дедуплицируются
    3. Каждый токен работает независимо
    4. Уникальность event_id/order_id в рамках token_id
    """
    
    def __init__(self, db: Session):
        """
        Инициализация сервиса дедупликации
        
        Args:
            db: Сессия базы данных
        """
        self.db = db
        
    def should_process_order(self, allegro_order_id: str, token_id: UUID) -> Dict[str, Any]:
        """
        Проверка, нужно ли обрабатывать заказ для данного токена.
        
        Args:
            allegro_order_id: ID заказа в Allegro
            token_id: ID токена, который хочет обработать заказ
            
        Returns:
            Dict: Результат проверки с причиной
        """
        
        try:
            # Получаем информацию о токене
            token = self.db.exec(
                select(UserToken).where(UserToken.id == token_id)
            ).first()
            
            if not token:
                return {
                    "should_process": False,
                    "reason": "Токен не найден"
                }
            
            # Проверяем, существует ли заказ для этого токена
            existing_order = self.db.exec(
                select(Order).where(
                    Order.allegro_order_id == allegro_order_id,
                    Order.token_id == token_id
                )
            ).first()
            
            if existing_order:
                # Заказ уже существует для этого токена
                return {
                    "should_process": False,
                    "reason": f"Заказ уже существует для токена {token_id}",
                    "existing_order_id": existing_order.id
                }
            else:
                # Новый заказ для этого токена
                return {
                    "should_process": True,
                    "reason": "Новый заказ для токена"
                }
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке заказа {allegro_order_id}: {e}")
            return {
                "should_process": False,
                "reason": f"Ошибка проверки: {str(e)}"
            }
            
    def should_process_event(self, event_id: str, token_id: UUID) -> Dict[str, Any]:
        """
        Проверка, нужно ли обрабатывать событие для данного токена.
        
        Args:
            event_id: ID события в Allegro
            token_id: ID токена, который хочет обработать событие
            
        Returns:
            Dict: Результат проверки с причиной
        """
        
        try:
            # Получаем информацию о токене
            token = self.db.exec(
                select(UserToken).where(UserToken.id == token_id)
            ).first()
            
            if not token:
                return {
                    "should_process": False,
                    "reason": "Токен не найден"
                }
            
            # Проверяем, существует ли событие для этого токена
            existing_event = self.db.exec(
                select(OrderEvent).where(
                    OrderEvent.event_id == event_id,
                    OrderEvent.token_id == token_id
                )
            ).first()
            
            if existing_event:
                # Событие уже существует для этого токена
                return {
                    "should_process": False,
                    "reason": f"Событие уже существует для токена {token_id}",
                    "existing_event_id": existing_event.id
                }
            else:
                # Новое событие для этого токена
                return {
                    "should_process": True,
                    "reason": "Новое событие для токена"
                }
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке события {event_id}: {e}")
            return {
                "should_process": False,
                "reason": f"Ошибка проверки: {str(e)}"
            }
            
    def mark_as_duplicate(self, record_id: UUID, record_type: str = "event") -> Dict[str, Any]:
        """
        Помечает запись как дубликат.
        
        Args:
            record_id: ID записи для пометки
            record_type: Тип записи ('event' или 'order')
            
        Returns:
            Dict: Результат операции
        """
        
        try:
            if record_type == "event":
                # Помечаем событие как дубликат
                event = self.db.exec(
                    select(OrderEvent).where(OrderEvent.id == record_id)
                ).first()
                
                if event:
                    event.is_duplicate = True
                    self.db.add(event)
                    self.db.commit()
                    
                    return {
                        "success": True,
                        "message": f"Событие {record_id} помечено как дубликат"
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Событие {record_id} не найдено"
                    }
                    
            elif record_type == "order":
                # Помечаем заказ как удаленный (или дубликат)
                order = self.db.exec(
                    select(Order).where(Order.id == record_id)
                ).first()
                
                if order:
                    order.is_deleted = True
                    self.db.add(order)
                    self.db.commit()
                    
                    return {
                        "success": True,
                        "message": f"Заказ {record_id} помечен как удаленный"
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Заказ {record_id} не найден"
                    }
                    
        except Exception as e:
            logger.error(f"❌ Ошибка при пометке записи как дубликат: {e}")
            return {
                "success": False,
                "message": f"Ошибка: {str(e)}"
            }
            
    def get_deduplication_stats(self, token_id: UUID, hours: int = 24) -> Dict[str, Any]:
        """
        Получение статистики дедупликации для конкретного токена.
        
        Args:
            token_id: ID токена
            hours: Временное окно для анализа (часы)
            
        Returns:
            Dict: Статистика дедупликации
        """
        
        try:
            from datetime import timedelta
            
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Статистика по заказам
            orders_query = select(func.count(Order.id)).where(
                Order.token_id == token_id,
                Order.created_at >= cutoff_time
            )
            orders_count = self.db.exec(orders_query).one()
            
            # Статистика по событиям
            events_query = select(func.count(OrderEvent.id)).where(
                OrderEvent.token_id == token_id,
                OrderEvent.occurred_at >= cutoff_time
            )
            events_count = self.db.exec(events_query).one()
            
            # Количество дублированных событий
            duplicate_events_query = select(func.count(OrderEvent.id)).where(
                OrderEvent.token_id == token_id,
                OrderEvent.is_duplicate == True,
                OrderEvent.occurred_at >= cutoff_time
            )
            duplicate_events_count = self.db.exec(duplicate_events_query).one()
            
            return {
                "token_id": str(token_id),
                "hours_analyzed": hours,
                "orders_count": orders_count,
                "events_count": events_count,
                "duplicate_events_count": duplicate_events_count,
                "deduplication_rate": (duplicate_events_count / events_count * 100) if events_count > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики дедупликации: {e}")
            return {
                "token_id": str(token_id),
                "error": str(e)
            }
            
    def get_token_info(self, token_id: UUID) -> Dict[str, Any]:
        """
        Получение информации о токене.
        
        Args:
            token_id: ID токена
            
        Returns:
            Dict: Информация о токене
        """
        
        try:
            token = self.db.exec(
                select(UserToken).where(UserToken.id == token_id)
            ).first()
            
            if not token:
                return {
                    "token_id": str(token_id),
                    "error": "Токен не найден"
                }
            
            return {
                "token_id": str(token_id),
                "user_id": token.user_id,
                "is_active": token.is_active,
                "created_at": token.created_at.isoformat() if token.created_at else None,
                "expires_at": token.expires_at.isoformat() if token.expires_at else None
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении информации о токене: {e}")
            return {
                "token_id": str(token_id),
                "error": str(e)
            }
            
    def cleanup_old_duplicates(self, days: int = 30) -> Dict[str, Any]:
        """
        Очистка старых записей о дублированных событиях.
        
        Args:
            days: Возраст записей для удаления (дни)
            
        Returns:
            Dict: Результат очистки
        """
        
        try:
            from datetime import timedelta
            
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Удаляем старые дублированные события
            old_duplicates_query = select(OrderEvent).where(
                OrderEvent.is_duplicate == True,
                OrderEvent.occurred_at < cutoff_date
            )
            
            old_duplicates = self.db.exec(old_duplicates_query).all()
            deleted_count = len(old_duplicates)
            
            for duplicate in old_duplicates:
                self.db.delete(duplicate)
                
            self.db.commit()
            
            logger.info(f"🧹 Удалено {deleted_count} старых дублированных событий")
            
            return {
                "success": True,
                "deleted_duplicates": deleted_count,
                "cutoff_date": cutoff_date.isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке дублированных событий: {e}")
            self.db.rollback()
            return {
                "success": False,
                "error": str(e)
                }
          