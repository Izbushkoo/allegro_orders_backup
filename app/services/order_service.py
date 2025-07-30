"""
@file: order_service.py
@description: Основной сервис для работы с заказами Allegro API
@dependencies: OrderProtectionService, DataMonitoringService, AllegroAuthService
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlmodel import Session, select, func
import httpx

from app.models.order import Order
from app.models.order_event import OrderEvent
from app.models.sync_history import SyncHistory
from app.models.order_technical_flags import OrderTechnicalFlags
from app.services.order_protection_service import OrderProtectionService, DataIntegrityError
from app.services.data_monitoring_service import DataMonitoringService
from app.services.allegro_auth_service import AllegroAuthService
from app.services.order_technical_flags_service import OrderTechnicalFlagsService
from app.core.database import get_sync_db_session_direct

logger = logging.getLogger(__name__)

class OrderService:
    """
    Основной сервис для работы с заказами Allegro API.
    
    Возможности:
    - Получение событий заказов от Allegro
    - Синхронизация заказов с защитой данных
    - Получение списка заказов с фильтрацией
    - Поиск заказов по различным критериям
    - Статистика и аналитика заказов
    - Интеграция с защитными механизмами
    """
    
    # Allegro API endpoints
    EVENTS_URL = "https://api.allegro.pl/order/events"
    EVENT_STATS_URL = "https://api.allegro.pl/order/event-stats"
    CHECKOUT_FORMS_URL = "https://api.allegro.pl/order/checkout-forms"
    
    # Типы событий Allegro
    EVENT_TYPES = {
        "BOUGHT": "Zakup - zamówienie złożone",
        "FILLED_IN": "Uzupełnione - zamówienie uzupełnione o dane",
        "READY_FOR_PROCESSING": "Gotowe do realizacji",
        "BUYER_CANCELLED": "Anulowane przez kupującego",
        "FULFILLMENT_STATUS_CHANGED": "Zmiana statusu realizacji"
    }
    
    def __init__(self, user_id: str, token_id: str):
        """
        Инициализация сервиса для работы с заказами
        
        Args:
            user_id: ID пользователя
            token_id: ID конкретного токена (обязательный параметр)
        """
        self.user_id = user_id
        self.token_id = token_id
        self.db = get_sync_db_session_direct()
        self.protection_service = OrderProtectionService(self.db, self.token_id)
        self.monitoring_service = DataMonitoringService(self.db)
        self.auth_service = AllegroAuthService(self.db)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрываем сессию БД при выходе из контекста"""
        if self.db:
            self.db.close()
            
    def get_order_events(self, limit: int = 100, from_timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Получение событий заказов от Allegro API.
        
        Args:
            limit: Максимальное количество событий (1-1000)
            from_timestamp: Получить события после указанного времени
            
        Returns:
            Dict: Результат с событиями и метаданными
        """
        
        result = {
            "success": False,
            "events": [],
            "total_count": 0,
            "has_more": False,
            "error": None
        }
        
        try:
            # Получаем токен доступа
            token = self.auth_service.get_valid_access_token_sync(self.user_id, self.token_id)
            if not token:
                result["error"] = f"Токен {self.token_id} недействителен или не принадлежит пользователю"
                return result
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.allegro.public.v1+json"
            }
            
            # Параметры запроса
            params = {"limit": min(limit, 1000)}  # Максимум 1000 согласно API
            
            if from_timestamp:
                params["from"] = from_timestamp.isoformat()
                
            logger.info(f"📥 Запрос событий заказов: limit={limit}, from={from_timestamp}")
            
            # Выполняем запрос к Allegro API
            with httpx.Client() as client:
                response = client.get(self.EVENTS_URL, headers=headers, params=params, timeout=30.0)
                response.raise_for_status()
                
                data = response.json()
                events = data.get("events", [])
                
                result.update({
                    "success": True,
                    "events": events,
                    "total_count": len(events),
                    "has_more": len(events) == limit  # Если получили максимум, возможно есть еще
                })
                
                logger.info(f"✅ Получено {len(events)} событий заказов")
                
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP ошибка при получении событий: {e.response.status_code}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        except httpx.TimeoutException:
            error_msg = "Timeout при получении событий от Allegro API"
            logger.error(error_msg)
            result["error"] = error_msg
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        return result
        
    def get_order_details(self, order_id: str) -> Dict[str, Any]:
        """
        Получение детальной информации о заказе.
        
        Args:
            order_id: ID заказа в Allegro
            
        Returns:
            Dict: Детали заказа или ошибка
        """
        
        result = {
            "success": False,
            "order": None,
            "error": None
        }
        
        try:
            # Получаем токен доступа
            token = self.auth_service.get_valid_access_token_sync(self.user_id, self.token_id)
            if not token:
                result["error"] = f"Токен {self.token_id} недействителен или не принадлежит пользователю"
                return result
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.allegro.public.v1+json"
            }
            
            url = f"{self.CHECKOUT_FORMS_URL}/{order_id}"
            
            logger.info(f"📋 Запрос деталей заказа: {order_id}")
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers, timeout=15.0)
                
                if response.status_code == 404:
                    result["error"] = f"Заказ {order_id} не найден"
                    return result
                    
                response.raise_for_status()
                order_data = response.json()
                
                # Получаем технические флаги для заказа
                technical_data = None
                try:
                    with OrderTechnicalFlagsService(self.user_id, self.token_id) as flags_service:
                        flags = flags_service.get_or_create_flags(order_id)
                        technical_data = {
                            "is_stock_updated": flags.is_stock_updated,
                            "has_invoice_created": flags.has_invoice_created,
                            "invoice_id": flags.invoice_id,
                            "created_at": flags.created_at.isoformat(),
                            "updated_at": flags.updated_at.isoformat()
                        }
                except Exception as e:
                    logger.warning(f"Не удалось получить технические флаги для заказа {order_id}: {e}")
                
                # Добавляем технические флаги к данным заказа
                order_data["technical_flags"] = technical_data
                
                result.update({
                    "success": True,
                    "order": order_data
                })
                
                logger.info(f"✅ Получены детали заказа {order_id}")
                
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP ошибка при получении заказа {order_id}: {e.response.status_code}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        except Exception as e:
            error_msg = f"Ошибка при получении заказа {order_id}: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
            
        return result
        
    def sync_single_order(self, order_id: str) -> Dict[str, Any]:
        """
        Синхронизация одного заказа с полной защитой данных.
        
        Args:
            order_id: ID заказа для синхронизации
            
        Returns:
            Dict: Результат синхронизации
        """
        
        logger.info(f"🔄 Начало синхронизации заказа {order_id}")
        
        # Получаем детали заказа от Allegro
        order_result = self.get_order_details(order_id)
        if not order_result["success"]:
            return {
                "success": False,
                "order_id": order_id,
                "message": f"Не удалось получить данные заказа: {order_result['error']}"
            }
            
        order_data = order_result["order"]
        revision = order_data.get("revision")
        
        # Используем защищенное обновление
        try:
            sync_result = self.protection_service.safe_order_update(
                order_id=order_id,
                new_data=order_data,
                allegro_revision=revision
            )
            
            if sync_result["success"]:
                logger.info(f"✅ Заказ {order_id} синхронизирован: {sync_result['action']}")
            else:
                logger.warning(f"⚠️ Заказ {order_id} не синхронизирован: {sync_result['message']}")
                
            return sync_result
            
        except DataIntegrityError as e:
            logger.error(f"❌ Ошибка целостности данных для заказа {order_id}: {e}")
            return {
                "success": False,
                "order_id": order_id,
                "message": f"Ошибка целостности данных: {str(e)}"
            }
            
    def get_orders_list(self, 
                       limit: int = 50,
                       offset: int = 0,
                       status_filter: Optional[str] = None,
                       from_date: Optional[datetime] = None,
                       to_date: Optional[datetime] = None,
                       stock_updated_filter: Optional[bool] = None,
                       invoice_created_filter: Optional[bool] = None,
                       invoice_id_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Получение списка заказов из локальной БД с фильтрацией.
        
        Args:
            limit: Количество заказов на страницу
            offset: Смещение для пагинации
            status_filter: Фильтр по статусу заказа
            from_date: Заказы от указанной даты
            to_date: Заказы до указанной даты
            stock_updated_filter: Фильтр по флагу обновления стока
            invoice_created_filter: Фильтр по флагу создания инвойса
            invoice_id_filter: Фильтр по конкретному ID инвойса
            
        Returns:
            Dict: Список заказов с метаданными
        """
        
        try:
            # Проверяем, нужны ли фильтры по техническим флагам
            need_flags_join = any([
                stock_updated_filter is not None,
                invoice_created_filter is not None,
                invoice_id_filter is not None
            ])
            
            if need_flags_join:
                # Строим запрос с JOIN по техническим флагам
                query = select(Order).join(
                    OrderTechnicalFlags,
                    Order.allegro_order_id == OrderTechnicalFlags.allegro_order_id
                ).where(
                    Order.token_id == self.token_id,
                    OrderTechnicalFlags.token_id == self.token_id
                )
                
                # Применяем фильтры по техническим флагам
                if stock_updated_filter is not None:
                    query = query.where(OrderTechnicalFlags.is_stock_updated == stock_updated_filter)
                    
                if invoice_created_filter is not None:
                    query = query.where(OrderTechnicalFlags.has_invoice_created == invoice_created_filter)
                    
                if invoice_id_filter:
                    query = query.where(OrderTechnicalFlags.invoice_id == invoice_id_filter)
                    
            else:
                # Строим базовый запрос без JOIN
                query = select(Order).where(Order.token_id == self.token_id)
            
            # Применяем остальные фильтры
            if status_filter:
                query = query.where(Order.order_data['status'].as_string() == status_filter)
                
            if from_date:
                query = query.where(Order.order_date >= from_date)
                
            if to_date:
                query = query.where(Order.order_date <= to_date)
                
            # Добавляем сортировку и пагинацию (по дате заказа, самые свежие первыми)
            query = query.order_by(Order.order_date.desc()).offset(offset).limit(limit)
            
            # Выполняем запрос
            orders = self.db.exec(query).all()
            
            # Получаем общее количество (для пагинации) с теми же фильтрами
            if need_flags_join:
                count_query = select(func.count(Order.id)).join(
                    OrderTechnicalFlags,
                    Order.allegro_order_id == OrderTechnicalFlags.allegro_order_id
                ).where(
                    Order.token_id == self.token_id,
                    OrderTechnicalFlags.token_id == self.token_id
                )
                
                # Применяем фильтры по техническим флагам для подсчета
                if stock_updated_filter is not None:
                    count_query = count_query.where(OrderTechnicalFlags.is_stock_updated == stock_updated_filter)
                    
                if invoice_created_filter is not None:
                    count_query = count_query.where(OrderTechnicalFlags.has_invoice_created == invoice_created_filter)
                    
                if invoice_id_filter:
                    count_query = count_query.where(OrderTechnicalFlags.invoice_id == invoice_id_filter)
            else:
                count_query = select(func.count(Order.id)).where(Order.token_id == self.token_id)
                
            # Применяем остальные фильтры для подсчета
            if status_filter:
                count_query = count_query.where(Order.order_data['status'].as_string() == status_filter)
            if from_date:
                count_query = count_query.where(Order.order_date >= from_date)
            if to_date:
                count_query = count_query.where(Order.order_date <= to_date)
                
            total_count = self.db.exec(count_query).one()
            
            # Получаем технические флаги для всех заказов одним запросом
            order_ids = [order.allegro_order_id for order in orders]
            technical_flags = {}
            if order_ids:
                try:
                    with OrderTechnicalFlagsService(self.user_id, self.token_id) as flags_service:
                        technical_flags = flags_service.get_multiple_flags(order_ids)
                        
                        # Данные уже извлечены как обычные Python объекты
                                
                except Exception as e:
                    logger.warning(f"Не удалось получить технические флаги для заказов: {e}")
                    technical_flags = {}
            
            # Конвертируем в dict для API
            orders_data = []
            for order in orders:
                order_dict = self._format_order_data(order, technical_flags.get(order.allegro_order_id))
                orders_data.append(order_dict)
                
            return {
                "success": True,
                "orders": orders_data,
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total_count,
                    "has_prev": offset > 0
                },
                "filters": {
                    "status": status_filter,
                    "from_date": from_date.isoformat() if from_date else None,
                    "to_date": to_date.isoformat() if to_date else None,
                    "stock_updated": stock_updated_filter,
                    "invoice_created": invoice_created_filter,
                    "invoice_id": invoice_id_filter
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении списка заказов: {e}")
            return {
                "success": False,
                "error": str(e),
                "orders": [],
                "pagination": {"total": 0, "limit": limit, "offset": offset}
            }
            
    def search_orders(self, 
                     search_query: str, 
                     limit: int = 50,
                     stock_updated_filter: Optional[bool] = None,
                     invoice_created_filter: Optional[bool] = None,
                     invoice_id_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Поиск заказов по различным критериям.
        
        Args:
            search_query: Поисковый запрос (email покупателя, ID заказа, имя)
            limit: Максимальное количество результатов
            stock_updated_filter: Фильтр по флагу обновления стока
            invoice_created_filter: Фильтр по флагу создания инвойса
            invoice_id_filter: Фильтр по конкретному ID инвойса
            
        Returns:
            Dict: Результаты поиска
        """
        
        try:
            search_term = f"%{search_query.lower()}%"
            
            # Проверяем, нужны ли фильтры по техническим флагам
            need_flags_join = any([
                stock_updated_filter is not None,
                invoice_created_filter is not None,
                invoice_id_filter is not None
            ])
            
            # Формируем базовые условия поиска
            search_conditions = (
                # Поиск по ID заказа
                Order.order_data['id'].as_string().ilike(search_term) |
                # Поиск по email покупателя
                Order.order_data['buyer']['email'].as_string().ilike(search_term) |
                # Поиск по имени покупателя
                Order.order_data['buyer']['firstName'].as_string().ilike(search_term) |
                # Поиск по фамилии покупателя  
                Order.order_data['buyer']['lastName'].as_string().ilike(search_term) |
                # Поиск по логину покупателя
                Order.order_data['buyer']['login'].as_string().ilike(search_term) |
                # Поиск по названию компании
                Order.order_data['buyer']['companyName'].as_string().ilike(search_term)
            )
            
            if need_flags_join:
                # Строим запрос с JOIN по техническим флагам
                query = select(Order).join(
                    OrderTechnicalFlags,
                    Order.allegro_order_id == OrderTechnicalFlags.allegro_order_id
                ).where(
                    search_conditions,
                    Order.token_id == self.token_id,
                    OrderTechnicalFlags.token_id == self.token_id
                )
                
                # Применяем фильтры по техническим флагам
                if stock_updated_filter is not None:
                    query = query.where(OrderTechnicalFlags.is_stock_updated == stock_updated_filter)
                    
                if invoice_created_filter is not None:
                    query = query.where(OrderTechnicalFlags.has_invoice_created == invoice_created_filter)
                    
                if invoice_id_filter:
                    query = query.where(OrderTechnicalFlags.invoice_id == invoice_id_filter)
                    
            else:
                # Строим базовый запрос без JOIN
                query = select(Order).where(
                    search_conditions,
                    Order.token_id == self.token_id
                )
            
            # Добавляем сортировку (по дате заказа, самые свежие первыми)
            query = query.order_by(Order.order_date.desc())
            
            # Для поиска применяем лимит сразу, без пагинации
            orders = self.db.exec(query.limit(limit)).all()
            
            # Получаем технические флаги для найденных заказов
            order_ids = [order.allegro_order_id for order in orders]
            technical_flags = {}
            if order_ids:
                try:
                    with OrderTechnicalFlagsService(self.user_id, self.token_id) as flags_service:
                        technical_flags = flags_service.get_multiple_flags(order_ids)
                        
                        # Данные уже извлечены как обычные Python объекты
                                
                except Exception as e:
                    logger.warning(f"Не удалось получить технические флаги для поиска: {e}")
                    technical_flags = {}
            
            # Конвертируем результаты в стандартный формат
            orders_data = []
            for order in orders:
                order_dict = self._format_order_data(order, technical_flags.get(order.allegro_order_id))
                # Добавляем relevance score для поиска
                order_dict["relevance_score"] = self._calculate_relevance(order, search_query)
                orders_data.append(order_dict)
                
            # Сортируем по релевантности
            orders_data.sort(key=lambda x: x["relevance_score"], reverse=True)
            
            return {
                "success": True,
                "orders": orders_data,
                "pagination": {
                    "total": len(orders_data),
                    "limit": limit,
                    "offset": 0,
                    "has_next": False,
                    "has_prev": False
                },
                "filters": {
                    "search_query": search_query,
                    "search_type": "text_search",
                    "stock_updated": stock_updated_filter,
                    "invoice_created": invoice_created_filter,
                    "invoice_id": invoice_id_filter
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска заказов: {e}")
            return {
                "success": False,
                "error": str(e),
                "orders": [],
                "pagination": {"total": 0, "limit": limit, "offset": 0},
                "filters": {
                    "search_query": search_query, 
                    "search_type": "text_search",
                    "stock_updated": stock_updated_filter,
                    "invoice_created": invoice_created_filter,
                    "invoice_id": invoice_id_filter
                }
            }
            
    def get_orders_statistics(self, days: int = 30) -> Dict[str, Any]:
        """
        Получение статистики заказов за период.
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            Dict: Статистика заказов
        """
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Общая статистика
            total_orders = self.db.exec(select(func.count(Order.id))).one()
            recent_orders = self.db.exec(
                select(func.count(Order.id)).where(Order.created_at >= cutoff_date)
            ).one()
            
            # Статистика по статусам
            status_stats = {}
            status_query = select(Order.status, func.count(Order.id)).group_by(Order.status)
            status_results = self.db.exec(status_query).all()
            
            for status, count in status_results:
                status_stats[status] = count
                
            # Финансовая статистика
            revenue_query = select(
                func.sum(Order.total_price_amount),
                func.avg(Order.total_price_amount),
                func.count(Order.id)
            ).where(Order.created_at >= cutoff_date)
            
            revenue_result = self.db.exec(revenue_query).first()
            total_revenue = revenue_result[0] or 0
            avg_order_value = revenue_result[1] or 0
            revenue_orders_count = revenue_result[2] or 0
            
            # Топ покупатели
            top_buyers_query = select(
                Order.buyer_data["email"].as_string().label("email"),
                func.count(Order.id).label("orders_count"),
                func.sum(Order.total_price_amount).label("total_spent")
            ).where(
                Order.created_at >= cutoff_date
            ).group_by(
                Order.buyer_data["email"].as_string()
            ).order_by(
                func.count(Order.id).desc()
            ).limit(10)
            
            top_buyers = []
            for row in self.db.exec(top_buyers_query).all():
                if row.email:  # Пропускаем записи с NULL email
                    top_buyers.append({
                        "email": row.email,
                        "orders_count": row.orders_count,
                        "total_spent": float(row.total_spent or 0)
                    })
                    
            return {
                "success": True,
                "period_days": days,
                "total_orders": total_orders,
                "recent_orders": recent_orders,
                "status_distribution": status_stats,
                "financial": {
                    "total_revenue": float(total_revenue),
                    "average_order_value": float(avg_order_value),
                    "orders_with_revenue": revenue_orders_count
                },
                "top_buyers": top_buyers,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    def get_sync_history(self, limit: int = 50) -> Dict[str, Any]:
        """
        Получение истории синхронизации заказов.
        
        Args:
            limit: Количество записей
            
        Returns:
            Dict: История синхронизации
        """
        
        try:
            query = select(SyncHistory).where(
                SyncHistory.user_id == self.user_id
            ).order_by(SyncHistory.started_at.desc()).limit(limit)
            
            sync_records = self.db.exec(query).all()
            
            history = []
            for record in sync_records:
                history_item = {
                    "id": record.id,
                    "sync_type": record.sync_type,
                    "status": record.status,
                    "started_at": record.started_at.isoformat() if record.started_at else None,
                    "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                    "orders_processed": record.orders_processed or 0,
                    "orders_created": record.orders_created or 0,
                    "orders_updated": record.orders_updated or 0,
                    "orders_failed": record.orders_failed or 0,
                    "error_message": record.error_message,
                    "duration_seconds": None
                }
                
                # Рассчитываем длительность
                if record.started_at and record.completed_at:
                    duration = record.completed_at - record.started_at
                    history_item["duration_seconds"] = duration.total_seconds()
                    
                history.append(history_item)
                
            return {
                "success": True,
                "history": history,
                "total_records": len(history)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении истории синхронизации: {e}")
            return {
                "success": False,
                "error": str(e),
                "history": []
            }
            
    def _format_order_data(self, order: Order, technical_flags=None) -> Dict[str, Any]:
        """
        Форматирование данных заказа в единый формат для API.
        Возвращает полные данные заказа из order_data + технические флаги.
        
        Args:
            order: Объект заказа из БД
            technical_flags: Технические флаги заказа (опционально)
            
        Returns:
            Dict: Полные данные заказа с техническими флагами
        """
        # Формируем технические флаги с защитой от detached objects
        technical_data = {
            "is_stock_updated": False,
            "has_invoice_created": False,
            "invoice_id": None
        }
        
        if technical_flags:
            # Технические флаги уже в виде обычных Python данных
            technical_data = {
                "is_stock_updated": technical_flags.get("is_stock_updated", False),
                "has_invoice_created": technical_flags.get("has_invoice_created", False),
                "invoice_id": technical_flags.get("invoice_id")
            }
        
        # Берем полные данные заказа из order_data JSON и добавляем метаданные
        full_order_data = order.order_data.copy() if order.order_data else {}
        
        # Добавляем метаданные из БД
        full_order_data.update({
            "db_id": str(order.id),  # UUID из БД как строка
            "token_id": str(order.token_id),
            "allegro_order_id": order.allegro_order_id,
            "db_created_at": order.created_at.isoformat() if order.created_at else None,
            "db_updated_at": order.updated_at.isoformat() if order.updated_at else None,
            "technical_flags": technical_data
        })
        
        return full_order_data
    
    def _calculate_relevance(self, order: Order, search_query: str) -> float:
        """Расчет релевантности заказа для поискового запроса на основе JSON структуры"""
        
        relevance = 0.0
        query_lower = search_query.lower()
        
        # Получаем данные из JSON структуры
        order_data = order.order_data or {}
        buyer_data = order_data.get("buyer", {})
        
        # Точное совпадение ID заказа - максимальная релевантность
        order_id = order_data.get("id", "")
        if order_id and query_lower in order_id.lower():
            relevance += 10.0
            
        # Совпадения в данных покупателя
        email = buyer_data.get("email", "")
        if email and query_lower in email.lower():
            relevance += 5.0
            
        first_name = buyer_data.get("firstName", "")
        if first_name and query_lower in first_name.lower():
            relevance += 3.0
            
        last_name = buyer_data.get("lastName", "")
        if last_name and query_lower in last_name.lower():
            relevance += 3.0
            
        login = buyer_data.get("login", "")
        if login and query_lower in login.lower():
            relevance += 2.0
            
        company_name = buyer_data.get("companyName", "")
        if company_name and query_lower in company_name.lower():
            relevance += 2.0
            
        return relevance
        
    def get_data_quality_report(self) -> Dict[str, Any]:
        """
        Получение отчета о качестве данных заказов.
        
        Returns:
            Dict: Отчет о качестве данных
        """
        
        try:
            # Используем наш monitoring service
            health_metrics = self.monitoring_service.check_data_health(time_window_hours=24)
            quality_report = self.monitoring_service.generate_data_quality_report(days=7)
            
            return {
                "success": True,
                "health_metrics": {
                    "total_orders": health_metrics.total_orders,
                    "orders_with_issues": health_metrics.orders_with_issues,
                    "missing_data_ratio": health_metrics.data_regression_ratio,
                    "regression_ratio": health_metrics.data_regression_ratio,
                    "anomaly_score": health_metrics.anomaly_score,
                    "last_successful_sync": health_metrics.last_successful_sync.isoformat(),
                    "critical_issues": health_metrics.critical_issues
                },
                "quality_report": quality_report,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении отчета о качестве данных: {e}")
            return {
                "success": False,
                "error": str(e)
            } 