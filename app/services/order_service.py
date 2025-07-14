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
from app.services.order_protection_service import OrderProtectionService, DataIntegrityError
from app.services.data_monitoring_service import DataMonitoringService
from app.services.allegro_auth_service import AllegroAuthService
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
            
            logger.debug(f"📋 Запрос деталей заказа: {order_id}")
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers, timeout=15.0)
                
                if response.status_code == 404:
                    result["error"] = f"Заказ {order_id} не найден"
                    return result
                    
                response.raise_for_status()
                order_data = response.json()
                
                result.update({
                    "success": True,
                    "order": order_data
                })
                
                logger.debug(f"✅ Получены детали заказа {order_id}")
                
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
                       to_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Получение списка заказов из локальной БД с фильтрацией.
        
        Args:
            limit: Количество заказов на страницу
            offset: Смещение для пагинации
            status_filter: Фильтр по статусу заказа
            from_date: Заказы от указанной даты
            to_date: Заказы до указанной даты
            
        Returns:
            Dict: Список заказов с метаданными
        """
        
        try:
            # Строим базовый запрос
            query = select(Order)
            
            # Применяем фильтры
            if status_filter:
                query = query.where(Order.status == status_filter)
                
            if from_date:
                query = query.where(Order.created_at >= from_date)
                
            if to_date:
                query = query.where(Order.created_at <= to_date)
                
            # Добавляем сортировку и пагинацию
            query = query.order_by(Order.created_at.desc()).offset(offset).limit(limit)
            
            # Выполняем запрос
            orders = self.db.exec(query).all()
            
            # Получаем общее количество (для пагинации)
            count_query = select(func.count(Order.id))
            if status_filter:
                count_query = count_query.where(Order.status == status_filter)
            if from_date:
                count_query = count_query.where(Order.created_at >= from_date)
            if to_date:
                count_query = count_query.where(Order.created_at <= to_date)
                
            total_count = self.db.exec(count_query).one()
            
            # Конвертируем в dict для API
            orders_data = []
            for order in orders:
                order_dict = {
                    "id": order.id,
                    "order_id": order.order_id,
                    "status": order.status,
                    "buyer_data": order.buyer_data,
                    "total_price_amount": order.total_price_amount,
                    "total_price_currency": order.total_price_currency,
                    "line_items_count": len(order.line_items or []),
                    "created_at": order.created_at.isoformat() if order.created_at else None,
                    "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                    "allegro_revision": order.order_data.get("revision")
                }
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
                    "to_date": to_date.isoformat() if to_date else None
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
            
    def search_orders(self, search_query: str, limit: int = 50) -> Dict[str, Any]:
        """
        Поиск заказов по различным критериям.
        
        Args:
            search_query: Поисковый запрос (email покупателя, ID заказа, имя)
            limit: Максимальное количество результатов
            
        Returns:
            Dict: Результаты поиска
        """
        
        try:
            search_term = f"%{search_query.lower()}%"
            
            # Ищем по различным полям
            query = select(Order).where(
                Order.order_id.ilike(search_term) |  # По ID заказа
                Order.buyer_data["email"].as_string().ilike(search_term) |  # По email
                Order.buyer_data["firstName"].as_string().ilike(search_term) |  # По имени
                Order.buyer_data["lastName"].as_string().ilike(search_term)  # По фамилии
            ).order_by(Order.created_at.desc()).limit(limit)
            
            orders = self.db.exec(query).all()
            
            # Конвертируем результаты
            results = []
            for order in orders:
                buyer = order.buyer_data or {}
                result = {
                    "id": order.id,
                    "order_id": order.order_id,
                    "status": order.status,
                    "buyer_email": buyer.get("email"),
                    "buyer_name": f"{buyer.get('firstName', '')} {buyer.get('lastName', '')}".strip(),
                    "total_amount": order.total_price_amount,
                    "currency": order.total_price_currency,
                    "created_at": order.created_at.isoformat() if order.created_at else None,
                    "relevance_score": self._calculate_relevance(order, search_query)
                }
                results.append(result)
                
            # Сортируем по релевантности
            results.sort(key=lambda x: x["relevance_score"], reverse=True)
            
            return {
                "success": True,
                "query": search_query,
                "results": results,
                "total_found": len(results)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска заказов: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": search_query,
                "results": []
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
            
    def _calculate_relevance(self, order: Order, search_query: str) -> float:
        """Расчет релевантности заказа для поискового запроса"""
        
        relevance = 0.0
        query_lower = search_query.lower()
        
        # Точное совпадение ID заказа - максимальная релевантность
        if order.order_id and query_lower in order.order_id.lower():
            relevance += 10.0
            
        # Совпадения в данных покупателя
        buyer = order.buyer_data or {}
        
        if buyer.get("email") and query_lower in buyer["email"].lower():
            relevance += 5.0
            
        if buyer.get("firstName") and query_lower in buyer["firstName"].lower():
            relevance += 3.0
            
        if buyer.get("lastName") and query_lower in buyer["lastName"].lower():
            relevance += 3.0
            
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