"""
@file: data_monitoring_service.py  
@description: Сервис мониторинга качества данных и раннего выявления проблем
@dependencies: OrderProtectionService, logging, metrics
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from sqlmodel import Session, select, func

from app.models.order import Order
from app.models.order_event import OrderEvent

logger = logging.getLogger(__name__)

@dataclass
class DataHealthMetrics:
    """Метрики здоровья данных"""
    total_orders: int
    orders_with_issues: int
    data_regression_ratio: float
    missing_data_ratio: float
    anomaly_score: float
    last_successful_sync: datetime
    critical_issues: List[str]

class DataMonitoringService:
    """
    Сервис мониторинга качества данных заказов.
    
    Отслеживает:
    - Качество входящих данных от Allegro
    - Аномалии в паттернах синхронизации  
    - Потенциальные потери данных
    - Производительность синхронизации
    """
    
    # Пороги для алертов
    CRITICAL_MISSING_DATA_RATIO = 0.20  # 20% заказов с неполными данными
    WARNING_MISSING_DATA_RATIO = 0.10   # 10% заказов с неполными данными
    CRITICAL_REGRESSION_RATIO = 0.15    # 15% заказов с деградацией
    MAX_SYNC_GAP_HOURS = 2             # Максимальный перерыв в синхронизации
    
    def __init__(self, db: Session):
        self.db = db
        
    def check_data_health(self, time_window_hours: int = 24) -> DataHealthMetrics:
        """
        Проверка общего здоровья данных за указанный период.
        
        Args:
            time_window_hours: Период для анализа в часах
            
        Returns:
            DataHealthMetrics: Детальные метрики состояния данных
        """
        
        cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
        
        # Получаем события синхронизации за период
        sync_events = self.db.exec(
            select(OrderEvent)
            .where(OrderEvent.occurred_at >= cutoff_time)
        ).all()
        
        # Анализируем качество данных
        total_orders = len(sync_events)
        orders_with_issues = 0
        regression_count = 0
        missing_data_count = 0
        critical_issues = []
        
        for event in sync_events:
            issues = self._analyze_event_data_quality(event)
            if issues["has_missing_data"]:
                missing_data_count += 1
            if issues["has_regression"]:
                regression_count += 1
            if issues["critical_issues"]:
                orders_with_issues += 1
                critical_issues.extend(issues["critical_issues"])
                
        # Расчет метрик
        missing_data_ratio = missing_data_count / total_orders if total_orders > 0 else 0
        regression_ratio = regression_count / total_orders if total_orders > 0 else 0
        
        # Аномальный скор (0-1, где 1 = критично)
        anomaly_score = self._calculate_anomaly_score(
            missing_data_ratio, regression_ratio, total_orders
        )
        
        # Последняя успешная синхронизация
        last_sync = self._get_last_successful_sync()
        
        metrics = DataHealthMetrics(
            total_orders=total_orders,
            orders_with_issues=orders_with_issues,
            data_regression_ratio=regression_ratio,
            missing_data_ratio=missing_data_ratio,
            anomaly_score=anomaly_score,
            last_successful_sync=last_sync,
            critical_issues=list(set(critical_issues))  # Убираем дубликаты
        )
        
        # Логируем результаты
        self._log_health_metrics(metrics)
        
        # Отправляем алерты при необходимости
        self._send_alerts_if_needed(metrics)
        
        return metrics
        
    def _analyze_event_data_quality(self, event: OrderEvent) -> Dict[str, Any]:
        """Анализ качества данных в конкретном событии"""
        
        event_data = event.event_data or {}
        
        # Проверка на отсутствующие данные
        required_fields = ["id", "status", "buyer", "lineItems"]
        missing_fields = [field for field in required_fields if not event_data.get(field)]
        
        # Проверка на пустые критические поля
        buyer_data = event_data.get("buyer", {})
        empty_buyer_fields = []
        for field in ["email", "firstName"]:
            if not buyer_data.get(field):
                empty_buyer_fields.append(field)
                
        line_items = event_data.get("lineItems", [])
        
        # Определяем проблемы
        has_missing_data = bool(missing_fields or empty_buyer_fields)
        has_regression = len(line_items) == 0  # Simplified check
        
        critical_issues = []
        if missing_fields:
            critical_issues.append(f"Отсутствуют поля: {missing_fields}")
        if empty_buyer_fields:
            critical_issues.append(f"Пустые поля покупателя: {empty_buyer_fields}")
        if not line_items:
            critical_issues.append("Отсутствуют товары в заказе")
            
        return {
            "has_missing_data": has_missing_data,
            "has_regression": has_regression,
            "critical_issues": critical_issues,
            "missing_fields": missing_fields,
            "empty_buyer_fields": empty_buyer_fields
        }
        
    def _calculate_anomaly_score(self, missing_ratio: float, 
                                regression_ratio: float, total_orders: int) -> float:
        """
        Расчет общего скора аномальности данных.
        
        Факторы:
        - Доля заказов с неполными данными
        - Доля заказов с деградацией
        - Общее количество заказов (слишком мало = подозрительно)
        """
        
        score = 0.0
        
        # Штраф за неполные данные
        if missing_ratio >= self.CRITICAL_MISSING_DATA_RATIO:
            score += 0.4
        elif missing_ratio >= self.WARNING_MISSING_DATA_RATIO:
            score += 0.2
            
        # Штраф за деградацию данных
        if regression_ratio >= self.CRITICAL_REGRESSION_RATIO:
            score += 0.4
        elif regression_ratio >= 0.05:  # 5% порог для предупреждения
            score += 0.2
            
        # Штраф за подозрительно малое количество заказов
        expected_min_orders = 10  # Минимальное ожидаемое количество за 24 часа
        if total_orders < expected_min_orders:
            score += 0.3
            
        return min(score, 1.0)  # Максимум 1.0
        
    def _get_last_successful_sync(self) -> datetime:
        """Получение времени последней успешной синхронизации"""
        
        last_event = self.db.exec(
            select(OrderEvent)
            .where(OrderEvent.event_type == "ORDER_SYNC")
            .order_by(OrderEvent.occurred_at.desc())
            .limit(1)
        ).first()
        
        if last_event:
            return last_event.occurred_at
        else:
            # Если событий нет, возвращаем очень старую дату
            return datetime.utcnow() - timedelta(days=365)
            
    def _log_health_metrics(self, metrics: DataHealthMetrics):
        """Логирование метрик здоровья данных"""
        
        if metrics.anomaly_score >= 0.7:
            log_level = logging.CRITICAL
            emoji = "🚨"
        elif metrics.anomaly_score >= 0.4:
            log_level = logging.WARNING  
            emoji = "⚠️"
        else:
            log_level = logging.INFO
            emoji = "✅"
            
        logger.log(log_level, 
            f"{emoji} Метрики здоровья данных:\n"
            f"  Всего заказов: {metrics.total_orders}\n"
            f"  С проблемами: {metrics.orders_with_issues}\n"
            f"  Неполные данные: {metrics.missing_data_ratio:.1%}\n"
            f"  Деградация данных: {metrics.data_regression_ratio:.1%}\n"
            f"  Скор аномальности: {metrics.anomaly_score:.2f}\n"
            f"  Последняя синхронизация: {metrics.last_successful_sync}\n"
            f"  Критические проблемы: {len(metrics.critical_issues)}"
        )
        
    def _send_alerts_if_needed(self, metrics: DataHealthMetrics):
        """Отправка алертов при критических проблемах"""
        
        alerts = []
        
        # Критическая доля неполных данных
        if metrics.missing_data_ratio >= self.CRITICAL_MISSING_DATA_RATIO:
            alerts.append(
                f"🚨 КРИТИЧНО: {metrics.missing_data_ratio:.1%} заказов имеют неполные данные!"
            )
            
        # Критическая деградация данных
        if metrics.data_regression_ratio >= self.CRITICAL_REGRESSION_RATIO:
            alerts.append(
                f"🚨 КРИТИЧНО: {metrics.data_regression_ratio:.1%} заказов показывают деградацию данных!"
            )
            
        # Долгий перерыв в синхронизации
        sync_gap = datetime.utcnow() - metrics.last_successful_sync
        if sync_gap.total_seconds() > self.MAX_SYNC_GAP_HOURS * 3600:
            alerts.append(
                f"🚨 КРИТИЧНО: Синхронизация не работает уже {sync_gap.total_seconds()/3600:.1f} часов!"
            )
            
        # Высокий общий скор аномальности
        if metrics.anomaly_score >= 0.8:
            alerts.append(
                f"🚨 КРИТИЧНО: Очень высокий скор аномальности данных: {metrics.anomaly_score:.2f}"
            )
            
        # Отправляем алерты
        for alert in alerts:
            logger.critical(alert)
            # Здесь можно добавить отправку в Slack, email, SMS и т.д.
            
    def should_pause_sync(self) -> bool:
        """
        Определяет, нужно ли приостановить синхронизацию из-за проблем с данными.
        
        Circuit Breaker Pattern для защиты от массовой порчи данных.
        
        Returns:
            bool: True если синхронизацию нужно остановить
        """
        
        metrics = self.check_data_health(time_window_hours=1)  # Проверяем последний час
        
        # Критерии для остановки синхронизации
        should_pause = (
            metrics.anomaly_score >= 0.9 or  # Очень высокий скор аномальности
            metrics.missing_data_ratio >= 0.5 or  # Более 50% заказов с проблемами
            len(metrics.critical_issues) >= 10  # Много критических проблем
        )
        
        if should_pause:
            logger.critical(
                f"🛑 ОСТАНОВКА СИНХРОНИЗАЦИИ! "
                f"Критические проблемы с данными: "
                f"anomaly_score={metrics.anomaly_score:.2f}, "
                f"missing_data_ratio={metrics.missing_data_ratio:.1%}"
            )
            
        return should_pause
        
    def generate_data_quality_report(self, days: int = 7) -> Dict[str, Any]:
        """
        Генерация подробного отчета о качестве данных за период.
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            Dict: Подробный отчет с метриками и рекомендациями
        """
        
        cutoff_time = datetime.utcnow() - timedelta(days=days)
        
        # Собираем статистику по дням
        daily_metrics = []
        for day in range(days):
            day_start = cutoff_time + timedelta(days=day)
            day_end = day_start + timedelta(days=1)
            
            day_events = self.db.exec(
                select(OrderEvent)
                .where(OrderEvent.occurred_at >= day_start)
                .where(OrderEvent.occurred_at < day_end)
                .where(OrderEvent.event_type == "ORDER_SYNC")
            ).all()
            
            if day_events:
                day_metrics = self._analyze_daily_metrics(day_events, day_start.date())
                daily_metrics.append(day_metrics)
                
        # Общая статистика
        total_events = self.db.exec(
            select(func.count(OrderEvent.id))
            .where(OrderEvent.occurred_at >= cutoff_time)
            .where(OrderEvent.event_type == "ORDER_SYNC")
        ).one()
        
        # Топ проблем
        top_issues = self._get_top_data_issues(cutoff_time)
        
        return {
            "period": f"{days} days",
            "start_date": cutoff_time.date().isoformat(),
            "end_date": datetime.utcnow().date().isoformat(),
            "total_sync_events": total_events,
            "daily_metrics": daily_metrics,
            "top_issues": top_issues,
            "recommendations": self._generate_recommendations(daily_metrics, top_issues)
        }
        
    def _analyze_daily_metrics(self, events: List[OrderEvent], date) -> Dict[str, Any]:
        """Анализ метрик за один день"""
        
        total_orders = len(events)
        problematic_orders = 0
        missing_data_orders = 0
        
        for event in events:
            analysis = self._analyze_event_data_quality(event)
            if analysis["critical_issues"]:
                problematic_orders += 1
            if analysis["has_missing_data"]:
                missing_data_orders += 1
                
        return {
            "date": date.isoformat(),
            "total_orders": total_orders,
            "problematic_orders": problematic_orders,
            "missing_data_orders": missing_data_orders,
            "health_score": 1.0 - (problematic_orders / total_orders) if total_orders > 0 else 1.0
        }
        
    def _get_top_data_issues(self, since: datetime) -> List[Dict[str, Any]]:
        """Получение топа самых частых проблем с данными"""
        
        # Заглушка - здесь можно добавить более сложную аналитику
        return [
            {"issue": "Missing buyer email", "count": 45, "severity": "medium"},
            {"issue": "Empty line items", "count": 12, "severity": "high"}, 
            {"issue": "Missing delivery data", "count": 23, "severity": "low"}
        ]
        
    def _generate_recommendations(self, daily_metrics: List[Dict], 
                                 top_issues: List[Dict]) -> List[str]:
        """Генерация рекомендаций по улучшению качества данных"""
        
        recommendations = []
        
        # Анализируем тренды
        if len(daily_metrics) >= 2:
            recent_health = daily_metrics[-1]["health_score"]
            previous_health = daily_metrics[-2]["health_score"]
            
            if recent_health < previous_health - 0.1:
                recommendations.append(
                    "❗ Качество данных ухудшается. Проверьте изменения в Allegro API."
                )
                
        # Рекомендации на основе частых проблем
        if any(issue["severity"] == "high" for issue in top_issues):
            recommendations.append(
                "🔧 Обнаружены критические проблемы с данными. Рассмотрите добавление дополнительной валидации."
            )
            
        if not recommendations:
            recommendations.append("✅ Качество данных в пределах нормы.")
            
        return recommendations 
    
    def detect_data_anomalies(self, order_events: List[Dict[str, Any]]) -> List[str]:
        """
        Обнаружение аномалий в списке событий заказов.
        
        Анализирует:
        - Структуру событий и данных заказов
        - Частоту появления различных типов событий
        - Качество данных в событиях
        - Паттерны, указывающие на проблемы с API
        
        Args:
            order_events: Список событий заказов от Allegro API
            
        Returns:
            List[str]: Список обнаруженных аномалий
        """
        
        anomalies = []
        
        if not order_events:
            return ["⚠️ Получен пустой список событий заказов"]
            
        total_events = len(order_events)
        events_with_missing_data = 0
        events_with_malformed_data = 0
        
        # Разделяем статистику по источникам данных
        events_api_stats = {"event_ids": set(), "order_ids": set(), "count": 0}
        checkout_forms_stats = {"order_ids": {}, "count": 0}
        other_stats = {"count": 0}
        
        logger.info(f"🔍 Анализ аномалий в {total_events} событиях заказов...")
        
        for event in order_events:
            source = event.get('source', "unknown")
            logger.debug(f"🔍 Анализ события из источника: {source}")
            
            # Проверяем базовую структуру события
            if not self._validate_event_structure(event, source):
                logger.debug(f"❌ Событие не прошло проверку структуры: {source}")
                events_with_malformed_data += 1
                continue
                
            # Извлекаем order_id безопасно
            order_id = self._extract_order_id_safe(event, source)
            if not order_id:
                logger.debug(f"❌ Не удалось извлечь order_id из события: {source}")
                events_with_missing_data += 1
                continue
                
            # Собираем статистику по источникам данных
            if source == 'events_api':
                events_api_stats["count"] += 1
                events_api_stats["order_ids"].add(order_id)
                # Для Events API собираем event_id для проверки дубликатов
                event_info = event.get('event', {})
                event_id = event_info.get('id')
                if event_id:
                    events_api_stats["event_ids"].add(event_id)
            elif source in ['checkout_forms_api', 'full_api_details']:
                checkout_forms_stats["count"] += 1
                checkout_forms_stats["order_ids"][order_id] = checkout_forms_stats["order_ids"].get(order_id, 0) + 1
            else:
                other_stats["count"] += 1
            
            # Проверяем качество данных заказа
            if not self._validate_order_data_quality(event, source):
                logger.debug(f"❌ Событие {order_id} не прошло проверку качества данных: {source}")
                events_with_missing_data += 1
            else:
                logger.debug(f"✅ Событие {order_id} прошло все проверки: {source}")
                
        # Анализ дубликатов ТОЛЬКО для Events API (по event_id)
        if events_api_stats["count"] > 0:
            total_events_api = events_api_stats["count"]
            unique_event_ids = len(events_api_stats["event_ids"])
            if unique_event_ids < total_events_api:
                duplicate_events = total_events_api - unique_event_ids
                anomalies.append(f"⚠️ Обнаружено {duplicate_events} дублированных событий (по event_id) из {total_events_api}")

        # Анализ дубликатов для Checkout Forms API (по order_id - здесь это действительно дубликаты)
        checkout_duplicates = {oid: count for oid, count in checkout_forms_stats["order_ids"].items() if count > 1}
        if checkout_duplicates:
            duplicate_count = sum(checkout_duplicates.values()) - len(checkout_duplicates)
            anomalies.append(f"⚠️ Обнаружено {duplicate_count} дублированных заказов в Checkout Forms API")
        
        # Анализ качества данных
        missing_data_ratio = events_with_missing_data / total_events
        if missing_data_ratio > self.WARNING_MISSING_DATA_RATIO:
            anomalies.append(f"⚠️ {missing_data_ratio:.1%} событий имеют неполные данные")
            
        if missing_data_ratio > self.CRITICAL_MISSING_DATA_RATIO:
            anomalies.append(f"❌ КРИТИЧНО: {missing_data_ratio:.1%} событий с серьезными проблемами данных")
        
        # Анализ структуры событий
        malformed_ratio = events_with_malformed_data / total_events
        if malformed_ratio > 0.05:  # Более 5% событий с проблемами структуры
            anomalies.append(f"❌ {malformed_ratio:.1%} событий имеют некорректную структуру")
            
        # Проверка разнообразия ТОЛЬКО для Checkout Forms API (для Events API это нормально)
        if checkout_forms_stats["count"] > 0:
            unique_checkout_orders = len(checkout_forms_stats["order_ids"])
            checkout_total = checkout_forms_stats["count"]
            if unique_checkout_orders < checkout_total * 0.5:  # Слишком много повторов в заказах
                anomalies.append(f"⚠️ Низкое разнообразие в Checkout Forms API: {unique_checkout_orders} уникальных из {checkout_total} заказов")
            
        if not anomalies:
            logger.info("✅ Аномалий в данных событий не обнаружено")
        else:
            logger.warning(f"⚠️ Обнаружено {len(anomalies)} аномалий в данных")
            
        return anomalies
    
    def _validate_event_structure(self, event: Dict[str, Any], source: str) -> bool:
        """Проверка базовой структуры события"""
        try:
            logger.debug(f"🔍 Проверка структуры события из источника: {source}")
            
            if source == 'events_api':
                # Для Events API требуем поля event с order внутри
                required_root_fields = ['event', 'order_id', 'source']
                if not all(field in event for field in required_root_fields):
                    logger.debug(f"❌ Отсутствуют корневые поля для events_api: {required_root_fields}")
                    return False
                    
                event_info = event.get('event', {})
                required_event_fields = ['id', 'type']
                if not all(field in event_info for field in required_event_fields):
                    logger.debug(f"❌ Отсутствуют поля события для events_api: {required_event_fields}")
                    return False
                    
                return True
                
            elif source in ['checkout_forms_api', 'full_api_details']:
                # Для Checkout Forms API и Full API Details требуем поля order с данными заказа внутри
                # full_api_details использует ту же структуру что и checkout_forms_api
                required_root_fields = ['order', 'order_id', 'source']
                if not all(field in event for field in required_root_fields):
                    logger.debug(f"❌ Отсутствуют корневые поля для {source}: {required_root_fields}")
                    return False
                    
                order_data = event.get('order', {})
                if not order_data:
                    logger.debug("❌ Пустые данные заказа")
                    return False
                    
                # Проверяем ключевые поля заказа
                required_order_fields = ['id', 'status', 'buyer', 'lineItems', 'delivery', 'fulfillment']
                missing_fields = [field for field in required_order_fields if not order_data.get(field)]
                
                if missing_fields:
                    logger.debug(f"❌ Отсутствуют поля заказа: {missing_fields}")
                    return False
                    
                return True
                
            else:
                logger.error(f"❌ Неизвестный источник данных: {source}")
                return False
                
        except (AttributeError, KeyError, TypeError) as e:
            logger.debug(f"❌ Ошибка валидации структуры события: {e}")
            return False
    
    def _extract_order_id_safe(self, event: Dict[str, Any], source: str) -> Optional[str]:
        """Безопасное извлечение order_id из события"""
        try:
            if source == 'events_api':
                # Для Events API order_id уже извлечен и находится в корне
                order_id = event.get('order_id')
                logger.debug(f"🔍 Извлечен order_id из events_api: {order_id}")
                return order_id
                
            elif source in ['checkout_forms_api', 'full_api_details']:
                # Для Checkout Forms API и Full API Details order_id уже извлечен и находится в корне
                order_id = event.get('order_id')
                logger.debug(f"🔍 Извлечен order_id из {source}: {order_id}")
                return order_id
                
            else:
                logger.error(f"❌ Неизвестный источник данных: {source}")
                return None
            
        except (AttributeError, KeyError, TypeError) as e:
            logger.debug(f"❌ Ошибка извлечения order_id: {e}")
            return None
    
    def _validate_order_data_quality(self, event: Dict[str, Any], source: str) -> bool:
        """Проверка качества данных заказа в событии"""
        try:
            if source in ['events_api', 'checkout_forms_api', 'full_api_details']:
                order_data = event.get('order', {})
            else:
                logger.error(f"❌ Неизвестный источник данных: {source}")
                return False
            
            # Базовые поля, которые должны присутствовать
            if not order_data:
                logger.debug("❌ Отсутствуют данные заказа")
                return False
                
            # Проверка качества данных в зависимости от источника
            if source == 'events_api':
                # Для Events API проверяем специфичную структуру
                has_checkout_form = bool(order_data.get('checkoutForm'))
                has_checkout_id = bool(order_data.get('checkoutForm', {}).get('id')) if has_checkout_form else False
                has_buyer = bool(order_data.get('buyer'))
                has_line_items = bool(order_data.get('lineItems'))
                
                quality_check = has_checkout_form and has_checkout_id and has_buyer and has_line_items
                
                if not quality_check:
                    missing_parts = []
                    if not has_checkout_form: missing_parts.append("checkoutForm")
                    if not has_checkout_id: missing_parts.append("checkoutForm.id")
                    if not has_buyer: missing_parts.append("buyer")
                    if not has_line_items: missing_parts.append("lineItems")
                    logger.debug(f"❌ Неполные данные заказа Events API, отсутствуют: {missing_parts}")
                    
            else:
                # Для Checkout Forms API и Full API Details проверяем полную структуру
                has_id = bool(order_data.get('id'))
                has_status = bool(order_data.get('status'))
                has_buyer = bool(order_data.get('buyer'))
                has_line_items = bool(order_data.get('lineItems'))
                
                quality_check = has_id and has_status and has_buyer and has_line_items
                
                if not quality_check:
                    missing_parts = []
                    if not has_id: missing_parts.append("id")
                    if not has_status: missing_parts.append("status")
                    if not has_buyer: missing_parts.append("buyer")
                    if not has_line_items: missing_parts.append("lineItems")
                    logger.debug(f"❌ Неполные данные заказа {source}, отсутствуют: {missing_parts}")
            
            return quality_check
            
        except (AttributeError, KeyError, TypeError) as e:
            logger.debug(f"❌ Ошибка валидации качества данных: {e}")
            return False 