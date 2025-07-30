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
            .where(OrderEvent.event_type == "ORDER_SYNC")
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
        duplicate_order_ids = set()
        order_id_counts = {}
        
        logger.info(f"🔍 Анализ аномалий в {total_events} событиях заказов...")
        
        for event in order_events:
            # Проверяем базовую структуру события
            if not self._validate_event_structure(event):
                events_with_malformed_data += 1
                continue
                
            # Извлекаем order_id безопасно
            order_id = self._extract_order_id_safe(event)
            if not order_id:
                events_with_missing_data += 1
                continue
                
            # Подсчитываем события для каждого заказа
            order_id_counts[order_id] = order_id_counts.get(order_id, 0) + 1
            
            # Проверяем качество данных заказа
            if not self._validate_order_data_quality(event):
                events_with_missing_data += 1
                
        # Анализ дубликатов
        duplicates = {oid: count for oid, count in order_id_counts.items() if count > 1}
        if duplicates:
            duplicate_count = sum(duplicates.values()) - len(duplicates)
            anomalies.append(f"⚠️ Обнаружено {duplicate_count} дублированных событий для {len(duplicates)} заказов")
        
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
            
        # Проверка разнообразия событий
        unique_orders = len(order_id_counts)
        if unique_orders < total_events * 0.5:  # Слишком много повторов
            anomalies.append(f"⚠️ Низкое разнообразие заказов: {unique_orders} уникальных из {total_events} событий")
            
        if not anomalies:
            logger.info("✅ Аномалий в данных событий не обнаружено")
        else:
            logger.warning(f"⚠️ Обнаружено {len(anomalies)} аномалий в данных")
            
        return anomalies
    
    def _validate_event_structure(self, event: Dict[str, Any]) -> bool:
        """Проверка базовой структуры события"""
        required_fields = ['id', 'type', 'event']
        return all(field in event for field in required_fields)
    
    def _extract_order_id_safe(self, event: Dict[str, Any]) -> Optional[str]:
        """Безопасное извлечение order_id из события"""
        try:
            # Пробуем разные пути извлечения order_id
            order_data = event.get('order', {})
            
            # Сначала пробуем checkoutForm.id (правильный путь для Allegro)
            if 'checkoutForm' in order_data and 'id' in order_data['checkoutForm']:
                return order_data['checkoutForm']['id']
                
            # Затем пробуем прямо order.id (старый путь)
            if 'id' in order_data:
                return order_data['id']
                
            # Если ничего не найдено
            return None
            
        except (AttributeError, KeyError, TypeError):
            return None
    
    def _validate_order_data_quality(self, event: Dict[str, Any]) -> bool:
        """Проверка качества данных заказа в событии"""
        try:
            order_data = event.get('order', {})
            
            # Базовые поля, которые должны присутствовать
            if not order_data:
                return False
                
            # Проверяем наличие ключевых полей
            checkout_form = order_data.get('checkoutForm', {})
            if not checkout_form:
                return False
                
            # Минимальные данные для валидного заказа
            has_id = bool(checkout_form.get('id'))
            has_status = bool(checkout_form.get('status'))
            has_buyer = bool(checkout_form.get('buyer'))
            
            return has_id and has_status and has_buyer
            
        except (AttributeError, KeyError, TypeError):
            return False 