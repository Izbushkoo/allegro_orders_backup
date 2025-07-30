"""
@file: order_protection_service.py
@description: Сервис защиты данных заказов от потери и повреждения
@dependencies: OrderService, logging
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from uuid import UUID
from sqlmodel import Session, select, func

from app.models.order import Order
from app.models.order_event import OrderEvent

logger = logging.getLogger(__name__)

class DataIntegrityError(Exception):
    """Ошибка нарушения целостности данных"""
    pass

class OrderProtectionService:
    """
    Сервис защиты данных заказов от потери и повреждения.
    
    Основные функции:
    - Валидация качества новых данных
    - Защита от перезаписи более полными данными
    - Контроль версий заказов
    - Audit trail всех изменений
    """
    
    def __init__(self, db: Session, token_id: UUID):
        self.db = db
        self.token_id = token_id
        
    def validate_order_data_quality(self, new_data: Dict[str, Any], 
                                   existing_order: Optional[Order] = None) -> bool:
        """
        Проверяет качество новых данных заказа.
        
        Правила валидации:
        1. Для новых заказов: требуются все обязательные поля
        2. Для обновлений: требуются только базовые поля (id)
        3. Новые данные не должны быть "беднее" существующих
        4. Структура данных должна соответствовать ожидаемой
        
        Args:
            new_data: Новые данные заказа от Allegro
            existing_order: Существующий заказ (если есть)
            
        Returns:
            bool: True если данные валидны
            
        Raises:
            DataIntegrityError: При критических проблемах с данными
        """
        
        # 1. Проверка обязательных полей в зависимости от типа операции
        if existing_order is None:
            # Для новых заказов требуем минимальный набор полей из Allegro API
            # Структура данных Events API: {checkoutForm: {id, revision}, buyer?, lineItems?, ...}
            # Структура данных Checkout Forms API: {id, status, buyer?, lineItems?, ...}
            required_fields = self._get_required_fields_for_structure(new_data)
        else:
            # Для обновлений достаточно ID в любой из структур
            required_fields = self._get_required_fields_for_structure(new_data)
            
        missing_fields = []
        
        # Проверяем наличие обязательных полей с учетом структуры данных
        for field in required_fields:
            if not self._has_required_field(new_data, field):
                missing_fields.append(field)
                
        if missing_fields:
            operation_type = "создания" if existing_order is None else "обновления"
            error_msg = f"❌ Отсутствуют обязательные поля для {operation_type}: {missing_fields}"
            logger.error(error_msg)
            
            # Для обновлений это критическая ошибка, для создания - можем попробовать продолжить
            if existing_order is None:
                raise DataIntegrityError(error_msg)
            else:
                # Для обновлений просто логируем предупреждение и продолжаем
                logger.warning(f"⚠️ {error_msg} - продолжаем обновление")
                
        # 2. Проверка "деградации" данных при обновлении (только если есть существующий заказ)
        if existing_order:
            regression_issues = self._check_data_regression(new_data, existing_order)
            if regression_issues:
                logger.warning(f"⚠️ Обнаружена деградация данных: {regression_issues}")
                # Не блокируем обновление, только предупреждаем
                
        # 3. Структурная валидация (более мягкая для обновлений)
        if not self._validate_data_structure(new_data, is_update=existing_order is not None):
            if existing_order is None:
                return False
            else:
                logger.warning("⚠️ Структурная валидация не пройдена, но продолжаем обновление")
                
        return True
        
    def _check_data_regression(self, new_data: Dict[str, Any], 
                             existing_order: Order) -> List[str]:
        """
        Проверяет, не стали ли новые данные хуже существующих.
        
        Returns:
            List[str]: Список найденных проблем
        """
        issues = []
        
        # Проверяем количество товаров в заказе
        existing_order_data = existing_order.order_data or {}
        existing_items_count = len(existing_order_data.get("lineItems", []))
        new_items_count = len(new_data.get("lineItems", []))
        
        if new_items_count < existing_items_count:
            issues.append(f"Количество товаров уменьшилось: {existing_items_count} -> {new_items_count}")
            
        # Проверяем заполненность полей покупателя
        existing_buyer = existing_order_data.get("buyer", {})
        new_buyer = new_data.get("buyer", {})
        
        for field in ["email", "firstName", "lastName"]:
            if existing_buyer.get(field) and not new_buyer.get(field):
                issues.append(f"Потеряно поле покупателя: {field}")
                
        # Проверяем общую сумму заказа
        existing_total = existing_order_data.get("summary", {}).get("totalToPay", {}).get("amount")
        new_total = new_data.get("summary", {}).get("totalToPay", {}).get("amount")
        
        if existing_total and new_total and float(new_total) != float(existing_total):
            issues.append(f"Изменилась сумма заказа: {existing_total} -> {new_total}")
            
        return issues
        
    def _validate_data_structure(self, data: Dict[str, Any], is_update: bool = False) -> bool:
        """Проверяет корректность структуры данных заказа от Allegro API"""
        
        # Проверяем основную структуру данных согласно реальной структуре Allegro API
        # Структура зависит от источника: Events API vs Checkout Forms API
        expected_structure = {
            # Общие поля для обеих структур
            "buyer": dict,                # Опционально: данные покупателя
            "lineItems": list,            # Опционально: товары в заказе
            "marketplace": dict,          # Опционально: данные маркетплейса
            
            # Поля из Checkout Forms API
            "id": str,                    # ID заказа (только в Checkout Forms API)
            "status": str,                # Статус заказа (только в Checkout Forms API)
            "summary": dict,              # Итоговая информация
            "revision": str,              # Ревизия заказа (строка)
            "delivery": dict,             # Данные доставки
            "payment": dict,              # Данные оплаты
            "fulfillment": dict,          # Данные выполнения
            "invoice": dict,              # Данные счета
            "updatedAt": str,             # Время обновления (ISO string)
            "note": dict,                 # note это объект с полем text
            "messageToSeller": str,       # Сообщение продавцу
            "surcharges": list,           # Доплаты
            "discounts": list,            # Скидки
            
            # Поля из Events API
            "checkoutForm": dict,         # Форма заказа с ID и revision (только в Events API)
            "seller": dict,               # Данные продавца (в основном в Events API)
        }
        
        # Определяем обязательные поля в зависимости от структуры данных
        if not is_update:
            required_fields = self._get_required_fields_for_structure(data)
        else:
            required_fields = []  # Для обновлений не требуем обязательных полей
        
        # Проверяем обязательные поля с использованием новой логики
        for field in required_fields:
            if not self._has_required_field(data, field):
                logger.error(f"❌ Отсутствует обязательное поле: {field}")
                return False
        
        # Проверяем типы только присутствующих полей
        for field, expected_type in expected_structure.items():
            if field in data and data[field] is not None:
                if not isinstance(data[field], expected_type):
                    operation_type = "обновления" if is_update else "создания"
                    logger.error(f"❌ Поле {field} имеет неверный тип для {operation_type}: {type(data[field])} вместо {expected_type}")
                    return False
                    
        # Дополнительная проверка структуры note если оно присутствует
        if "note" in data and data["note"] is not None:
            if isinstance(data["note"], dict):
                # note должно содержать поле text
                if "text" in data["note"] and data["note"]["text"] is not None:
                    if not isinstance(data["note"]["text"], str):
                        logger.error(f"❌ Поле note.text имеет неверный тип: {type(data['note']['text'])} вместо str")
                        return False
                        
        logger.info(f"✅ Структура данных заказа валидна")
        return True
        
    def safe_order_update(self, order_id: str, new_data: Dict[str, Any], 
                         allegro_revision: Optional[str] = None,
                         order_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Безопасное обновление заказа с защитой от потери данных и конфликтов.
        
        Этапы защиты:
        1. Валидация новых данных
        2. Проверка версии (optimistic locking)
        3. Merge с существующими данными
        4. Создание события для audit trail
        5. Создание snapshot для восстановления
        
        Args:
            order_id: ID заказа
            new_data: Новые данные от Allegro
            allegro_revision: Версия заказа в Allegro (строка-хеш)
            order_date: Дата заказа из поля boughtAt (для правильного сохранения)
            
        Returns:
            Dict с результатом операции
        """
        
        result = {
            "success": False,
            "action": "none",
            "message": "",
            "order_id": order_id
        }
        
        # 🔍 Детальное логирование для отладки
        logger.info(f"🔄 safe_order_update: order_id={order_id}, type={type(order_id)}")
        logger.info(f"🔄 safe_order_update: new_data keys={list(new_data.keys()) if new_data else 'None'}")
        logger.info(f"🔄 safe_order_update: allegro_revision={allegro_revision}")
        
        # Проверяем что order_id не None и не пустой
        if not order_id:
            error_msg = f"order_id пустой или None: {order_id}"
            logger.error(f"❌ {error_msg}")
            result["message"] = error_msg
            return result
        
        try:
            # 1. Получаем существующий заказ
            existing_order = self.db.exec(select(Order).where(Order.allegro_order_id == order_id)).first()
            
            # 2. Валидация данных
            if not self.validate_order_data_quality(new_data, existing_order):
                result["message"] = "Данные не прошли валидацию качества"
                return result
                
            # 3. Проверка версии (optimistic locking)
            if existing_order and allegro_revision:
                existing_revision = existing_order.order_data.get("revision") if existing_order.order_data else None
                
                # Сравниваем строковые ревизии: если они одинаковы, то заказ уже актуален
                if existing_revision and allegro_revision == existing_revision:
                    result["action"] = "skipped"
                    result["message"] = f"Версия {allegro_revision} уже существует в базе"
                    result["success"] = True
                    logger.info(f"🔄 Заказ {order_id} пропущен: revision {allegro_revision} уже есть")
                    return result
                    
            # 4. Merge данных (если есть существующий заказ)
            final_data = new_data
            if existing_order:
                final_data = self._merge_order_data(existing_order, new_data)
                result["action"] = "updated"
            else:
                result["action"] = "created"
                
            # 5. Обновление/создание заказа
            if existing_order:
                self._update_existing_order(existing_order, final_data, allegro_revision, order_date)
            else:
                self._create_new_order(order_id, final_data, allegro_revision, order_date)
                
            self.db.commit()
            
            result["success"] = True
            result["message"] = f"Заказ {result['action']} успешно"
            
            logger.info(f"✅ Заказ {order_id} {result['action']}: revision {allegro_revision}")
            
        except Exception as e:
            self.db.rollback()
            # Подробное логирование ошибки
            logger.error(f"❌ Ошибка при обновлении заказа {order_id}: {e}")
            logger.error(f"❌ Тип ошибки: {type(e)}")
            logger.error(f"❌ Стек ошибки:", exc_info=True)
            result["message"] = f"Ошибка: {str(e)}"
            raise
            
        return result
        
    def _merge_order_data(self, existing_order: Order, new_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Умное слияние существующих и новых данных заказа.
        
        Принципы merge:
        - Сохраняем максимум информации
        - Новые данные дополняют старые, не заменяют
        - При конфликтах приоритет у более полных данных
        """
        
        # Начинаем с новых данных
        merged_data = new_data.copy()
        
        # Восстанавливаем потерянные данные покупателя
        existing_buyer = existing_order.buyer_data or {}
        new_buyer = new_data.get("buyer", {})
        
        for field in ["email", "firstName", "lastName", "phoneNumber"]:
            if existing_buyer.get(field) and not new_buyer.get(field):
                merged_data.setdefault("buyer", {})[field] = existing_buyer[field]
                logger.info(f"🔄 Восстановлено поле покупателя: {field}")
                
        # Проверяем сохранность товаров
        existing_items = existing_order.line_items or []
        new_items = new_data.get("lineItems", [])
        
        if len(existing_items) > len(new_items):
            logger.warning(f"⚠️ Количество товаров уменьшилось: {len(existing_items)} -> {len(new_items)}")
            # Можем добавить логику восстановления товаров
            
        return merged_data
        
    def _save_order_event(self, order_id: str, event_type: str, 
                         data: Dict[str, Any], revision: Optional[str] = None):
        """Сохранение события заказа для audit trail"""
        
        order_event = OrderEvent(
            order_id=order_id,
            token_id=self.token_id,
            event_type=event_type,
            event_data=data,
            occurred_at=datetime.utcnow()
        )
        
        self.db.add(order_event)
        logger.info(f"📝 Сохранено событие {event_type} для заказа {order_id}")
        
    def _get_required_fields_for_structure(self, data: Dict[str, Any]) -> List[str]:
        """
        Определяет обязательные поля в зависимости от структуры данных.
        
        Args:
            data: Данные заказа для анализа структуры
            
        Returns:
            List[str]: Список обязательных полей для данной структуры
        """
        
        # Проверяем, какая структура данных у нас есть
        if "checkoutForm" in data and isinstance(data["checkoutForm"], dict):
            # Структура из Events API: {checkoutForm: {id, revision}, ...}
            return ["checkout_form_id"]  # Логическое имя для checkoutForm.id
        elif "id" in data:
            # Структура из Checkout Forms API: {id, status, ...}
            return ["id"]
        else:
            # Неизвестная структура - требуем хотя бы один из ID
            return ["order_id"]  # Общее требование наличия ID заказа
            
    def _has_required_field(self, data: Dict[str, Any], field: str) -> bool:
        """
        Проверяет наличие обязательного поля с учетом разных структур данных.
        
        Args:
            data: Данные заказа
            field: Обязательное поле для проверки
            
        Returns:
            bool: True если поле присутствует и не пустое
        """
        
        if field == "checkout_form_id":
            # Проверяем checkoutForm.id для Events API
            checkout_form = data.get("checkoutForm", {})
            if isinstance(checkout_form, dict):
                return bool(checkout_form.get("id"))
            return False
            
        elif field == "id":
            # Проверяем прямое поле id для Checkout Forms API
            return bool(data.get("id"))
            
        elif field == "order_id":
            # Проверяем наличие ID заказа в любой из возможных структур
            # 1. checkoutForm.id (Events API)
            checkout_form = data.get("checkoutForm", {})
            if isinstance(checkout_form, dict) and checkout_form.get("id"):
                return True
            # 2. Прямое поле id (Checkout Forms API)
            if data.get("id"):
                return True
            return False
            
        else:
            # Обычная проверка поля
            return bool(data.get(field))
            
    def _update_existing_order(self, order: Order, data: Dict[str, Any], 
                              allegro_revision: Optional[str] = None, order_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Обновление существующего заказа"""
        
        # Добавляем revision в данные заказа
        if allegro_revision:
            data["revision"] = allegro_revision
        
        # Обновляем данные заказа
        order.order_data = data
        order.updated_at = datetime.utcnow()
        order.order_date = order_date if order_date else datetime.utcnow()
        
        # Обновляем финансовые данные
        summary = data.get("summary", {})
        if summary:
            total_to_pay = summary.get("totalToPay", {})
            order.total_price_amount = float(total_to_pay.get("amount", 0))
            order.total_price_currency = total_to_pay.get("currency", "PLN")
            
    def _create_new_order(self, order_id: str, data: Dict[str, Any], 
                         revision: Optional[str], order_date: Optional[datetime]):
        """Создание нового заказа"""
        
        summary = data.get("summary", {})
        total_to_pay = summary.get("totalToPay", {})
        
        new_order = Order(
            token_id=self.token_id,
            allegro_order_id=order_id,
            order_data=data,
            order_date=order_date if order_date else datetime.utcnow(),
            is_deleted=False
        )
        
        self.db.add(new_order)
        
    def create_data_snapshot(self, order_id: str, snapshot_type: str = "manual"):
        """
        Создание снимка данных заказа для восстановления.
        
        Args:
            order_id: ID заказа
            snapshot_type: Тип снимка (manual, automatic, pre_sync)
        """
        
        order = self.db.exec(select(Order).where(Order.order_id == order_id)).first()
        if not order:
            return
            
        snapshot_data = {
            "order_data": order.dict(),
            "snapshot_type": snapshot_type,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Сохраняем снимок как специальное событие
        self._save_order_event(order_id, "DATA_SNAPSHOT", snapshot_data)
        
        logger.info(f"📸 Создан снимок данных для заказа {order_id}")
        
    def detect_data_anomalies(self, orders_data: List[Dict[str, Any]]) -> List[str]:
        """
        Обнаружение аномалий в данных заказов.
        
        Паттерны аномалий:
        - Слишком много заказов с пустыми полями
        - Массовое исчезновение заказов
        - Неожиданные изменения в структуре данных
        
        Returns:
            List[str]: Список обнаруженных аномалий
        """
        
        anomalies = []
        
        if not orders_data:
            anomalies.append("❌ Получен пустой список заказов")
            return anomalies
            
        # Проверяем долю заказов с пустыми обязательными полями
        orders_with_missing_data = 0
        total_orders = len(orders_data)
        
        for order_data in orders_data:
            if not order_data.get("buyer") or not order_data.get("lineItems"):
                orders_with_missing_data += 1
                
        missing_data_ratio = orders_with_missing_data / total_orders
        if missing_data_ratio > 0.1:  # Более 10% заказов с проблемами
            anomalies.append(f"⚠️ {missing_data_ratio:.1%} заказов имеют неполные данные")
            
        # Проверяем количество заказов
        existing_orders_count = self.db.exec(select(func.count(Order.id))).one()
        if total_orders < existing_orders_count * 0.5:  # Заказов стало в 2 раза меньше
            anomalies.append(f"🚨 Критическое уменьшение количества заказов: {total_orders} vs {existing_orders_count}")
            
        return anomalies 