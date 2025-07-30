"""
@file: order_sync_service.py
@description: Основной сервис синхронизации заказов с защитой данных
@dependencies: OrderProtectionService, DataMonitoringService, AllegroAuthService, DeduplicationService
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import UUID
from sqlmodel import Session

from app.services.order_protection_service import OrderProtectionService, DataIntegrityError
from app.services.data_monitoring_service import DataMonitoringService
from app.services.allegro_auth_service import AllegroAuthService
from app.services.deduplication_service import DeduplicationService
from app.models.sync_history import SyncHistory, SyncStatus
from app.models.order_event import OrderEvent
import httpx

logger = logging.getLogger(__name__)

class SyncPausedException(Exception):
    """Исключение при принудительной остановке синхронизации"""
    pass

class OrderSyncService:
    """
    Основной сервис синхронизации заказов с полной защитой данных.
    
    Ключевые принципы безопасности:
    1. Валидация данных перед сохранением
    2. Мониторинг качества данных  
    3. Автоматическая остановка при аномалиях
    4. Полный audit trail
    5. Возможность восстановления
    6. Дедупликация при использовании нескольких токенов
    """
    
    def __init__(self, db: Session, user_id: str = None, token_id: str = None):
        """
        Инициализация сервиса синхронизации заказов
        
        Args:
            db: Сессия базы данных
            user_id: ID пользователя
            token_id: ID конкретного токена (обязательный параметр)
        """
        self.db = db
        self.user_id = user_id
        self.token_id = token_id
        if token_id:
            self.protection_service = OrderProtectionService(db, UUID(token_id))
        else:
            self.protection_service = None
        self.monitoring_service = DataMonitoringService(db)
        self.deduplication_service = DeduplicationService(db)
        
    def sync_orders_safe(self, full_sync: bool = False, sync_from_date: Optional[datetime] = None, sync_to_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Безопасная синхронизация заказов с полной защитой данных.
        
        Args:
            full_sync: Полная синхронизация или инкрементальная
            sync_from_date: Синхронизация с даты
            sync_to_date: Синхронизация по дату
            
        Returns:
            Dict: Результат синхронизации с детальной статистикой
        """
        
        if not self.token_id:
            raise ValueError("token_id обязателен для синхронизации заказов")
        
        sync_result = {
            "success": False,
            "started_at": datetime.utcnow(),
            "sync_type": "full" if full_sync else "incremental",
            "sync_from_date": sync_from_date,
            "sync_to_date": sync_to_date,
            "orders_processed": 0,
            "orders_created": 0,
            "orders_updated": 0,
            "orders_skipped": 0,
            "orders_failed": 0,
            "orders_deduplicated": 0,
            "events_saved": 0,
            "events_deduplicated": 0,
            "data_quality_score": 0.0,
            "critical_issues": [],
            "warnings": [],
            "paused_due_to_anomalies": False
        }
        
        try:
            # 🔍 1. Предварительная проверка состояния данных
            logger.info("🔍 Проверка состояния данных перед синхронизацией...")
            
            should_pause = self.monitoring_service.should_pause_sync()
            if should_pause:
                sync_result["paused_due_to_anomalies"] = True
                raise SyncPausedException("Синхронизация остановлена из-за аномалий в данных")
                
            # 📥 2. Получение данных от Allegro
            logger.info(f"📥 Получение данных заказов от Allegro (с {sync_from_date} по {sync_to_date})...")
            
            # ЛОГИКА ВЫБОРА API:
            # - Если указана sync_from_date → используем Checkout Forms API (эффективнее для периода)
            # - Если нет sync_from_date → используем Events API (инкрементальная синхронизация)
            
            orders_data = []
            
            if sync_from_date:
                # Используем Checkout Forms API для получения заказов по датам
                logger.info(f"🗓️ Использование Checkout Forms API для периода {sync_from_date} - {sync_to_date or 'сейчас'}")
                orders_data = self._fetch_orders_by_date(sync_from_date, sync_to_date)
                
            else:
                # Используем Events API для инкрементальной синхронизации
                logger.info("📡 Использование Events API для инкрементальной синхронизации")
                
                # Определяем event ID для начала синхронизации
                from_event_id = None
                
                if not full_sync:
                    # Получаем последний event_id из базы данных
                    from_event_id = self._get_last_event_id_from_db()
                    if from_event_id:
                        logger.info(f"🔄 Инкрементальная синхронизация с event_id: {from_event_id}")
                    else:
                        logger.info("🔄 Первая синхронизация - начинаем с начала")
                else:
                    logger.info("🔄 Полная синхронизация всех событий")
                
                # Получаем события заказов
                orders_data = self._fetch_order_events_safe(from_event_id=from_event_id, sync_to_date=sync_to_date)
            
            if not orders_data:
                logger.warning("⚠️ Не получено данных для обработки")
                return sync_result
                
            # 📥 3. Обработка событий заказов
            logger.info(f"📥 Получено {len(orders_data)} событий для обработки")
            
            # Обрабатываем полученные данные (события или заказы)
            for data_item in orders_data:
                try:
                    source = data_item.get("source", "events_api")
                    
                    if source == "checkout_forms_api":
                        # Данные получены напрямую через Checkout Forms API - НЕ создаем события
                        logger.debug("📋 Обработка заказа из Checkout Forms API")
                        
                        # Проверяем дедупликацию заказа
                        order_id = data_item.get("order_id")
                        if order_id:
                            order_decision = self.deduplication_service.should_process_order(
                                order_id, UUID(self.token_id)
                            )
                            
                            if not order_decision["should_process"]:
                                logger.info(f"🔄 Заказ {order_id} пропущен: {order_decision['reason']}")
                                sync_result["orders_deduplicated"] += 1
                                continue
                        
                        # Обрабатываем заказ напрямую (без события)
                        result = self._process_single_order_safe(data_item)
                        
                        # Обновляем статистику
                        sync_result["orders_processed"] += 1
                        if result["action"] == "created":
                            sync_result["orders_created"] += 1
                        elif result["action"] == "updated":
                            sync_result["orders_updated"] += 1
                        elif result["action"] == "skipped":
                            sync_result["orders_skipped"] += 1
                            
                    else:
                        # Данные получены через Events API - сохраняем события
                        logger.debug("📡 Обработка события из Events API")
                        
                        # Извлекаем ID события для дедупликации
                        event_info = data_item.get("event", {})
                        allegro_event_id = event_info.get("id")
                        
                        # Проверяем, нужно ли обрабатывать это событие
                        if allegro_event_id:
                            event_decision = self.deduplication_service.should_process_event(
                                allegro_event_id, UUID(self.token_id)
                            )
                            
                            if not event_decision["should_process"]:
                                logger.info(f"🔄 Событие {allegro_event_id} пропущено: {event_decision['reason']}")
                                sync_result["events_deduplicated"] += 1
                                continue
                        
                        # Сохраняем событие в базу данных
                        self._save_all_events_to_db(data_item)
                        sync_result["events_saved"] += 1
                        
                        # Обрабатываем заказ только для определенных типов событий
                        event_type = event_info.get("type")
                        if event_type in ["BOUGHT", "FILLED_IN", "READY_FOR_PROCESSING", "BUYER_CANCELLED", "FULFILLMENT_STATUS_CHANGED", "AUTO_CANCELLED"]:
                            
                            # Проверяем, нужно ли обрабатывать этот заказ
                            order_id = data_item.get("order_id")
                            if order_id:
                                order_decision = self.deduplication_service.should_process_order(
                                    order_id, UUID(self.token_id)
                                )
                                
                                if not order_decision["should_process"]:
                                    logger.info(f"🔄 Заказ {order_id} пропущен: {order_decision['reason']}")
                                    sync_result["orders_deduplicated"] += 1
                                    continue
                            
                            # Обрабатываем заказ
                            result = self._process_single_order_safe(data_item)
                            
                            # Обновляем статистику
                            sync_result["orders_processed"] += 1
                            if result["action"] == "created":
                                sync_result["orders_created"] += 1
                            elif result["action"] == "updated":
                                sync_result["orders_updated"] += 1
                            elif result["action"] == "skipped":
                                sync_result["orders_skipped"] += 1
                        else:
                            logger.debug(f"📝 Событие {event_type} сохранено, но заказ не обработан")
                        
                except DataIntegrityError as e:
                    logger.error(f"❌ Ошибка целостности данных для события {data_item.get('event', {}).get('id', 'unknown')}: {e}")
                    sync_result["orders_failed"] += 1
                    sync_result["critical_issues"].append(str(e))
                    
                except Exception as e:
                    logger.error(f"❌ Неожиданная ошибка при обработке события {data_item.get('event', {}).get('id', 'unknown')}: {e}")
                    sync_result["orders_failed"] += 1
            
            # 📝 4. Создание записи о начале синхронизации
            sync_history = self._create_sync_history_record(sync_result["sync_type"])
            
            # 🔍 4. Анализ качества полученных данных
            logger.info(f"🔍 Анализ качества {len(orders_data)} событий...")
            
            anomalies = self.monitoring_service.detect_data_anomalies(orders_data)
            if anomalies:
                sync_result["warnings"].extend(anomalies)
                logger.warning(f"⚠️ Обнаружены аномалии: {anomalies}")
                
                # Критические аномалии - останавливаем синхронизацию
                critical_anomalies = [a for a in anomalies if "🚨" in a]
                if critical_anomalies:
                    sync_result["critical_issues"] = critical_anomalies
                    raise SyncPausedException(f"Критические аномалии в данных: {critical_anomalies}")
                    
            # 📊 6. Финальная оценка качества данных
            health_metrics = self.monitoring_service.check_data_health(time_window_hours=1)
            sync_result["data_quality_score"] = 1.0 - health_metrics.anomaly_score
            
            # ✅ 7. Обновление записи синхронизации
            self._update_sync_history_record(sync_history, sync_result, success=True)
            
            sync_result["success"] = True
            sync_result["completed_at"] = datetime.utcnow()
            
            logger.info(
                f"✅ Синхронизация завершена успешно: "
                f"обработано {sync_result['orders_processed']}, "
                f"создано {sync_result['orders_created']}, "
                f"обновлено {sync_result['orders_updated']}, "
                f"пропущено {sync_result['orders_skipped']}, "
                f"дедуплицировано заказов {sync_result['orders_deduplicated']}, "
                f"событий сохранено {sync_result['events_saved']}, "
                f"событий дедуплицировано {sync_result['events_deduplicated']}, "
                f"ошибок {sync_result['orders_failed']}"
            )
            
            return sync_result
            
        except SyncPausedException as e:
            logger.error(f"🛑 Синхронизация остановлена: {e}")
            sync_result["critical_issues"].append(str(e))
            return sync_result
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка синхронизации: {e}")
            sync_result["critical_issues"].append(str(e))
            return sync_result
        
    def _fetch_orders_by_date(self, sync_from_date: datetime, sync_to_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Получение заказов по датам через Checkout Forms API.
        
        Используется когда задана дата sync_from_date - более эффективный способ
        получения заказов за определенный период.
        
        Args:
            sync_from_date: Дата начала синхронизации (обязательно)
            sync_to_date: Дата окончания синхронизации (опционально)
            
        Returns:
            List: Список заказов с полными данными от Allegro API
        """
        
        try:
            # Получаем токен пользователя
            from sqlmodel import select
            from app.models.user_token import UserToken
            from uuid import UUID
            
            try:
                token_uuid = UUID(self.token_id)
                query = select(UserToken).where(
                    UserToken.id == token_uuid,
                    UserToken.user_id == self.user_id,
                    UserToken.is_active == True,
                    UserToken.expires_at > datetime.utcnow()
                )
                
                token_record = self.db.exec(query).first()
                if not token_record:
                    logger.error(f"❌ Токен {self.token_id} недействителен или не принадлежит пользователю {self.user_id}")
                    return []
                    
                token = token_record.allegro_token
                logger.info(f"✅ Используется токен {self.token_id} для получения заказов по датам")
                
            except ValueError:
                logger.error(f"❌ Некорректный UUID токена: {self.token_id}")
                return []
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.allegro.public.v1+json"
            }
            
            # URL для получения заказов (checkout-forms)
            url = "https://api.allegro.pl/order/checkout-forms"
            
            # Параметры запроса с фильтрацией по датам
            params = {
                "limit": 100,  # Максимум 100 заказов за запрос
                "lineItems.boughtAt.gte": sync_from_date.isoformat(),
                "sort": "lineItems.boughtAt"  # Сортировка по дате покупки (ascending)
            }
            
            if sync_to_date:
                params["lineItems.boughtAt.lte"] = sync_to_date.isoformat()
                
            all_orders = []
            offset = 0
            
            while True:
                params["offset"] = offset
                
                logger.info(f"🔄 Получение заказов: offset={offset}, limit={params['limit']}")
                
                # Выполняем запрос к Allegro API
                with httpx.Client() as client:
                    response = client.get(url, headers=headers, params=params, timeout=30.0)
                    response.raise_for_status()
                    
                    data = response.json()
                    orders = data.get("checkoutForms", [])
                    
                    if not orders:
                        logger.info("📋 Больше заказов не найдено, завершаем получение")
                        break
                        
                    logger.info(f"📥 Получено {len(orders)} заказов от Allegro API")
                    
                    # Преобразуем каждый заказ в формат для обработки
                    # НЕ создаем искусственные события - это прямые данные заказов
                    for order in orders:
                        order_record = {
                            "order": order,  # Полные данные заказа
                            "order_id": order.get("id"),
                            "source": "checkout_forms_api"  # Помечаем источник данных
                        }
                        all_orders.append(order_record)
                    
                    # Если получили меньше чем лимит, значит больше данных нет
                    if len(orders) < params["limit"]:
                        logger.info("📋 Получены все доступные заказы")
                        break
                        
                    offset += params["limit"]
                    
                    # Защита от бесконечного цикла
                    if offset > 10000:  # Максимум 10K заказов за раз
                        logger.warning("⚠️ Достигнут лимит в 10K заказов, прерываем получение")
                        break
            
            logger.info(f"✅ Всего получено {len(all_orders)} заказов за период {sync_from_date} - {sync_to_date or 'сейчас'}")
            return all_orders
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP ошибка при получении заказов по датам: {e.response.status_code}")
            logger.error(f"❌ Ответ API: {e.response.text}")
            return []
            
        except httpx.TimeoutException:
            logger.error("❌ Timeout при получении заказов по датам от Allegro API")
            return []
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении заказов по датам: {e}")
            return []

    def _get_last_event_id_from_db(self) -> Optional[str]:
        """
        Получает последний event_id из базы данных для правильной пагинации Events API.
        Если event_id не найден, получает текущую точку через Allegro API Statistics.
        
        Returns:
            Optional[str]: Последний event_id или None если ошибка
        """
        try:
            from sqlmodel import select, desc
            from app.models.order_event import OrderEvent
            from uuid import UUID
            
            # Ищем последнее событие для данного токена
            query = select(OrderEvent).where(
                OrderEvent.token_id == UUID(self.token_id),
                OrderEvent.event_id.isnot(None)  # Только события с event_id
            ).order_by(desc(OrderEvent.occurred_at)).limit(1)
            
            last_event = self.db.exec(query).first()
            
            if last_event and last_event.event_id:
                logger.info(f"🔍 Найден последний event_id в БД: {last_event.event_id}")
                return last_event.event_id
            else:
                logger.info("🔍 Event_id в БД не найден, получаем текущую точку от Allegro API")
                
                # Получаем текущую точку событий от Allegro API
                current_event_id = self._get_current_event_point_from_api()
                
                if current_event_id:
                    # Сохраняем стартовую точку в БД как специальное событие
                    self._save_starting_point_event(current_event_id)
                    logger.info(f"🎯 Установлена стартовая точка event_id: {current_event_id}")
                    return current_event_id
                else:
                    logger.warning("⚠️ Не удалось получить текущую точку событий")
                    return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении event_id: {e}")
            return None

    def _get_current_event_point_from_api(self) -> Optional[str]:
        """
        Получает текущую точку событий от Allegro API через Statistics endpoint.
        
        Использует API: GET /order/events/statistics
        
        Returns:
            Optional[str]: Текущий event_id или None при ошибке
        """
        
        try:
            # Получаем токен пользователя
            from sqlmodel import select
            from app.models.user_token import UserToken
            from uuid import UUID
            
            try:
                token_uuid = UUID(self.token_id)
                query = select(UserToken).where(
                    UserToken.id == token_uuid,
                    UserToken.user_id == self.user_id,
                    UserToken.is_active == True,
                    UserToken.expires_at > datetime.utcnow()
                )
                
                token_record = self.db.exec(query).first()
                if not token_record:
                    logger.error(f"❌ Токен {self.token_id} недействителен или не принадлежит пользователю {self.user_id}")
                    return None
                    
                token = token_record.allegro_token
                logger.info(f"✅ Получение текущей точки событий для токена {self.token_id}")
                
            except ValueError:
                logger.error(f"❌ Некорректный UUID токена: {self.token_id}")
                return None
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.allegro.public.v1+json"
            }
            
            # URL для получения статистики событий
            url = "https://api.allegro.pl/order/events/statistics"
            
            # Выполняем запрос к Allegro API
            with httpx.Client() as client:
                response = client.get(url, headers=headers, timeout=30.0)
                response.raise_for_status()
                
                data = response.json()
                latest_event = data.get("latestEvent", {})
                
                event_id = latest_event.get("id")
                occurred_at = latest_event.get("occurredAt")
                
                if event_id:
                    logger.info(f"📊 Получена текущая точка событий: id={event_id}, time={occurred_at}")
                    return event_id
                else:
                    logger.warning("⚠️ В ответе API отсутствует event_id")
                    return None
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP ошибка при получении статистики событий: {e.response.status_code}")
            logger.error(f"❌ Ответ API: {e.response.text}")
            return None
            
        except httpx.TimeoutException:
            logger.error("❌ Timeout при получении статистики событий от Allegro API")
            return None
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении статистики событий: {e}")
            return None

    def _save_starting_point_event(self, event_id: str):
        """
        Сохраняет стартовую точку событий как специальное событие в БД.
        
        Это позволит в будущем корректно продолжить синхронизацию с этой точки.
        
        Args:
            event_id: ID события - стартовая точка для синхронизации
        """
        
        from app.models.order_event import OrderEvent
        
        try:
            # Создаем специальное событие для маркировки стартовой точки
            starting_point_event = OrderEvent(
                order_id=None,  # Это не связано с конкретным заказом
                token_id=self.token_id,
                event_type="SYNC_STARTING_POINT",
                event_data={
                    "event_id": event_id,
                    "purpose": "starting_point_for_incremental_sync",
                    "created_at": datetime.utcnow().isoformat(),
                    "source": "allegro_events_statistics_api"
                },
                occurred_at=datetime.utcnow(),
                event_id=event_id,  # Сохраняем event_id для пагинации
                is_duplicate=False
            )
            
            self.db.add(starting_point_event)
            self.db.commit()
            
            logger.info(f"📍 Сохранена стартовая точка событий: event_id={event_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения стартовой точки событий: {e}")
            self.db.rollback()
            # Не прерываем процесс из-за ошибки сохранения стартовой точки

    def _fetch_order_events_safe(self, from_event_id: Optional[str] = None, sync_to_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Безопасное получение событий заказов от Allegro API.
        
        ВАЖНО: Events API возвращает только метаданные событий, без полных данных заказа!
        Для каждого события нужно дополнительно получать данные заказа через Checkout Forms API.
        
        Args:
            from_event_id: ID события, с которого начинать извлечение (опционально)
            sync_to_date: Дата окончания синхронизации - события новее этой даты будут отфильтрованы
            
        Returns:
            List: Список событий с полными данными заказов от Allegro API
        """
        
        try:
            # Получаем конкретный токен пользователя из базы данных
            from sqlmodel import select
            from app.models.user_token import UserToken
            from datetime import datetime
            from uuid import UUID
            
            # Получаем конкретный токен по ID
            try:
                token_uuid = UUID(self.token_id)
                query = select(UserToken).where(
                    UserToken.id == token_uuid,
                    UserToken.user_id == self.user_id,
                    UserToken.is_active == True,
                    UserToken.expires_at > datetime.utcnow()
                )
                
                token_record = self.db.exec(query).first()
                if not token_record:
                    logger.error(f"❌ Токен {self.token_id} недействителен или не принадлежит пользователю {self.user_id}")
                    return []
                    
                token = token_record.allegro_token
                logger.info(f"✅ Используется токен {self.token_id} для синхронизации")
                
            except ValueError:
                logger.error(f"❌ Некорректный UUID токена: {self.token_id}")
                return []
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.allegro.public.v1+json"
            }
            
            # URL для получения событий заказов
            url = "https://api.allegro.pl/order/events"
            
            # Параметры запроса
            params = {"limit": 1000}
            
            # Используем event ID для пагинации (а не дату)
            if from_event_id:
                params["from"] = from_event_id
                logger.info(f"🔄 Получение событий с event ID: {from_event_id}")
            
            # Выполняем запрос к Allegro API
            with httpx.Client() as client:
                response = client.get(url, headers=headers, params=params, timeout=30.0)
                response.raise_for_status()
                
                events_data = response.json()
                events = events_data.get("events", [])
                
                logger.info(f"📥 Получено {len(events)} событий от Allegro API")
                
                # Фильтруем события по дате окончания (если указана)
                if sync_to_date:
                    filtered_events = []
                    for event in events:
                        # Проверяем время события из правильного поля
                        event_time = None
                        if "occurredAt" in event:
                            event_time = datetime.fromisoformat(event["occurredAt"].replace("Z", "+00:00"))
                        
                        # Включаем событие только если оно не новее sync_to_date
                        if event_time and event_time <= sync_to_date:
                            filtered_events.append(event)
                        elif not event_time:
                            # Если время не найдено, включаем событие (безопасный подход)
                            filtered_events.append(event)
                    
                    events = filtered_events
                    logger.info(f"🗓️ После фильтрации по дате окончания: {len(events)} событий")
                
                # ВАЖНО: События содержат только метаданные, нужно получить полные данные заказов
                # Для каждого события с order_id получаем полные данные через Checkout Forms API
                valid_events = []
                
                for event in events:
                    event_data = {
                        "id": event.get("id"),
                        "type": event.get("type"),
                        "occurredAt": event.get("occurredAt"),
                        "publishedAt": event.get("publishedAt")
                    }
                    
                    # Пытаемся извлечь order_id из события согласно реальной структуре API
                    order_id = None
                    order_data = event.get("order", {})
                    
                    # Проверяем различные возможные места расположения order_id
                    if order_data:
                        # 1. Пытаемся найти в checkoutForm.id
                        checkout_form = order_data.get("checkoutForm", {})
                        if checkout_form and isinstance(checkout_form, dict):
                            order_id = checkout_form.get("id")
                        
                        # 2. Если не найдено, проверяем прямо в order.id  
                        if not order_id:
                            order_id = order_data.get("id")
                    
                    # 3. Если все еще не найдено, проверяем корневой уровень события
                    if not order_id:
                        # Иногда order_id может быть в корне события
                        order_id = event.get("orderId") or event.get("order_id")
                    
                    # 4. Последняя попытка - извлечь из других полей заказа
                    if not order_id and order_data:
                        # Если checkoutForm пустой объект {}, возможно order_id находится в другом месте
                        # Проверяем все поля order_data на наличие ID-подобных значений
                        for key, value in order_data.items():
                            if key in ['id', 'orderId', 'checkoutFormId'] and value:
                                order_id = value
                                break
                    
                    if order_id:
                        # Получаем полные данные заказа через отдельный API вызов
                        full_order_data = self._fetch_order_details_safe(order_id, headers)
                        
                        if full_order_data:
                            event_record = {
                                "event": event_data,
                                "order": full_order_data,  # Полные данные заказа
                                "order_id": order_id,
                                "source": "events_api"  # Помечаем источник данных
                            }
                            valid_events.append(event_record)
                            logger.debug(f"✅ Событие {event.get('type')} для заказа {order_id} с полными данными добавлено")
                        else:
                            # Если не удалось получить данные заказа, сохраняем только событие
                            event_record = {
                                "event": event_data,
                                "order": {},  # Пустые данные заказа
                                "order_id": order_id,
                                "source": "events_api"
                            }
                            valid_events.append(event_record)
                            logger.warning(f"⚠️ Событие {event.get('type')} для заказа {order_id} без данных заказа")
                    else:
                        # Событие без order_id (может быть системное событие)
                        event_record = {
                            "event": event_data,
                            "order": {},  # Пустые данные заказа
                            "order_id": None,
                            "source": "events_api"
                        }
                        valid_events.append(event_record)
                        logger.debug(f"✅ Событие {event.get('type')} без order_id добавлено")
                        
                        # Логируем структуру события для отладки
                        logger.debug(f"🔍 Структура события без order_id: {event}")
                        
                        # Дополнительная диагностика для понимания структуры
                        if logger.isEnabledFor(logging.DEBUG):
                            event_keys = list(event.keys()) if isinstance(event, dict) else "НЕ СЛОВАРЬ"
                            order_info = event.get("order", "НЕТ ПОЛЯ ORDER")
                            if isinstance(order_info, dict):
                                order_keys = list(order_info.keys())
                                logger.debug(f"🔍 Ключи события: {event_keys}")
                                logger.debug(f"🔍 Ключи order: {order_keys}")
                                
                                # Анализируем все поля на предмет содержания ID
                                for key, value in order_info.items():
                                    if isinstance(value, (str, int)) and ("id" in key.lower() or len(str(value)) > 10):
                                        logger.debug(f"🔍 Потенциальный ID в order.{key}: {value}")
                                    elif isinstance(value, dict) and value:
                                        for sub_key, sub_value in value.items():
                                            if isinstance(sub_value, (str, int)) and ("id" in sub_key.lower() or len(str(sub_value)) > 10):
                                                logger.debug(f"🔍 Потенциальный ID в order.{key}.{sub_key}: {sub_value}")
                            else:
                                logger.debug(f"🔍 order не является словарем: {type(order_info)}")
                
                return valid_events
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP ошибка при получении событий: {e.response.status_code}")
            logger.error(f"❌ Ответ API: {e.response.text}")
            return []
            
        except httpx.TimeoutException:
            logger.error("❌ Timeout при получении событий от Allegro API")
            return []
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении событий: {e}")
            return []
            
    def _fetch_order_details_safe(self, order_id: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Безопасное получение деталей заказа.
        
        Args:
            order_id: ID заказа
            headers: HTTP заголовки с авторизацией
            
        Returns:
            Optional[Dict]: Данные заказа или None при ошибке
        """
        
        try:
            url = f"https://api.allegro.pl/order/checkout-forms/{order_id}"
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers, timeout=15.0)
                
                if response.status_code == 404:
                    logger.warning(f"⚠️ Заказ {order_id} не найден (возможно, объединен)")
                    return None
                    
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Ошибка при получении деталей заказа {order_id}: {e.response.status_code}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при получении заказа {order_id}: {e}")
            return None
            
    def _process_single_order_safe(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Безопасная обработка заказа с полной защитой данных.
        
        Работает с данными как из Events API (с событиями), так и из Checkout Forms API (без событий).
        
        Args:
            data_item: Данные заказа (с событием или без)
            
        Returns:
            Dict: Результат обработки заказа
        """
        
        # 🔍 Определяем источник данных и извлекаем информацию
        try:
            source = data_item.get("source", "events_api")
            order_data = data_item.get("order", {})
            order_id = data_item.get("order_id")
            
            if not order_id or not order_data:
                logger.error(f"❌ Некорректная структура данных: {data_item}")
                return {"success": False, "message": "Некорректная структура данных", "action": "failed"}
                
        except (KeyError, AttributeError) as e:
            logger.error(f"❌ Ошибка извлечения данных: {e}")
            return {"success": False, "message": "Ошибка извлечения данных", "action": "failed"}
        
        # 📅 Правильно извлекаем даты в зависимости от источника данных
        try:
            occurred_at = None
            order_date = None
            
            if source == "events_api":
                # Для данных из Events API используем время события
                event_data = data_item.get("event", {})
                if "occurredAt" in event_data:
                    occurred_at = datetime.fromisoformat(event_data["occurredAt"].replace("Z", "+00:00"))
            
            # Дата заказа из поля boughtAt (для товаров)
            line_items = order_data.get("lineItems", [])
            if line_items and isinstance(line_items, list) and len(line_items) > 0:
                # Берем дату покупки первого товара
                first_item = line_items[0]
                if "boughtAt" in first_item:
                    order_date = datetime.fromisoformat(first_item["boughtAt"].replace("Z", "+00:00"))
            
            # Если не нашли boughtAt, используем дату обновления заказа
            if not order_date and "updatedAt" in order_data:
                order_date = datetime.fromisoformat(order_data["updatedAt"].replace("Z", "+00:00"))
            
            # Fallback: используем текущее время
            if not order_date:
                order_date = occurred_at or datetime.utcnow()
            if not occurred_at:
                occurred_at = order_date
                
            logger.debug(f"📅 Заказ {order_id}: source={source}, occurred_at={occurred_at}, order_date={order_date}")
            
        except (ValueError, TypeError) as e:
            logger.error(f"❌ Ошибка парсинга дат для заказа {order_id}: {e}")
            occurred_at = datetime.utcnow()
            order_date = datetime.utcnow()
        
        # 🔄 Извлекаем revision из данных заказа
        revision = order_data.get("revision")
        if not revision:
            # Fallback: используем временную метку как revision
            revision = str(int(occurred_at.timestamp())) if occurred_at else str(int(datetime.utcnow().timestamp()))
        
        logger.debug(f"🔄 Обработка заказа {order_id}, revision {revision}, source={source}")
        
        # 📊 Сохраняем событие ТОЛЬКО для данных из Events API
        if source == "events_api":
            try:
                event_data = data_item.get("event", {})
                self._save_order_event(
                    order_id=order_id,
                    event_type=event_data.get("type", "UNKNOWN"),
                    event_data=event_data,
                    occurred_at=occurred_at
                )
                logger.debug(f"📊 Событие сохранено для заказа {order_id}")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения события для заказа {order_id}: {e}")
        else:
            logger.debug(f"📋 Заказ {order_id} из Checkout Forms API - событие НЕ создается")
        
        # 🛡️ Используем защищенное обновление заказа
        result = self.protection_service.safe_order_update(
            order_id=order_id,
            new_data=order_data,
            allegro_revision=revision,
            order_date=order_date
        )
        
        if not result["success"]:
            logger.warning(f"⚠️ Заказ {order_id} не обновлен: {result['message']}")
        else:
            logger.info(f"✅ Заказ {order_id} обработан: {result['action']} (источник: {source})")
            
        return result
        
    def _create_sync_history_record(self, sync_type: str) -> SyncHistory:
        """Создание записи о начале синхронизации"""
        
        sync_history = SyncHistory(
            token_id=self.token_id,
            sync_started_at=datetime.utcnow(),
            sync_status=SyncStatus.RUNNING
        )
        
        self.db.add(sync_history)
        self.db.commit()
        self.db.refresh(sync_history)
        
        return sync_history
        
    def _update_sync_history_record(self, sync_history: SyncHistory, 
                                   sync_result: Dict[str, Any],
                                   success: bool, error: Optional[str] = None):
        """Обновление записи синхронизации"""
        
        sync_history.sync_completed_at = datetime.utcnow()
        sync_history.sync_status = SyncStatus.COMPLETED if success else SyncStatus.FAILED
        sync_history.orders_processed = sync_result["orders_processed"]
        sync_history.orders_added = sync_result.get("orders_created", 0)
        sync_history.orders_updated = sync_result.get("orders_updated", 0)
        sync_history.error_message = error
        
        self.db.commit()
        
    def emergency_restore_from_events(self, order_id: str, 
                                     target_timestamp: Optional[datetime] = None) -> bool:
        """
        Экстренное восстановление заказа из событий.
        
        Используется для восстановления данных в случае их повреждения.
        
        Args:
            order_id: ID заказа для восстановления
            target_timestamp: Временная точка для восстановления (последнее валидное состояние если None)
            
        Returns:
            bool: True если восстановление успешно
        """
        
        try:
            # Получаем все события заказа
            from sqlmodel import select
            events = self.db.exec(
                select(OrderEvent)
                .where(OrderEvent.order_id == order_id)
                .order_by(OrderEvent.occurred_at.desc())
            ).all()
            
            if not events:
                logger.error(f"❌ События для заказа {order_id} не найдены")
                return False
                
            # Находим последнее валидное событие
            target_event = None
            for event in events:
                if target_timestamp is None or event.occurred_at <= target_timestamp:
                    # Проверяем качество данных события
                    if self.protection_service.validate_order_data_quality(event.event_data):
                        target_event = event
                        break
                        
            if not target_event:
                logger.error(f"❌ Валидное событие для восстановления заказа {order_id} не найдено")
                return False
                
            # Восстанавливаем заказ из события
            logger.info(f"🔄 Восстановление заказа {order_id} из события {target_event.id}")
            
            result = self.protection_service.safe_order_update(
                order_id=order_id,
                new_data=target_event.event_data,
                allegro_revision=target_event.event_data.get("revision")
            )
            
            if result["success"]:
                logger.info(f"✅ Заказ {order_id} успешно восстановлен")
                
                # Создаем событие о восстановлении
                self.protection_service._save_order_event(
                    order_id, "ORDER_RESTORED", {
                        "restored_from_event_id": target_event.id,
                        "restored_at": datetime.utcnow().isoformat(),
                        "reason": "emergency_restore"
                    }
                )
                
                return True
            else:
                logger.error(f"❌ Не удалось восстановить заказ {order_id}: {result['message']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении заказа {order_id}: {e}")
            return False 

    def _save_all_events_to_db(self, event_data: Dict[str, Any]):
        """
        Сохраняет все события в базу данных для полноты audit trail.
        
        Args:
            event_data: Данные события с заказом
        """
        
        from app.models.order_event import OrderEvent
        
        try:
            event_info = event_data.get("event", {})
            order_id = event_data.get("order_id")
            allegro_event_id = event_info.get("id")
            
            # Парсим дату события
            occurred_at = datetime.utcnow()
            if "occurredAt" in event_info:
                try:
                    occurred_at = datetime.fromisoformat(event_info["occurredAt"].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    logger.warning(f"⚠️ Не удалось распарсить дату события: {event_info.get('occurredAt')}")
            
            # Создаем запись события
            order_event = OrderEvent(
                order_id=order_id,
                token_id=self.token_id,
                event_type=event_info.get("type", "UNKNOWN"),
                event_data=event_info,
                occurred_at=occurred_at,
                event_id=allegro_event_id,  # Используем allegro_event_id для pagination
                is_duplicate=False
            )
            
            self.db.add(order_event)
            self.db.commit()
            
            logger.debug(f"📝 Сохранено событие {event_info.get('type')} для заказа {order_id or 'unknown'}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения события в базу: {e}")
            self.db.rollback()
            # Не прерываем процесс из-за ошибки сохранения события
            
    def _save_order_event(self, order_id: str, event_type: str, 
                         event_data: Dict[str, Any], occurred_at: Optional[datetime] = None):
        """
        Сохранение события заказа в базу данных.
        
        Args:
            order_id: ID заказа
            event_type: Тип события  
            event_data: Данные события
            occurred_at: Время события (если не указано, используется текущее время)
        """
        
        from app.models.order_event import OrderEvent
        
        if not occurred_at:
            occurred_at = datetime.utcnow()
        
        try:
            order_event = OrderEvent(
                order_id=order_id,
                token_id=self.token_id,
                event_type=event_type,
                event_data=event_data,
                occurred_at=occurred_at
            )
            
            self.db.add(order_event)
            self.db.commit()
            logger.debug(f"📝 Сохранено событие {event_type} для заказа {order_id} в {occurred_at}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения события для заказа {order_id}: {e}")
            self.db.rollback()
            raise
    
    # Заглушки для методов работы с историей синхронизации
    def get_user_sync_history(self, user_id: str, page: int = 1, per_page: int = 10, 
                            token_id=None, status=None, date_from=None, date_to=None):
        """Заглушка для получения истории синхронизации"""
        return {
            "syncs": [],
            "total": 0,
            "page": page,
            "per_page": per_page
        }
    
    def start_token_sync(self, user_id: str, token_id, sync_from_date=None, 
                        sync_to_date=None, force_full_sync=False):
        """Заглушка для запуска синхронизации токена"""
        return {
            "task_id": "test-task-id",
            "status": "PENDING", 
            "message": "Синхронизация запущена",
            "started_at": datetime.utcnow()
        }
    
    def get_sync_status(self, sync_id: str, user_id: str):
        """Заглушка для получения статуса синхронизации"""
        return None
    
    def cancel_sync(self, sync_id: str, user_id: str):
        """Заглушка для отмены синхронизации"""
        return {"success": False, "message": "Синхронизация не найдена"}
    
    def get_user_sync_stats(self, user_id: str, date_from=None, date_to=None):
        """Заглушка для получения статистики синхронизации"""
        return {
            "total_syncs": 0,
            "successful_syncs": 0,
            "failed_syncs": 0,
            "running_syncs": 0,
            "total_orders_processed": 0,
            "total_orders_added": 0,
            "total_orders_updated": 0,
            "last_sync_date": None,
            "average_sync_duration": None
        }
    
    def get_running_syncs(self, user_id: str):
        """Заглушка для получения активных синхронизаций"""
        return [] 