"""
@file: order_sync_service.py
@description: Основной сервис синхронизации заказов с защитой данных
@dependencies: OrderProtectionService, DataMonitoringService, AllegroAuthService, DeduplicationService
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from uuid import UUID
from sqlmodel import Session

from app.services.order_protection_service import OrderProtectionService, DataIntegrityError
from app.services.data_monitoring_service import DataMonitoringService
from app.services.allegro_auth_service import AllegroAuthService
from app.services.deduplication_service import DeduplicationService
from app.models.sync_history import SyncHistory, SyncStatus
from app.models.order_event import OrderEvent
from app.models.failed_order_processing import FailedOrderProcessing, FailedOrderStatus
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
                        logger.info("📋 Обработка заказа из Checkout Forms API")
                        
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
                        # Данные получены через Events API - НОВАЯ ЛОГИКА
                        logger.info("📡 Обработка события из Events API")
                        
                        # Извлекаем ID события для дедупликации
                        event_info = data_item.get("event", {})
                        allegro_event_id = event_info.get("id")
                        order_id = data_item.get("order_id")
                        
                        # Проверяем, нужно ли обрабатывать это событие
                        if allegro_event_id:
                            event_decision = self.deduplication_service.should_process_event(
                                allegro_event_id, UUID(self.token_id)
                            )
                            
                            if not event_decision["should_process"]:
                                logger.info(f"🔄 Событие {allegro_event_id} пропущено: {event_decision['reason']}")
                                sync_result["events_deduplicated"] += 1
                                continue
                        
                        # ✅ ШАГ 1: Сохраняем событие в базу данных (независимо от типа)
                        self._save_all_events_to_db(data_item)
                        sync_result["events_saved"] += 1
                        
                        # ✅ ШАГ 2: Проверяем нужно ли создавать/обновлять заказ
                        if order_id:
                            # Проверяем дедупликацию заказа
                            order_decision = self.deduplication_service.should_process_order(
                                order_id, UUID(self.token_id)
                            )
                            
                            if not order_decision["should_process"]:
                                logger.info(f"🔄 Заказ {order_id} пропущен: {order_decision['reason']}")
                                sync_result["orders_deduplicated"] += 1
                                continue
                            
                            # Извлекаем revision из checkoutForm
                            checkout_form = data_item.get("order", {}).get("checkoutForm", {})
                            new_revision = checkout_form.get("revision")
                            
                            if not new_revision:
                                logger.warning(f"⚠️ Revision не найдена в событии для заказа {order_id}")
                                # Используем fallback revision на основе времени события
                                event_occurred_at = event_info.get("occurredAt")
                                if event_occurred_at:
                                    try:
                                        occurred_dt = datetime.fromisoformat(event_occurred_at.replace("Z", "+00:00"))
                                        new_revision = str(int(occurred_dt.timestamp()))
                                    except:
                                        new_revision = str(int(datetime.utcnow().timestamp()))
                                else:
                                    new_revision = str(int(datetime.utcnow().timestamp()))
                                logger.info(f"🔧 Используется fallback revision: {new_revision}")
                            
                            # Проверяем нужно ли создавать/обновлять заказ
                            update_check = self._check_order_needs_update(order_id, new_revision)
                            
                            if update_check["action"] == "skip":
                                logger.info(f"⏭️ Заказ {order_id} пропущен - revision не изменилась")
                                sync_result["orders_skipped"] += 1
                                continue
                            
                            # ✅ ШАГ 3: Получаем полные детали заказа и создаем/обновляем
                            logger.info(f"🔍 Получение полных деталей заказа {order_id} (действие: {update_check['action']})")
                            
                            order_details = self._get_order_details_from_api(order_id)
                            
                            if order_details:
                                logger.info(f"💾 {update_check['action'].title()} заказа {order_id} на основе полных деталей")
                                
                                # Создаем структуру данных для обработки заказа
                                order_data_item = {
                                    "order": order_details,
                                    "order_id": order_id,
                                    "source": "full_api_details"
                                }
                                
                                # Обрабатываем заказ с полными деталями
                                result = self._process_single_order_safe(order_data_item)
                                
                                # Обновляем статистику
                                sync_result["orders_processed"] += 1
                                if result["action"] == "created":
                                    sync_result["orders_created"] += 1
                                elif result["action"] == "updated":
                                    sync_result["orders_updated"] += 1
                                elif result["action"] == "skipped":
                                    sync_result["orders_skipped"] += 1
                                    
                                logger.info(f"✅ Заказ {order_id} обработан: {result['action']}")
                                
                            else:
                                # ❌ Не удалось получить детали заказа - сохраняем для повторной обработки
                                logger.warning(f"⚠️ Не удалось получить детали заказа {order_id}, сохраняем для повторной обработки")
                                
                                error_message = f"Не удалось получить детали заказа через API после retry"
                                saved = self._save_failed_order(
                                    order_id=order_id,
                                    action_required=update_check['action'],
                                    error_message=error_message,
                                    error_type="api_fetch_failed",
                                    event_data=data_item,
                                    expected_revision=new_revision
                                )
                                
                                if saved:
                                    logger.info(f"💾 Заказ {order_id} сохранен для повторной обработки")
                                else:
                                    logger.error(f"❌ Не удалось сохранить проблемный заказ {order_id}")
                                    
                                sync_result["orders_failed"] += 1
                        else:
                            logger.info("📝 Событие сохранено, но order_id отсутствует - заказ не обработан")
                        
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
            
            # Ищем последнее событие для данного токена (включая специальные события)
            query = select(OrderEvent).where(
                OrderEvent.token_id == UUID(self.token_id),
                OrderEvent.event_id.isnot(None)  # Только события с event_id
            ).order_by(desc(OrderEvent.occurred_at)).limit(1)
            
            last_event = self.db.exec(query).first()
            
            if last_event and last_event.event_id:
                if last_event.order_id == "SYNC_STARTING_POINT":
                    logger.info(f"🔍 Найден последний event_id стартовой точки в БД: {last_event.event_id}")
                else:
                    logger.info(f"🔍 Найден последний event_id в БД: {last_event.event_id}")
                return last_event.event_id
            else:
                logger.info("🔍 Event_id в БД не найден, получаем текущую точку от Allegro API")
                
                # Получаем текущую точку событий от Allegro API
                current_event = self._get_current_event_point_from_api()
                
                if current_event:
                    # Сохраняем стартовую точку в БД как специальное событие
                    self._save_starting_point_event(current_event["event_id"], current_event["occurred_at"])
                    logger.info(f"🎯 Установлена стартовая точка event_id: {current_event['event_id']}")
                    return current_event["event_id"]
                else:
                    logger.warning("⚠️ Не удалось получить текущую точку событий")
                    return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении event_id: {e}")
            return None

    def _get_current_event_point_from_api(self) -> Optional[Dict[str, Any]]:
        """
        Получает текущую точку событий от Allegro API через Statistics endpoint.
        
        Использует API: GET /order/event-stats
        
        Returns:
            Optional[Dict]: Словарь с event_id и occurred_at (datetime объект) или None при ошибке
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
            url = "https://api.allegro.pl/order/event-stats"
            
            # Выполняем запрос к Allegro API
            with httpx.Client() as client:
                response = client.get(url, headers=headers, timeout=30.0)
                response.raise_for_status()
                
                data = response.json()
                latest_event = data.get("latestEvent", {})
                
                event_id = latest_event.get("id")
                occurred_at_str = latest_event.get("occurredAt")
                
                if event_id and occurred_at_str:
                    # Парсим строку даты в объект datetime
                    try:
                        occurred_at = datetime.fromisoformat(occurred_at_str.replace("Z", "+00:00"))
                        logger.info(f"📊 Получена текущая точка событий: id={event_id}, time={occurred_at}")
                        return {"event_id": event_id, "occurred_at": occurred_at}
                    except (ValueError, TypeError) as e:
                        logger.error(f"❌ Ошибка парсинга даты события {occurred_at_str}: {e}")
                        return None
                else:
                    logger.warning("⚠️ В ответе API отсутствует event_id или occurredAt")
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

    def _save_starting_point_event(self, event_id: str, occurred_at: datetime):
        """
        Сохраняет стартовую точку событий как специальное событие в БД.
        
        Это позволит в будущем корректно продолжить синхронизацию с этой точки.
        
        Args:
            event_id: ID события - стартовая точка для синхронизации
            occurred_at: Время события (объект datetime)
        """
        
        from app.models.order_event import OrderEvent
        
        try:
            # Создаем специальное событие для маркировки стартовой точки
            # Используем специальное значение order_id вместо None
            starting_point_event = OrderEvent(
                order_id="SYNC_STARTING_POINT",  # Специальное значение вместо None
                token_id=self.token_id,
                event_type="SYNC_STARTING_POINT",
                event_data={
                    "event_id": event_id,
                    "purpose": "starting_point_for_incremental_sync",
                    "created_at": datetime.utcnow().isoformat(),
                    "source": "allegro_events_statistics_api"
                },
                occurred_at=occurred_at,
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

    def _extract_order_id_from_event(self, event: Dict[str, Any]) -> Optional[str]:
        """
        Простое извлечение order_id из события.
        
        Args:
            event: Событие от Allegro API
            
        Returns:
            Optional[str]: order_id или None
        """
        order_data = event.get("order", {})
        if not order_data:
            return None
            
        checkout_form = order_data.get("checkoutForm", {})
        if isinstance(checkout_form, dict):
            return checkout_form.get("id")
            
        return None

    def _fetch_order_events_safe(self, from_event_id: Optional[str] = None, sync_to_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Простое получение событий заказов от Allegro API.
        
        Метод только получает события и возвращает их в простом формате.
        Вся сложная логика обработки событий выполняется в sync_orders_safe.
        
        Args:
            from_event_id: ID события, с которого начинать извлечение (опционально)
            sync_to_date: Дата окончания синхронизации - события новее этой даты будут отфильтрованы
            
        Returns:
            List: Простой список событий для обработки в sync_orders_safe
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
                
                # Возвращаем события в простом формате
                # Вся обработка структуры данных теперь в sync_orders_safe
                simple_events = []
                
                for event in events:
                    # Простая структура для sync_orders_safe
                    event_record = {
                        "event": event,  # Полное событие как есть от API
                        "order": event.get("order", {}),  # Данные заказа
                        "order_id": self._extract_order_id_from_event(event),  # Простое извлечение order_id
                        "source": "events_api"
                    }
                    simple_events.append(event_record)
                
                logger.info(f"✅ Обработано {len(simple_events)} событий для sync_orders_safe")
                return simple_events
                
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
            
    def _process_single_order_safe(self, data_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Безопасная обработка заказа с полной защитой данных.
        
        ВАЖНО: Этот метод ТОЛЬКО обрабатывает заказ. События уже сохраняются в родительском методе!
        
        Работает с данными как из Events API (с событиями), так и из Checkout Forms API (без событий).
        
        Args:
            data_item: Данные заказа (с событием или без)
            
        Returns:
            Dict: Результат обработки заказа
        """
        
        # 🔍 Определяем источник данных и извлекаем информацию
        try:
            source = data_item.get("source", "Unknown")
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
                
            logger.info(f"📅 Заказ {order_id}: source={source}, occurred_at={occurred_at}, order_date={order_date}")
            
        except (ValueError, TypeError) as e:
            logger.error(f"❌ Ошибка парсинга дат для заказа {order_id}: {e}")
            occurred_at = datetime.utcnow()
            order_date = datetime.utcnow()
        
        # 🔄 Извлекаем revision из данных заказа в зависимости от источника
        revision = None
        
        if source == "events_api":
            # Для Events API revision находится в checkoutForm
            checkout_form = order_data.get("checkoutForm", {})
            revision = checkout_form.get("revision")
        else:
            # Для full_api_details и checkout_forms_api revision в корне заказа
            revision = order_data.get("revision")
        
        # Fallback: используем временную метку как revision
        if not revision:
            revision = str(int(occurred_at.timestamp())) if occurred_at else str(int(datetime.utcnow().timestamp()))
        
        logger.info(f"🔄 Обработка заказа {order_id}, revision {revision}, source={source}")
        
        # 🛡️ Используем защищенное обновление заказа
        # ПРИМЕЧАНИЕ: OrderProtectionService повторно проверит revision (optimistic locking)
        # Это нормально - двойная проверка добавляет безопасность против race conditions
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

    def _check_order_needs_update(self, order_id: str, new_revision: str) -> Dict[str, Any]:
        """
        Проверяет существует ли заказ в БД и нужно ли его обновлять по revision.
        
        Args:
            order_id: ID заказа в системе Allegro
            new_revision: Новая revision из события
            
        Returns:
            Dict с информацией о необходимости создания/обновления:
            {
                "exists": bool,
                "needs_update": bool,
                "current_revision": str,
                "action": "create" | "update" | "skip"
            }
        """
        try:
            from sqlmodel import select
            from app.models.order import Order
            from uuid import UUID
            
            # Ищем заказ в БД по order_id и token_id
            query = select(Order).where(
                Order.allegro_order_id == order_id,
                Order.token_id == UUID(self.token_id),
                Order.is_deleted == False
            )
            
            existing_order = self.db.exec(query).first()
            
            if not existing_order:
                logger.info(f"📝 Заказ {order_id} не найден в БД, требуется создание")
                return {
                    "exists": False,
                    "needs_update": False,
                    "current_revision": None,
                    "action": "create"
                }
            
            # Извлекаем текущую revision из данных заказа
            order_data = existing_order.order_data or {}
            current_revision = order_data.get("revision")
            
            if current_revision != new_revision:
                logger.info(f"🔄 Заказ {order_id} требует обновления: {current_revision} → {new_revision}")
                return {
                    "exists": True,
                    "needs_update": True,
                    "current_revision": current_revision,
                    "action": "update"
                }
            else:
                logger.info(f"✅ Заказ {order_id} актуален (revision: {current_revision}), обновление не требуется")
                return {
                    "exists": True,
                    "needs_update": False,
                    "current_revision": current_revision,
                    "action": "skip"
                }
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки заказа {order_id}: {e}")
            # В случае ошибки лучше попытаться обновить
            return {
                "exists": False,
                "needs_update": True,
                "current_revision": None,
                "action": "create"
            }

    def _get_order_details_from_api(self, order_id: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Получает полные детали заказа через Allegro API с retry механизмом.
        
        Args:
            order_id: ID заказа в системе Allegro
            max_retries: Максимальное количество попыток
            
        Returns:
            Dict с деталями заказа или None при ошибке
        """
        import time
        from sqlmodel import select
        from app.models.user_token import UserToken
        from uuid import UUID
        
        # Получаем токен доступа
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
                logger.error(f"❌ Токен {self.token_id} недействителен для получения деталей заказа {order_id}")
                return None
                
            token = token_record.allegro_token
            
        except ValueError:
            logger.error(f"❌ Некорректный UUID токена: {self.token_id}")
            return None
            
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json"
        }
        
        url = f"https://api.allegro.pl/order/checkout-forms/{order_id}"
        
        # Retry логика с экспоненциальным backoff
        for attempt in range(max_retries):
            try:
                logger.debug(f"🔍 Получение деталей заказа {order_id} через API (попытка {attempt + 1}/{max_retries})")
                
                with httpx.Client() as client:
                    response = client.get(url, headers=headers, timeout=15.0)
                    
                    if response.status_code == 404:
                        logger.warning(f"⚠️ Заказ {order_id} не найден в API")
                        return None
                        
                    response.raise_for_status()
                    order_data = response.json()
                    
                    logger.info(f"✅ Получены детали заказа {order_id} (попытка {attempt + 1})")
                    return order_data
                    
            except (httpx.ConnectError, httpx.TimeoutException, ConnectionError) as e:
                # Временные сетевые ошибки - делаем retry
                error_msg = f"Сетевая ошибка при получении деталей заказа {order_id}: {e}"
                
                if attempt < max_retries - 1:
                    # Exponential backoff: 1, 2, 4 секунды
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️ {error_msg}. Повторная попытка через {wait_time}с...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ {error_msg}. Все попытки исчерпаны.")
                    return None
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code in [429, 500, 502, 503, 504]:
                    # Временные ошибки сервера - делаем retry
                    error_msg = f"Временная ошибка API при получении деталей заказа {order_id}: {e.response.status_code}"
                    
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"⚠️ {error_msg}. Повторная попытка через {wait_time}с...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ {error_msg}. Все попытки исчерпаны.")
                        return None
                else:
                    # Постоянные ошибки (401, 403, 404) - не ретраим
                    logger.error(f"❌ HTTP ошибка при получении деталей заказа {order_id}: {e.response.status_code}")
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Неожиданная ошибка при получении деталей заказа {order_id}: {e}")
                return None
                
        return None

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
    
    def _save_failed_order(self, order_id: str, action_required: str, error_message: str, 
                          error_type: str = "api_error", event_data: Optional[Dict[str, Any]] = None,
                          expected_revision: Optional[str] = None) -> bool:
        """
        Сохраняет проблемный заказ для последующей переобработки.
        
        Args:
            order_id: ID заказа 
            action_required: Требуемое действие (create, update, skip)
            error_message: Сообщение об ошибке
            error_type: Тип ошибки
            event_data: Данные события
            expected_revision: Ожидаемая revision
            
        Returns:
            bool: True если успешно сохранен
        """
        try:
            from uuid import UUID
            
            # Проверяем, нет ли уже такого проблемного заказа в статусе PENDING/RETRYING
            from sqlmodel import select
            existing_query = select(FailedOrderProcessing).where(
                FailedOrderProcessing.order_id == order_id,
                FailedOrderProcessing.token_id == UUID(self.token_id),
                FailedOrderProcessing.status.in_([FailedOrderStatus.PENDING, FailedOrderStatus.RETRYING])
            )
            
            existing_failed = self.db.exec(existing_query).first()
            
            if existing_failed:
                # Обновляем существующую запись
                existing_failed.mark_for_retry(error_message, error_type)
                if event_data:
                    existing_failed.event_data = event_data
                if expected_revision:
                    existing_failed.expected_revision = expected_revision
                    
                logger.info(f"🔄 Обновлена запись проблемного заказа {order_id} (попытка {existing_failed.retry_count})")
            else:
                # Создаем новую запись
                failed_order = FailedOrderProcessing(
                    order_id=order_id,
                    token_id=UUID(self.token_id),
                    action_required=action_required,
                    error_type=error_type,
                    error_message=error_message,
                    event_data=event_data,
                    expected_revision=expected_revision,
                    next_retry_at=datetime.utcnow() + timedelta(minutes=1)  # Первая попытка через 1 минуту
                )
                
                self.db.add(failed_order)
                logger.info(f"💾 Сохранен проблемный заказ {order_id} для переобработки")
            
            self.db.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения проблемного заказа {order_id}: {e}")
            self.db.rollback()
            return False
    
    def process_failed_orders(self, limit: int = 50) -> Dict[str, Any]:
        """
        Переобрабатывает проблемные заказы, которые готовы к повторной попытке.
        
        Args:
            limit: Максимальное количество заказов для обработки за раз
            
        Returns:
            Dict: Статистика переобработки
        """
        if not self.token_id:
            raise ValueError("token_id обязателен для переобработки проблемных заказов")
            
        result = {
            "processed": 0,
            "resolved": 0,
            "failed": 0,
            "abandoned": 0,
            "started_at": datetime.utcnow()
        }
        
        try:
            from sqlmodel import select
            from uuid import UUID
            
            # Получаем проблемные заказы готовые к повторной обработке
            query = select(FailedOrderProcessing).where(
                FailedOrderProcessing.token_id == UUID(self.token_id),
                FailedOrderProcessing.status == FailedOrderStatus.PENDING,
                FailedOrderProcessing.next_retry_at <= datetime.utcnow()
            ).limit(limit)
            
            failed_orders = self.db.exec(query).all()
            
            if not failed_orders:
                logger.info("✅ Проблемных заказов для переобработки не найдено")
                return result
                
            logger.info(f"🔄 Найдено {len(failed_orders)} проблемных заказов для переобработки")
            
            for failed_order in failed_orders:
                try:
                    result["processed"] += 1
                    
                    # Отмечаем как в процессе обработки
                    failed_order.status = FailedOrderStatus.RETRYING
                    failed_order.last_retry_at = datetime.utcnow()
                    self.db.commit()
                    
                    logger.info(f"🔄 Переобработка заказа {failed_order.order_id} (попытка {failed_order.retry_count + 1})")
                    
                    # Пытаемся получить детали заказа
                    order_details = self._get_order_details_from_api(failed_order.order_id)
                    
                    if order_details:
                        # Создаем структуру данных для обработки
                        order_data_item = {
                            "order": order_details,
                            "order_id": failed_order.order_id,
                            "source": "full_api_details"
                        }
                        
                        # Обрабатываем заказ
                        process_result = self._process_single_order_safe(order_data_item)
                        
                        if process_result["success"]:
                            # Успех - помечаем как разрешенный
                            failed_order.mark_resolved()
                            result["resolved"] += 1
                            logger.info(f"✅ Проблемный заказ {failed_order.order_id} успешно обработан: {process_result['action']}")
                        else:
                            # Ошибка обработки
                            error_msg = f"Ошибка обработки заказа: {process_result.get('message', 'Unknown error')}"
                            failed_order.mark_for_retry(error_msg, "processing_error")
                            result["failed"] += 1
                            
                            if failed_order.status == FailedOrderStatus.ABANDONED:
                                result["abandoned"] += 1
                            
                    else:
                        # Не удалось получить детали - отмечаем для retry
                        failed_order.mark_for_retry("Не удалось получить детали заказа", "api_fetch_failed")
                        result["failed"] += 1
                        
                        if failed_order.status == FailedOrderStatus.ABANDONED:
                            result["abandoned"] += 1
                            
                    self.db.commit()
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при переобработке заказа {failed_order.order_id}: {e}")
                    
                    # Откатываем изменения для этого заказа
                    self.db.rollback()
                    
                    # Пытаемся отметить заказ как проблемный
                    try:
                        failed_order.mark_for_retry(f"Неожиданная ошибка: {str(e)}", "unexpected_error")
                        if failed_order.status == FailedOrderStatus.ABANDONED:
                            result["abandoned"] += 1
                        self.db.commit()
                    except:
                        pass
                        
                    result["failed"] += 1
                    
            result["completed_at"] = datetime.utcnow()
            
            logger.info(
                f"✅ Переобработка завершена: обработано {result['processed']}, "
                f"разрешено {result['resolved']}, ошибок {result['failed']}, "
                f"отброшено {result['abandoned']}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка переобработки проблемных заказов: {e}")
            result["error"] = str(e)
            return result
            
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