"""
@file: app/celery_app.py
@description: Конфигурация Celery приложения
@dependencies: celery, redis
"""

from celery import Celery
from celery.schedules import crontab

from app.core.settings import settings
from app.core.logging import setup_logging, get_logger

# Инициализация логирования
setup_logging()
logger = get_logger(__name__)

# Логирование настроек Celery
logger.info("Initializing Celery application...")
config_info = settings.log_configuration()

# Логируем настройки Celery
logger.info("Celery configuration:", extra={
    "broker_url": config_info["celery"]["broker_url"],
    "result_backend": config_info["celery"]["result_backend"],
    "timezone": config_info["celery"]["timezone"],
    "environment_variables": {
        k: v for k, v in config_info["environment_variables"].items() 
        if k.startswith('CELERY_')
    }
})

# Создание Celery приложения
celery_app = Celery(
    "allegro_orders_backup",
    broker=settings.celery.broker_url,
    backend=settings.celery.result_backend,
    include=[
        "app.tasks.token_tasks",
        "app.tasks.sync_tasks", 
        "app.tasks.cleanup_tasks",
    ]
)

# Конфигурация Celery
celery_app.conf.update(
    task_serializer=settings.celery.task_serializer,
    result_serializer=settings.celery.result_serializer,
    accept_content=settings.celery.accept_content,
    timezone=settings.celery.timezone,
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 минут
    task_soft_time_limit=25 * 60,  # 25 минут
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# Расписание задач
celery_app.conf.beat_schedule = {
    # Обновление токенов каждые 30 минут
    "refresh-tokens": {
        "task": "app.tasks.token_tasks.refresh_all_tokens",
        "schedule": crontab(minute="*/30"),
    },
    
    # Проверка событий заказов каждые 3 минуты
    "check-order-events": {
        "task": "app.tasks.sync_tasks.sync_order_events",
        "schedule": crontab(minute="*/3"),
    },
    
    # Полная синхронизация заказов каждые 6 часов
    "full-sync-orders": {
        "task": "app.tasks.sync_tasks.full_sync_all_orders",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    
    # Очистка старых записей ежедневно в 2:00
    "cleanup-old-data": {
        "task": "app.tasks.cleanup_tasks.cleanup_old_sync_history",
        "schedule": crontab(hour=2, minute=0),
    },
    
    # Очистка старых событий еженедельно в воскресенье в 3:00
    "cleanup-old-events": {
        "task": "app.tasks.cleanup_tasks.cleanup_old_order_events",
        "schedule": crontab(hour=3, minute=0, day_of_week=0),
    },
}

logger.info("Celery application configured successfully") 