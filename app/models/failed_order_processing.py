"""
@file: failed_order_processing.py
@description: Модель для хранения проблемных заказов, которые не удалось обработать
@dependencies: Base model, UUID, datetime
"""

from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID, uuid4
import json
from sqlmodel import SQLModel, Field
from app.models.base import BaseModel

class FailedOrderStatus:
    """Статусы проблемных заказов"""
    PENDING = "pending"           # Ожидает повторной обработки
    RETRYING = "retrying"         # В процессе повторной обработки  
    RESOLVED = "resolved"         # Успешно обработан
    ABANDONED = "abandoned"       # Отброшен после множественных попыток

class FailedOrderProcessing(BaseModel, table=True):
    """
    Модель для хранения заказов, которые не удалось обработать из-за ошибок.
    
    Используется для:
    - Retry механизма при временных ошибках API
    - Отложенной обработки проблемных заказов
    - Мониторинга качества работы с Allegro API
    """
    
    __tablename__ = "failed_order_processing"
    
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Основные данные заказа
    order_id: str = Field(index=True, description="ID заказа в Allegro")
    token_id: UUID = Field(index=True, description="ID токена пользователя")
    
    # Информация об ошибке
    error_type: str = Field(description="Тип ошибки (network, api, timeout, etc)")
    error_message: str = Field(description="Сообщение об ошибке")
    error_details_json: Optional[str] = Field(default=None, description="Дополнительные детали ошибки (JSON строка)")
    
    # Контекст обработки
    action_required: str = Field(description="Требуемое действие: create, update, skip")
    event_data_json: Optional[str] = Field(default=None, description="Данные события, вызвавшего проблему (JSON строка)")
    expected_revision: Optional[str] = Field(default=None, description="Ожидаемая revision заказа")
    
    # Статус и попытки
    status: str = Field(default=FailedOrderStatus.PENDING, index=True, description="Текущий статус")
    retry_count: int = Field(default=0, description="Количество попыток обработки")
    max_retries: int = Field(default=5, description="Максимальное количество попыток")
    
    # Временные метки
    first_failed_at: datetime = Field(default_factory=datetime.utcnow, description="Время первой неудачи")
    last_retry_at: Optional[datetime] = Field(default=None, description="Время последней попытки")
    next_retry_at: Optional[datetime] = Field(default=None, index=True, description="Время следующей попытки")
    resolved_at: Optional[datetime] = Field(default=None, description="Время успешного разрешения")
    
    # Приоритет обработки
    priority: int = Field(default=1, description="Приоритет обработки (1-высокий, 10-низкий)")
    
    class Config:
        """Конфигурация модели"""
        json_encoders = {
            UUID: str,
            datetime: lambda v: v.isoformat() if v else None
        }
        
    def __repr__(self) -> str:
        return f"<FailedOrderProcessing(order_id='{self.order_id}', status='{self.status}', retry_count={self.retry_count})>"
        
    def can_retry(self) -> bool:
        """Проверяет, можно ли повторить обработку"""
        return (
            self.status in [FailedOrderStatus.PENDING, FailedOrderStatus.RETRYING] and
            self.retry_count < self.max_retries and
            (self.next_retry_at is None or self.next_retry_at <= datetime.utcnow())
        )
        
    def calculate_next_retry(self) -> datetime:
        """Вычисляет время следующей попытки с экспоненциальным backoff"""
        # Exponential backoff: 1min, 5min, 15min, 1h, 4h
        backoff_minutes = [1, 5, 15, 60, 240]
        
        if self.retry_count < len(backoff_minutes):
            minutes = backoff_minutes[self.retry_count]
        else:
            minutes = 240  # Максимум 4 часа
            
        from datetime import timedelta
        return datetime.utcnow() + timedelta(minutes=minutes)
        
    def mark_for_retry(self, error_message: str, error_type: str = "api_error") -> None:
        """Помечает заказ для повторной обработки"""
        self.retry_count += 1
        self.error_message = error_message
        self.error_type = error_type
        self.last_retry_at = datetime.utcnow()
        
        if self.retry_count >= self.max_retries:
            self.status = FailedOrderStatus.ABANDONED
            self.next_retry_at = None
        else:
            self.status = FailedOrderStatus.PENDING
            self.next_retry_at = self.calculate_next_retry()
            
    def mark_resolved(self) -> None:
        """Помечает заказ как успешно обработанный"""
        self.status = FailedOrderStatus.RESOLVED
        self.resolved_at = datetime.utcnow()
        self.next_retry_at = None
        
    @property
    def error_details(self) -> Optional[Dict[str, Any]]:
        """Возвращает детали ошибки как словарь"""
        if self.error_details_json:
            try:
                return json.loads(self.error_details_json)
            except (json.JSONDecodeError, TypeError):
                return None
        return None
        
    @error_details.setter
    def error_details(self, value: Optional[Dict[str, Any]]) -> None:
        """Устанавливает детали ошибки из словаря"""
        if value is not None:
            self.error_details_json = json.dumps(value, ensure_ascii=False)
        else:
            self.error_details_json = None
            
    @property
    def event_data(self) -> Optional[Dict[str, Any]]:
        """Возвращает данные события как словарь"""
        if self.event_data_json:
            try:
                return json.loads(self.event_data_json)
            except (json.JSONDecodeError, TypeError):
                return None
        return None
        
    @event_data.setter
    def event_data(self, value: Optional[Dict[str, Any]]) -> None:
        """Устанавливает данные события из словаря"""
        if value is not None:
            self.event_data_json = json.dumps(value, ensure_ascii=False)
        else:
            self.event_data_json = None 