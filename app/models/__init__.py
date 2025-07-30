"""
@file: app/models/__init__.py  
@description: Модели данных для SQLModel ORM
@dependencies: sqlmodel, pydantic
"""

from .base import BaseModel
from .user_token import (
    UserToken, 
    UserTokenCreate, 
    UserTokenRead, 
    UserTokenUpdate
)
from .order import (
    Order, 
    OrderCreate, 
    OrderRead, 
    OrderUpdate, 
    OrderSummary
)
from .order_event import (
    OrderEvent, 
    OrderEventCreate, 
    OrderEventRead, 
    OrderEventUpdate
)
from .sync_history import (
    SyncHistory, 
    SyncHistoryCreate, 
    SyncHistoryRead, 
    SyncHistoryUpdate,
    SyncStatus,
    SyncStatistics
)
from .order_technical_flags import (
    OrderTechnicalFlags,
    OrderTechnicalFlagsCreate,
    OrderTechnicalFlagsRead,
    OrderTechnicalFlagsUpdate,
    StockStatusUpdate,
    InvoiceStatusUpdate,
    OrderWithTechnicalFlags
)
from .failed_order_processing import (
    FailedOrderProcessing,
    FailedOrderStatus
)

__all__ = [
    "BaseModel",
    "UserToken",
    "UserTokenCreate", 
    "UserTokenRead",
    "UserTokenUpdate",
    "Order",
    "OrderCreate",
    "OrderRead",
    "OrderUpdate",
    "OrderSummary",
    "OrderEvent",
    "OrderEventCreate",
    "OrderEventRead",
    "OrderEventUpdate",
    "SyncHistory",
    "SyncHistoryCreate",
    "SyncHistoryRead",
    "SyncHistoryUpdate",
    "SyncStatus",
    "SyncStatistics",
    "OrderTechnicalFlags",
    "OrderTechnicalFlagsCreate",
    "OrderTechnicalFlagsRead",
    "OrderTechnicalFlagsUpdate",
    "StockStatusUpdate",
    "InvoiceStatusUpdate",
    "OrderWithTechnicalFlags",
    "FailedOrderProcessing",
    "FailedOrderStatus",
] 