"""
@file: app/exceptions.py
@description: Кастомные исключения для приложения
@dependencies: fastapi
"""

from fastapi import HTTPException, status


class BaseAppException(Exception):
    """Базовое исключение приложения"""
    pass


class NotFoundError(BaseAppException):
    """Ошибка - ресурс не найден"""
    pass


class ValidationError(BaseAppException):
    """Ошибка валидации данных"""
    pass


class AuthenticationError(BaseAppException):
    """Ошибка аутентификации"""
    pass


class DatabaseError(BaseAppException):
    """Ошибка базы данных"""
    pass


class AllegroAPIError(BaseAppException):
    """Ошибка API Allegro"""
    pass


# HTTP исключения для FastAPI

class TokenNotFoundHTTPException(HTTPException):
    """HTTP исключение - токен не найден"""
    def __init__(self, token_id: str = None):
        detail = f"Token with id {token_id} not found" if token_id else "Token not found"
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class OrderNotFoundHTTPException(HTTPException):
    """HTTP исключение - заказ не найден"""
    def __init__(self, order_id: str = None):
        detail = f"Order with id {order_id} not found" if order_id else "Order not found"
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class UserNotFoundHTTPException(HTTPException):
    """HTTP исключение - пользователь не найден"""
    def __init__(self, user_id: str = None):
        detail = f"User with id {user_id} not found" if user_id else "User not found"
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ValidationHTTPException(HTTPException):
    """HTTP исключение - ошибка валидации"""
    def __init__(self, detail: str = "Validation error"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class ConflictHTTPException(HTTPException):
    """HTTP исключение - конфликт данных"""
    def __init__(self, detail: str = "Conflict"):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail)


class InternalServerErrorHTTPException(HTTPException):
    """HTTP исключение - внутренняя ошибка сервера"""
    def __init__(self, detail: str = "Internal server error"):
        super().__init__(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail) 