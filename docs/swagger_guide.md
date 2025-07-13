# Руководство по использованию Swagger UI

## Доступ к документации

### 🔗 Основные ссылки
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc  
- **OpenAPI JSON**: http://localhost:8000/api/v1/openapi.json

### 📋 Требования
- Запущенный проект (`docker compose up -d`)
- Доступность порта 8000

## Обзор API

### 🏷️ Группы эндпоинтов

#### **General** - Общие операции
- `GET /` - Информация о сервисе
- `GET /health` - Проверка состояния
- `GET /config` - Конфигурация приложения

#### **Tokens** - Управление токенами
- `POST /api/v1/tokens/` - Создать токен
- `GET /api/v1/tokens/` - Список токенов
- `GET /api/v1/tokens/{token_id}` - Получить токен
- `PUT /api/v1/tokens/{token_id}` - Обновить токен
- `DELETE /api/v1/tokens/{token_id}` - Удалить токен
- `POST /api/v1/tokens/{token_id}/refresh` - Обновить через refresh token
- `GET /api/v1/tokens/user/{user_id}` - Токены пользователя

#### **Orders** - Операции с заказами
- `GET /api/v1/orders/` - Список заказов
- `GET /api/v1/orders/{order_id}` - Получить заказ
- `GET /api/v1/orders/allegro/{allegro_order_id}` - Заказ по Allegro ID
- `DELETE /api/v1/orders/{order_id}` - Удалить заказ
- `GET /api/v1/orders/user/{user_id}` - Заказы пользователя
- `GET /api/v1/orders/stats/summary` - Статистика заказов
- `GET /api/v1/orders/events/` - События заказов
- `GET /api/v1/orders/{order_id}/events` - События заказа

#### **Sync** - Синхронизация
- `POST /api/v1/sync/start` - Запустить синхронизацию
- `POST /api/v1/sync/start/user/{user_id}` - Синхронизация пользователя
- `POST /api/v1/sync/start/token/{token_id}` - Синхронизация токена
- `GET /api/v1/sync/history` - История синхронизаций
- `GET /api/v1/sync/status/{sync_id}` - Статус синхронизации
- `POST /api/v1/sync/cancel/{sync_id}` - Отменить синхронизацию
- `GET /api/v1/sync/stats` - Статистика синхронизаций
- `GET /api/v1/sync/running` - Активные синхронизации
- `GET /api/v1/sync/task/{task_id}` - Статус задачи Celery

## Тестирование эндпоинтов

### 🚀 Быстрый старт

1. **Откройте Swagger UI**: http://localhost:8000/docs
2. **Проверьте health check**:
   - Разверните секцию "general"
   - Нажмите на `GET /health`
   - Нажмите "Try it out" → "Execute"
   - Должен вернуться статус `healthy`

### 📝 Пример тестирования

#### Проверка конфигурации
```bash
# Через curl
curl -X GET "http://localhost:8000/config" | jq

# Через Swagger UI
# 1. Перейдите к GET /config
# 2. Нажмите "Try it out"
# 3. Нажмите "Execute"
```

#### Получение списка токенов
```bash
# Через curl
curl -X GET "http://localhost:8000/api/v1/tokens/" | jq

# Через Swagger UI
# 1. Перейдите к GET /api/v1/tokens/
# 2. Нажмите "Try it out"
# 3. Настройте параметры фильтрации (опционально)
# 4. Нажмите "Execute"
```

### 🔧 Параметры запросов

#### Фильтрация и пагинация
Большинство эндпоинтов поддерживают:
- `page` - номер страницы (по умолчанию 1)
- `per_page` - элементов на странице (1-100)
- `user_id` - фильтр по пользователю
- `date_from` / `date_to` - фильтр по датам

#### Форматы данных
- **DateTime**: `2024-01-15T10:30:00`
- **UUID**: `550e8400-e29b-41d4-a716-446655440000`
- **JSON**: Используется для сложных данных

### 📊 Модели данных

#### TokenCreate
```json
{
  "user_id": "user123",
  "allegro_token": "Bearer token...",
  "refresh_token": "refresh_token...",
  "expires_at": "2024-01-15T10:30:00"
}
```

#### SyncTrigger
```json
{
  "user_id": "user123",
  "token_id": "550e8400-e29b-41d4-a716-446655440000",
  "sync_from_date": "2024-01-01T00:00:00",
  "sync_to_date": "2024-01-15T23:59:59",
  "force_full_sync": false
}
```

## Текущий статус

### ✅ Готово к тестированию
- Все эндпоинты доступны
- Swagger UI полностью функционален
- Документация содержит подробные описания
- Модели данных с валидацией

### ⚠️ Ограничения
- Все эндпоинты возвращают HTTP 501 "Не реализовано"
- Это заготовки для будущей реализации
- Используйте для изучения API структуры

## Следующие шаги

1. **Изучите API структуру** через Swagger UI
2. **Протестируйте базовые эндпоинты** (health, config)
3. **Ознакомьтесь с моделями данных**
4. **Начните реализацию** конкретных эндпоинтов

---

**Обновлено**: 2024-01-15  
**Статус**: Готово к использованию 