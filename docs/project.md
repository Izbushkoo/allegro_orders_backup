# Allegro Orders Backup - Микросервис интеграции

## Описание проекта

**Allegro Orders Backup** - это автономный микросервис для интеграции с Allegro API, основная задача которого заключается в создании надежного механизма резервного копирования заказов для зарегистрированных токенов пользователей.

## 🎯 Статус проекта

### ✅ ЭТАП 1: Device Code Flow авторизация - ЗАВЕРШЕН
- **Готовность**: 100%
- **Статус**: Полностью функциональная система авторизации
- **Компоненты**: AllegroAuthService, API эндпоинты, Celery polling, автоматическое создание токенов

### 🚀 ЭТАП 2: Синхронизация заказов - В РАЗРАБОТКЕ  
- **Готовность**: 0%
- **Фокус**: Реализация event-driven синхронизации заказов из Allegro API
- **Цель**: Создание надежного механизма получения и сохранения заказов

## Цели проекта

### Основные цели
- Вынесение всей логики работы с Allegro API в отдельный микросервис
- Автоматическое резервное копирование заказов для каждого зарегистрированного токена
- Обеспечение надежности данных независимо от политик хранения данных Allegro
- Создание простого механизма идентификации пользователей без собственной системы авторизации

### Функциональные требования
- Регистрация и управление токенами через Device Code Flow
- Автоматический рефреш токенов
- Периодическая синхронизация заказов в cron-режиме
- Ручной запуск синхронизации с возможностью указания временных рамок
- Отслеживание периодов последнего обновления
- Обновление существующих данных или пропуск дубликатов

## Архитектура системы

### Компоненты системы

```mermaid
graph TB
    A[FastAPI Application] --> B[Token Manager]
    A --> C[Order Sync Service]
    A --> D[Allegro API Client]
    
    B --> E[PostgreSQL Database]
    C --> E
    
    F[Celery Beat] --> G[Celery Worker]
    G --> C
    G --> B
    
    H[Redis] --> F
    H --> G
    
    D --> I[Allegro API]
```

### Схема базы данных

```sql
-- Токены пользователей
CREATE TABLE user_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL,
    allegro_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(user_id)
);

-- Заказы
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_token_id UUID REFERENCES user_tokens(id) ON DELETE CASCADE,
    allegro_order_id VARCHAR(255) NOT NULL,
    order_data JSONB NOT NULL,
    order_date TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_deleted BOOLEAN DEFAULT FALSE,
    UNIQUE(user_token_id, allegro_order_id)
);

-- История синхронизации
CREATE TABLE sync_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_token_id UUID REFERENCES user_tokens(id) ON DELETE CASCADE,
    sync_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    sync_completed_at TIMESTAMP WITH TIME ZONE,
    sync_status VARCHAR(50) NOT NULL, -- 'running', 'completed', 'failed'
    orders_processed INTEGER DEFAULT 0,
    orders_added INTEGER DEFAULT 0,
    orders_updated INTEGER DEFAULT 0,
    error_message TEXT,
    sync_from_date TIMESTAMP WITH TIME ZONE,
    sync_to_date TIMESTAMP WITH TIME ZONE
);

-- События заказов (на основе Allegro API events)
CREATE TABLE order_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(255) NOT NULL,
    token_id UUID NOT NULL REFERENCES user_tokens(id) ON DELETE CASCADE,
    event_type VARCHAR(100) NOT NULL, -- ORDER_STATUS_CHANGED, PAYMENT_STATUS_CHANGED, etc.
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    event_data JSONB NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(order_id, occurred_at, event_type)
);

-- Индексы для оптимизации производительности
CREATE INDEX idx_orders_token_created ON orders(token_id, created_at);
CREATE INDEX idx_orders_status ON orders USING gin((order_data->>'status'));
CREATE INDEX idx_order_events_occurred ON order_events(occurred_at);
CREATE INDEX idx_sync_history_token_timestamp ON sync_history(token_id, sync_timestamp);
```

## Архитектура Celery очередей по токену

- Для каждого токена создаётся отдельная очередь Celery (sync_{token_id})
- Задачи синхронизации для каждого токена отправляются в свою очередь
- Воркеры Celery масштабируются горизонтально и могут обслуживать одну или несколько очередей
- Все статусы и результаты задач хранятся в таблице task_history (Postgres)
- API позволяет запускать, останавливать и получать статус задач по токену и task_id

### Схема взаимодействия

```mermaid
flowchart TD
    User -- API: start sync --> FastAPI
    FastAPI -- .apply_async(queue=sync_{token_id}) --> Celery
    Celery -- task_id, status --> Postgres[(task_history)]
    FastAPI -- API: status/stop/list --> Postgres
    FastAPI -- revoke(task_id) --> Celery
    Celery -- обновляет статус --> Postgres
    Celery -. масштабируется .-> Celery
```

### Масштабирование
- В docker-compose можно запускать несколько воркеров, каждый обслуживает одну или несколько очередей
- Для высокой нагрузки рекомендуется использовать autoscaling (например, в Kubernetes)
- Очереди создаются динамически по мере появления новых токенов

### Best practices
- Для большого количества токенов очереди можно группировать (например, по пользователю или по хешу токена)
- Для мониторинга использовать таблицу task_history и Celery Flower

## Allegro API Integration

### Device Code Flow авторизации

Для получения токенов доступа к Allegro API используется Device Code Flow - безопасный способ авторизации для приложений без браузера. Процесс полностью автоматизирован через Celery задачи.

```mermaid
sequenceDiagram
    participant User as Пользователь
    participant API as FastAPI App
    participant Allegro as Allegro API
    participant Celery as Celery Worker
    participant DB as База данных

    User->>API: POST /auth/initialize {user_id}
    API->>Allegro: Запрос device_code и user_code
    Allegro->>API: device_code, user_code, verification_uri, expires_in
    
    API->>Celery: Запуск задачи poll_authorization_status
    Celery->>API: task_id
    
    API->>User: Коды авторизации + task_id + ссылка
    
    Note over User: Пользователь переходит по ссылке<br/>и вводит user_code в Allegro
    
    User->>Allegro: Авторизация с user_code
    Allegro->>User: Подтверждение авторизации
    
    loop Автоматический polling (каждые 5 сек, макс 30 попыток)
        Celery->>Allegro: Проверка статуса с device_code
        alt Авторизация ещё не завершена
            Allegro->>Celery: status: pending
            Note over Celery: Ожидание 5 секунд
        else Авторизация завершена
            Allegro->>Celery: access_token + refresh_token
            Celery->>DB: Сохранение токена в UserToken
            DB->>Celery: Токен сохранён
            Note over Celery: Завершение задачи SUCCESS
        end
    end
    
    opt Проверка статуса задачи
        User->>API: GET /auth/task/{task_id}
        API->>Celery: Проверка статуса задачи
        Celery->>API: PENDING/SUCCESS/FAILURE + результат
        API->>User: Статус авторизации
    end
    
    opt Использование токена
        User->>API: GET /user/{user_id} (получить токены)
        API->>DB: Получение активных токенов пользователя
        DB->>API: Список токенов
        API->>User: Активные токены
    end
```

### Ключевые API endpoints

**GET /order/events**:
- **Назначение**: Event-driven синхронизация изменений заказов
- **Rate limit**: 60 запросов/минуту
- **Параметры**: `from` (RFC3339), `limit` (max 1000), `type[]`
- **Использование**: Запрос каждые 2-3 минуты для получения новых событий

**GET /order/checkout-forms**:
- **Назначение**: Получение списка заказов пользователя
- **Rate limit**: 100 запросов/минуту
- **Параметры**: `offset`, `limit`, `status`, `createdAt.gte/lte`
- **Использование**: Полная синхронизация и initial load

**GET /order/checkout-forms/{id}**:
- **Назначение**: Детальная информация о заказе
- **Rate limit**: 1000 запросов/минуту
- **Использование**: Получение полных данных при обработке событий

### Стратегия синхронизации

**Event-driven подход**:
1. **Мониторинг событий**: Регулярный опрос GET /order/events
2. **Типы событий**:
   - ORDER_STATUS_CHANGED - изменение статуса заказа
   - PAYMENT_STATUS_CHANGED - изменение статуса платежа
   - DELIVERY_INFO_CHANGED - изменение данных доставки
   - ORDER_CANCELLED - отмена заказа

3. **Алгоритм обработки**:
   ```python
   # Псевдокод синхронизации
   last_sync = get_last_sync_timestamp(token_id)
   events = allegro_client.get_order_events(from=last_sync)
   
   for event in events:
       if event.type in TRACKED_EVENTS:
           order_data = allegro_client.get_order_details(event.order.id)
           upsert_order(token_id, event.order.id, order_data)
           save_order_event(event)
   
   update_sync_timestamp(token_id, now())
   ```

### Rate Limiting и Error Handling

**Rate Limits**:
- Общие API: 1000 запросов/минуту  
- Orders API: 100 запросов/минуту
- Events API: 60 запросов/минуту
- Device Code Flow: 10 запросов/минуту

**Стратегии retry**:
- 429 Too Many Requests: exponential backoff + Retry-After header
- 5xx errors: exponential backoff (максимум 3 попытки)
- Network timeouts: немедленный retry
- 401 Unauthorized: автоматический refresh токена

**Мониторинг лимитов**:
- Отслеживание X-RateLimit-Remaining header
- Предотвращение превышения лимитов через Celery rate limiting
- Логирование всех 429 ответов

## Технологический стек

### Backend Framework
- **FastAPI** - основной веб-фреймворк
- **SQLModel** - ORM для работы с базой данных
- **Pydantic v2** - валидация данных

### База данных
- **PostgreSQL** - основная база данных
- **Alembic** - миграции базы данных

### Асинхронные задачи
- **Celery** - обработка фоновых задач
- **Celery Beat** - планировщик задач
- **Redis** - брокер сообщений для Celery

### Инфраструктура
- **Docker Compose** - контейнеризация сервисов
- **Poetry** - управление зависимостями
- **python-dotenv** - загрузка переменных окружения

### Дополнительные библиотеки
- **httpx** - HTTP клиент для Allegro API
- **python-multipart** - обработка multipart данных
- **uvicorn** - ASGI сервер

## API Эндпоинты

### Управление токенами
```
# Базовые операции с токенами
POST /api/v1/tokens/                    # Создать токен
GET /api/v1/tokens/                     # Получить список токенов
GET /api/v1/tokens/{token_id}           # Получить токен по ID
PUT /api/v1/tokens/{token_id}           # Обновить токен
DELETE /api/v1/tokens/{token_id}        # Удалить токен (деактивировать)
GET /api/v1/tokens/user/{user_id}       # Получить токены пользователя
POST /api/v1/tokens/{token_id}/refresh  # Обновить токен через refresh_token
POST /api/v1/tokens/{token_id}/validate # Проверить и обновить токен

# Device Code Flow авторизация
POST /api/v1/tokens/auth/initialize     # Инициализация авторизации
GET /api/v1/tokens/auth/status/{device_code} # Проверка статуса авторизации
GET /api/v1/tokens/auth/task/{task_id}  # Статус задачи авторизации
```

### Синхронизация
```
POST /api/v1/sync/manual/{user_id}      # Ручная синхронизация
GET /api/v1/sync/status/{user_id}       # Статус синхронизации
GET /api/v1/sync/history/{user_id}      # История синхронизации
```

### Заказы
```
GET /api/v1/orders/{user_id}            # Получить заказы пользователя
GET /api/v1/orders/{user_id}/{order_id} # Получить конкретный заказ
```

### Мониторинг
```
GET /health                             # Проверка состояния сервиса
GET /metrics                            # Метрики приложения
```

## Celery задачи

### Автоматические задачи
- `refresh_all_tokens()` - рефреш истекающих токенов (каждые 30 минут)
- `sync_order_events()` - проверка новых событий заказов через GET /order/events (каждые 3 минуты)
- `full_sync_all_orders()` - полная синхронизация заказов для всех активных токенов (каждые 6 часов)
- `cleanup_old_sync_history()` - очистка старых записей истории (ежедневно)
- `cleanup_old_order_events()` - очистка старых событий заказов (еженедельно)

### Авторизационные задачи
- `poll_authorization_status(device_code, user_id, expires_at_iso, interval_seconds)` - автоматический polling авторизации пользователя через Device Code Flow (разовая задача на каждую авторизацию)

### Event-driven задачи
- `process_order_events(token_id)` - обработка событий для конкретного токена
- `update_order_from_event(token_id, event_data)` - обновление заказа на основе события
- `sync_missing_orders(token_id, order_ids)` - синхронизация пропущенных заказов

### Ручные задачи
- `sync_user_orders(user_id, from_date=None)` - синхронизация для конкретного пользователя
- `full_resync_user_orders(user_id)` - полная пересинхронизация
- `manual_process_events(user_id, from_date=None)` - ручная обработка событий

## Конфигурация (.env)

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/allegro_backup
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=allegro_backup
DATABASE_USER=user
DATABASE_PASSWORD=password

# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# FastAPI
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=False
LOG_LEVEL=INFO

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Allegro API
ALLEGRO_CLIENT_ID=your_client_id
ALLEGRO_CLIENT_SECRET=your_client_secret
ALLEGRO_API_URL=https://api.allegro.pl

# Logging
LOG_FILE_PATH=./logs/app.log
LOG_MAX_BYTES=5242880  # 5MB
LOG_BACKUP_COUNT=3
```

## Логирование

### Конфигурация логирования
- **Формат**: Структурированные логи в JSON формате
- **Ротация**: Максимум 5MB на файл, не более 3 файлов
- **Уровни**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Выходы**: Файл и stdout (в зависимости от окружения)

### Структура лог-записи
```json
{
    "timestamp": "2024-01-15T10:30:00Z",
    "level": "INFO",
    "logger": "allegro_backup.services.sync",
    "message": "Starting order sync for user",
    "user_id": "user123",
    "correlation_id": "uuid",
    "extra": {}
}
```

## Принципы разработки

### Архитектурные принципы
- **SOLID** - соблюдение принципов объектно-ориентированного программирования
- **DRY** - избежание дублирования кода
- **KISS** - простота и понятность решений
- **Clean Architecture** - разделение на слои и зависимости

### Паттерны проектирования
- **Repository Pattern** - для работы с данными
- **Service Layer** - для бизнес-логики
- **Factory Pattern** - для создания клиентов API
- **Observer Pattern** - для уведомлений о событиях

## Безопасность

### Управление токенами
- Безопасное хранение refresh токенов в базе данных
- Автоматическая ротация токенов
- Шифрование чувствительных данных

### API безопасность
- Валидация всех входящих данных
- Rate limiting для API эндпоинтов
- Логирование всех операций

## Мониторинг и метрики

### Health Checks
- Проверка подключения к базе данных
- Проверка Redis соединения
- Проверка доступности Allegro API

### Метрики
- Количество активных токенов
- Статистика синхронизации
- Производительность API
- Ошибки и исключения

## Автосинхронизация заказов (periodic tasks)

- Для каждого токена можно включить автосинхронизацию через API (POST /sync/activate)
- Для автосинхронизации используется celery-sqlalchemy-scheduler (расписание хранится в Postgres)
- Отключение автосинхронизации — через API (POST /sync/deactivate)
- Список активных автосинхронизаций доступен через API (GET /sync/active)
- Все активные автосинхронизации хранятся в таблице active_sync_schedules (user_id, token_id, interval, статус, last_run, last_success, ...)
- Для работы с celery-sqlalchemy-scheduler используется функция get_alchemy_session
- Best practices: всегда закрывать сессии, использовать уникальные task_name для каждого токена, хранить мониторинг в БД

## Развертывание

### Docker Compose
```yaml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "${API_PORT}:${API_PORT}"
    depends_on:
      - db
      - redis
    env_file:
      - .env

  worker:
    build: .
    command: celery -A app.celery worker -l info
    depends_on:
      - db
      - redis
    env_file:
      - .env

  beat:
    build: .
    command: celery -A app.celery beat -l info
    depends_on:
      - db
      - redis
    env_file:
      - .env

  db:
    image: postgres:15
    environment:
      POSTGRES_DB: ${DATABASE_NAME}
      POSTGRES_USER: ${DATABASE_USER}
      POSTGRES_PASSWORD: ${DATABASE_PASSWORD}
    ports:
      - "${DATABASE_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "${REDIS_PORT}:6379"

volumes:
  postgres_data:
```

## Этапы разработки

### Этап 1: Базовая инфраструктура (Критический)
- Настройка проекта с Poetry
- Создание моделей SQLModel
- Настройка FastAPI приложения
- Docker контейнеризация
- Базовая конфигурация логирования

### Этап 2: Allegro API интеграция (Критический) ✅ 90% ЗАВЕРШЕНО
- ✅ Реализация Device Code Flow с автоматическим polling
- ✅ Создание HTTP клиента для Allegro API (httpx)
- ✅ Управление токенами и их рефреш
- ✅ Полный набор эндпоинтов для работы с токенами
- ✅ Автоматизированный процесс авторизации через Celery
- ✅ AllegroAuthService для всех операций с авторизацией

### Этап 3: Синхронизация заказов (Высокий)
- Реализация получения заказов из Allegro API
- Сохранение и обновление заказов в БД
- Ручная синхронизация через API эндпоинты
- Обработка дубликатов и конфликтов

### Этап 4: Celery интеграция (Высокий)
- Настройка Celery и Redis
- Создание автоматических задач синхронизации
- Планировщик задач с Celery Beat
- Мониторинг и логирование задач

### Этап 5: Продвинутые функции (Средний)
- Incremental синхронизация
- Детальная обработка ошибок
- Метрики и мониторинг
- API для получения статистики

### Этап 6: Тестирование и документация (Средний)
- Unit тесты для всех компонентов
- Integration тесты
- API документация
- Руководство по развертыванию

## Требования к качеству

### Производительность
- Время ответа API: < 200ms для простых операций
- Пропускная способность: > 100 RPS
- Синхронизация больших объемов данных без блокировки

### Надежность
- Uptime: 99.9%
- Автоматическое восстановление после сбоев
- Retry механизмы для внешних API

### Масштабируемость
- Горизонтальное масштабирование Celery workers
- Поддержка множественных экземпляров приложения
- Эффективная работа с растущими объемами данных

---

**Версия документа**: 1.1  
**Дата создания**: 2024-01-15  
**Последнее обновление**: 2024-01-21  
**Автор**: System Architect  
**Статус**: В разработке - Device Code Flow реализован 