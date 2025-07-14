# Allegro Orders Backup

Микросервис для резервного копирования заказов из Allegro API с использованием Device Code Flow и event-driven синхронизации.

## Функции

- 🔐 Автоматическая авторизация через Device Code Flow
- 📦 Event-driven синхронизация заказов
- 🔄 Автоматический рефреш токенов
- 📊 Мониторинг и логирование
- 🐳 Docker Compose развертывание
- 📈 Celery для фоновых задач

## Быстрый старт

### 1. Подготовка окружения

```bash
# Клонирование и переход в директорию
git clone <repository-url>
cd allegro_orders_backup

# Копирование конфигурации
cp .env.example .env
```

### 2. Настройка конфигурации

Отредактируйте `.env` файл, указав ваши данные:

```env
# Allegro API
ALLEGRO_CLIENT_ID=your_client_id_here
ALLEGRO_CLIENT_SECRET=your_client_secret_here

# Database
DATABASE_PASSWORD=secure_password_here

# Security
SECRET_KEY=very_long_and_secure_secret_key_here
```

### 3. Запуск через Docker Compose

```bash
# Запуск всех сервисов
docker compose up -d

# Просмотр логов
docker compose logs -f app

# Запуск с мониторингом Flower
docker compose --profile monitoring up -d
```

### 4. Инициализация базы данных

```bash
# Создание миграций
docker compose exec app poetry run alembic revision --autogenerate -m "Initial migration"

# Применение миграций
docker compose exec app poetry run alembic upgrade head
```

## Разработка

### Локальная установка

```bash
# Установка Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Установка зависимостей
poetry install

# Активация виртуального окружения
poetry shell
```

### Запуск для разработки

```bash
# Запуск только инфраструктуры
docker compose up -d postgres redis

# Запуск приложения локально
poetry run uvicorn app.main:app --reload

# Запуск Celery worker
poetry run celery -A app.celery_app worker --loglevel=info

# Запуск Celery beat
poetry run celery -A app.celery_app beat --loglevel=info
```

### База данных

```bash
# Создание новой миграции
poetry run alembic revision --autogenerate -m "Description"

# Применение миграций
poetry run alembic upgrade head

# Откат миграций
poetry run alembic downgrade -1
```

## API Документация

После запуска приложения документация доступна по адресам:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/api/v1/openapi.json

## Основные эндпоинты

### Мониторинг
- `GET /` - Информация о сервисе
- `GET /health` - Health check

### Управление токенами (✅ Реализовано)
- `GET /api/v1/tokens/` - Список токенов с пагинацией
- `POST /api/v1/tokens/` - Создание токена вручную
- `GET /api/v1/tokens/{token_id}` - Получение конкретного токена
- `PUT /api/v1/tokens/{token_id}` - Обновление токена
- `DELETE /api/v1/tokens/{token_id}` - Удаление токена
- `GET /api/v1/tokens/user/{user_id}` - Токены пользователя

### Device Code Flow авторизация (✅ Реализовано)
- `POST /api/v1/tokens/auth/initialize` - Инициализация авторизации с автоматическим polling
- `GET /api/v1/tokens/auth/status/{device_code}` - Проверка статуса авторизации (ручная)
- `GET /api/v1/tokens/auth/task/{task_id}` - Отслеживание прогресса авторизации
- `POST /api/v1/tokens/{token_id}/refresh` - Обновление токена
- `POST /api/v1/tokens/{token_id}/validate` - Проверка и автообновление токена

### Будущие эндпоинты (в разработке)
- `POST /api/v1/sync/manual/{user_id}` - Ручная синхронизация
- `GET /api/v1/orders/{user_id}` - Получение заказов
- `GET /api/v1/sync/history/{user_id}` - История синхронизации

## Мониторинг

### Flower (Celery UI)
```bash
# Запуск с мониторингом
docker compose --profile monitoring up -d

# Доступ к Flower
http://localhost:5555
```

### Логи
```bash
# Логи приложения
docker compose logs -f app

# Логи Celery worker
docker compose logs -f celery_worker

# Логи Celery beat
docker compose logs -f celery_beat
```

## Архитектура

### Компоненты
- **FastAPI** - REST API сервер
- **PostgreSQL** - Основная база данных
- **Redis** - Брокер сообщений для Celery
- **Celery** - Фоновые задачи
- **Celery Beat** - Планировщик задач

### Структура проекта
```
app/
├── core/           # Конфигурация и базовые компоненты
├── models/         # SQLModel модели
├── api/            # FastAPI роутеры (в разработке)
├── services/       # Бизнес логика (в разработке) 
├── tasks/          # Celery задачи
└── utils/          # Утилиты (в разработке)

alembic/            # Миграции базы данных
docs/               # Документация проекта
```

## Статус разработки

✅ **Завершено (Этап 1 - Базовая инфраструктура)**:
- Настройка Poetry и зависимостей
- Docker Compose конфигурация
- SQLModel модели базы данных
- Настройка Alembic миграций
- FastAPI приложение с логированием
- Базовая настройка Celery

🚧 **В разработке**:
- Allegro API интеграция
- Синхронизация заказов
- API эндпоинты

## Документация

Подробная техническая документация находится в папке `docs/`:

- [Архитектура проекта](docs/project.md)
- [Планы разработки](docs/tasktracker.md)
- [Архитектурные вопросы](docs/qa.md)
- [История изменений](docs/changelog.md)

## Вклад в развитие

1. Fork репозитория
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push в branch (`git push origin feature/amazing-feature`)
5. Создайте Pull Request

## Лицензия

Этот проект лицензирован под MIT License. 

## 🧪 Быстрое тестирование

### 1. Запустить контейнеры
```bash
docker-compose up -d
```

### 2. Создать тестовый JWT токен
```bash
curl -X POST "http://localhost:8000/test-jwt/create?user_id=testuser&username=testing"
```
Скопируйте `access_token` из ответа.

### 3. Проверить JWT аутентификацию
```bash
curl -X GET "http://localhost:8000/test-jwt" \
  -H "Authorization: Bearer <access_token>"
```

### 4. Тестировать синхронизацию заказов
```bash
curl -X POST "http://localhost:8000/api/v1/sync/test" \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json"
```

### 5. Проверить логи
```bash
docker-compose logs -f allegro_app
```

В логах вы увидите:
- Извлечение `user_id` из JWT токена
- Процесс синхронизации с Allegro API
- Защита данных и мониторинг качества

## 📋 API Documentation

Swagger UI: http://localhost:8000/docs  
ReDoc: http://localhost:8000/redoc

## 🎯 Основные endpoints

- **JWT Test**: `POST /test-jwt/create` - создание тестового токена
- **JWT Auth**: `GET /test-jwt` - проверка аутентификации
- **Sync Test**: `POST /api/v1/sync/test` - тестовая синхронизация
- **Full Sync**: `POST /api/v1/sync/start` - полная синхронизация
- **Tokens**: `/api/v1/tokens/*` - управление токенами Allegro
- **Orders**: `/api/v1/orders/*` - работа с заказами 