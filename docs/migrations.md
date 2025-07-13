# Управление миграциями базы данных

## Обзор

Проект использует Alembic для управления миграциями базы данных. Все команды автоматизированы через Makefile и Python скрипты.

## Быстрый старт

### 1. Запустить инфраструктуру
```bash
make env    # Создать .env файл
make up     # Запустить сервисы
```

### 2. Создать начальную миграцию
```bash
make init-db  # Создать и применить начальную миграцию
```

### 3. Проверить результат
```bash
make db-current  # Показать текущую ревизию БД
```

## Команды управления миграциями

### Через Makefile (рекомендуется)
```bash
make migration    # Создать новую миграцию
make upgrade      # Применить все миграции  
make db-current   # Показать текущую ревизию
make db-history   # Показать историю миграций
```

### Через Docker
```bash
docker compose exec app python scripts/create_migration.py create -m "Описание"
docker compose exec app python scripts/create_migration.py upgrade
docker compose exec app python scripts/create_migration.py current
docker compose exec app python scripts/create_migration.py history
```

## Рабочий процесс

1. **Изменить модель SQLModel**
2. **Создать миграцию**: `make migration`
3. **Применить миграцию**: `make upgrade`
4. **Проверить результат**: `make db-current` 