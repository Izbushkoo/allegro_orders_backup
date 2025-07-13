# Allegro Orders Backup - Makefile
# Команды для управления проектом

.PHONY: help build up down logs shell migration upgrade db-current db-history clean

# Показать справку
help:
	@echo "Доступные команды:"
	@echo "  build         - Пересобрать Docker образы"
	@echo "  up            - Запустить все сервисы"
	@echo "  down          - Остановить все сервисы"
	@echo "  logs          - Показать логи всех сервисов"
	@echo "  shell         - Подключиться к контейнеру приложения"
	@echo "  migration     - Создать новую миграцию"
	@echo "  upgrade       - Применить миграции к БД"
	@echo "  db-current    - Показать текущую ревизию БД"
	@echo "  db-history    - Показать историю миграций"
	@echo "  clean         - Очистить Docker образы и volumes"

# Docker команды
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

shell:
	docker compose exec app bash

# Команды для работы с базой данных и миграциями
migration:
	@echo "Создание новой миграции..."
	@read -p "Введите описание миграции: " message; \
	docker compose exec app python scripts/create_migration.py create -m "$$message"

upgrade:
	@echo "Применение миграций к базе данных..."
	docker compose exec app python scripts/create_migration.py upgrade

db-current:
	@echo "Текущая ревизия базы данных:"
	docker compose exec app python scripts/create_migration.py current

db-history:
	@echo "История миграций:"
	docker compose exec app python scripts/create_migration.py history

# Создание первой миграции с автогенерацией
init-db:
	@echo "Создание начальной миграции..."
	docker compose exec app python scripts/create_migration.py create -m "Initial database schema"
	@echo "Применение начальной миграции..."
	docker compose exec app python scripts/create_migration.py upgrade

# Полная очистка
clean:
	docker compose down -v
	docker system prune -f

# Перезапуск с пересборкой
restart: down build up

# Быстрый запуск для разработки
dev: up logs

# Создание env файла из примера
env:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env файл создан из .env.example"; \
	else \
		echo ".env файл уже существует"; \
	fi 