#!/bin/bash
set -e

# Функция для ожидания готовности PostgreSQL
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    while ! pg_isready -h ${DATABASE_HOST:-postgres} -p ${DATABASE_PORT:-5432} -U ${DATABASE_USER:-allegro_user}; do
        echo "PostgreSQL is unavailable - sleeping"
        sleep 1
    done
    echo "PostgreSQL is up - executing command"
}

# Функция для запуска миграций
run_migrations() {
    echo "Running database migrations..."
    python -m alembic upgrade head
    echo "Migrations completed successfully"
}

# Функция для инициализации Celery Beat таблиц
init_celery_beat() {
    echo "Initializing Celery Beat tables..."
    python scripts/init_celery_beat_tables.py
    echo "Celery Beat tables initialized"
}

# Ожидание готовности базы данных
wait_for_postgres

# Запуск миграций
run_migrations

# Инициализация Celery Beat (если нужно)
if [[ "$1" == "celery-beat" ]]; then
    init_celery_beat
fi

# Запуск переданной команды
exec "$@"