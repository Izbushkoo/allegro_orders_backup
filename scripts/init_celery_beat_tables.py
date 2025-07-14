"""
@file: scripts/init_celery_beat_tables.py
@description: Инициализация таблиц для sqlalchemy_celery_beat (periodic tasks) в базе данных Postgres
@dependencies: sqlalchemy_celery_beat.session.ModelBase, sqlalchemy, dotenv
"""

import os
from sqlalchemy_celery_beat.session import ModelBase
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()

# Получаем строку подключения из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задана в .env")

engine = create_engine(DATABASE_URL)

if __name__ == "__main__":
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS celery_schema;"))
        conn.commit()
    print(f"Создание таблиц sqlalchemy_celery_beat в базе: {DATABASE_URL}")
    ModelBase.metadata.create_all(engine)
    print("Таблицы успешно созданы (или уже существуют)") 