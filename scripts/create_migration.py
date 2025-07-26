#!/usr/bin/env python3
"""
@file: scripts/create_migration.py
@description: Скрипт для создания миграций Alembic
@dependencies: alembic
"""

import os
import sys
import subprocess
from pathlib import Path

# Добавляем путь к приложению
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from app.core.logging import setup_logging, get_logger

# Настройка логирования
setup_logging()
logger = get_logger(__name__)


def create_migration(message: str = None, autogenerate: bool = True):
    """
    Создает новую миграцию Alembic
    
    Args:
        message: Сообщение для миграции
        autogenerate: Использовать автогенерацию
    """
    if not message:
        message = input("Введите описание миграции: ").strip()
        if not message:
            message = "Auto-generated migration"
    
    logger.info(f"Creating migration: {message}")
    
    # Формируем команду
    cmd = ["alembic", "revision"]
    
    if autogenerate:
        cmd.append("--autogenerate")
    
    cmd.extend(["-m", message])
    
    try:
        # Переходим в корневую директорию проекта
        os.chdir(PROJECT_ROOT)
        
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        logger.info("Migration created successfully!")
        logger.info(f"Output: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"Warnings: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create migration: {e}")
        logger.error(f"Error output: {e.stderr}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


def upgrade_database():
    """Применяет миграции к базе данных"""
    logger.info("Upgrading database...")
    
    try:
        os.chdir(PROJECT_ROOT)
        
        cmd = ["alembic", "upgrade", "head"]
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        logger.info("Database upgraded successfully!")
        logger.info(f"Output: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"Warnings: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to upgrade database: {e}")
        logger.error(f"Error output: {e.stderr}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


def show_current_revision():
    """Показывает текущую ревизию базы данных"""
    try:
        os.chdir(PROJECT_ROOT)
        
        cmd = ["alembic", "current"]
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        logger.info("Current database revision:")
        logger.info(result.stdout or "No revision found")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get current revision: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def show_migration_history():
    """Показывает историю миграций"""
    try:
        os.chdir(PROJECT_ROOT)
        
        cmd = ["alembic", "history", "--verbose"]
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        logger.info("Migration history:")
        logger.info(result.stdout or "No migrations found")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get migration history: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Alembic migration management")
    parser.add_argument("action", choices=["create", "upgrade", "current", "history"], 
                       help="Action to perform")
    parser.add_argument("-m", "--message", help="Migration message")
    parser.add_argument("--no-autogenerate", action="store_true", 
                       help="Disable autogeneration")
    
    args = parser.parse_args()
    
    if args.action == "create":
        create_migration(
            message=args.message,
            autogenerate=not args.no_autogenerate
        )
    elif args.action == "upgrade":
        upgrade_database()
    elif args.action == "current":
        show_current_revision()
    elif args.action == "history":
        show_migration_history() 