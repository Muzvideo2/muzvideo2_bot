#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для экспорта данных клиента из базы данных в JSON-файл.

Этот скрипт извлекает данные клиента из таблиц user_profiles и purchased_products
и сохраняет их в JSON-файл для последующего анализа.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_exporter.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Получение DATABASE_URL из переменных окружения
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Создает и возвращает соединение с базой данных."""
    if not DATABASE_URL:
        raise ConnectionError("Переменная окружения DATABASE_URL не установлена!")
    return psycopg2.connect(DATABASE_URL)

def fetch_client_data(conv_id):
    """
    Извлекает данные клиента из таблицы user_profiles по conv_id.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
        
    Returns:
        dict: Данные клиента или None, если не найдены
    """
    logger.info(f"Извлечение данных клиента с conv_id: {conv_id}")
    
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM user_profiles WHERE conv_id = %s", (conv_id,))
            client_data = cur.fetchone()
            
            if client_data:
                logger.info(f"Данные клиента успешно извлечены")
                return dict(client_data)
            else:
                logger.warning(f"Данные клиента с conv_id {conv_id} не найдены")
                return None
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных клиента: {e}")
        raise
    finally:
        if conn:
            conn.close()

def fetch_purchased_products(conv_id):
    """
    Извлекает список купленных продуктов из таблицы purchased_products по conv_id.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
        
    Returns:
        list: Список купленных продуктов
    """
    logger.info(f"Извлечение списка купленных продуктов для conv_id: {conv_id}")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT product_name FROM purchased_products WHERE conv_id = %s", (conv_id,))
            products = [row[0] for row in cur.fetchall()]
            logger.info(f"Извлечено {len(products)} купленных продуктов")
            return products
    except Exception as e:
        logger.error(f"Ошибка при извлечении купленных продуктов: {e}")
        raise
    finally:
        if conn:
            conn.close()
def fetch_recent_messages(conv_id, limit=30):
    """
    Извлекает последние сообщения из таблицы dialogues по conv_id.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
        limit (int): Количество сообщений для извлечения (по умолчанию 30)
        
    Returns:
        list: Список последних сообщений, отсортированных по времени (от новых к старым)
    """
    logger.info(f"Извлечение последних {limit} сообщений для conv_id: {conv_id}")
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role, message 
                FROM dialogues 
                WHERE conv_id = %s 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (conv_id, limit))
            messages = [{"sender": row[0], "text": row[1]} for row in cur.fetchall()]
            logger.info(f"Извлечено {len(messages)} последних сообщений")
            return messages
    except Exception as e:
        logger.error(f"Ошибка при извлечении последних сообщений: {e}")
        raise
    finally:
        if conn:
            conn.close()

def transform_client_data(client_data, purchased_products, recent_messages):
    """
    Преобразует данные клиента в формат, ожидаемый client_card_analyzer.py.
    
    Args:
        client_data (dict): Данные клиента из БД
        purchased_products (list): Список купленных продуктов
recent_messages (list): Список последних сообщений
        
    Returns:
        dict: Преобразованные данные клиента
    """
    logger.info("Преобразование данных клиента в формат JSON")
    
    # Преобразуем данные в формат, ожидаемый анализатором
    transformed_data = {
        "client_id": str(client_data.get("conv_id", "")),
        "client_level": client_data.get("client_level", []),
        "learning_goals": client_data.get("learning_goals", []),
        "purchased_products": purchased_products,
        "client_pains": client_data.get("client_pains", []),
        "email": client_data.get("email", []),
        "lead_qualification": client_data.get("lead_qualification", []),
        "funnel_stage": client_data.get("funnel_stage", ""),
        "client_activity": client_data.get("client_activity", ""),
        "dialogue_summary": client_data.get("dialogue_summary", ""),
        "recent_messages": recent_messages
    }
    
    logger.info("Данные клиента успешно преобразованы")
    return transformed_data

def save_to_json(data, conv_id):
    """
    Сохраняет данные в JSON-файл.
    
    Args:
        data (dict): Данные для сохранения
        conv_id (int): Идентификатор диалога клиента
        
    Returns:
        str: Путь к созданному файлу
    """
    logger.info("Сохранение данных в JSON-файл")
    
    # Создание директории exported_data, если она не существует
    output_dir = "exported_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Формирование имени файла с timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"client_data_{conv_id}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Сохранение данных в файл
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Данные успешно сохранены в файл: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в файл: {e}")
        raise

def main(conv_id):
    """
    Основная функция скрипта.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
    """
    logger.info(f"Запуск экспорта данных для клиента с conv_id: {conv_id}")
    
    try:
        # 1. Извлечение данных клиента
        client_data = fetch_client_data(conv_id)
        if not client_data:
            logger.error(f"Данные клиента с conv_id {conv_id} не найдены")
            print(f"Ошибка: Данные клиента с conv_id {conv_id} не найдены")
            sys.exit(1)
        
        # 2. Извлечение списка купленных продуктов
        purchased_products = fetch_purchased_products(conv_id)
        
        # 3. Извлечение последних сообщений
        recent_messages = fetch_recent_messages(conv_id)
        
        # 4. Преобразование данных в нужный формат
        transformed_data = transform_client_data(client_data, purchased_products, recent_messages)
        
        # 5. Сохранение данных в JSON-файл
        filepath = save_to_json(transformed_data, conv_id)
        
        # 6. Вывод пути к файлу
        print(f"Данные клиента успешно экспортированы в файл: {filepath}")
        logger.info(f"Экспорт данных завершен. Файл: {filepath}")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных клиента: {e}")
        print(f"Ошибка при экспорте данных клиента: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description="Экспорт данных клиента из базы данных в JSON-файл")
    parser.add_argument("conv_id", type=int, help="Идентификатор диалога клиента (conv_id)")
    
    args = parser.parse_args()
    
    # Запуск основной функции
    main(args.conv_id)