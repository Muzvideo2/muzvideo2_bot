#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестовый скрипт для проверки учетных данных Vertex AI
"""

import os
import json
import logging
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_vertex_ai.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def test_vertex_ai_credentials():
    """Тестирование учетных данных Vertex AI"""
    try:
        # Проверяем переменные окружения
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        logging.info(f"[TEST] Путь к credentials: {credentials_path}")
        
        if not credentials_path:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS не установлена")
        
        credentials_path = credentials_path.strip(' "')
        logging.info(f"[TEST] Очищенный путь: {credentials_path}")
        
        if not os.path.exists(credentials_path):
            raise RuntimeError(f"Файл учетных данных не найден: {credentials_path}")
        
        # Загружаем учетные данные
        logging.info(f"[TEST] Загружаем credentials из файла...")
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        logging.info(f"[TEST] Credentials загружены успешно")
        logging.info(f"[TEST] Project ID из credentials: {credentials.project_id}")
        logging.info(f"[TEST] Service Account Email: {credentials.service_account_email}")
        
        # Проверяем содержимое файла учетных данных
        with open(credentials_path, 'r', encoding='utf-8') as f:
            creds_data = json.load(f)
            logging.info(f"[TEST] Тип учетной записи: {creds_data.get('type', 'unknown')}")
            project_id_from_file = creds_data.get('project_id', 'unknown')
            logging.info(f"[TEST] Project ID в файле: {project_id_from_file}")
            logging.info(f"[TEST] Private key ID: {creds_data.get('private_key_id', 'unknown')[:10]}...")
        
        # Используем PROJECT_ID из файла учетных данных вместо жестко заданного
        PROJECT_ID = project_id_from_file
        LOCATION = "us-central1"
        
        logging.info(f"[TEST] Инициализируем Vertex AI: project={PROJECT_ID}, location={LOCATION}")
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        logging.info(f"[TEST] Vertex AI инициализирован успешно")
        
        # Пытаемся создать модель
        MODEL_NAME = "gemini-2.5-pro"
        logging.info(f"[TEST] Создаём модель: {MODEL_NAME}")
        model = GenerativeModel(MODEL_NAME)
        logging.info(f"[TEST] Модель создана успешно")
        
        # Пытаемся выполнить простой запрос
        logging.info(f"[TEST] Выполняем тестовый запрос...")
        response = model.generate_content("Привет, это тестовый запрос для проверки работы Vertex AI.")
        logging.info(f"[TEST] Тестовый запрос выполнен успешно")
        logging.info(f"[TEST] Ответ: {response.text[:100]}...")
        
        logging.info(f"[TEST] Все проверки пройдены успешно!")
        return True
        
    except Exception as e:
        logging.error(f"[TEST] Ошибка при тестировании Vertex AI: {e}")
        logging.error(f"[TEST] Тип ошибки: {type(e)}")
        return False

if __name__ == "__main__":
    logging.info("=== ТЕСТ УЧЕТНЫХ ДАННЫХ VERTEX AI ===")
    success = test_vertex_ai_credentials()
    if success:
        logging.info("✅ Тест пройден успешно")
    else:
        logging.error("❌ Тест не пройден")