#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностический скрипт для отладки анализатора карточек
"""

import os
import json
import logging
from dotenv import load_dotenv
import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Константы
PROJECT_ID = "zeta-tracer-462306-r7"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-pro"

def init_gemini():
    """Инициализация Gemini"""
    try:
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS не установлена")
        
        credentials_path = credentials_path.strip(' "')
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        model = GenerativeModel(MODEL_NAME)
        
        print("[OK] Vertex AI инициализирован успешно")
        return model
        
    except Exception as e:
        print(f"[ERROR] Ошибка инициализации Vertex AI: {e}")
        return None

def load_client_data(file_path):
    """Загрузка данных клиента"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[OK] Данные клиента загружены из {file_path}")
        return data
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки данных: {e}")
        return None

def test_simple_prompt(model, client_data):
    """Тест с упрощенным промптом"""
    
    simple_prompt = f"""
Проанализируй данные клиента и верни результат в JSON формате.

ДАННЫЕ КЛИЕНТА:
{json.dumps(client_data, ensure_ascii=False, indent=2)}

Верни результат СТРОГО в формате:
{{
  "lead_qualification": "горячий",
  "funnel_stage": "решение принято",
  "client_level": ["продолжающий"],
  "learning_goals": ["импровизация", "аккомпанемент"],
  "client_pains": ["нет времени"],
  "action_priority": "высокий"
}}
"""
    
    try:
        print("[INFO] Отправляем запрос к Gemini...")
        response = model.generate_content(simple_prompt)
        response_text = response.text.strip()
        
        print("\n=== СЫРОЙ ОТВЕТ ОТ GEMINI ===")
        print(response_text)
        print("=== КОНЕЦ ОТВЕТА ===\n")
        
        # Попытка парсинга
        import re
        
        # Ищем JSON в markdown блоке
        json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
            print("[OK] JSON найден в markdown блоке")
        else:
            # Ищем JSON структуру
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                json_str = response_text[json_start:json_end].strip()
                print(f"[OK] JSON найден в тексте, позиции: {json_start}-{json_end}")
            else:
                json_str = response_text.strip()
                print("[WARN] JSON структура не найдена четко, используем весь текст")
        
        print(f"\n=== ИЗВЛЕЧЕННЫЙ JSON ===")
        print(json_str)
        print("=== КОНЕЦ JSON ===\n")
        
        # Парсим JSON
        result = json.loads(json_str)
        print("[OK] JSON успешно распарсен!")
        print(f"Результат: {json.dumps(result, ensure_ascii=False, indent=2)}")
        
        return result
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] Ошибка парсинга JSON: {e}")
        print(f"Проблемная строка: {repr(json_str)}")
        return None
        
    except Exception as e:
        print(f"[ERROR] Общая ошибка: {e}")
        return None

def main():
    print("=== ДИАГНОСТИКА АНАЛИЗАТОРА КАРТОЧЕК ===")
    
    # Инициализируем Gemini
    model = init_gemini()
    if not model:
        return
    
    # Загружаем данные клиента
    client_data = load_client_data("exported_data/client_data_181601225_20250820_224826.json")
    if not client_data:
        return
    
    print(f"\n[INFO] Анализируем клиента: {client_data.get('client_id')}")
    print(f"[INFO] Сообщений в истории: {len(client_data.get('recent_messages', []))}")
    print(f"[INFO] Текущий этап воронки: {client_data.get('funnel_stage')}")
    
    # Тестируем простой промпт
    result = test_simple_prompt(model, client_data)
    
    if result:
        print("\n[SUCCESS] УСПЕХ! Анализ выполнен корректно")
        
        # Сохраняем результат
        output_file = "debug_analysis_result.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"[INFO] Результат сохранен в {output_file}")
    else:
        print("\n[ERROR] НЕУДАЧА! Не удалось выполнить анализ")

if __name__ == "__main__":
    main()