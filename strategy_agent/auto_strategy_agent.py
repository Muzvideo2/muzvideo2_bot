#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
АВТОМАТИЧЕСКИЙ СТРАТЕГИЧЕСКИЙ АГЕНТ
Полный цикл: поиск клиентов -> анализ -> постановка напоминаний
"""

import os
import sys
import logging
import importlib.util
from datetime import datetime
from pathlib import Path

# Настройка переменных окружения
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Asus\Documents\MuzVideo2\multi_agent_piano_school\sekrety\muzvideo2-c16c31353196.json'
os.environ['GOOGLE_CLOUD_PROJECT'] = 'muzvideo2'
os.environ['VERTEX_AI_LOCATION'] = 'us-central1'

# Добавляем текущую директорию в путь Python
sys.path.insert(0, str(Path(__file__).parent))

# Импорты модулей
try:
    from search_people import PeopleSearcher
    from client_card_analyzer import ClientCardAnalyzer
except ImportError as e:
    print(f"[ОШИБКА] Не удалось импортировать модули: {e}")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_strategy_agent.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Настройка окружения"""
    logger.info("=== НАСТРОЙКА ОКРУЖЕНИЯ ===")
    
    # Проверяем Google credentials
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_path:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS не установлена")
        return False
    
    creds_path = creds_path.strip(' "')
    logger.info(f"GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")
    
    if not os.path.exists(creds_path):
        logger.error(f"Файл учетных данных не найден: {creds_path}")
        return False
    
    # Проверяем содержимое файла учетных данных
    try:
        import json
        with open(creds_path, 'r', encoding='utf-8') as f:
            creds_data = json.load(f)
            logger.info(f"Тип учетной записи: {creds_data.get('type', 'unknown')}")
            project_id_from_file = creds_data.get('project_id', 'unknown')
            logger.info(f"Project ID в файле: {project_id_from_file}")
            logger.info(f"Service account email: {creds_data.get('client_email', 'unknown')}")
            
            # Обновляем переменные окружения на основе файла учетных данных
            os.environ['GOOGLE_CLOUD_PROJECT'] = project_id_from_file
            logger.info(f"Обновлен GOOGLE_CLOUD_PROJECT: {project_id_from_file}")
    except Exception as e:
        logger.error(f"Ошибка чтения файла учетных данных: {e}")
        return False
    
    logger.info("Окружение настроено успешно")
    return True

def load_latest_search_results():
    """Загружает последние результаты поиска клиентов"""
    try:
        # Ищем последний файл founded_people_*.py
        import glob
        search_files = glob.glob('founded_people_*.py')
        if not search_files:
            logger.error("Не найден файл с результатами поиска клиентов")
            return []
        
        # Берём последний по времени
        latest_file = max(search_files, key=os.path.getctime)
        logger.info(f"Загружаем результаты из: {latest_file}")
        
        # Импортируем модуль
        module_name = latest_file.replace('.py', '')
        spec = importlib.util.spec_from_file_location(module_name, latest_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Получаем список клиентов
        if hasattr(module, 'PRIORITIZED_CONV_IDS'):
            client_list = module.PRIORITIZED_CONV_IDS
            logger.info(f"Загружено {len(client_list)} клиентов для обработки")
            return client_list
        else:
            logger.error(f"Не найден PRIORITIZED_CONV_IDS в {latest_file}")
            return []
            
    except Exception as e:
        logger.error(f"Ошибка загрузки результатов поиска: {e}")
        return []

def analyze_clients(client_list):
    """Анализ клиентов с помощью AI"""
    logger.info("=== AI АНАЛИЗ КЛИЕНТОВ ===")
    
    if not client_list:
        logger.warning("Список клиентов пуст")
        return False
    
    logger.info(f"Обрабатываем {len(client_list)} клиентов")
    print(f"\n⚙️  НАЧИНАЕМ ОБРАБОТКУ {len(client_list)} КЛИЕНТОВ")
    print("\u2139️  Можно остановить обработку нажав Ctrl+C")
    print("="*80)
    
    try:
        analyzer = ClientCardAnalyzer()
        
        success_count = 0
        error_count = 0
        
        for i, conv_id in enumerate(client_list, 1):
            logger.info(f"[{i}/{len(client_list)}] Анализируем клиента {conv_id}")
            
            try:
                # Загружаем данные клиента
                client_data = analyzer.load_client_data_from_db(conv_id)
                
                # Проводим анализ
                try:
                    analysis_result = analyzer.analyze_client_card(client_data)
                    analysis_status = 'normal'
                except Exception as analysis_error:
                    logger.warning(f"AI анализ провалился для {conv_id}: {analysis_error}")
                    analysis_result = analyzer._create_fallback_analysis(client_data)
                    analysis_status = 'fallback'
                
                # Обновляем профиль
                profile_updated = analyzer.update_client_profile(conv_id, analysis_result)
                
                # Создаём напоминание только для качественного анализа
                reminder_created = False
                if analysis_status == 'normal':
                    reminder_created = analyzer.create_strategic_reminder(conv_id, analysis_result)
                else:
                    logger.info(f"Fallback анализ для {conv_id} - напоминание не создаём")
                
                if profile_updated:
                    success_count += 1
                    logger.info(f"[OK] Клиент {conv_id} обработан успешно")
                else:
                    error_count += 1
                    logger.warning(f"[WARN] Проблемы с обработкой клиента {conv_id}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"[ERROR] Ошибка обработки клиента {conv_id}: {e}")
                continue
        
        logger.info(f"Анализ завершён: {success_count} успешно, {error_count} с ошибками")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Критическая ошибка анализа: {e}")
        return False

def main():
    """Главная функция автоматического запуска"""
    start_time = datetime.now()
    
    print("=" * 60)
    print("АВТОМАТИЧЕСКИЙ СТРАТЕГИЧЕСКИЙ АГЕНТ MuzVideo2")
    print(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. Настройка окружения
    if not setup_environment():
        logger.error("Не удалось настроить окружение")
        return 1
    
    # 2. Загрузка результатов поиска
    logger.info("ЭТАП 1/1: Загрузка результатов поиска клиентов")
    try:
        client_list = load_latest_search_results()
        if not client_list:
            logger.error("Не удалось загрузить список клиентов")
            print("⚠️  ОШИБКА: Не найдено клиентов для обработки")
            print("Проверьте, что search_people.py запустился успешно")
            return 1
    except Exception as e:
        logger.error(f"Ошибка на этапе загрузки клиентов: {e}")
        return 1
    
    # 3. Анализ клиентов
    logger.info("ЭТАП 1/1: AI анализ и постановка напоминаний")
    try:
        success = analyze_clients(client_list)  # Обрабатываем всех найденных клиентов
        if not success:
            logger.error("Анализ клиентов завершился неудачно")
            return 1
    except Exception as e:
        logger.error(f"Ошибка на этапе анализа: {e}")
        return 1
    
    # Итоги
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("=" * 60)
    print("АВТОМАТИЧЕСКИЙ ЗАПУСК ЗАВЕРШЁН УСПЕШНО!")
    print(f"Время выполнения: {duration}")
    print(f"Время завершения: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    logger.info("Автоматический запуск завершён успешно")
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[ПРЕРЫВАНИЕ] Получен сигнал остановки")
        logger.info("Работа прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА] {e}")
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)