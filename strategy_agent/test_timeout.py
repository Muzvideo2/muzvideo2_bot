#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест функциональности тайм-аута для проверки исправления зависания скрипта
"""

import sys
import os
import time
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Добавляем текущую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_timeout_functionality():
    """Тест функциональности тайм-аута"""
    try:
        from client_card_analyzer import with_timeout, TimeoutError
        
        @with_timeout(5)  # 5 секунд тайм-аут
        def slow_function():
            """Функция, которая занимает много времени"""
            time.sleep(10)  # Спим 10 секунд (больше тайм-аута)
            return "Не должно дойти до этого!"
        
        @with_timeout(5)  # 5 секунд тайм-аут  
        def fast_function():
            """Быстрая функция"""
            time.sleep(2)  # Спим 2 секунды (меньше тайм-аута)
            return "Успех!"
        
        # Тест 1: Функция, которая должна завершиться по тайм-ауту
        logging.info("=== ТЕСТ 1: Проверка тайм-аута ===")
        try:
            result = slow_function()
            logging.error(f"ОШИБКА: Функция не была прервана по тайм-ауту: {result}")
            return False
        except TimeoutError as e:
            logging.info(f"✅ УСПЕХ: Тайм-аут сработал правильно: {e}")
        except Exception as e:
            logging.error(f"ОШИБКА: Неожиданное исключение: {e}")
            return False
        
        # Тест 2: Функция, которая должна завершиться успешно
        logging.info("=== ТЕСТ 2: Проверка успешного выполнения ===")
        try:
            result = fast_function()
            logging.info(f"✅ УСПЕХ: Быстрая функция выполнилась: {result}")
        except Exception as e:
            logging.error(f"ОШИБКА: Быстрая функция не выполнилась: {e}")
            return False
        
        logging.info("🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        return True
        
    except ImportError as e:
        logging.error(f"Ошибка импорта: {e}")
        return False
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    logging.info("Запуск тестов функциональности тайм-аута...")
    success = test_timeout_functionality()
    
    if success:
        logging.info("✅ Все тесты прошли успешно! Тайм-аут работает корректно.")
        sys.exit(0)
    else:
        logging.error("❌ Тесты провалились! Проверьте реализацию тайм-аута.")
        sys.exit(1)