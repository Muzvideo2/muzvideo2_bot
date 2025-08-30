#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест для проверки функции remove_internal_tags
"""

import re

def remove_internal_tags(message):
    """
    Удаляет внутренние размышления бота, ограниченные тегами <internal> и <internal_analysis>.
    """
    # Удаляем теги <internal> и </internal>
    message = re.sub(r'<internal>.*?</internal>', '', message, flags=re.DOTALL | re.IGNORECASE)
    
    # Удаляем теги <internal_analysis> и </internal_analysis>
    message = re.sub(r'<internal_analysis>.*?</internal_analysis>', '', message, flags=re.DOTALL | re.IGNORECASE)
    
    # Убираем лишние пробелы и переносы строк
    message = re.sub(r'\n\s*\n', '\n', message)  # Убираем множественные переносы
    message = message.strip()  # Убираем пробелы в начале и конце
    
    return message

def test_internal_filter():
    """Тестирует функцию фильтрации internal тегов"""
    
    # Тест 1: Удаление тегов <internal>
    test1 = """Привет! <internal>Тут мои размышления о клиенте</internal> Как дела?"""
    expected1 = "Привет!  Как дела?"
    result1 = remove_internal_tags(test1)
    print(f"Тест 1:")
    print(f"Исходное: {test1}")
    print(f"Результат: {result1}")
    print(f"Ожидаемое: {expected1}")
    print(f"Успех: {result1.strip() == expected1.strip()}\n")
    
    # Тест 2: Удаление тегов <internal_analysis>
    test2 = """<internal_analysis>
Анализирую психологический портрет клиента...
1. Он расстроен
2. Нужно поддержать
</internal_analysis>
Понимаю ваше беспокойство. Давайте разберемся с этим вопросом."""
    expected2 = "Понимаю ваше беспокойство. Давайте разберемся с этим вопросом."
    result2 = remove_internal_tags(test2)
    print(f"Тест 2:")
    print(f"Исходное: {test2}")
    print(f"Результат: {result2}")
    print(f"Ожидаемое: {expected2}")
    print(f"Успех: {result2.strip() == expected2.strip()}\n")
    
    # Тест 3: Оба типа тегов в одном сообщении
    test3 = """<internal_analysis>Анализ клиента</internal_analysis>Привет! <internal>внутренние мысли</internal> Рад помочь!"""
    expected3 = "Привет!  Рад помочь!"
    result3 = remove_internal_tags(test3)
    print(f"Тест 3:")
    print(f"Исходное: {test3}")
    print(f"Результат: {result3}")
    print(f"Ожидаемое: {expected3}")
    print(f"Успех: {result3.strip() == expected3.strip()}\n")
    
    # Тест 4: Сообщение без internal тегов (не должно изменяться)
    test4 = "Обычное сообщение клиенту без внутренних размышлений."
    expected4 = "Обычное сообщение клиенту без внутренних размышлений."
    result4 = remove_internal_tags(test4)
    print(f"Тест 4:")
    print(f"Исходное: {test4}")
    print(f"Результат: {result4}")
    print(f"Ожидаемое: {expected4}")
    print(f"Успех: {result4 == expected4}\n")
    
    # Тест 5: Регистр тегов (должны удаляться независимо от регистра)
    test5 = """<INTERNAL>Большие буквы</INTERNAL>Привет! <Internal_Analysis>Смешанный регистр</Internal_Analysis>Ответ клиенту."""
    expected5 = "Привет! Ответ клиенту."
    result5 = remove_internal_tags(test5)
    print(f"Тест 5:")
    print(f"Исходное: {test5}")
    print(f"Результат: {result5}")
    print(f"Ожидаемое: {expected5}")
    print(f"Успех: {result5.strip() == expected5.strip()}\n")

if __name__ == "__main__":
    print("=== ТЕСТИРОВАНИЕ ФУНКЦИИ ФИЛЬТРАЦИИ INTERNAL ТЕГОВ ===\n")
    test_internal_filter()
    print("=== ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ===")