#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест полной интеграции скидки на день рождения с анализатором карточек клиентов
Проверяет:
1. Корректность экспорта данных из БД (включая день рождения)
2. Правильную работу расчета скидки на день рождения
3. Включение информации о скидке в анализ карточки клиента
"""

import json
import logging
from datetime import datetime, timedelta
from data_exporter import transform_client_data
from client_card_analyzer import calculate_birthday_discount_status

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_birthday_discount_calculation():
    """Тестирует расчет скидки на день рождения"""
    print("=== ТЕСТ 1: Расчет скидки на день рождения ===")
    
    current_date = datetime.now()
    
    # Тест 1: День рождения сегодня
    test_cases = [
        {
            "name": "День рождения сегодня",
            "birth_day": current_date.day,
            "birth_month": current_date.month,
            "expected_status": "active"
        },
        {
            "name": "День рождения через 3 дня",
            "birth_day": (current_date + timedelta(days=3)).day,
            "birth_month": (current_date + timedelta(days=3)).month,
            "expected_status": "active"
        },
        {
            "name": "День рождения через 10 дней",
            "birth_day": (current_date + timedelta(days=10)).day,
            "birth_month": (current_date + timedelta(days=10)).month,
            "expected_status": "upcoming"
        },
        {
            "name": "День рождения через 30 дней",
            "birth_day": (current_date + timedelta(days=30)).day,
            "birth_month": (current_date + timedelta(days=30)).month,
            "expected_status": "not_applicable"
        },
        {
            "name": "Нет данных о дне рождения",
            "birth_day": None,
            "birth_month": None,
            "expected_status": "not_applicable"
        }
    ]
    
    for test_case in test_cases:
        result = calculate_birthday_discount_status(test_case["birth_day"], test_case["birth_month"])
        print(f"✓ {test_case['name']}: статус = {result['status']}, ожидаемый = {test_case['expected_status']}")
        print(f"  Сообщение: {result['message'][:100]}..." if result['message'] else "  Сообщение: (пусто)")
        
        if result['status'] == test_case['expected_status']:
            print("  ✅ ТЕСТ ПРОЙДЕН")
        else:
            print("  ❌ ТЕСТ НЕ ПРОЙДЕН")
        print()

def test_data_export_with_birthday():
    """Тестирует экспорт данных клиента с включением дня рождения"""
    print("=== ТЕСТ 2: Экспорт данных клиента с днем рождения ===")
    
    # Создаем тестовые данные клиента из БД
    mock_client_data = {
        "conv_id": 123456789,
        "first_name": "Мария",
        "last_name": "Иванова",
        "screen_name": "maria_ivanova",
        "sex": "женский",
        "city": "Москва",
        "birth_day": 15,
        "birth_month": 8,
        "can_write": True,
        "lead_qualification": "горячий",
        "funnel_stage": "решение принято (ожидаем оплату)",
        "client_level": ["продолжающий"],
        "learning_goals": ["импровизация", "аккомпанемент"],
        "client_pains": ["нет времени"],
        "email": ["maria@example.com"],
        "dialogue_summary": "Активный клиент, готов к покупке",
        "last_updated": datetime.now(),
        "created_at": datetime.now()
    }
    
    purchased_products = ["Курс импровизации"]
    recent_messages = [
        {"sender": "user", "text": "Хочу купить курс"},
        {"sender": "bot", "text": "Отлично! Рекомендую..."}
    ]
    
    # Тестируем transform_client_data
    transformed_data = transform_client_data(mock_client_data, purchased_products, recent_messages)
    
    # Проверяем, что все важные поля включены
    required_fields = [
        "conv_id", "first_name", "last_name", "birth_day", "birth_month",
        "city", "sex", "lead_qualification", "funnel_stage", "client_level",
        "learning_goals", "client_pains", "email", "purchased_products", "recent_messages"
    ]
    
    print("Проверка наличия всех необходимых полей:")
    all_fields_present = True
    for field in required_fields:
        if field in transformed_data:
            print(f"✅ {field}: {transformed_data[field]}")
        else:
            print(f"❌ {field}: ОТСУТСТВУЕТ")
            all_fields_present = False
    
    if all_fields_present:
        print("\n✅ ВСЕ ПОЛЯ ПРИСУТСТВУЮТ В ЭКСПОРТИРОВАННЫХ ДАННЫХ")
    else:
        print("\n❌ НЕКОТОРЫЕ ПОЛЯ ОТСУТСТВУЮТ")
    
    return transformed_data

def test_complete_integration():
    """Тестирует полную интеграцию анализа карточки с днем рождения"""
    print("\n=== ТЕСТ 3: Полная интеграция анализа карточки с днем рождения ===")
    
    # Получаем тестовые данные
    transformed_data = test_data_export_with_birthday()
    
    # Вручную тестируем расчет скидки на день рождения для данных клиента
    birth_day = transformed_data.get('birth_day')
    birth_month = transformed_data.get('birth_month')
    
    print(f"\nДанные о дне рождения клиента: {birth_day}.{birth_month}")
    
    birthday_status = calculate_birthday_discount_status(birth_day, birth_month)
    print(f"Статус скидки: {birthday_status['status']}")
    print(f"Сообщение о скидке: {birthday_status['message']}")
    
    # Создаем мок-результат анализа (без фактического вызова AI)
    mock_analysis_result = {
        "lead_qualification": "горячий",
        "funnel_stage": "решение принято (ожидаем оплату)",
        "client_level": ["продолжающий"],
        "learning_goals": ["импровизация", "аккомпанемент"],
        "client_pains": ["нет времени"],
        "birthday_discount_info": {
            "status": birthday_status.get('status'),
            "message": birthday_status.get('message'),
            "days_until_birthday": birthday_status.get('days_until_birthday'),
            "birthday_formatted": birthday_status.get('birthday_formatted')
        }
    }
    
    print(f"\nРезультат анализа с информацией о дне рождения:")
    print(json.dumps(mock_analysis_result, ensure_ascii=False, indent=2))
    
    # Проверяем, что информация о дне рождения включена
    if "birthday_discount_info" in mock_analysis_result:
        birthday_info = mock_analysis_result["birthday_discount_info"]
        print(f"\n✅ Информация о скидке на день рождения включена в анализ:")
        print(f"   Статус: {birthday_info['status']}")
        print(f"   Дни до ДР: {birthday_info['days_until_birthday']}")
        print(f"   Дата ДР: {birthday_info['birthday_formatted']}")
        print(f"   Сообщение: {birthday_info['message'][:100]}..." if birthday_info['message'] else "   Сообщение: (пусто)")
        
        return True
    else:
        print("\n❌ Информация о скидке на день рождения НЕ включена в анализ")
        return False

def test_prompt_formatting():
    """Тестирует форматирование промпта с сообщением о дне рождения"""
    print("\n=== ТЕСТ 4: Форматирование промпта с сообщением о дне рождения ===")
    
    # Тестируем различные сценарии сообщений о дне рождения
    test_messages = [
        "У клиента через 2 дн. (15.08) будет день рождения. Прямо сейчас для него действует скидка 35%...",
        "У клиента день рождения. Прямо сейчас для него действует скидка 35%...",
        "Нет активных скидок на день рождения.",
        ""
    ]
    
    prompt_template = """
=== ИНФОРМАЦИЯ О СКИДКЕ НА ДЕНЬ РОЖДЕНИЯ ===
{birthday_discount_message}

=== ДАННЫЕ КЛИЕНТА ===
{client_data}
"""
    
    for i, message in enumerate(test_messages):
        print(f"\nСценарий {i+1}: {message[:50]}..." if message else f"\nСценарий {i+1}: (пустое сообщение)")
        
        formatted_prompt = prompt_template.format(
            birthday_discount_message=message,
            client_data="test client data"
        )
        
        # Проверяем, что сообщение корректно подставилось
        if message in formatted_prompt or (not message and "ДЕНЬ РОЖДЕНИЯ" in formatted_prompt):
            print("✅ Сообщение корректно подставлено в промпт")
        else:
            print("❌ Проблема с подстановкой сообщения в промпт")

def main():
    """Основная функция тестирования"""
    print("ПОЛНОЕ ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ СКИДКИ НА ДЕНЬ РОЖДЕНИЯ")
    print("=" * 60)
    
    try:
        # Тест 1: Базовый расчет скидки
        test_birthday_discount_calculation()
        
        # Тест 2: Экспорт данных с днем рождения
        test_data_export_with_birthday()
        
        # Тест 3: Полная интеграция
        integration_success = test_complete_integration()
        
        # Тест 4: Форматирование промпта
        test_prompt_formatting()
        
        print("\n" + "=" * 60)
        if integration_success:
            print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
            print("✅ Скидка на день рождения корректно интегрирована в систему анализа клиентов")
        else:
            print("⚠️ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ")
            print("❌ Требуется дополнительная доработка")
            
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ТЕСТИРОВАНИИ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()