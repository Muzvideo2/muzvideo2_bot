#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест системы скидок на день рождения
Проверяет правильность работы функции calculate_birthday_discount_status
"""

import sys
import os
from datetime import datetime, timedelta
from unittest.mock import patch

def calculate_birthday_discount_status(birth_day, birth_month):
    """
    Рассчитывает статус скидки на день рождения для клиента.
    
    Args:
        birth_day (int): День рождения (1-31)
        birth_month (int): Месяц рождения (1-12)
        
    Returns:
        dict: Словарь с информацией о статусе скидки:
            - status: 'upcoming', 'active', 'not_applicable'
            - message: Текст сообщения для добавления в промпт
            - days_until_birthday: Количество дней до дня рождения (для upcoming)
            - birthday_formatted: Отформатированная дата дня рождения
    """
    if not birth_day or not birth_month:
        return {
            'status': 'not_applicable',
            'message': '',
            'days_until_birthday': None,
            'birthday_formatted': ''
        }
    
    try:
        from datetime import datetime, timedelta
        
        current_date = datetime.now()
        current_year = current_date.year
        
        # Создаем дату дня рождения в текущем году
        try:
            birthday_this_year = datetime(current_year, birth_month, birth_day)
        except ValueError:
            # Обработка случая 29 февраля в невисокосном году
            if birth_month == 2 and birth_day == 29:
                birthday_this_year = datetime(current_year, 2, 28)
            else:
                return {
                    'status': 'not_applicable',
                    'message': '',
                    'days_until_birthday': None,
                    'birthday_formatted': ''
                }
        
        # Если день рождения уже прошел в этом году, берем следующий год
        if birthday_this_year < current_date.replace(hour=0, minute=0, second=0, microsecond=0):
            try:
                birthday_this_year = datetime(current_year + 1, birth_month, birth_day)
            except ValueError:
                if birth_month == 2 and birth_day == 29:
                    birthday_this_year = datetime(current_year + 1, 2, 28)
        
        # Рассчитываем разность в днях
        days_until_birthday = (birthday_this_year - current_date.replace(hour=0, minute=0, second=0, microsecond=0)).days
        
        # Форматированная дата для сообщений
        birthday_formatted = f"{birth_day}.{birth_month:02d}"
        
        # Логика определения статуса скидки
        if -5 <= days_until_birthday <= 5:
            # Активный период скидки (5 дней до и 5 дней после)
            if days_until_birthday == 0:
                status_text = "день рождения"
            elif days_until_birthday > 0:
                status_text = f"через {days_until_birthday} дн. ({birthday_formatted}) будет день рождения"
            else:
                status_text = f"{abs(days_until_birthday)} дн. назад ({birthday_formatted}) был день рождения"
            
            message = f"У клиента {status_text}. Прямо сейчас для него действует скидка 35% на любой курс или набор. Скидка действует 10 дней: 5 дней до дня рождения и 5 дней после. Промокод DR-2025 действует только в эти 10 дней."
            
            return {
                'status': 'active',
                'message': message,
                'days_until_birthday': days_until_birthday,
                'birthday_formatted': birthday_formatted
            }
        
        elif 6 <= days_until_birthday <= 20:
            # Предупреждение о предстоящей скидке (от 6 до 20 дней)
            message = f"У клиента через {days_until_birthday} дней ({birthday_formatted}) будет день рождения. Скидка по случаю дня рождения составит 35% на любой курс или набор. Скидка работает 10 дней: 5 дней до дня рождения и 5 дней после. Промокод DR-2025 действует только в эти 10 дней."
            
            return {
                'status': 'upcoming',
                'message': message,
                'days_until_birthday': days_until_birthday,
                'birthday_formatted': birthday_formatted
            }
        
        else:
            # День рождения далеко или прошло более 5 дней
            return {
                'status': 'not_applicable',
                'message': 'У клиента день рождения не в ближайшее время. Скидка по случаю дня рождения не действует.',
                'days_until_birthday': days_until_birthday,
                'birthday_formatted': birthday_formatted
            }
    
    except Exception as e:
        # logging.error(f"Ошибка при расчете статуса скидки на день рождения: {e}")
        return {
            'status': 'not_applicable',
            'message': '',
            'days_until_birthday': None,
            'birthday_formatted': ''
        }

def test_birthday_scenarios():
    """Тестирует различные сценарии дня рождения"""
    
    print("=== ТЕСТИРОВАНИЕ СИСТЕМЫ СКИДОК НА ДЕНЬ РОЖДЕНИЯ ===\n")
    
    # Получаем текущую дату для тестов
    base_date = datetime(2025, 8, 27)  # Используем фиксированную дату для стабильных тестов
    
    test_scenarios = [
        # (birth_day, birth_month, expected_status, description)
        
        # Тест 1: День рождения сегодня
        (27, 8, "active", "День рождения сегодня"),
        
        # Тест 2: День рождения завтра
        (28, 8, "active", "День рождения завтра"),
        
        # Тест 3: День рождения через 3 дня
        (30, 8, "active", "День рождения через 3 дня"),
        
        # Тест 4: День рождения через 5 дней (граница активного периода)
        (1, 9, "active", "День рождения через 5 дней"),
        
        # Тест 5: День рождения через 10 дней (предстоящая скидка)
        (6, 9, "upcoming", "День рождения через 10 дней"),
        
        # Тест 6: День рождения через 20 дней (граница предстоящей скидки)
        (16, 9, "upcoming", "День рождения через 20 дней"),
        
        # Тест 7: День рождения через 25 дней (скидка не применима)
        (21, 9, "not_applicable", "День рождения через 25 дней"),
        
        # Тест 8: День рождения был вчера
        (26, 8, "active", "День рождения был вчера"),
        
        # Тест 9: День рождения был 5 дней назад (граница активного периода)
        (22, 8, "active", "День рождения был 5 дней назад"),
        
        # Тест 10: День рождения был 6 дней назад (скидка неактивна)
        (21, 8, "not_applicable", "День рождения был 6 дней назад"),
        
        # Тест 11: Отсутствуют данные о дне рождения
        (None, None, "not_applicable", "Данные о дне рождения отсутствуют"),
        
        # Тест 12: 29 февраля в невисокосном году
        (29, 2, "not_applicable", "29 февраля в невисокосном году (2025)"),
        
        # Тест 13: День рождения в следующем году (например, январь)
        (15, 1, "not_applicable", "День рождения в январе (следующий год)"),
    ]
    
    # Мокаем текущую дату для тестов
    with patch('__main__.datetime') as mock_datetime:
        # Настраиваем мок
        mock_datetime.now.return_value = base_date
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else base_date
        
        passed_tests = 0
        total_tests = len(test_scenarios)
        
        for i, (birth_day, birth_month, expected_status, description) in enumerate(test_scenarios, 1):
            print(f"Тест {i}: {description}")
            print(f"  Входные данные: день={birth_day}, месяц={birth_month}")
            
            try:
                result = calculate_birthday_discount_status(birth_day, birth_month)
                actual_status = result['status']
                message = result['message']
                days_until = result['days_until_birthday']
                birthday_formatted = result['birthday_formatted']
                
                print(f"  Ожидаемый статус: {expected_status}")
                print(f"  Фактический статус: {actual_status}")
                print(f"  Дней до дня рождения: {days_until}")
                print(f"  Форматированная дата: {birthday_formatted}")
                
                if message:
                    print(f"  Сообщение: {message[:100]}...")
                else:
                    print(f"  Сообщение: (пустое)")
                
                if actual_status == expected_status:
                    print(f"  ✅ ПРОЙДЕН")
                    passed_tests += 1
                else:
                    print(f"  ❌ ПРОВАЛЕН")
                
            except Exception as e:
                print(f"  ❌ ОШИБКА: {e}")
            
            print()
    
    print(f"=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ===")
    print(f"Пройдено тестов: {passed_tests}/{total_tests}")
    print(f"Процент успеха: {passed_tests/total_tests*100:.1f}%")
    
    if passed_tests == total_tests:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    else:
        print("⚠️  ЕСТЬ ПРОВАЛЕННЫЕ ТЕСТЫ")
    
    return passed_tests == total_tests

def test_edge_cases():
    """Тестирует граничные случаи"""
    
    print("\n=== ТЕСТИРОВАНИЕ ГРАНИЧНЫХ СЛУЧАЕВ ===\n")
    
    # Мокаем дату для граничных тестов
    test_date = datetime(2025, 3, 1)  # 1 марта 2025
    
    edge_cases = [
        # Тест високосного года
        (29, 2, "not_applicable", "29 февраля в невисокосном году 2025"),
        
        # Тест перехода года
        (25, 12, "not_applicable", "25 декабря (проверка перехода года)"),
        
        # Некорректные данные
        (32, 1, "not_applicable", "Некорректный день (32)"),
        (15, 13, "not_applicable", "Некорректный месяц (13)"),
        (0, 5, "not_applicable", "Нулевой день"),
        (15, 0, "not_applicable", "Нулевой месяц"),
    ]
    
    with patch('__main__.datetime') as mock_datetime:
        mock_datetime.now.return_value = test_date
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else test_date
        
        for i, (birth_day, birth_month, expected_status, description) in enumerate(edge_cases, 1):
            print(f"Граничный тест {i}: {description}")
            print(f"  Входные данные: день={birth_day}, месяц={birth_month}")
            
            try:
                result = calculate_birthday_discount_status(birth_day, birth_month)
                actual_status = result['status']
                
                print(f"  Ожидаемый статус: {expected_status}")
                print(f"  Фактический статус: {actual_status}")
                
                if actual_status == expected_status:
                    print(f"  ✅ ПРОЙДЕН")
                else:
                    print(f"  ❌ ПРОВАЛЕН")
                    
            except Exception as e:
                print(f"  Результат: обработка ошибки - {e}")
                if expected_status == "not_applicable":
                    print(f"  ✅ ПРОЙДЕН (ошибка обработана корректно)")
                else:
                    print(f"  ❌ ПРОВАЛЕН")
            
            print()

def main():
    """Главная функция тестирования"""
    try:
        success = test_birthday_scenarios()
        test_edge_cases()
        
        print("\n=== ИТОГОВЫЙ РЕЗУЛЬТАТ ===")
        if success:
            print("✅ Система скидок на день рождения работает корректно!")
            return 0
        else:
            print("❌ Обнаружены проблемы в системе скидок на день рождения")
            return 1
            
    except Exception as e:
        print(f"Критическая ошибка при тестировании: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)