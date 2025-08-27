#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой тест функции расчета скидок на день рождения
"""

import sys
from datetime import datetime

def calculate_birthday_discount_status(birth_day, birth_month, current_date=None):
    """Функция расчета скидок с возможностью передать текущую дату"""
    if not birth_day or not birth_month:
        return {
            'status': 'not_applicable',
            'message': '',
            'days_until_birthday': None,
            'birthday_formatted': ''
        }
    
    try:
        if current_date is None:
            current_date = datetime.now()
        
        current_year = current_date.year
        
        try:
            birthday_this_year = datetime(current_year, birth_month, birth_day)
        except ValueError:
            if birth_month == 2 and birth_day == 29:
                birthday_this_year = datetime(current_year, 2, 28)
            else:
                return {
                    'status': 'not_applicable',
                    'message': '',
                    'days_until_birthday': None,
                    'birthday_formatted': ''
                }
        
        # Рассчитываем разность в днях от текущей даты до дня рождения в этом году
        current_date_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        days_until_birthday_this_year = (birthday_this_year - current_date_start).days
        
        # Если день рождения был недавно (в пределах 5 дней), используем отрицательное значение
        if -5 <= days_until_birthday_this_year <= 5:
            days_until_birthday = days_until_birthday_this_year
            actual_birthday = birthday_this_year
        else:
            # Если день рождения уже прошел давно, берем следующий год
            if birthday_this_year < current_date_start:
                try:
                    actual_birthday = datetime(current_year + 1, birth_month, birth_day)
                except ValueError:
                    if birth_month == 2 and birth_day == 29:
                        actual_birthday = datetime(current_year + 1, 2, 28)
                days_until_birthday = (actual_birthday - current_date_start).days
            else:
                days_until_birthday = days_until_birthday_this_year
                actual_birthday = birthday_this_year
        birthday_formatted = f"{birth_day}.{birth_month:02d}"
        
        if -5 <= days_until_birthday <= 5:
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
            message = f"У клиента через {days_until_birthday} дней ({birthday_formatted}) будет день рождения. Скидка по случаю дня рождения составит 35% на любой курс или набор. Скидка работает 10 дней: 5 дней до дня рождения и 5 дней после. Промокод DR-2025 действует только в эти 10 дней."
            
            return {
                'status': 'upcoming',
                'message': message,
                'days_until_birthday': days_until_birthday,
                'birthday_formatted': birthday_formatted
            }
        
        else:
            return {
                'status': 'not_applicable',
                'message': 'У клиента день рождения не в ближайшее время. Скидка по случаю дня рождения не действует.',
                'days_until_birthday': days_until_birthday,
                'birthday_formatted': birthday_formatted
            }
    
    except Exception as e:
        return {
            'status': 'not_applicable',
            'message': '',
            'days_until_birthday': None,
            'birthday_formatted': ''
        }

def test_birthday_function():
    """Тест функции расчета скидок"""
    print("🎂 ФИНАЛЬНЫЙ ТЕСТ РАСЧЕТА СКИДОК НА ДЕНЬ РОЖДЕНИЯ\n")
    
    # Используем фиксированную дату для тестов
    test_date = datetime(2025, 8, 27)
    
    test_cases = [
        (27, 8, "active", "День рождения сегодня"),
        (1, 9, "active", "Через 5 дней"),
        (6, 9, "upcoming", "Через 10 дней"),
        (22, 8, "active", "5 дней назад"),
        (21, 8, "not_applicable", "6 дней назад"),
        (None, None, "not_applicable", "Нет данных"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for i, (day, month, expected, desc) in enumerate(test_cases, 1):
        print(f"Тест {i}: {desc}")
        print(f"  Дата: {day}.{month if month else 'N/A'}")
        
        try:
            result = calculate_birthday_discount_status(day, month, test_date)
            actual = result['status']
            days_until = result.get('days_until_birthday')
            
            if actual == expected:
                print(f"  ✅ УСПЕХ ({actual})")
                if days_until is not None:
                    print(f"     Дней до ДР: {days_until}")
                passed += 1
            else:
                print(f"  ❌ ПРОВАЛ (ожидался {expected}, получен {actual})")
                if days_until is not None:
                    print(f"     Дней до ДР: {days_until}")
            
            if result['message']:
                print(f"  📝 {result['message'][:60]}...")
                
        except Exception as e:
            print(f"  ❌ ОШИБКА: {e}")
        
        print()
    
    print(f"РЕЗУЛЬТАТ: {passed}/{total} тестов пройдено")
    return passed == total

if __name__ == "__main__":
    success = test_birthday_function()
    if success:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    else:
        print("⚠️ ЕСТЬ ПРОБЛЕМЫ В ТЕСТАХ")
    sys.exit(0 if success else 1)