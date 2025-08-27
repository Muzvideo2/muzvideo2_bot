#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест функции расчета скидок на день рождения
"""

import sys
from datetime import datetime
from unittest.mock import patch

def calculate_birthday_discount_status(birth_day, birth_month):
    """Копия функции для тестирования"""
    if not birth_day or not birth_month:
        return {
            'status': 'not_applicable',
            'message': '',
            'days_until_birthday': None,
            'birthday_formatted': ''
        }
    
    try:
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
        
        if birthday_this_year < current_date.replace(hour=0, minute=0, second=0, microsecond=0):
            try:
                birthday_this_year = datetime(current_year + 1, birth_month, birth_day)
            except ValueError:
                if birth_month == 2 and birth_day == 29:
                    birthday_this_year = datetime(current_year + 1, 2, 28)
        
        days_until_birthday = (birthday_this_year - current_date.replace(hour=0, minute=0, second=0, microsecond=0)).days
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
    print("🎂 ТЕСТ РАСЧЕТА СКИДОК НА ДЕНЬ РОЖДЕНИЯ\n")
    
    base_date = datetime(2025, 8, 27)
    
    test_cases = [
        (27, 8, "active", "День рождения сегодня"),
        (1, 9, "active", "Через 5 дней"),
        (6, 9, "upcoming", "Через 10 дней"),
        (22, 8, "active", "5 дней назад"),
        (21, 8, "not_applicable", "6 дней назад"),
        (None, None, "not_applicable", "Нет данных"),
    ]
    
    with patch('__main__.datetime') as mock_dt:
        mock_dt.now.return_value = base_date
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw) if args else base_date
        
        # Also patch the datetime import inside the function
        import builtins
        original_datetime = builtins.__dict__.get('datetime', datetime)
        builtins.__dict__['datetime'] = mock_dt
        
        passed = 0
        total = len(test_cases)
        
        for i, (day, month, expected, desc) in enumerate(test_cases, 1):
            print(f"Тест {i}: {desc}")
            
            try:
                result = calculate_birthday_discount_status(day, month)
                actual = result['status']
                
                if actual == expected:
                    print(f"  ✅ УСПЕХ ({actual})")
                    passed += 1
                else:
                    print(f"  ❌ ПРОВАЛ (ожидался {expected}, получен {actual})")
                
                if result['message']:
                    print(f"  📝 {result['message'][:50]}...")
                    
            except Exception as e:
                print(f"  ❌ ОШИБКА: {e}")
            
            print()
        
        print(f"Результат: {passed}/{total} тестов пройдено")
        return passed == total

if __name__ == "__main__":
    success = test_birthday_function()
    if success:
        print("🎉 Все тесты пройдены!")
    else:
        print("⚠️ Есть провалы в тестах")
    sys.exit(0 if success else 1)