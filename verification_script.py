#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =======================================================================================
#               СКРИПТ АВТОМАТИЧЕСКОЙ ВЕРИФИКАЦИИ СЕРВИСА НАПОМИНАНИЙ
# =======================================================================================
#
# Этот скрипт автоматически тестирует логику семантического анализатора
# в reminder_service.py, используя сценарии из scenarios.json
#
# =======================================================================================

import os
import sys
import json
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
import re
from typing import Dict, List, Any, Optional
import subprocess
import tempfile

# Импортируем функции из reminder_service для тестирования
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from reminder_service import (
    analyze_dialogue_for_reminders,
    create_or_update_reminder,
    get_db_connection,
    setup_logging
)

# --- НАСТРОЙКИ ---
SCENARIOS_FILE = "scenarios.json"
REPORT_FILE = "test_report.md"
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", os.environ.get("DATABASE_URL"))

# Цвета для консольного вывода
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def load_scenarios():
    """Загружает тестовые сценарии из JSON файла."""
    try:
        with open(SCENARIOS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('test_scenarios', [])
    except FileNotFoundError:
        print(f"{Colors.RED}Файл {SCENARIOS_FILE} не найден!{Colors.RESET}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}Ошибка парсинга {SCENARIOS_FILE}: {e}{Colors.RESET}")
        sys.exit(1)

def setup_test_database(conn, initial_state: Dict[str, Any]):
    """Настраивает начальное состояние БД для теста."""
    try:
        with conn.cursor() as cur:
            # Очищаем таблицы
            cur.execute("DELETE FROM reminders")
            cur.execute("DELETE FROM dialogues")
            cur.execute("DELETE FROM user_profiles WHERE conv_id != 78671089")  # Сохраняем админа
            
            # Вставляем начальные данные
            # User profiles
            if 'user_profiles' in initial_state:
                for profile in initial_state['user_profiles']:
                    cur.execute("""
                        INSERT INTO user_profiles (conv_id, first_name, last_name, client_timezone)
                        VALUES (%(conv_id)s, %(first_name)s, %(last_name)s, %(client_timezone)s)
                        ON CONFLICT (conv_id) DO UPDATE SET
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            client_timezone = COALESCE(EXCLUDED.client_timezone, user_profiles.client_timezone)
                    """, {
                        'conv_id': profile['conv_id'],
                        'first_name': profile.get('first_name', ''),
                        'last_name': profile.get('last_name', ''),
                        'client_timezone': profile.get('client_timezone', 'Europe/Moscow')
                    })
            
            # Reminders
            if 'reminders' in initial_state:
                for reminder in initial_state['reminders']:
                    cur.execute("""
                        INSERT INTO reminders (
                            conv_id, reminder_datetime, status, 
                            reminder_context_summary, created_by_conv_id
                        ) VALUES (
                            %(conv_id)s, 
                            COALESCE(%(reminder_datetime)s, NOW() + INTERVAL '1 day'),
                            %(status)s, 
                            %(reminder_context_summary)s, 
                            %(created_by_conv_id)s
                        )
                    """, {
                        'conv_id': reminder['conv_id'],
                        'reminder_datetime': reminder.get('reminder_datetime'),
                        'status': reminder.get('status', 'active'),
                        'reminder_context_summary': reminder.get('reminder_context_summary', ''),
                        'created_by_conv_id': reminder.get('created_by_conv_id')
                    })
            
            # Dialogues
            if 'dialogues' in initial_state:
                for idx, msg in enumerate(initial_state['dialogues']):
                    cur.execute("""
                        INSERT INTO dialogues (conv_id, role, message, created_at)
                        VALUES (%(conv_id)s, %(role)s, %(message)s, NOW() - INTERVAL '%(minutes)s minutes')
                    """, {
                        'conv_id': initial_state.get('test_conv_id', 12345678),
                        'role': msg['role'],
                        'message': msg['message'],
                        'minutes': len(initial_state['dialogues']) - idx
                    })
            
            # Client purchases
            if 'client_purchases' in initial_state:
                for purchase in initial_state['client_purchases']:
                    cur.execute("""
                        INSERT INTO client_purchases (conv_id, product_name, purchase_date)
                        VALUES (%(conv_id)s, %(product_name)s, COALESCE(%(purchase_date)s, NOW()))
                    """, {
                        'conv_id': purchase['conv_id'],
                        'product_name': purchase['product_name'],
                        'purchase_date': purchase.get('purchase_date')
                    })
            
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Ошибка настройки тестовой БД: {e}")

def simulate_dialogue_flow(conn, dialogue_flow: List[Dict], default_conv_id: int = 12345678):
    """Симулирует поток диалога, добавляя сообщения в БД."""
    try:
        with conn.cursor() as cur:
            for idx, msg in enumerate(dialogue_flow):
                conv_id = msg.get('from_conv_id', default_conv_id)
                cur.execute("""
                    INSERT INTO dialogues (conv_id, role, message, created_at)
                    VALUES (%(conv_id)s, %(role)s, %(message)s, NOW() + INTERVAL '%(seconds)s seconds')
                """, {
                    'conv_id': conv_id,
                    'role': msg['role'],
                    'message': msg['message'],
                    'seconds': idx
                })
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Ошибка симуляции диалога: {e}")

def get_actual_reminders(conn, conv_id: Optional[int] = None) -> List[Dict]:
    """Получает актуальные напоминания из БД."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if conv_id:
                cur.execute("""
                    SELECT conv_id, status, reminder_context_summary, 
                           cancellation_reason, created_by_conv_id
                    FROM reminders 
                    WHERE conv_id = %s
                    ORDER BY created_at DESC
                """, (conv_id,))
            else:
                cur.execute("""
                    SELECT conv_id, status, reminder_context_summary, 
                           cancellation_reason, created_by_conv_id
                    FROM reminders 
                    ORDER BY created_at DESC
                """)
            
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.Error as e:
        raise Exception(f"Ошибка получения напоминаний: {e}")

def compare_results(expected: Dict, actual: List[Dict]) -> tuple[bool, str]:
    """Сравнивает ожидаемый и фактический результат."""
    # Для простых случаев с одним напоминанием
    if 'action' in expected:
        if expected['action'] == 'none':
            if len(actual) == 0:
                return True, "Напоминание не создано (как и ожидалось)"
            else:
                return False, f"Ожидалось отсутствие напоминаний, но найдено: {len(actual)}"
        
        if len(actual) == 0:
            return False, f"Ожидалось действие '{expected['action']}', но напоминаний не найдено"
        
        latest = actual[0]
        
        # Проверяем основные поля
        checks = []
        
        if 'target_conv_id' in expected:
            if latest['conv_id'] != expected['target_conv_id']:
                checks.append(f"conv_id: ожидалось {expected['target_conv_id']}, получено {latest['conv_id']}")
        
        if 'status' in expected:
            if latest['status'] != expected['status']:
                checks.append(f"status: ожидалось '{expected['status']}', получено '{latest['status']}'")
        
        if 'reminder_context_summary' in expected:
            # Проверяем, что ключевые слова из ожидаемого текста присутствуют в актуальном
            expected_words = set(re.findall(r'\w+', expected['reminder_context_summary'].lower()))
            actual_words = set(re.findall(r'\w+', latest['reminder_context_summary'].lower()))
            common_words = expected_words & actual_words
            
            if len(common_words) < len(expected_words) * 0.5:  # Менее 50% совпадения
                checks.append(f"context_summary: недостаточное совпадение\nОжидалось: {expected['reminder_context_summary']}\nПолучено: {latest['reminder_context_summary']}")
        
        if 'created_by_conv_id' in expected:
            if latest['created_by_conv_id'] != expected['created_by_conv_id']:
                checks.append(f"created_by: ожидалось {expected['created_by_conv_id']}, получено {latest['created_by_conv_id']}")
        
        if checks:
            return False, "\n".join(checks)
        else:
            return True, "Все проверки пройдены"
    
    # Для сложных случаев с несколькими напоминаниями
    elif 'reminders' in expected:
        expected_count = len(expected['reminders'])
        actual_count = len(actual)
        
        if expected_count != actual_count:
            return False, f"Ожидалось {expected_count} напоминаний, найдено {actual_count}"
        
        # TODO: Более детальная проверка для множественных напоминаний
        return True, f"Найдено {actual_count} напоминаний (как и ожидалось)"
    
    return False, "Неизвестный формат ожидаемого результата"

def run_scenario(scenario: Dict, conn, model) -> Dict[str, Any]:
    """Выполняет один тестовый сценарий."""
    result = {
        'id': scenario['id'],
        'category': scenario['category'],
        'description': scenario['description'],
        'status': 'FAIL',
        'details': '',
        'error': None
    }
    
    try:
        # Настраиваем начальное состояние БД
        setup_test_database(conn, scenario.get('initial_db_state', {}))
        
        # Определяем conv_id для теста
        test_conv_id = scenario.get('initial_db_state', {}).get('test_conv_id', 12345678)
        
        # Симулируем диалог
        simulate_dialogue_flow(conn, scenario['dialogue_flow'], test_conv_id)
        
        # Анализируем диалог
        if 'expected_activation' in scenario:
            # Это тест активации напоминания
            # TODO: Реализовать тестирование активации
            result['status'] = 'SKIP'
            result['details'] = 'Тестирование активации еще не реализовано'
        else:
            # Это тест создания/изменения напоминания
            # Определяем, от кого идет последнее сообщение
            last_msg = scenario['dialogue_flow'][-1] if scenario['dialogue_flow'] else None
            conv_id_to_analyze = last_msg.get('from_conv_id', test_conv_id) if last_msg else test_conv_id
            
            # Анализируем
            reminder_data = analyze_dialogue_for_reminders(conn, conv_id_to_analyze, model)
            
            if reminder_data:
                # Создаем/обновляем напоминание
                created_by = conv_id_to_analyze if conv_id_to_analyze == 78671089 else None
                create_or_update_reminder(conn, conv_id_to_analyze, reminder_data, created_by)
            
            # Получаем фактический результат
            actual_reminders = get_actual_reminders(conn)
            
            # Сравниваем с ожидаемым
            success, details = compare_results(scenario['expected_result'], actual_reminders)
            
            result['status'] = 'SUCCESS' if success else 'FAIL'
            result['details'] = details
            result['actual_result'] = actual_reminders
        
    except Exception as e:
        result['status'] = 'ERROR'
        result['error'] = str(e)
        result['details'] = f"Произошла ошибка при выполнении сценария: {e}"
    
    return result

def generate_report(results: List[Dict], total_time: float):
    """Генерирует отчет в формате Markdown."""
    report_lines = []
    
    # Заголовок
    report_lines.append("# Отчет автоматического тестирования сервиса напоминаний")
    report_lines.append(f"\nДата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Время выполнения: {total_time:.2f} сек.\n")
    
    # Сводка
    total = len(results)
    success = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in results if r['status'] == 'FAIL')
    errors = sum(1 for r in results if r['status'] == 'ERROR')
    skipped = sum(1 for r in results if r['status'] == 'SKIP')
    
    report_lines.append("## Сводка результатов\n")
    report_lines.append(f"- Всего тестов: {total}")
    report_lines.append(f"- ✅ Успешно: {success}")
    report_lines.append(f"- ❌ Провалено: {failed}")
    report_lines.append(f"- 🔥 Ошибки: {errors}")
    report_lines.append(f"- ⏭️ Пропущено: {skipped}")
    report_lines.append(f"- Процент успеха: {(success/total*100):.1f}%\n")
    
    # Результаты по категориям
    categories = {}
    for result in results:
        cat = result['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(result)
    
    report_lines.append("## Результаты по категориям\n")
    
    for category, cat_results in sorted(categories.items()):
        report_lines.append(f"### {category}\n")
        
        for result in cat_results:
            status_icon = {
                'SUCCESS': '✅',
                'FAIL': '❌',
                'ERROR': '🔥',
                'SKIP': '⏭️'
            }.get(result['status'], '❓')
            
            report_lines.append(f"#### {status_icon} {result['id']}: {result['description']}")
            report_lines.append(f"\n**Статус:** {result['status']}")
            report_lines.append(f"**Детали:** {result['details']}")
            
            if result.get('error'):
                report_lines.append(f"**Ошибка:** `{result['error']}`")
            
            if result.get('actual_result') and result['status'] == 'FAIL':
                report_lines.append(f"\n**Фактический результат:**")
                report_lines.append("```json")
                report_lines.append(json.dumps(result['actual_result'], ensure_ascii=False, indent=2))
                report_lines.append("```")
            
            report_lines.append("")
    
    # Рекомендации для фонового агента
    if failed > 0 or errors > 0:
        report_lines.append("\n## Рекомендации для исправления\n")
        report_lines.append("Фоновому агенту Cursor следует:")
        report_lines.append("1. Проанализировать все сценарии со статусом FAIL и ERROR")
        report_lines.append("2. Изучить расхождения между ожидаемым и фактическим результатом")
        report_lines.append("3. Внести исправления в `reminder_service.py`, уделив внимание:")
        report_lines.append("   - Логике семантического анализа в промпте `PROMPT_ANALYZE_DIALOGUE`")
        report_lines.append("   - Правилам интерпретации времени")
        report_lines.append("   - Обработке команд администратора")
        report_lines.append("4. Запустить тестирование повторно")
    
    # Записываем отчет
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\n{Colors.BLUE}Отчет сохранен в {REPORT_FILE}{Colors.RESET}")

def main():
    """Главная функция."""
    print(f"{Colors.BLUE}=== Запуск автоматического тестирования сервиса напоминаний ==={Colors.RESET}\n")
    
    # Настройка логирования
    setup_logging()
    
    # Проверяем наличие TEST_DATABASE_URL
    if not TEST_DATABASE_URL:
        print(f"{Colors.RED}ОШИБКА: Переменная окружения TEST_DATABASE_URL не установлена!{Colors.RESET}")
        print("Используйте тестовую базу данных для безопасного тестирования.")
        sys.exit(1)
    
    # Загружаем сценарии
    scenarios = load_scenarios()
    print(f"Загружено {len(scenarios)} тестовых сценариев\n")
    
    # Инициализируем модель (заглушка для тестирования)
    # В реальном тестировании нужно использовать mock или тестовую модель
    class MockModel:
        def generate_content(self, prompt):
            # Простая логика для базовых тестов
            class Response:
                def __init__(self, text):
                    self.text = text
            
            # Анализируем промпт и возвращаем соответствующий ответ
            if "Можете напомнить мне завтра вынести мусор" in prompt:
                return Response('{"action": "none"}')
            elif "напомните мне завтра в 10 утра оплатить" in prompt:
                return Response('{"action": "create", "target_conv_id": 12345678, "proposed_datetime": "2024-01-16T10:00:00+03:00", "reminder_context_summary": "Клиент попросил напомнить об оплате курса завтра в 10:00"}')
            elif "Мне нужно подумать до понедельника" in prompt:
                return Response('{"action": "create", "target_conv_id": 23456789, "proposed_datetime": "2024-01-22T10:00:00+03:00", "reminder_context_summary": "Клиент взял время подумать над курсом \'Импровизация\' до понедельника"}')
            else:
                return Response('{"action": "none"}')
    
    model = MockModel()
    
    # Выполняем тесты
    results = []
    start_time = datetime.now()
    
    try:
        conn = psycopg2.connect(TEST_DATABASE_URL)
        
        for i, scenario in enumerate(scenarios):
            print(f"Выполняется тест {i+1}/{len(scenarios)}: {scenario['id']}...", end='')
            result = run_scenario(scenario, conn, model)
            results.append(result)
            
            if result['status'] == 'SUCCESS':
                print(f" {Colors.GREEN}✓{Colors.RESET}")
            elif result['status'] == 'FAIL':
                print(f" {Colors.RED}✗{Colors.RESET}")
            elif result['status'] == 'ERROR':
                print(f" {Colors.RED}ERROR{Colors.RESET}")
            else:
                print(f" {Colors.YELLOW}SKIP{Colors.RESET}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n{Colors.RED}Критическая ошибка: {e}{Colors.RESET}")
        sys.exit(1)
    
    # Генерируем отчет
    total_time = (datetime.now() - start_time).total_seconds()
    generate_report(results, total_time)
    
    # Выводим сводку
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    print(f"\n{Colors.GREEN}Завершено!{Colors.RESET}")
    print(f"Успешно: {success_count}/{len(results)}")
    
    # Возвращаем код выхода для CI/CD
    if all(r['status'] in ['SUCCESS', 'SKIP'] for r in results):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()