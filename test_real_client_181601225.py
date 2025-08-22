#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
КОМПЛЕКСНЫЙ ТЕСТ АНАЛИЗАТОРА КАРТОЧЕК НА РЕАЛЬНЫХ ДАННЫХ
Клиент conv_id = 181601225

Выполняет полный каскад операций:
1. Скачивание данных клиента из БД (data_exporter)
2. Анализ через Vertex AI (client_card_analyzer) 
3. Парсинг результатов (results_parser)
4. Обновление карточки клиента и создание напоминания в БД

Автор: AI Assistant
Дата: 2025-08-20
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timezone
import subprocess
from pathlib import Path

# Загружаем переменные окружения из .env файла
from dotenv import load_dotenv
load_dotenv()

# Добавляем текущую директорию в путь для импорта модулей
sys.path.insert(0, str(Path(__file__).parent))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_real_client_181601225.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Константы
TARGET_CONV_ID = 181601225
TEST_NAME = "REAL_CLIENT_TEST_181601225"

class RealClientTester:
    """Класс для проведения комплексного теста реального клиента"""
    
    def __init__(self):
        self.conv_id = TARGET_CONV_ID
        self.test_results = {
            'test_name': TEST_NAME,
            'conv_id': self.conv_id,
            'start_time': datetime.now(timezone.utc).isoformat(),
            'steps': {},
            'files_created': [],
            'errors': [],
            'success': False
        }
        
        # Проверка переменных окружения
        self.check_environment()
    
    def check_environment(self):
        """Проверка необходимых переменных окружения"""
        logging.info("=== ПРОВЕРКА ОКРУЖЕНИЯ ===")
        
        required_vars = [
            'DATABASE_URL',
            'GOOGLE_APPLICATION_CREDENTIALS'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.environ.get(var):
                missing_vars.append(var)
            else:
                logging.info(f"✅ {var}: установлена")
        
        if missing_vars:
            error_msg = f"Отсутствуют переменные окружения: {missing_vars}"
            logging.error(f"❌ {error_msg}")
            self.test_results['errors'].append(error_msg)
            raise RuntimeError(error_msg)
        
        # Проверка существования файла учетных данных Google
        creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if not os.path.exists(creds_path):
            error_msg = f"Файл учетных данных Google не найден: {creds_path}"
            logging.error(f"❌ {error_msg}")
            self.test_results['errors'].append(error_msg)
            raise RuntimeError(error_msg)
        
        logging.info("✅ Все переменные окружения настроены корректно")
    
    def step_1_export_client_data(self):
        """Шаг 1: Экспорт данных клиента из БД"""
        logging.info("=== ШАГ 1: ЭКСПОРТ ДАННЫХ КЛИЕНТА ===")
        step_name = "export_client_data"
        
        try:
            # Запускаем data_exporter.py
            cmd = [sys.executable, "data_exporter.py", str(self.conv_id)]
            logging.info(f"Выполняю команду: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=os.getcwd()
            )
            
            if result.returncode != 0:
                error_msg = f"data_exporter.py завершился с ошибкой: {result.stderr}"
                logging.error(f"❌ {error_msg}")
                self.test_results['errors'].append(error_msg)
                self.test_results['steps'][step_name] = {
                    'status': 'FAILED',
                    'error': error_msg,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
                return False
            
            # Ищем созданный файл в выводе
            stdout_lines = result.stdout.strip().split('\n')
            exported_file = None
            
            for line in stdout_lines:
                if 'экспортированы в файл:' in line or 'exported to:' in line:
                    exported_file = line.split(':')[-1].strip()
                    break
            
            if not exported_file or not os.path.exists(exported_file):
                # Попытка найти файл по паттерну
                import glob
                pattern = f"exported_data/client_data_{self.conv_id}_*.json"
                files = glob.glob(pattern)
                if files:
                    exported_file = max(files, key=os.path.getctime)  # Берем самый новый
                    logging.info(f"Найден файл данных: {exported_file}")
                else:
                    error_msg = "Не удалось найти экспортированный файл данных"
                    logging.error(f"❌ {error_msg}")
                    self.test_results['errors'].append(error_msg)
                    self.test_results['steps'][step_name] = {
                        'status': 'FAILED',
                        'error': error_msg,
                        'stdout': result.stdout
                    }
                    return False
            
            # Проверяем содержимое файла
            with open(exported_file, 'r', encoding='utf-8') as f:
                client_data = json.load(f)
            
            logging.info(f"✅ Данные клиента успешно экспортированы в: {exported_file}")
            logging.info(f"📊 Клиент ID: {client_data.get('client_id')}")
            logging.info(f"📊 Количество недавних сообщений: {len(client_data.get('recent_messages', []))}")
            logging.info(f"📊 Купленные продукты: {client_data.get('purchased_products', [])}")
            
            self.test_results['files_created'].append(exported_file)
            self.test_results['steps'][step_name] = {
                'status': 'SUCCESS',
                'exported_file': exported_file,
                'client_data_summary': {
                    'client_id': client_data.get('client_id'),
                    'messages_count': len(client_data.get('recent_messages', [])),
                    'purchased_products': client_data.get('purchased_products', []),
                    'funnel_stage': client_data.get('funnel_stage'),
                    'client_activity': client_data.get('client_activity')
                }
            }
            
            return exported_file
            
        except Exception as e:
            error_msg = f"Ошибка при экспорте данных клиента: {e}"
            logging.error(f"❌ {error_msg}")
            logging.error(traceback.format_exc())
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
            return False
    
    def step_2_analyze_client_card(self, exported_file):
        """Шаг 2: Анализ карточки клиента через AI"""
        logging.info("=== ШАГ 2: АНАЛИЗ КАРТОЧКИ КЛИЕНТА ===")
        step_name = "analyze_client_card"
        
        try:
            # Запускаем client_card_analyzer.py с файлом данных
            cmd = [sys.executable, "client_analyzer/client_card_analyzer.py", exported_file]
            logging.info(f"Выполняю команду: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=os.getcwd(),
                timeout=300  # 5 минут таймаут для AI анализа
            )
            
            if result.returncode != 0:
                error_msg = f"client_card_analyzer.py завершился с ошибкой: {result.stderr}"
                logging.error(f"❌ {error_msg}")
                self.test_results['errors'].append(error_msg)
                self.test_results['steps'][step_name] = {
                    'status': 'FAILED',
                    'error': error_msg,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
                return False
            
            # Ищем файл результата анализа в выводе
            stdout_lines = result.stdout.strip().split('\n')
            analysis_file = None
            
            for line in stdout_lines:
                if 'Результат сохранен в:' in line or 'saved in:' in line:
                    analysis_file = line.split(':')[-1].strip()
                    break
            
            if not analysis_file or not os.path.exists(analysis_file):
                # Попытка найти файл по паттерну
                import glob
                pattern = f"analysis_results/strategic_analysis_{self.conv_id}_*.json"
                files = glob.glob(pattern)
                if files:
                    analysis_file = max(files, key=os.path.getctime)  # Берем самый новый
                    logging.info(f"Найден файл результата анализа: {analysis_file}")
                else:
                    error_msg = "Не удалось найти файл результата анализа"
                    logging.error(f"❌ {error_msg}")
                    self.test_results['errors'].append(error_msg)
                    self.test_results['steps'][step_name] = {
                        'status': 'FAILED',
                        'error': error_msg,
                        'stdout': result.stdout
                    }
                    return False
            
            # Проверяем содержимое файла анализа
            with open(analysis_file, 'r', encoding='utf-8') as f:
                analysis_data = json.load(f)
            
            logging.info(f"✅ Анализ карточки клиента успешно завершен: {analysis_file}")
            logging.info(f"📊 Квалификация лида: {analysis_data.get('lead_qualification')}")
            logging.info(f"📊 Этап воронки: {analysis_data.get('funnel_stage')}")
            logging.info(f"📊 Уровень клиента: {analysis_data.get('client_level')}")
            logging.info(f"📊 Цели обучения: {analysis_data.get('learning_goals')}")
            
            # Проверяем рекомендации по времени напоминания
            optimal_timing = analysis_data.get('optimal_reminder_timing', {})
            logging.info(f"📊 Рекомендуемое время напоминания: через {optimal_timing.get('contact_in_days')} дней")
            
            self.test_results['files_created'].append(analysis_file)
            self.test_results['steps'][step_name] = {
                'status': 'SUCCESS',
                'analysis_file': analysis_file,
                'analysis_summary': {
                    'lead_qualification': analysis_data.get('lead_qualification'),
                    'funnel_stage': analysis_data.get('funnel_stage'),
                    'client_level': analysis_data.get('client_level'),
                    'learning_goals': analysis_data.get('learning_goals'),
                    'optimal_timing_days': optimal_timing.get('contact_in_days'),
                    'action_priority': analysis_data.get('action_priority')
                }
            }
            
            return analysis_file
            
        except subprocess.TimeoutExpired:
            error_msg = "Таймаут при анализе карточки клиента (превышено 5 минут)"
            logging.error(f"❌ {error_msg}")
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg
            }
            return False
            
        except Exception as e:
            error_msg = f"Ошибка при анализе карточки клиента: {e}"
            logging.error(f"❌ {error_msg}")
            logging.error(traceback.format_exc())
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
            return False
    
    def step_3_parse_results(self, analysis_file):
        """Шаг 3: Парсинг результатов и обновление БД"""
        logging.info("=== ШАГ 3: ПАРСИНГ РЕЗУЛЬТАТОВ И ОБНОВЛЕНИЕ БД ===")
        step_name = "parse_results_and_update_db"
        
        try:
            # Запускаем results_parser.py
            cmd = [sys.executable, "client_analyzer/results_parser.py", analysis_file]
            logging.info(f"Выполняю команду: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=os.getcwd(),
                timeout=120  # 2 минуты таймаут для обновления БД
            )
            
            if result.returncode != 0:
                error_msg = f"results_parser.py завершился с ошибкой: {result.stderr}"
                logging.error(f"❌ {error_msg}")
                self.test_results['errors'].append(error_msg)
                self.test_results['steps'][step_name] = {
                    'status': 'FAILED',
                    'error': error_msg,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
                return False
            
            # Анализируем вывод парсера
            stdout_lines = result.stdout.strip().split('\n')
            
            # Ищем ключевые индикаторы успешного выполнения
            profile_updated = False
            reminder_created = False
            
            for line in stdout_lines:
                if '✅ ПРОФИЛЬ' in line and 'обновлен' in line:
                    profile_updated = True
                    logging.info(f"✅ {line}")
                elif '✅ Напоминание создано' in line or '✅ НАПОМИНАНИЕ ПОДТВЕРЖДЕНО' in line:
                    reminder_created = True
                    logging.info(f"✅ {line}")
            
            logging.info(f"✅ Парсинг результатов завершен успешно")
            logging.info(f"📊 Профиль клиента обновлен: {profile_updated}")
            logging.info(f"📊 Напоминание создано: {reminder_created}")
            
            self.test_results['steps'][step_name] = {
                'status': 'SUCCESS',
                'profile_updated': profile_updated,
                'reminder_created': reminder_created,
                'parser_output': result.stdout
            }
            
            return True
            
        except subprocess.TimeoutExpired:
            error_msg = "Таймаут при парсинге результатов (превышено 2 минуты)"
            logging.error(f"❌ {error_msg}")
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg
            }
            return False
            
        except Exception as e:
            error_msg = f"Ошибка при парсинге результатов: {e}"
            logging.error(f"❌ {error_msg}")
            logging.error(traceback.format_exc())
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
            return False
    
    def step_4_verify_database_changes(self):
        """Шаг 4: Верификация изменений в БД"""
        logging.info("=== ШАГ 4: ВЕРИФИКАЦИЯ ИЗМЕНЕНИЙ В БД ===")
        step_name = "verify_database_changes"
        
        try:
            import psycopg2
            from psycopg2.extras import DictCursor
            
            # Подключаемся к БД для проверки изменений
            DATABASE_URL = os.environ.get("DATABASE_URL")
            conn = psycopg2.connect(DATABASE_URL)
            
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Проверяем обновленный профиль клиента
                cur.execute("""
                    SELECT lead_qualification, funnel_stage, client_level, 
                           learning_goals, client_pains, last_analysis_at,
                           dialogue_summary
                    FROM user_profiles 
                    WHERE conv_id = %s
                """, (self.conv_id,))
                
                profile = cur.fetchone()
                
                if not profile:
                    error_msg = f"Профиль клиента {self.conv_id} не найден в БД"
                    logging.error(f"❌ {error_msg}")
                    self.test_results['errors'].append(error_msg)
                    self.test_results['steps'][step_name] = {
                        'status': 'FAILED',
                        'error': error_msg
                    }
                    conn.close()
                    return False
                
                # Проверяем напоминания
                cur.execute("""
                    SELECT id, reminder_datetime, reminder_context_summary, 
                           status, created_at
                    FROM reminders 
                    WHERE conv_id = %s 
                    ORDER BY created_at DESC 
                    LIMIT 5
                """, (self.conv_id,))
                
                reminders = cur.fetchall()
                
                # Находим свежие напоминания (созданные в последние 10 минут)
                recent_reminders = []
                current_time = datetime.now(timezone.utc)
                
                for reminder in reminders:
                    created_at = reminder['created_at']
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    
                    time_diff = (current_time - created_at).total_seconds()
                    if time_diff < 600:  # 10 минут
                        recent_reminders.append(reminder)
                
                logging.info(f"✅ Профиль клиента найден в БД:")
                logging.info(f"📊 Квалификация: {profile['lead_qualification']}")
                logging.info(f"📊 Этап воронки: {profile['funnel_stage']}")
                logging.info(f"📊 Уровень: {profile['client_level']}")
                logging.info(f"📊 Цели: {profile['learning_goals']}")
                logging.info(f"📊 Последний анализ: {profile['last_analysis_at']}")
                
                if recent_reminders:
                    logging.info(f"✅ Найдено {len(recent_reminders)} свежих напоминаний:")
                    for reminder in recent_reminders:
                        logging.info(f"📊 ID {reminder['id']}: {reminder['reminder_datetime']} - {reminder['reminder_context_summary'][:100]}...")
                else:
                    logging.warning("⚠️ Свежие напоминания не найдены")
                
                conn.close()
                
                self.test_results['steps'][step_name] = {
                    'status': 'SUCCESS',
                    'profile_found': True,
                    'profile_data': {
                        'lead_qualification': profile['lead_qualification'],
                        'funnel_stage': profile['funnel_stage'],
                        'client_level': profile['client_level'],
                        'learning_goals': profile['learning_goals'],
                        'last_analysis_at': str(profile['last_analysis_at']) if profile['last_analysis_at'] else None
                    },
                    'reminders_count': len(reminders),
                    'recent_reminders_count': len(recent_reminders),
                    'recent_reminders': [
                        {
                            'id': r['id'],
                            'datetime': str(r['reminder_datetime']),
                            'summary': r['reminder_context_summary'][:200],
                            'status': r['status']
                        } for r in recent_reminders
                    ]
                }
                
                return True
                
        except Exception as e:
            error_msg = f"Ошибка при верификации БД: {e}"
            logging.error(f"❌ {error_msg}")
            logging.error(traceback.format_exc())
            self.test_results['errors'].append(error_msg)
            self.test_results['steps'][step_name] = {
                'status': 'FAILED',
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
            return False
    
    def run_full_test(self):
        """Запуск полного комплексного теста"""
        logging.info(f"==========================================")
        logging.info(f"НАЧАЛО КОМПЛЕКСНОГО ТЕСТА: {TEST_NAME}")
        logging.info(f"Клиент conv_id: {self.conv_id}")
        logging.info(f"Время начала: {self.test_results['start_time']}")
        logging.info(f"==========================================")
        
        try:
            # Шаг 1: Экспорт данных клиента
            exported_file = self.step_1_export_client_data()
            if not exported_file:
                self.test_results['success'] = False
                return self.generate_final_report()
            
            # Шаг 2: Анализ карточки клиента
            analysis_file = self.step_2_analyze_client_card(exported_file)
            if not analysis_file:
                self.test_results['success'] = False
                return self.generate_final_report()
            
            # Шаг 3: Парсинг результатов и обновление БД
            parse_success = self.step_3_parse_results(analysis_file)
            if not parse_success:
                self.test_results['success'] = False
                return self.generate_final_report()
            
            # Шаг 4: Верификация изменений в БД
            verify_success = self.step_4_verify_database_changes()
            if not verify_success:
                self.test_results['success'] = False
                return self.generate_final_report()
            
            # Все шаги прошли успешно
            self.test_results['success'] = True
            return self.generate_final_report()
            
        except Exception as e:
            error_msg = f"Критическая ошибка во время теста: {e}"
            logging.error(f"❌ {error_msg}")
            logging.error(traceback.format_exc())
            self.test_results['errors'].append(error_msg)
            self.test_results['success'] = False
            return self.generate_final_report()
    
    def generate_final_report(self):
        """Генерация финального отчета о тесте"""
        self.test_results['end_time'] = datetime.now(timezone.utc).isoformat()
        
        # Вычисляем общее время выполнения
        start_time = datetime.fromisoformat(self.test_results['start_time'].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(self.test_results['end_time'].replace('Z', '+00:00'))
        duration = (end_time - start_time).total_seconds()
        self.test_results['duration_seconds'] = duration
        
        # Создаем файл отчета
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"test_report_real_client_{self.conv_id}_{timestamp}.json"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"==========================================")
        logging.info(f"ЗАВЕРШЕНИЕ КОМПЛЕКСНОГО ТЕСТА: {TEST_NAME}")
        logging.info(f"Результат: {'✅ УСПЕШНО' if self.test_results['success'] else '❌ НЕУДАЧНО'}")
        logging.info(f"Длительность: {duration:.2f} секунд")
        logging.info(f"Создано файлов: {len(self.test_results['files_created'])}")
        logging.info(f"Ошибок: {len(self.test_results['errors'])}")
        logging.info(f"Отчет сохранен: {report_file}")
        logging.info(f"==========================================")
        
        return self.test_results


def main():
    """Основная функция запуска теста"""
    try:
        tester = RealClientTester()
        results = tester.run_full_test()
        
        if results['success']:
            print(f"\n[SUCCESS] ТЕСТ УСПЕШНО ЗАВЕРШЕН!")
            print(f"[OK] Все этапы каскада операций выполнены:")
            print(f"   1. [OK] Экспорт данных клиента из БД")
            print(f"   2. [OK] AI анализ карточки клиента")
            print(f"   3. [OK] Парсинг результатов и обновление БД")
            print(f"   4. [OK] Верификация изменений в БД")
            print(f"\n[INFO] СВОДКА ПО КЛИЕНТУ {TARGET_CONV_ID}:")
            
            # Выводим ключевые результаты из каждого шага
            for step_name, step_data in results['steps'].items():
                if step_data['status'] == 'SUCCESS':
                    print(f"   [STEP] {step_name}: SUCCESS")
        else:
            print(f"\n[ERROR] ТЕСТ ЗАВЕРШИЛСЯ С ОШИБКАМИ!")
            print(f"[ERROR] Количество ошибок: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   [ERR] {error}")
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        print(f"\n[CRITICAL] КРИТИЧЕСКАЯ ОШИБКА ТЕСТА: {e}")
        logging.error(f"Критическая ошибка: {e}")
        logging.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)