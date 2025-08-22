#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Упрощенный обработчик результатов анализа для реального теста
"""

import os
import json
import logging
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_db_connection():
    """Получение подключения к БД"""
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)
    else:
        raise ValueError("DATABASE_URL не установлен")

def process_analysis_results(analysis_file, conv_id):
    """Обработка результатов анализа"""
    
    print(f"[INFO] Загружаем результаты анализа из {analysis_file}")
    
    # Загружаем результаты
    with open(analysis_file, 'r', encoding='utf-8') as f:
        analysis_data = json.load(f)
    
    print(f"[INFO] Результат анализа: {analysis_data.get('lead_qualification', 'N/A')}")
    
    # Подключаемся к БД
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # 1. Обновляем основные поля профиля клиента
            print("[INFO] Обновляем профиль клиента...")
            
            # Проверяем существующий профиль
            cur.execute("SELECT conv_id FROM user_profiles WHERE conv_id = %s", (conv_id,))
            exists = cur.fetchone()
            
            if exists:
                # Обновляем базовые поля, которые точно есть в БД
                update_sql = """
                    UPDATE user_profiles SET
                        lead_qualification = %s,
                        funnel_stage = %s,
                        client_level = %s,
                        learning_goals = %s,
                        client_pains = %s,
                        dialogue_summary = %s,
                        last_updated = %s
                    WHERE conv_id = %s
                """
                
                # Правильно обрабатываем поля-массивы
                lead_qual = analysis_data.get('lead_qualification', '')
                if isinstance(lead_qual, str):
                    lead_qual_array = [lead_qual] if lead_qual else []
                else:
                    lead_qual_array = lead_qual
                
                client_level = analysis_data.get('client_level', [])
                if isinstance(client_level, str):
                    client_level = [client_level]
                
                learning_goals = analysis_data.get('learning_goals', [])
                if not isinstance(learning_goals, list):
                    learning_goals = []
                
                client_pains = analysis_data.get('client_pains', [])
                if not isinstance(client_pains, list):
                    client_pains = []
                
                cur.execute(update_sql, (
                    lead_qual_array,
                    analysis_data.get('funnel_stage', ''),
                    client_level,
                    learning_goals,
                    client_pains,
                    analysis_data.get('dialogue_summary', ''),
                    datetime.now(timezone.utc),
                    conv_id
                ))
                
                print(f"[OK] Профиль клиента {conv_id} обновлен")
            else:
                print(f"[WARN] Профиль клиента {conv_id} не найден, пропускаем обновление")
            
            # 2. Создаем напоминание, если нужно
            timing_data = analysis_data.get('optimal_reminder_timing', {})
            contact_in_days = timing_data.get('contact_in_days', 0)
            
            if contact_in_days > 0:
                print(f"[INFO] Создаем напоминание через {contact_in_days} дней...")
                
                reminder_datetime = datetime.now(timezone.utc) + timedelta(days=contact_in_days)
                contact_reason = timing_data.get('contact_reason', 'Напоминание о клиенте')
                
                # Проверяем, нет ли уже активного напоминания
                cur.execute("""
                    SELECT id FROM reminders 
                    WHERE conv_id = %s AND status = 'active'
                    AND reminder_datetime BETWEEN %s AND %s
                """, (
                    conv_id,
                    reminder_datetime - timedelta(hours=12),
                    reminder_datetime + timedelta(hours=12)
                ))
                
                existing = cur.fetchone()
                if not existing:
                    # Создаем напоминание
                    cur.execute("""
                        INSERT INTO reminders (
                            conv_id, reminder_datetime, reminder_context_summary,
                            status, created_at
                        ) VALUES (%s, %s, %s, 'active', %s)
                        RETURNING id
                    """, (
                        conv_id,
                        reminder_datetime,
                        f"{contact_reason} (AI анализ)",
                        datetime.now(timezone.utc)
                    ))
                    
                    reminder_id = cur.fetchone()[0]
                    print(f"[OK] Создано напоминание ID={reminder_id} на {reminder_datetime}")
                else:
                    print("[INFO] Напоминание уже существует, пропускаем создание")
            else:
                print("[INFO] Напоминание не требуется согласно анализу")
            
            conn.commit()
            print("[SUCCESS] Все операции выполнены успешно")
            
            return True
            
    except Exception as e:
        print(f"[ERROR] Ошибка обработки: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    import sys
    
    if len(sys.argv) != 2:
        print("Использование: python simple_results_processor.py <файл_результатов>")
        return 1
    
    analysis_file = sys.argv[1]
    
    # Извлекаем conv_id из имени файла
    import re
    match = re.search(r'(\d+)', os.path.basename(analysis_file))
    if match:
        conv_id = int(match.group(1))
    else:
        print("[ERROR] Не удалось извлечь conv_id из имени файла")
        return 1
    
    print(f"[INFO] Обрабатываем результаты для клиента {conv_id}")
    
    success = process_analysis_results(analysis_file, conv_id)
    
    if success:
        print("[SUCCESS] Обработка завершена успешно!")
        return 0
    else:
        print("[ERROR] Обработка завершилась с ошибками!")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())