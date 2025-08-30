#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания резервной копии данных пользователей перед тестированием стратегического агента
"""

import os
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

def backup_user_data(conv_ids):
    """
    Создаёт резервную копию данных пользователей
    """
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not found in environment variables!")
    
    backup_data = {
        'timestamp': datetime.now().isoformat(),
        'conv_ids': conv_ids,
        'users': {}
    }
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        for conv_id in conv_ids:
            print(f"Создаю бэкап для пользователя {conv_id}...")
            
            # Бэкап профиля пользователя
            cursor.execute("""
                SELECT conv_id, first_name, last_name, screen_name, sex, city, 
                       birth_day, birth_month, can_write, email, dialogue_summary,
                       lead_qualification, funnel_stage, client_level, learning_goals,
                       client_pains, last_updated, client_activity,
                       short_term_strategy, long_term_strategy, last_strategy_analysis,
                       strategy_analysis_data
                FROM user_profiles 
                WHERE conv_id = %s
            """, (conv_id,))
            
            profile_row = cursor.fetchone()
            if profile_row:
                profile_columns = [
                    'conv_id', 'first_name', 'last_name', 'screen_name', 'sex', 'city',
                    'birth_day', 'birth_month', 'can_write', 'email', 'dialogue_summary',
                    'lead_qualification', 'funnel_stage', 'client_level', 'learning_goals',
                    'client_pains', 'last_updated', 'client_activity',
                    'short_term_strategy', 'long_term_strategy', 'last_strategy_analysis',
                    'strategy_analysis_data'
                ]
                profile_data = dict(zip(profile_columns, profile_row))
                
                # Конвертируем datetime в строку для JSON
                if profile_data['last_updated']:
                    profile_data['last_updated'] = profile_data['last_updated'].isoformat()
                if profile_data['last_strategy_analysis']:
                    profile_data['last_strategy_analysis'] = profile_data['last_strategy_analysis'].isoformat()
                
                backup_data['users'][conv_id] = {
                    'profile': profile_data
                }
            else:
                print(f"Пользователь {conv_id} не найден в базе данных!")
                continue
            
            # Бэкап диалогов
            cursor.execute("""
                SELECT id, conv_id, role, message, created_at
                FROM dialogues 
                WHERE conv_id = %s
                ORDER BY created_at
            """, (conv_id,))
            
            dialogues = []
            for row in cursor.fetchall():
                dialogue_data = {
                    'id': row[0],
                    'conv_id': row[1],
                    'role': row[2],
                    'message': row[3],
                    'created_at': row[4].isoformat() if row[4] else None
                }
                dialogues.append(dialogue_data)
            
            backup_data['users'][conv_id]['dialogues'] = dialogues
            
            # Бэкап напоминаний
            cursor.execute("""
                SELECT id, conv_id, reminder_datetime, reminder_context_summary, 
                       created_by_conv_id, client_timezone, status, created_at, 
                       cancellation_reason
                FROM reminders 
                WHERE conv_id = %s
            """, (conv_id,))
            
            reminders = []
            for row in cursor.fetchall():
                reminder_data = {
                    'id': row[0],
                    'conv_id': row[1],
                    'reminder_datetime': row[2].isoformat() if row[2] else None,
                    'reminder_context_summary': row[3],
                    'created_by_conv_id': row[4],
                    'client_timezone': row[5],
                    'status': row[6],
                    'created_at': row[7].isoformat() if row[7] else None,
                    'cancellation_reason': row[8]
                }
                reminders.append(reminder_data)
            
            backup_data['users'][conv_id]['reminders'] = reminders
            
            # Бэкап покупок
            cursor.execute("""
                SELECT conv_id, product_name
                FROM purchased_products 
                WHERE conv_id = %s
            """, (conv_id,))
            
            purchases = []
            for row in cursor.fetchall():
                purchase_data = {
                    'conv_id': row[0],
                    'product_name': row[1]
                }
                purchases.append(purchase_data)
            
            backup_data['users'][conv_id]['purchases'] = purchases
            
            print(f"✅ Бэкап для пользователя {conv_id} создан")
        
        cursor.close()
        conn.close()
        
        # Сохраняем бэкап в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_users_{'-'.join(map(str, conv_ids))}_{timestamp}.json"
        
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        
        print(f"📁 Бэкап сохранён в файл: {backup_filename}")
        return backup_filename
        
    except Exception as e:
        print(f"❌ Ошибка при создании бэкапа: {e}")
        raise

if __name__ == "__main__":
    # Тестируем на пользователях 3 и 4 из списка
    from founded_people_20250821_192925 import PRIORITIZED_CONV_IDS
    
    test_users = [PRIORITIZED_CONV_IDS[2], PRIORITIZED_CONV_IDS[3]]  # 3-й и 4-й пользователи
    print(f"Создаю бэкап для пользователей: {test_users}")
    
    backup_file = backup_user_data(test_users)
    print(f"✅ Бэкап завершён: {backup_file}")