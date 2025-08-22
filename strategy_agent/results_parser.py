#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер результатов анализа клиента для обновления базы данных
"""

import json
import psycopg2
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path

# Добавляем родительскую директорию для импорта модулей
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_database_connection():
    """Получение подключения к базе данных"""
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)
    else:
        # Fallback к отдельным переменным
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT', 5432)
        )

def parse_analysis_results(result_file: str) -> Dict[str, Any]:
    """Парсинг результатов анализа из JSON файла"""
    logger.info(f"Загрузка результатов анализа из: {result_file}")
    
    with open(result_file, 'r', encoding='utf-8') as f:
        analysis_data = json.load(f)
    
    return analysis_data

def get_client_profile_before_update(client_id: str, conn) -> Dict[str, Any]:
    """Получение профиля клиента до обновления"""
    cursor = conn.cursor()
    
    try:
        sql = """
        SELECT conv_id, lead_qualification, funnel_stage, client_activity, 
               personality_type, primary_motivation, churn_risk, engagement_level,
               updated_at, analysis_data
        FROM user_profiles 
        WHERE conv_id = %s
        """
        
        cursor.execute(sql, (client_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                'conv_id': result[0],
                'lead_qualification': result[1],
                'funnel_stage': result[2],
                'client_activity': result[3],
                'personality_type': result[4],
                'primary_motivation': result[5],
                'churn_risk': result[6],
                'engagement_level': result[7],
                'updated_at': result[8],
                'analysis_data': result[9]
            }
        else:
            logger.warning(f"Профиль клиента {client_id} не найден в БД")
            return {}
            
    except Exception as e:
        logger.error(f"Ошибка при получении профиля: {e}")
        return {}
    finally:
        cursor.close()

def update_client_profile(client_id: str, analysis_data: Dict[str, Any], conn):
    """Обновление профиля клиента в БД на основе результатов анализа"""
    logger.info(f"Обновление профиля клиента {client_id}")
    
    cursor = conn.cursor()
    
    try:
        # Извлекаем данные для обновления профиля
        updates = {}
        
        # Квалификация клиента
        if 'client_qualification' in analysis_data:
            qual_data = analysis_data['client_qualification']
            if isinstance(qual_data, dict):
                updates['lead_qualification'] = qual_data.get('current_level')
                updates['qualification_confidence'] = qual_data.get('confidence_score')
        
        # Этап воронки
        if 'funnel_stage_analysis' in analysis_data:
            funnel_data = analysis_data['funnel_stage_analysis']
            if isinstance(funnel_data, dict):
                updates['funnel_stage'] = funnel_data.get('current_stage')
        
        # Психологический профиль
        if 'psychological_profile' in analysis_data:
            psych_data = analysis_data['psychological_profile']
            if isinstance(psych_data, dict):
                updates['personality_type'] = psych_data.get('personality_type')
                updates['primary_motivation'] = psych_data.get('primary_motivation')
                updates['communication_style'] = psych_data.get('communication_style')
        
        # Оценка рисков
        if 'risk_assessment' in analysis_data:
            risk_data = analysis_data['risk_assessment']
            if isinstance(risk_data, dict):
                updates['churn_risk'] = risk_data.get('churn_probability')
                updates['engagement_level'] = risk_data.get('engagement_level')
        
        # Строим SQL запрос для обновления
        if updates:
            set_clauses = []
            values = []
            
            for field, value in updates.items():
                if value is not None:
                    set_clauses.append(f"{field} = %s")
                    values.append(value)
            
            if set_clauses:
                values.extend([json.dumps(analysis_data, ensure_ascii=False), client_id])
                sql = f"""
                UPDATE user_profiles 
                SET {', '.join(set_clauses)}, 
                    updated_at = CURRENT_TIMESTAMP,
                    analysis_data = %s
                WHERE conv_id = %s
                """
                
                cursor.execute(sql, values)
                rows_updated = cursor.rowcount
                
                logger.info(f"✅ Профиль клиента {client_id} обновлен ({len(set_clauses)} полей, {rows_updated} записей)")
            else:
                logger.warning("Нет данных для обновления профиля")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка при обновлении профиля: {e}")
        raise
    finally:
        cursor.close()

def create_reminder(client_id: str, analysis_data: Dict[str, Any], conn) -> Optional[Dict[str, Any]]:
    """Создание напоминания на основе результатов анализа"""
    logger.info(f"Создание напоминания для клиента {client_id}")
    
    cursor = conn.cursor()
    
    try:
        # Извлекаем данные для создания напоминания
        timing_data = analysis_data.get('next_contact_timing', {})
        strategy_data = analysis_data.get('return_strategy', {})
        
        if not isinstance(timing_data, dict) or not isinstance(strategy_data, dict):
            logger.warning("Недостаточно данных для создания напоминания")
            return None
        
        # Определяем время напоминания
        recommended_timing = timing_data.get('recommended_timing', '1 день')
        
        # Конвертируем в дату
        reminder_date = datetime.now()
        if 'час' in recommended_timing.lower():
            try:
                hours = int(recommended_timing.split()[0])
                reminder_date += timedelta(hours=hours)
            except (ValueError, IndexError):
                reminder_date += timedelta(hours=24)
        elif 'день' in recommended_timing.lower() or 'дня' in recommended_timing.lower():
            try:
                days = int(recommended_timing.split()[0])
                reminder_date += timedelta(days=days)
            except (ValueError, IndexError):
                reminder_date += timedelta(days=1)
        elif 'неделя' in recommended_timing.lower() or 'недели' in recommended_timing.lower():
            try:
                weeks = int(recommended_timing.split()[0])
                reminder_date += timedelta(weeks=weeks)
            except (ValueError, IndexError):
                reminder_date += timedelta(days=7)
        else:
            reminder_date += timedelta(days=1)  # По умолчанию 1 день
        
        # Формируем текст напоминания
        actions = strategy_data.get('recommended_actions', ['Связаться с клиентом'])
        insights = strategy_data.get('key_insights', ['Данные анализа доступны в профиле'])
        
        # Преобразуем действия и инсайты в строки если они списки
        actions_text = '\n'.join(actions) if isinstance(actions, list) else str(actions)
        insights_text = '\n'.join(insights) if isinstance(insights, list) else str(insights)
        
        reminder_text = f"""🤖 АНАЛИЗ КЛИЕНТА: {client_id}

📊 Рекомендуемые действия:
{actions_text}

💡 Ключевые инсайты:
{insights_text}

⏰ Оптимальное время контакта: {timing_data.get('optimal_time', 'рабочие часы')}

📋 Источник: AI-анализ клиентской карточки
"""
        
        # Вставляем напоминание в БД
        sql = """
        INSERT INTO reminders (conv_id, reminder_text, reminder_date, status, created_at)
        VALUES (%s, %s, %s, 'active', CURRENT_TIMESTAMP)
        RETURNING id, reminder_date, status
        """
        
        cursor.execute(sql, (client_id, reminder_text, reminder_date))
        result = cursor.fetchone()
        
        if result:
            reminder_id, actual_date, status = result
            logger.info(f"✅ Напоминание создано (ID: {reminder_id}, дата: {actual_date}, статус: {status})")
            
            conn.commit()
            
            return {
                'id': reminder_id,
                'conv_id': client_id,
                'reminder_text': reminder_text,
                'reminder_date': actual_date,
                'status': status,
                'recommended_timing': recommended_timing
            }
        else:
            logger.error("Напоминание не было создано")
            return None
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка при создании напоминания: {e}")
        raise
    finally:
        cursor.close()

def verify_reminder_created(client_id: str, conn) -> Optional[Dict[str, Any]]:
    """Проверка создания напоминания в БД"""
    cursor = conn.cursor()
    
    try:
        sql = """
        SELECT id, conv_id, reminder_text, reminder_date, status, created_at
        FROM reminders 
        WHERE conv_id = %s 
        ORDER BY created_at DESC 
        LIMIT 1
        """
        
        cursor.execute(sql, (client_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                'id': result[0],
                'conv_id': result[1],
                'reminder_text': result[2],
                'reminder_date': result[3],
                'status': result[4],
                'created_at': result[5]
            }
        else:
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при проверке напоминания: {e}")
        return None
    finally:
        cursor.close()

def main(result_file: str):
    """Основная функция парсера"""
    logger.info("=== ЗАПУСК ПАРСЕРА РЕЗУЛЬТАТОВ АНАЛИЗА ===")
    
    try:
        # Парсим результаты
        analysis_data = parse_analysis_results(result_file)
        
        # Получаем ID клиента
        client_id = analysis_data.get('client_id')
        if not client_id:
            # Пытаемся извлечь из имени файла
            import re
            match = re.search(r'(\d+)', os.path.basename(result_file))
            if match:
                client_id = match.group(1)
            else:
                raise ValueError("Не удалось определить ID клиента")
        
        logger.info(f"ID клиента: {client_id}")
        
        # Подключаемся к БД
        conn = get_database_connection()
        
        try:
            # Получаем профиль до обновления
            logger.info("=== ПРОФИЛЬ ДО ОБНОВЛЕНИЯ ===")
            before_profile = get_client_profile_before_update(client_id, conn)
            if before_profile:
                logger.info(f"Квалификация: {before_profile.get('lead_qualification')}")
                logger.info(f"Этап воронки: {before_profile.get('funnel_stage')}")
                logger.info(f"Активность: {before_profile.get('client_activity')}")
                logger.info(f"Последнее обновление: {before_profile.get('updated_at')}")
            
            # Обновляем профиль клиента
            logger.info("=== ОБНОВЛЕНИЕ ПРОФИЛЯ ===")
            update_client_profile(client_id, analysis_data, conn)
            
            # Создаем напоминание
            logger.info("=== СОЗДАНИЕ НАПОМИНАНИЯ ===")
            reminder_info = create_reminder(client_id, analysis_data, conn)
            
            # Проверяем создание напоминания
            logger.info("=== ПРОВЕРКА НАПОМИНАНИЯ ===")
            verification = verify_reminder_created(client_id, conn)
            
            if verification:
                logger.info(f"✅ НАПОМИНАНИЕ ПОДТВЕРЖДЕНО В БД:")
                logger.info(f"   ID: {verification['id']}")
                logger.info(f"   Дата: {verification['reminder_date']}")
                logger.info(f"   Статус: {verification['status']}")
                logger.info(f"   Создано: {verification['created_at']}")
            else:
                logger.error("❌ Напоминание не найдено в БД")
            
            logger.info("✅ Парсинг и обновление БД завершены успешно!")
            
            # Возвращаем информацию для отчета
            return {
                'before_profile': before_profile,
                'after_analysis': analysis_data,
                'reminder_info': reminder_info,
                'verification': verification
            }
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка в работе парсера: {e}")
        import traceback
        logger.error(f"Детали ошибки: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python results_parser.py <файл_результатов>")
        sys.exit(1)
    
    main(sys.argv[1])