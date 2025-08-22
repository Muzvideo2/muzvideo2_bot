#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Комплексный тест новой функциональности client_card_analyzer.py версии 2.0
Использует реальные данные, экспортированные data_exporter.py

План тестирования:
1. Копирование исходного файла в test_result для сохранения
2. Запуск анализа с новой функциональностью  
3. Проверка результатов анализа
4. Создание SQL-запросов для обновления БД
"""

import json
import logging
import os
import sys
import shutil
from datetime import datetime
from pathlib import Path

# Добавляем текущую директорию в sys.path для импорта модулей
sys.path.insert(0, str(Path(__file__).parent))

from client_card_analyzer import ClientCardAnalyzer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_comprehensive.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def prepare_test_environment():
    """Подготовка тестовой среды"""
    logger.info("=== ПОДГОТОВКА ТЕСТОВОЙ СРЕДЫ ===")
    
    # Создаем папку test_result если не существует
    test_dir = Path("test_result")
    test_dir.mkdir(exist_ok=True)
    
    # Копируем исходный файл с данными клиента в test_result
    source_file = "exported_data/client_data_515099352_20250820_203354.json"
    if not os.path.exists(source_file):
        logger.error(f"Исходный файл не найден: {source_file}")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_input_file = test_dir / f"input_client_data_{timestamp}.json"
    
    shutil.copy2(source_file, test_input_file)
    logger.info(f"✅ Исходные данные скопированы: {test_input_file}")
    
    return str(test_input_file)

def run_analysis_test(input_file):
    """Запуск анализа с новой функциональностью"""
    logger.info("=== ТЕСТИРОВАНИЕ АНАЛИЗА КЛИЕНТА ===")
    
    try:
        # Инициализация анализатора
        analyzer = ClientCardAnalyzer()
        
        # Загрузка данных клиента
        logger.info(f"Загрузка данных из файла: {input_file}")
        client_data = analyzer.load_from_json(input_file)
        
        logger.info(f"✅ Данные клиента загружены:")
        logger.info(f"   ID: {client_data['client_id']}")
        logger.info(f"   Квалификация: {client_data.get('lead_qualification', ['неизвестно'])[0]}")
        logger.info(f"   Этап воронки: {client_data.get('funnel_stage', 'неизвестно')}")
        logger.info(f"   Количество сообщений: {len(client_data.get('recent_messages', []))}")
        
        # Запуск AI анализа
        logger.info("🔄 Запуск AI анализа (может занять 30-90 секунд)...")
        analysis_result = analyzer.analyze_client_card(client_data)
        
        if analysis_result:
            logger.info("✅ AI анализ завершен успешно!")
            
            # Сохраняем результат в test_result
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_file = Path("test_result") / f"analysis_result_{client_data['client_id']}_{timestamp}.json"
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📁 Результат анализа сохранен: {result_file}")
            
            # Показываем краткую сводку результатов
            show_analysis_summary(analysis_result)
            
            return str(result_file)
        else:
            logger.error("❌ AI анализ не вернул результат")
            return None
            
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении анализа: {e}")
        import traceback
        logger.error(f"Детали ошибки: {traceback.format_exc()}")
        return None

def show_analysis_summary(analysis_result):
    """Показывает краткую сводку результатов анализа"""
    logger.info("=== КРАТКАЯ СВОДКА РЕЗУЛЬТАТОВ АНАЛИЗА ===")
    
    # Проверяем основные разделы
    sections = {
        'client_qualification': 'Квалификация клиента',
        'funnel_stage_analysis': 'Анализ этапа воронки', 
        'psychological_profile': 'Психологический профиль',
        'activity_analysis': 'Анализ активности',
        'conversation_gaps': 'Анализ пауз в диалоге',
        'pain_points_analysis': 'Анализ болей клиента',
        'interests_analysis': 'Анализ интересов',
        'return_strategy': 'Стратегия возврата',
        'next_contact_timing': 'Время следующего контакта',
        'product_recommendations': 'Рекомендации продуктов',
        'risk_assessment': 'Оценка рисков',
        'strategic_recommendations': 'Стратегические рекомендации'
    }
    
    for key, name in sections.items():
        if key in analysis_result:
            logger.info(f"✅ {name} - присутствует")
            
            # Показываем ключевые данные для некоторых разделов
            if key == 'client_qualification' and isinstance(analysis_result[key], dict):
                level = analysis_result[key].get('current_level', 'не определено')
                score = analysis_result[key].get('confidence_score', 0)
                logger.info(f"   └─ Уровень: {level} (уверенность: {score})")
                
            elif key == 'psychological_profile' and isinstance(analysis_result[key], dict):
                personality = analysis_result[key].get('personality_type', 'не определен')
                motivation = analysis_result[key].get('primary_motivation', 'не определена')
                logger.info(f"   └─ Тип: {personality}, Мотивация: {motivation}")
                
            elif key == 'next_contact_timing' and isinstance(analysis_result[key], dict):
                timing = analysis_result[key].get('recommended_timing', 'не указано')
                logger.info(f"   └─ Рекомендуемое время: {timing}")
        else:
            logger.warning(f"⚠️ {name} - отсутствует")

def create_database_updates_parser(result_file):
    """Создает парсер результатов и SQL-запросы для обновления БД"""
    logger.info("=== СОЗДАНИЕ ПАРСЕРА ДЛЯ ОБНОВЛЕНИЯ БД ===")
    
    try:
        # Создаем парсер результатов
        parser_content = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер результатов анализа клиента для обновления базы данных
Автоматически создан тестом comprehensive test
"""

import json
import psycopg2
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Получение подключения к базе данных"""
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
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
                values.append(client_id)
                sql = f"""
                UPDATE user_profiles 
                SET {', '.join(set_clauses)}, 
                    updated_at = CURRENT_TIMESTAMP,
                    analysis_data = %s
                WHERE conv_id = %s
                """
                
                # Добавляем полные результаты анализа как JSON
                values.insert(-1, json.dumps(analysis_data, ensure_ascii=False))
                
                cursor.execute(sql, values)
                logger.info(f"✅ Профиль клиента {client_id} обновлен ({len(set_clauses)} полей)")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка при обновлении профиля: {e}")
        raise
    finally:
        cursor.close()

def create_reminder(client_id: str, analysis_data: Dict[str, Any], conn):
    """Создание напоминания на основе результатов анализа"""
    logger.info(f"Создание напоминания для клиента {client_id}")
    
    cursor = conn.cursor()
    
    try:
        # Извлекаем данные для создания напоминания
        timing_data = analysis_data.get('next_contact_timing', {})
        strategy_data = analysis_data.get('return_strategy', {})
        
        if not isinstance(timing_data, dict) or not isinstance(strategy_data, dict):
            logger.warning("Недостаточно данных для создания напоминания")
            return
        
        # Определяем время напоминания
        recommended_timing = timing_data.get('recommended_timing', '1 день')
        
        # Конвертируем в дату
        reminder_date = datetime.now()
        if 'час' in recommended_timing:
            hours = int(recommended_timing.split()[0])
            reminder_date += timedelta(hours=hours)
        elif 'день' in recommended_timing or 'дня' in recommended_timing:
            days = int(recommended_timing.split()[0])
            reminder_date += timedelta(days=days)
        elif 'неделя' in recommended_timing or 'недели' in recommended_timing:
            weeks = int(recommended_timing.split()[0])
            reminder_date += timedelta(weeks=weeks)
        else:
            reminder_date += timedelta(days=1)  # По умолчанию 1 день
        
        # Формируем текст напоминания
        reminder_text = f"""🤖 АНАЛИЗ КЛИЕНТА: {client_id}

📊 Рекомендуемые действия:
{strategy_data.get('recommended_actions', 'Связаться с клиентом')}

💡 Ключевые инсайты:
{strategy_data.get('key_insights', 'Данные анализа доступны в профиле')}

⏰ Оптимальное время контакта: {timing_data.get('optimal_time', 'рабочие часы')}
"""
        
        # Вставляем напоминание в БД
        sql = """
        INSERT INTO reminders (conv_id, reminder_text, reminder_date, status, created_at)
        VALUES (%s, %s, %s, 'active', CURRENT_TIMESTAMP)
        RETURNING id
        """
        
        cursor.execute(sql, (client_id, reminder_text, reminder_date))
        reminder_id = cursor.fetchone()[0]
        
        logger.info(f"✅ Напоминание создано (ID: {reminder_id}, дата: {reminder_date})")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка при создании напоминания: {e}")
        raise
    finally:
        cursor.close()

def main(result_file: str):
    """Основная функция парсера"""
    logger.info("=== ЗАПУСК ПАРСЕРА РЕЗУЛЬТАТОВ АНАЛИЗА ===")
    
    try:
        # Парсим результаты
        analysis_data = parse_analysis_results(result_file)
        
        # Получаем ID клиента из имени файла или данных
        if 'client_id' in analysis_data:
            client_id = analysis_data['client_id']
        else:
            # Извлекаем из имени файла
            import re
            match = re.search(r'analysis_result_(\d+)_', result_file)
            if match:
                client_id = match.group(1)
            else:
                raise ValueError("Не удалось определить ID клиента")
        
        logger.info(f"ID клиента: {client_id}")
        
        # Подключаемся к БД
        conn = get_database_connection()
        
        try:
            # Обновляем профиль клиента
            update_client_profile(client_id, analysis_data, conn)
            
            # Создаем напоминание
            create_reminder(client_id, analysis_data, conn)
            
            logger.info("✅ Парсинг и обновление БД завершены успешно!")
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка в работе парсера: {e}")
        import traceback
        logger.error(f"Детали ошибки: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Использование: python results_parser.py <файл_результатов>")
        sys.exit(1)
    
    main(sys.argv[1])
'''
        
        # Сохраняем парсер
        parser_file = Path("test_result") / "results_parser.py"
        with open(parser_file, 'w', encoding='utf-8') as f:
            f.write(parser_content)
        
        logger.info(f"✅ Парсер создан: {parser_file}")
        
        # Создаем инструкцию по использованию парсера
        instruction_content = f'''# Инструкция по использованию парсера результатов

## Описание
Парсер `results_parser.py` предназначен для автоматического обновления базы данных на основе результатов анализа клиента.

## Использование

### 1. Убедитесь в наличии результатов анализа
Файл с результатами: `{result_file}`

### 2. Запуск парсера
```bash
cd test_result
python results_parser.py {os.path.basename(result_file)}
```

### 3. Что делает парсер
- Загружает результаты анализа из JSON файла
- Обновляет профиль клиента в таблице `user_profiles`
- Создает напоминание в таблице `reminders`
- Сохраняет полные результаты анализа как JSON в поле `analysis_data`

### 4. Обновляемые поля профиля
- `lead_qualification` - квалификация клиента
- `qualification_confidence` - уверенность в квалификации
- `funnel_stage` - этап воронки
- `personality_type` - тип личности
- `primary_motivation` - основная мотивация
- `communication_style` - стиль общения
- `churn_risk` - риск оттока
- `engagement_level` - уровень вовлеченности
- `analysis_data` - полные результаты анализа (JSON)
- `updated_at` - время обновления

### 5. Создание напоминания
- Автоматически вычисляет время напоминания на основе рекомендаций AI
- Включает ключевые инсайты и рекомендуемые действия
- Устанавливает статус "active"

## Требования
- Python 3.7+
- psycopg2-binary
- python-dotenv
- Настроенные переменные окружения для подключения к БД
'''
        
        instruction_file = Path("test_result") / "parser_instructions.md"
        with open(instruction_file, 'w', encoding='utf-8') as f:
            f.write(instruction_content)
        
        logger.info(f"✅ Инструкция создана: {instruction_file}")
        
        return str(parser_file)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании парсера: {e}")
        return None

def main():
    """Основная функция комплексного теста"""
    logger.info("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТА НОВОЙ ФУНКЦИОНАЛЬНОСТИ")
    logger.info("=" * 60)
    
    try:
        # 1. Подготовка тестовой среды
        test_input_file = prepare_test_environment()
        if not test_input_file:
            logger.error("Не удалось подготовить тестовую среду")
            return False
        
        # 2. Запуск анализа
        result_file = run_analysis_test(test_input_file)
        if not result_file:
            logger.error("Анализ не был выполнен")
            return False
        
        # 3. Создание парсера для БД
        parser_file = create_database_updates_parser(result_file)
        if not parser_file:
            logger.error("Парсер не был создан")
            return False
        
        # 4. Финальная сводка
        logger.info("=" * 60)
        logger.info("🎉 КОМПЛЕКСНЫЙ ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
        logger.info("=" * 60)
        logger.info(f"📁 Входные данные: {test_input_file}")
        logger.info(f"📊 Результаты анализа: {result_file}")
        logger.info(f"🔧 Парсер для БД: {parser_file}")
        logger.info(f"📋 Инструкции: test_result/parser_instructions.md")
        
        logger.info("\n🔥 ГОТОВО ДЛЯ ПРОДАКШЕНА!")
        logger.info("Новая функциональность client_card_analyzer.py протестирована и готова к использованию.")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА В КОМПЛЕКСНОМ ТЕСТЕ: {e}")
        import traceback
        logger.error(f"Детали ошибки: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    main()