#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для выборки 500 conv_id из базы данных с приоритизацией
Исключает пользователей с активными напоминаниями и покупателей премиум-наборов
"""

import os
import sys
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from typing import Dict, List, Tuple, Any
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('search_people.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы для системы баллов
WARMTH_SCORES = {
    'горячий': 4,
    'клиент': 3, 
    'тёплый': 2,
    'холодный': 1
}

FUNNEL_SCORES = {
    'решение принято (ожидаем оплату)': 6,
    'клиент думает': 5,
    'у клиента есть возражения': 4,
    'сделано предложение по продуктам': 3,
    'сделано новое предложение': 2,
    'предложение по продуктам ещё не сделано, покупка совершена': 1
}

# Исключаемые продукты (фразы для поиска)
EXCLUDED_PRODUCTS = ['всё включено', '6 шагов']

class PeopleSearcher:
    """Класс для поиска и приоритизации пользователей"""
    
    def __init__(self):
        """Инициализация"""
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL не установлен в переменных окружения")
        
        self.stats = {
            'total_collected': 0,
            'warmth_distribution': {'горячий': 0, 'клиент': 0, 'тёплый': 0, 'холодный': 0, 'неизвестно': 0},
            'funnel_distribution': {},
            'excluded_with_reminders': 0,
            'excluded_premium_buyers': 0
        }
    
    def get_db_connection(self) -> psycopg2.extensions.connection:
        """Получение подключения к БД"""
        try:
            conn = psycopg2.connect(self.database_url)
            return conn
        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
    
    def _calculate_warmth_score(self, lead_qualification: List[str]) -> int:
        """Подсчет баллов за теплоту клиента"""
        if not lead_qualification:
            return WARMTH_SCORES['холодный']  # По умолчанию холодный
        
        # Берем первое значение из массива или ищем максимальный балл
        max_score = 0
        for qual in lead_qualification:
            if qual in WARMTH_SCORES:
                max_score = max(max_score, WARMTH_SCORES[qual])
        
        return max_score if max_score > 0 else WARMTH_SCORES['холодный']
    
    def _calculate_funnel_score(self, funnel_stage: str) -> int:
        """Подсчет баллов за этап воронки"""
        if not funnel_stage:
            return 0
        
        # Ищем точное совпадение
        if funnel_stage in FUNNEL_SCORES:
            return FUNNEL_SCORES[funnel_stage]
        
        # Если точного совпадения нет, возвращаем 0
        return 0
    
    def _is_premium_buyer(self, conv_id: int) -> bool:
        """Проверка, покупал ли пользователь премиум-наборы"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Проверяем в purchased_products
                    for product_phrase in EXCLUDED_PRODUCTS:
                        cur.execute("""
                            SELECT COUNT(*) FROM purchased_products 
                            WHERE conv_id = %s 
                            AND LOWER(product_name) LIKE %s
                        """, (conv_id, f'%{product_phrase.lower()}%'))
                        
                        if cur.fetchone()[0] > 0:
                            return True
                    
                    # Проверяем в client_purchases
                    for product_phrase in EXCLUDED_PRODUCTS:
                        cur.execute("""
                            SELECT COUNT(*) FROM client_purchases 
                            WHERE conv_id = %s 
                            AND LOWER(product_name) LIKE %s
                        """, (conv_id, f'%{product_phrase.lower()}%'))
                        
                        if cur.fetchone()[0] > 0:
                            return True
                    
                    return False
                    
        except Exception as e:
            logger.warning(f"Ошибка проверки покупок для {conv_id}: {e}")
            return False
    
    def search_prioritized_people(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Поиск и приоритизация пользователей с оптимизированным SQL
        
        Args:
            limit: Максимальное количество результатов
            
        Returns:
            Список пользователей с приоритизацией
        """
        logger.info("Начинаем поиск приоритизированных пользователей")
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    # Сначала подсчитаем исключенных пользователей для статистики
                    try:
                        cur.execute("""
                            SELECT COUNT(*) FROM reminders WHERE status = 'active'
                        """)
                        result = cur.fetchone()
                        active_reminders_count = result[0] if result else 0
                        self.stats['excluded_with_reminders'] = active_reminders_count
                        logger.info(f"Исключено пользователей с активными напоминаниями: {active_reminders_count}")
                    except Exception as e:
                        logger.warning(f"Ошибка подсчета активных напоминаний: {e}")
                        self.stats['excluded_with_reminders'] = 0
                    
                    # Подсчитываем покупателей премиум-наборов
                    try:
                        cur.execute("""
                            SELECT COUNT(DISTINCT conv_id) FROM (
                                SELECT conv_id FROM purchased_products
                                WHERE LOWER(product_name) LIKE %s
                                   OR LOWER(product_name) LIKE %s
                                UNION
                                SELECT conv_id FROM client_purchases
                                WHERE LOWER(product_name) LIKE %s
                                   OR LOWER(product_name) LIKE %s
                                   AND conv_id IS NOT NULL
                            ) AS premium_buyers
                        """, ('%всё включено%', '%6 шагов%', '%всё включено%', '%6 шагов%'))
                        result = cur.fetchone()
                        premium_buyers_count = result[0] if result else 0
                        self.stats['excluded_premium_buyers'] = premium_buyers_count
                        logger.info(f"Исключено покупателей премиум-наборов: {premium_buyers_count}")
                    except Exception as e:
                        logger.warning(f"Ошибка подсчета премиум-покупателей: {e}")
                        self.stats['excluded_premium_buyers'] = 0
                    
                    # Оптимизированный основной запрос с исключениями в одном SQL
                    query = """
                    SELECT
                        up.conv_id,
                        up.first_name,
                        up.last_name,
                        up.lead_qualification,
                        up.funnel_stage,
                        up.dialogue_summary,
                        -- Расчет баллов прямо в SQL
                        CASE
                            WHEN %s = ANY(up.lead_qualification) THEN 4
                            WHEN %s = ANY(up.lead_qualification) THEN 3
                            WHEN %s = ANY(up.lead_qualification) THEN 2
                            ELSE 1
                        END as warmth_score,
                        CASE up.funnel_stage
                            WHEN %s THEN 6
                            WHEN %s THEN 5
                            WHEN %s THEN 4
                            WHEN %s THEN 3
                            WHEN %s THEN 2
                            WHEN %s THEN 1
                            ELSE 0
                        END as funnel_score
                    FROM user_profiles up
                    WHERE
                        -- Исключаем пользователей с активными напоминаниями
                        up.conv_id NOT IN (
                            SELECT conv_id FROM reminders
                            WHERE status = 'active' AND conv_id IS NOT NULL
                        )
                        -- Исключаем покупателей премиум-наборов
                        AND up.conv_id NOT IN (
                            SELECT DISTINCT conv_id FROM purchased_products
                            WHERE conv_id IS NOT NULL AND (
                                LOWER(product_name) LIKE %s
                                OR LOWER(product_name) LIKE %s
                            )
                        )
                        AND up.conv_id NOT IN (
                            SELECT DISTINCT conv_id FROM client_purchases
                            WHERE conv_id IS NOT NULL AND (
                                LOWER(product_name) LIKE %s
                                OR LOWER(product_name) LIKE %s
                            )
                        )
                        -- Исключаем пользователей с пустым conv_id
                        AND up.conv_id IS NOT NULL
                    ORDER BY (
                        CASE
                            WHEN %s = ANY(up.lead_qualification) THEN 4
                            WHEN %s = ANY(up.lead_qualification) THEN 3
                            WHEN %s = ANY(up.lead_qualification) THEN 2
                            ELSE 1
                        END +
                        CASE up.funnel_stage
                            WHEN %s THEN 6
                            WHEN %s THEN 5
                            WHEN %s THEN 4
                            WHEN %s THEN 3
                            WHEN %s THEN 2
                            WHEN %s THEN 1
                            ELSE 0
                        END
                    ) DESC
                    LIMIT %s
                    """
                    
                    query_params = (
                        'горячий', 'клиент', 'тёплый',  # warmth_score
                        'решение принято (ожидаем оплату)', 'клиент думает', 'у клиента есть возражения',
                        'сделано предложение по продуктам', 'сделано новое предложение',
                        'предложение по продуктам ещё не сделано, покупка совершена',  # funnel_score
                        '%всё включено%', '%6 шагов%',  # purchased_products exclusion
                        '%всё включено%', '%6 шагов%',  # client_purchases exclusion
                        'горячий', 'клиент', 'тёплый',  # ORDER BY warmth
                        'решение принято (ожидаем оплату)', 'клиент думает', 'у клиента есть возражения',
                        'сделано предложение по продуктам', 'сделано новое предложение',
                        'предложение по продуктам ещё не сделано, покупка совершена',  # ORDER BY funnel
                        limit
                    )
                    
                    cur.execute(query, query_params)
                    results = cur.fetchall()
                    
                    logger.info(f"Отобрано {len(results)} приоритизированных пользователей")
                    
                    # Преобразуем результаты и собираем статистику
                    prioritized_users = []
                    
                    for row in results:
                        # Определяем теплоту для статистики
                        warmth = 'неизвестно'
                        if row['lead_qualification']:
                            for qual in row['lead_qualification']:
                                if qual in WARMTH_SCORES:
                                    warmth = qual
                                    break
                        
                        user_data = {
                            'conv_id': row['conv_id'],
                            'first_name': row['first_name'],
                            'last_name': row['last_name'],
                            'lead_qualification': row['lead_qualification'],
                            'funnel_stage': row['funnel_stage'],
                            'dialogue_summary': row['dialogue_summary'],
                            'warmth_score': row['warmth_score'],
                            'funnel_score': row['funnel_score'],
                            'total_score': row['warmth_score'] + row['funnel_score'],
                            'warmth_category': warmth
                        }
                        
                        prioritized_users.append(user_data)
                        
                        # Обновляем статистику
                        self.stats['warmth_distribution'][warmth] += 1
                        funnel_stage = row['funnel_stage'] or 'неизвестно'
                        self.stats['funnel_distribution'][funnel_stage] = self.stats['funnel_distribution'].get(funnel_stage, 0) + 1
                    
                    self.stats['total_collected'] = len(prioritized_users)
                    
                    logger.info(f"Финальный результат: {len(prioritized_users)} приоритизированных пользователей")
                    return prioritized_users
                    
        except Exception as e:
            logger.error(f"Ошибка поиска пользователей: {e}")
            raise
    
    def save_results(self, users: List[Dict[str, Any]]) -> str:
        """
        Сохранение результатов в файл
        
        Args:
            users: Список пользователей
            
        Returns:
            Путь к созданному файлу
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"founded_people_{timestamp}.py"
        
        # Подготавливаем данные для сохранения в Python-формате
        conv_ids = [user['conv_id'] for user in users]
        
        content = f"""#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
Результаты поиска приоритизированных пользователей
Создано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

ЛОГИКА РАБОТЫ:
1. Из базы ИСКЛЮЧЕНЫ пользователи с активными напоминаниями: {self.stats['excluded_with_reminders']}
2. Из базы ИСКЛЮЧЕНЫ покупатели премиум-наборов: {self.stats['excluded_premium_buyers']}
3. Из оставшихся пользователей ОТОБРАНО лучших: {len(users)}

ИТОГО В СПИСКЕ: {len(users)} приоритизированных пользователей (БЕЗ исключенных)
\"\"\"

# Список conv_id приоритизированных пользователей (топ-{len(users)})
# Отсортированы по убыванию приоритета (теплота + воронка)
PRIORITIZED_CONV_IDS = {conv_ids}

# Статистика поиска (только для выбранных {len(users)} пользователей)
SEARCH_STATS = {{
    'selected_users': {self.stats['total_collected']},
    'excluded_with_reminders': {self.stats['excluded_with_reminders']},
    'excluded_premium_buyers': {self.stats['excluded_premium_buyers']},
    'warmth_distribution': {dict(self.stats['warmth_distribution'])},
    'funnel_distribution': {dict(self.stats['funnel_distribution'])},
    'search_timestamp': '{timestamp}'
}}

if __name__ == "__main__":
    print(f"Загружено {{len(PRIORITIZED_CONV_IDS)}} приоритизированных пользователей")
    print(f"Исключено пользователей с активными напоминаниями: {{SEARCH_STATS['excluded_with_reminders']}}")
    print(f"Исключено покупателей премиум-наборов: {{SEARCH_STATS['excluded_premium_buyers']}}")
    print(f"Статистика выбранных пользователей: {{SEARCH_STATS}}")
"""
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Результаты сохранены в файл: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Ошибка сохранения файла: {e}")
            raise
    
    def print_statistics(self):
        """Вывод статистики в лог"""
        logger.info("=== СТАТИСТИКА ПОИСКА ===")
        logger.info("ЛОГИКА: Сначала исключили пользователей с активными напоминаниями и покупателей премиум-наборов,")
        logger.info("        затем из оставшихся отобрали 500 лучших по приоритету")
        logger.info("")
        logger.info(f"ИСКЛЮЧЕНО из поиска:")
        logger.info(f"  • Пользователей с активными напоминаниями: {self.stats['excluded_with_reminders']}")
        logger.info(f"  • Покупателей премиум-наборов: {self.stats['excluded_premium_buyers']}")
        logger.info("")
        logger.info(f"ИТОГО СОБРАНО в список: {self.stats['total_collected']} человек")
        logger.info("")
        
        logger.info("Распределение СОБРАННЫХ пользователей по теплоте:")
        for warmth, count in self.stats['warmth_distribution'].items():
            if count > 0:
                logger.info(f"  {warmth}: {count}")
        
        logger.info("")
        logger.info("Распределение СОБРАННЫХ пользователей по этапам воронки:")
        for stage, count in sorted(self.stats['funnel_distribution'].items()):
            if count > 0:
                logger.info(f"  {stage}: {count}")

def main():
    """Основная функция"""
    try:
        logger.info("Запуск скрипта поиска приоритизированных пользователей")
        
        # Создаем экземпляр поисковика
        searcher = PeopleSearcher()
        
        # Выполняем поиск
        users = searcher.search_prioritized_people(limit=500)
        
        if not users:
            logger.warning("Пользователи не найдены")
            return
        
        # Сохраняем результаты
        filename = searcher.save_results(users)
        
        # Выводим статистику
        searcher.print_statistics()
        
        print(f"\n{'='*50}")
        print(f"ПОИСК ЗАВЕРШЕН УСПЕШНО")
        print(f"{'='*50}")
        print(f"Найдено пользователей: {len(users)}")
        print(f"Результаты сохранены в: {filename}")
        print(f"{'='*50}")
        
        logger.info("Скрипт завершен успешно")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()