#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Анализатор карточек клиентов с расширенной функциональностью
Версия 2.1 - Добавлена логика скидок на день рождения
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account

# Импорт функций для расчета скидки на день рождения
try:
    from main import calculate_birthday_discount_status
except ImportError:
    # Если не можем импортировать, определяем функцию локально
    def calculate_birthday_discount_status(birth_day, birth_month):
        """
        Локальная копия функции для расчета статуса скидки на день рождения.
        """
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
            
            current_date_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            days_until_birthday_this_year = (birthday_this_year - current_date_start).days
            
            if -5 <= days_until_birthday_this_year <= 5:
                days_until_birthday = days_until_birthday_this_year
                actual_birthday = birthday_this_year
            else:
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
            logging.error(f"Ошибка при расчете статуса скидки на день рождения: {e}")
            return {
                'status': 'not_applicable',
                'message': '',
                'days_until_birthday': None,
                'birthday_formatted': ''
            }

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('client_card_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Константы
PROJECT_ID = "zeta-tracer-462306-r7"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-pro"
DATABASE_URL = os.environ.get("DATABASE_URL")

# Расширенный промпт для анализа карточки клиента
CARD_ANALYSIS_PROMPT = """
Ты — эксперт-аналитик онлайн-школы фортепиано MuzVideo2.ru, специализирующийся на глубоком анализе клиентских карточек.

ТВОЯ ЗАДАЧА: Провести комплексный анализ клиента и предоставить стратегические рекомендации для улучшения взаимодействия.

=== ИНФОРМАЦИЯ О ПРОДУКТАХ ШКОЛЫ ===
{products_info}

=== ДАННЫЕ КЛИЕНТА ===
{client_data}

=== ИНФОРМАЦИЯ О СКИДКЕ НА ДЕНЬ РОЖДЕНИЯ ===
{birthday_discount_message}

=== ТРЕБОВАНИЯ К АНАЛИЗУ ===

1. **КВАЛИФИКАЦИЯ ЛИДА** (lead_qualification):
   - холодный: не проявляет интереса, нужен прогрев
   - тёплый: есть интерес, но нужна мотивация
   - горячий: готов к покупке, нужно предложение
   - клиент: уже купил, нужна поддержка

2. **ЭТАП ВОРОНКИ** (funnel_stage):
   - Первый контакт: только узнал о школе
   - Изучение предложения: рассматривает курсы
   - Сравнение вариантов: выбирает между курсами
   - Готовность к покупке: почти решился
   - Покупка: совершил заказ
   - Обучение: проходит курсы
   - Завершение: закончил обучение

3. **УРОВЕНЬ КЛИЕНТА** (client_level):
   - Новичок: никогда не играл
   - Начинающий: базовые навыки
   - Продолжающий: средний уровень
   - Продвинутый: высокий уровень

4. **ЦЕЛИ ОБУЧЕНИЯ** (learning_goals) - выбери наиболее подходящие:
   - Научиться играть популярные песни
   - Освоить классическую музыку
   - Развить импровизацию
   - Изучить аккорды и гармонию
   - Восстановить утраченные навыки
   - Подготовиться к выступлениям
   - Обучение для удовольствия

5. **БОЛИ КЛИЕНТА** (client_pains) - определи основные проблемы:
   - Нет времени на регулярные занятия
   - Сомнения в своих способностях
   - Финансовые ограничения
   - Отсутствие инструмента
   - Страх неудачи
   - Прошлый негативный опыт обучения
   - Сложность выбора курса

6. **АНАЛИЗ АКТИВНОСТИ КЛИЕНТА**:
   - Частота сообщений (высокая/средняя/низкая)
   - Время ответов (быстро/средне/медленно)
   - Инициативность в диалоге (активный/пассивный)
   - Качество вопросов (детальные/поверхностные)

7. **АНАЛИЗ ПРОБЕЛОВ В РАЗГОВОРЕ**:
   - Есть ли длительные паузы в диалоге?
   - Когда был последний контакт?
   - Нужно ли возобновить общение?
   - Причины затишья в диалоге

8. **СТРАТЕГИИ ВОЗВРАТА**:
   - Если клиент пропал - как его вернуть?
   - Какие триггеры использовать?
   - Персональные предложения
   - Временные рамки для контакта

9. **ОПТИМАЛЬНОЕ ВРЕМЯ НАПОМИНАНИЯ**:
   - Когда лучше всего связаться?
   - Через сколько дней/часов?
   - В какое время суток?
   - Какой повод для контакта?

10. **РЕКОМЕНДАЦИИ ПО ПРОДУКТАМ**:
    - Какие курсы подойдут лучше всего?
    - Стоит ли предложить набор курсов?
    - Нужны ли скидки или бонусы?
    - Альтернативные варианты

11. **ПСИХОЛОГИЧЕСКИЙ ПРОФИЛЬ**:
    - Тип личности (аналитик/мечтатель/практик/скептик)
    - Мотивационные факторы
    - Страхи и сомнения
    - Предпочтения в общении

12. **СТРАТЕГИЯ ОБЩЕНИЯ**:
    - Тон и стиль коммуникации
    - Ключевые аргументы
    - Что подчеркнуть, что избегать
    - Персонализация подхода

ВЕРНИ РЕЗУЛЬТАТ СТРОГО В JSON ФОРМАТЕ:
{
  "lead_qualification": "холодный/тёплый/горячий/клиент",
  "funnel_stage": "этап воронки",
  "client_level": ["уровень1", "уровень2"],
  "learning_goals": ["цель1", "цель2", "цель3"],
  "client_pains": ["боль1", "боль2", "боль3"],
  "activity_analysis": {
    "message_frequency": "высокая/средняя/низкая",
    "response_time": "быстро/средне/медленно", 
    "initiative_level": "активный/пассивный",
    "question_quality": "детальные/поверхностные"
  },
  "conversation_gaps": {
    "has_long_pauses": true/false,
    "last_contact_days": число_дней,
    "needs_reengagement": true/false,
    "gap_reasons": ["причина1", "причина2"]
  },
  "return_strategies": {
    "recommended_approach": "описание стратегии",
    "triggers_to_use": ["триггер1", "триггер2"],
    "personal_offers": ["предложение1", "предложение2"],
    "urgency_level": "высокий/средний/низкий"
  },
  "optimal_reminder_timing": {
    "contact_in_days": число_дней,
    "best_time_of_day": "утро/день/вечер",
    "contact_reason": "причина для связи",
    "message_tone": "дружелюбный/деловой/мотивирующий"
  },
  "product_recommendations": {
    "primary_courses": ["курс1", "курс2"],
    "course_bundles": ["набор1", "набор2"],
    "discount_needed": true/false,
    "alternative_options": ["вариант1", "вариант2"]
  },
  "psychological_profile": {
    "personality_type": "аналитик/мечтатель/практик/скептик",
    "motivation_factors": ["фактор1", "фактор2"],
    "fears_and_doubts": ["страх1", "страх2"],
    "communication_preferences": ["предпочтение1", "предпочтение2"]
  },
  "communication_strategy": {
    "recommended_tone": "описание тона",
    "key_arguments": ["аргумент1", "аргумент2"],
    "emphasize": ["что подчеркнуть1", "что подчеркнуть2"],
    "avoid": ["чего избегать1", "чего избегать2"],
    "personalization_tips": ["совет1", "совет2"]
  },
  "dialogue_summary": "Краткое резюме диалога и ключевых моментов",
  "action_priority": "высокий/средний/низкий",
  "next_steps": ["шаг1", "шаг2", "шаг3"]
}
"""

class ClientCardAnalyzer:
    """Анализатор карточек клиентов с интеграцией БД"""
    
    def __init__(self):
        """Инициализация анализатора"""
        self.model = None
        self.products_info = ""
        self._initialize_vertex_ai()
        self._load_products_info()
    
    def _initialize_vertex_ai(self):
        """Инициализация Vertex AI"""
        try:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if not credentials_path:
                raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS не установлена")
            
            credentials_path = credentials_path.strip(' "')
            if not os.path.exists(credentials_path):
                raise RuntimeError(f"Файл учетных данных не найден: {credentials_path}")
            
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
            self.model = GenerativeModel(MODEL_NAME)
            
            logging.info("Vertex AI успешно инициализирован")
            
        except Exception as e:
            logging.error(f"Ошибка инициализации Vertex AI: {e}")
            raise
    
    def _load_products_info(self):
        """Загрузка информации о продуктах из базы знаний"""
        try:
            knowledge_base_path = "knowledge_base.json"
            if os.path.exists(knowledge_base_path):
                with open(knowledge_base_path, "r", encoding="utf-8") as f:
                    knowledge_base = json.load(f)
                
                # Извлекаем информацию о курсах и продуктах
                product_entries = []
                for key, value in knowledge_base.items():
                    if any(keyword in key.lower() for keyword in [
                        'курс', 'мастер-класс', 'набор', 'обучение', 'стоимость'
                    ]):
                        product_entries.append(f"• {key}: {value}")
                
                self.products_info = "\n".join(product_entries[:20])  # Ограничиваем размер
                logging.info("Информация о продуктах загружена")
            else:
                self.products_info = "Информация о продуктах недоступна"
                logging.warning("Файл knowledge_base.json не найден")
                
        except Exception as e:
            logging.error(f"Ошибка загрузки информации о продуктах: {e}")
            self.products_info = "Ошибка загрузки информации о продуктах"
    
    def get_db_connection(self) -> psycopg2.extensions.connection:
        """Получение соединения с БД"""
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL не настроен")
        
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except psycopg2.Error as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            raise
    
    def load_client_data_from_db(self, conv_id: int) -> Dict[str, Any]:
        """Загрузка данных клиента из БД"""
        client_data = {
            "conv_id": conv_id,
            "profile": {},
            "dialogue_history": [],
            "purchases": [],
            "active_reminders": []
        }
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    # Профиль клиента
                    cur.execute("""
                        SELECT * FROM user_profiles WHERE conv_id = %s
                    """, (conv_id,))
                    profile = cur.fetchone()
                    if profile:
                        client_data["profile"] = dict(profile)
                    
                    # История диалогов (последние 20 сообщений)
                    cur.execute("""
                        SELECT role, message, created_at 
                        FROM dialogues 
                        WHERE conv_id = %s 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """, (conv_id,))
                    messages = cur.fetchall()
                    client_data["dialogue_history"] = [dict(msg) for msg in reversed(messages)]
                    
                    # Покупки
                    cur.execute("""
                        SELECT product_name, purchase_date, amount
                        FROM client_purchases 
                        WHERE conv_id = %s
                        ORDER BY purchase_date DESC
                    """, (conv_id,))
                    purchases = cur.fetchall()
                    client_data["purchases"] = [dict(purchase) for purchase in purchases]
                    
                    # Активные напоминания
                    cur.execute("""
                        SELECT reminder_datetime, reminder_context_summary, created_at
                        FROM reminders 
                        WHERE conv_id = %s AND status = 'active'
                        ORDER BY reminder_datetime
                    """, (conv_id,))
                    reminders = cur.fetchall()
                    client_data["active_reminders"] = [dict(reminder) for reminder in reminders]
            
            logging.info(f"Данные клиента {conv_id} загружены из БД")
            return client_data
            
        except Exception as e:
            logging.error(f"Ошибка загрузки данных клиента {conv_id}: {e}")
            raise
    
    def analyze_client_card(self, client_data: Dict[str, Any]) -> Dict[str, Any]:
        """Анализ карточки клиента с помощью AI и скидкой на день рождения"""
        try:
            # Рассчитываем скидку на день рождения
            birth_day = client_data.get('birth_day')
            birth_month = client_data.get('birth_month')
            
            birthday_status = calculate_birthday_discount_status(birth_day, birth_month)
            birthday_discount_message = birthday_status.get('message', '')
            
            if not birthday_discount_message:
                birthday_discount_message = "Нет активных скидок на день рождения."
            
            logging.info(f"Статус скидки на ДР: {birthday_status.get('status')}, сообщение: {birthday_discount_message[:100]}...")
            
            # Подготовка данных для промпта
            client_data_str = json.dumps(client_data, ensure_ascii=False, indent=2, default=str)
            
            # Формирование промпта с скидкой на день рождения
            prompt = CARD_ANALYSIS_PROMPT.format(
                products_info=self.products_info,
                client_data=client_data_str,
                birthday_discount_message=birthday_discount_message
            )
            
            # Запрос к AI
            logging.info(f"Отправка запроса к Gemini для анализа клиента {client_data.get('conv_id')}")
            logging.info(f"Размер промпта: {len(prompt)} символов")
            
            try:
                response = self.model.generate_content(prompt)
                response_text = response.text.strip()
                logging.info(f"Получен ответ от AI, размер: {len(response_text)} символов")
                logging.info(f"Первые 500 символов ответа: {response_text[:500]}")
                logging.info(f"Последние 200 символов ответа: {response_text[-200:]}")
                
            except Exception as e:
                logging.error(f"Ошибка при запросе к AI: {e}")
                raise
            
            # Более надежное извлечение JSON из ответа
            import re
            json_str = None
            
            # 1. Ищем JSON в markdown блоке
            json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1).strip()
            else:
                # 2. Ищем JSON по фигурным скобкам (более точно)
                brace_count = 0
                json_start = -1
                json_end = -1
                
                for i, char in enumerate(response_text):
                    if char == '{':
                        if brace_count == 0:
                            json_start = i
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0 and json_start != -1:
                            json_end = i + 1
                            break
                
                if json_start != -1 and json_end != -1:
                    json_str = response_text[json_start:json_end]
                else:
                    # 3. Если ничего не найдено, берем весь ответ
                    json_str = response_text
            
            # Дополнительная очистка JSON
            if json_str:
                json_str = json_str.strip()
                # Убираем лишние символы в начале
                json_str = re.sub(r'^[^\{]*', '', json_str)
                # Убираем лишние символы в конце
                json_str = re.sub(r'[^\}]*$', '', json_str)
            
            if not json_str or len(json_str.strip()) < 10:
                logging.error(f"JSON слишком короткий или пустой: '{json_str}'")
                logging.error(f"Полный ответ AI: {response_text}")
                raise ValueError("AI вернул некорректный или пустой JSON")
            
            logging.info(f"Извлеченный JSON: {json_str[:200]}...")
            analysis_result = json.loads(json_str)
            
            # Добавляем информацию о скидке на день рождения в результат
            analysis_result["birthday_discount_info"] = {
                "status": birthday_status.get('status'),
                "message": birthday_discount_message,
                "days_until_birthday": birthday_status.get('days_until_birthday'),
                "birthday_formatted": birthday_status.get('birthday_formatted')
            }
            
            logging.info(f"Анализ клиента {client_data.get('conv_id')} завершен успешно (с учетом ДР)")
            return analysis_result
            
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка парсинга JSON ответа: {e}")
            logging.error(f"Извлеченный JSON: '{json_str}'")
            logging.error(f"Полный ответ AI: {response_text}")
            
            # Fallback: создаем базовый результат анализа с ДР
            birth_day = client_data.get('birth_day')
            birth_month = client_data.get('birth_month')
            birthday_status = calculate_birthday_discount_status(birth_day, birth_month)
            
            fallback_result = {
                "lead_qualification": "тёплый",
                "funnel_stage": "Изучение предложения",
                "client_level": ["продолжающий"],
                "learning_goals": ["Обучение для удовольствия"],
                "client_pains": ["Нет времени на регулярные занятия"],
                "birthday_discount_info": {
                    "status": birthday_status.get('status'),
                    "message": birthday_status.get('message', ''),
                    "days_until_birthday": birthday_status.get('days_until_birthday'),
                    "birthday_formatted": birthday_status.get('birthday_formatted')
                },
                "activity_analysis": {
                    "message_frequency": "средняя",
                    "response_time": "средне",
                    "initiative_level": "пассивный",
                    "question_quality": "поверхностные"
                },
                "conversation_gaps": {
                    "has_long_pauses": True,
                    "last_contact_days": 30,
                    "needs_reengagement": True,
                    "gap_reasons": ["длительное молчание"]
                },
                "return_strategies": {
                    "recommended_approach": "Мягкое напоминание о курсах",
                    "triggers_to_use": ["новые материалы", "скидки"],
                    "personal_offers": ["персональная консультация"],
                    "urgency_level": "средний"
                },
                "optimal_reminder_timing": {
                    "contact_in_days": 3,
                    "best_time_of_day": "день",
                    "contact_reason": "Проверка интереса к обучению",
                    "message_tone": "дружелюбный"
                },
                "product_recommendations": {
                    "primary_courses": ["базовый курс"],
                    "course_bundles": ["стартовый набор"],
                    "discount_needed": False,
                    "alternative_options": ["пробный урок"]
                },
                "psychological_profile": {
                    "personality_type": "практик",
                    "motivation_factors": ["достижение целей"],
                    "fears_and_doubts": ["сомнения в способностях"],
                    "communication_preferences": ["простое общение"]
                },
                "communication_strategy": {
                    "recommended_tone": "дружелюбный и поддерживающий",
                    "key_arguments": ["простота обучения", "гибкий график"],
                    "emphasize": ["удобство", "результат"],
                    "avoid": ["сложные термины", "давление"],
                    "personalization_tips": ["учесть занятость", "показать понимание"]
                },
                "dialogue_summary": "AI анализ не удался, использован базовый профиль",
                "action_priority": "средний",
                "next_steps": ["связаться с клиентом", "предложить консультацию"]
            }
            
            logging.warning("Использован fallback результат анализа")
            return fallback_result
            
        except Exception as e:
            logging.error(f"Ошибка анализа карточки клиента: {e}")
            raise
    
    def update_client_profile(self, conv_id: int, analysis_result: Dict[str, Any]) -> bool:
        """Обновление профиля клиента в БД на основе анализа"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Подготовка данных для обновления
                    update_data = {
                        'lead_qualification': analysis_result.get('lead_qualification'),
                        'funnel_stage': analysis_result.get('funnel_stage'),
                        'client_level': analysis_result.get('client_level', []),
                        'learning_goals': analysis_result.get('learning_goals', []),
                        'client_pains': analysis_result.get('client_pains', []),
                        'dialogue_summary': analysis_result.get('dialogue_summary', ''),
                        'last_analysis_at': datetime.now(timezone.utc)
                    }
                    
                    # Проверяем, существует ли профиль
                    cur.execute("SELECT conv_id FROM user_profiles WHERE conv_id = %s", (conv_id,))
                    exists = cur.fetchone()
                    
                    if exists:
                        # Обновляем существующий профиль
                        cur.execute("""
                            UPDATE user_profiles SET
                                lead_qualification = %s,
                                funnel_stage = %s,
                                client_level = %s,
                                learning_goals = %s,
                                client_pains = %s,
                                dialogue_summary = %s,
                                last_analysis_at = %s
                            WHERE conv_id = %s
                        """, (
                            update_data['lead_qualification'],
                            update_data['funnel_stage'],
                            update_data['client_level'],
                            update_data['learning_goals'],
                            update_data['client_pains'],
                            update_data['dialogue_summary'],
                            update_data['last_analysis_at'],
                            conv_id
                        ))
                        logging.info(f"Профиль клиента {conv_id} обновлен")
                    else:
                        # Создаем новый профиль (базовый)
                        cur.execute("""
                            INSERT INTO user_profiles (
                                conv_id, lead_qualification, funnel_stage, 
                                client_level, learning_goals, client_pains,
                                dialogue_summary, last_analysis_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            conv_id,
                            update_data['lead_qualification'],
                            update_data['funnel_stage'],
                            update_data['client_level'],
                            update_data['learning_goals'],
                            update_data['client_pains'],
                            update_data['dialogue_summary'],
                            update_data['last_analysis_at']
                        ))
                        logging.info(f"Создан новый профиль для клиента {conv_id}")
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logging.error(f"Ошибка обновления профиля клиента {conv_id}: {e}")
            return False
    
    def create_reminder_if_needed(self, conv_id: int, analysis_result: Dict[str, Any]) -> bool:
        """Создание напоминания на основе анализа"""
        try:
            optimal_timing = analysis_result.get('optimal_reminder_timing', {})
            return_strategies = analysis_result.get('return_strategies', {})
            
            # Проверяем, нужно ли создавать напоминание
            contact_in_days = optimal_timing.get('contact_in_days', 0)
            needs_reengagement = analysis_result.get('conversation_gaps', {}).get('needs_reengagement', False)
            
            if not needs_reengagement or contact_in_days <= 0:
                logging.info(f"Напоминание для клиента {conv_id} не требуется")
                return True
            
            # Вычисляем время напоминания
            reminder_datetime = datetime.now(timezone.utc) + timedelta(days=contact_in_days)
            
            # Формируем контекст напоминания
            contact_reason = optimal_timing.get('contact_reason', 'Возобновление диалога')
            recommended_approach = return_strategies.get('recommended_approach', 'Мягкое напоминание')
            
            reminder_context = f"{contact_reason}. Стратегия: {recommended_approach}"
            
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Проверяем, нет ли уже активного напоминания на ближайшее время
                    cur.execute("""
                        SELECT id FROM reminders 
                        WHERE conv_id = %s AND status = 'active'
                        AND reminder_datetime BETWEEN %s AND %s
                    """, (
                        conv_id,
                        reminder_datetime - timedelta(hours=12),
                        reminder_datetime + timedelta(hours=12)
                    ))
                    
                    existing_reminder = cur.fetchone()
                    if existing_reminder:
                        logging.info(f"Напоминание для клиента {conv_id} уже существует")
                        return True
                    
                    # Создаем новое напоминание
                    cur.execute("""
                        INSERT INTO reminders (
                            conv_id, reminder_datetime, reminder_context_summary,
                            created_by_conv_id, client_timezone, status
                        ) VALUES (%s, %s, %s, %s, %s, 'active')
                        RETURNING id
                    """, (
                        conv_id,
                        reminder_datetime,
                        reminder_context,
                        None,  # Создано автоматически анализатором
                        'Europe/Moscow'  # По умолчанию московское время
                    ))
                    
                    reminder_id = cur.fetchone()[0]
                    conn.commit()
                    
                    logging.info(f"Создано напоминание ID={reminder_id} для клиента {conv_id} на {reminder_datetime}")
                    return True
                    
        except Exception as e:
            logging.error(f"Ошибка создания напоминания для клиента {conv_id}: {e}")
            return False
    
    def save_analysis_result(self, conv_id: int, analysis_result: Dict[str, Any]) -> bool:
        """Сохранение результата анализа в файл"""
        try:
            # Создаем директорию для результатов, если её нет
            results_dir = "analysis_results"
            os.makedirs(results_dir, exist_ok=True)
            
            # Формируем имя файла с временной меткой
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"client_{conv_id}_analysis_{timestamp}.json"
            filepath = os.path.join(results_dir, filename)
            
            # Добавляем метаданные
            result_with_meta = {
                "conv_id": conv_id,
                "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
                "analyzer_version": "2.0",
                "analysis_result": analysis_result
            }
            
            # Сохраняем в файл
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result_with_meta, f, ensure_ascii=False, indent=2, default=str)
            
            logging.info(f"Результат анализа сохранен в {filepath}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка сохранения результата анализа: {e}")
            return False
    
    def analyze_client(self, conv_id: int, save_to_file: bool = True, exported_data_file: str = None) -> Dict[str, Any]:
        """Полный анализ клиента с обновлением БД"""
        try:
            logging.info(f"Начинаем полный анализ клиента {conv_id}")
            
            # 1. Загружаем данные клиента
            if exported_data_file and os.path.exists(exported_data_file):
                logging.info(f"Используем экспортированные данные из {exported_data_file}")
                with open(exported_data_file, 'r', encoding='utf-8') as f:
                    client_data = json.load(f)
            else:
                logging.info("Загружаем данные клиента из БД")
                client_data = self.load_client_data_from_db(conv_id)
            
            # 2. Проводим анализ с помощью AI
            analysis_result = self.analyze_client_card(client_data)
            
            # 3. Обновляем профиль клиента в БД
            profile_updated = self.update_client_profile(conv_id, analysis_result)
            if not profile_updated:
                logging.warning(f"Не удалось обновить профиль клиента {conv_id}")
            
            # 4. Создаем напоминание, если необходимо
            reminder_created = self.create_reminder_if_needed(conv_id, analysis_result)
            if not reminder_created:
                logging.warning(f"Не удалось создать напоминание для клиента {conv_id}")
            
            # 5. Сохраняем результат в файл (опционально)
            if save_to_file:
                self.save_analysis_result(conv_id, analysis_result)
            
            # Добавляем информацию о выполненных действиях
            analysis_result["_metadata"] = {
                "profile_updated": profile_updated,
                "reminder_created": reminder_created,
                "analysis_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logging.info(f"Полный анализ клиента {conv_id} завершен успешно")
            return analysis_result
            
        except Exception as e:
            logging.error(f"Ошибка полного анализа клиента {conv_id}: {e}")
            raise

def main():
    """Основная функция для тестирования"""
    try:
        # Инициализация анализатора
        analyzer = ClientCardAnalyzer()
        
        # Пример анализа клиента (замените на реальный conv_id)
        test_conv_id = 123456789  # Замените на реальный ID
        
        print(f"Анализируем клиента {test_conv_id}...")
        result = analyzer.analyze_client(test_conv_id)
        
        print("\n=== РЕЗУЛЬТАТ АНАЛИЗА ===")
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        
    except Exception as e:
        logging.error(f"Ошибка в main: {e}")
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    main()