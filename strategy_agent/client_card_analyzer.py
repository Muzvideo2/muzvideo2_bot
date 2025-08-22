#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Анализатор карточек клиентов с расширенной функциональностью
Версия 2.0 - Полная переработка с интеграцией БД и напоминаний
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

# Загружаем переменные окружения из .env файла
from dotenv import load_dotenv
load_dotenv()

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
        """Загрузка информации о продуктах из prompt.txt"""
        try:
            prompt_path = "../prompt.txt"  # Относительно папки client_analyzer
            if not os.path.exists(prompt_path):
                prompt_path = "prompt.txt"  # В текущей папке
                
            if os.path.exists(prompt_path):
                with open(prompt_path, "r", encoding="utf-8") as f:
                    prompt_content = f.read()
                
                # Извлекаем информацию о школе и продуктах из промпта
                self.products_info = """
ОНЛАЙН-ШКОЛА ФОРТЕПИАНО СЕРГЕЯ ФИЛИМОНОВА

О ШКОЛЕ:
- Основана в 2011 году (более 13 лет опыта)
- Более 4500 учеников прошли обучение
- Уникальная система из 12 курсов альтернативного обучения
- Специализация: аккорды, гармония, импровизация, свободная игра
- Авторские методики от Сергея Филимонова
- Куратор с высшим музыкальным образованием
- Дружная атмосфера в чате учеников и онлайн-концерты

ОСНОВНЫЕ КУРСЫ (в порядке изучения):
1. "Виртуозная техника игры за 12 минут в день" - развитие техники, убирает "деревянные пальцы"
2. "Первые шаги" - базовые навыки, ноты, постановка рук, простые аккорды и аккомпанемент (3-6 мес для начинающих)
3. "Игра по нотам с листа" - около 400 произведений, развитие координации рук, ритм и темп
4. "Аккорд Мастер" - простые, сложные и джазовые аккорды, буквенные обозначения, авторский метод формул
5. "Мастер Гармонии" - аккордовые взаимоотношения через интервалы
6. "Импровизация с нуля" - преодоление барьера импровизации, пошаговые упражнения
7. "Мастер Аранжировки" - создание фортепианных аранжировок, требует все предыдущие навыки

ДОПОЛНИТЕЛЬНЫЕ КУРСЫ:
- "Ноты прочь! Играю что хочу!" - свободная игра без нот
- "Блюз Мастер" - игра блюза на фортепиано
- "Баллад Мастер" - игра баллад
- "10 шагов к музыкальной свободе"
- "Качественная запись фортепиано"

НАБОРЫ КУРСОВ И ЦЕНЫ:
1. "Всё включено" (12 курсов) - 59990 руб (со скидкой 54990 руб)
2. "6 шагов" (6 курсов) - 49990 руб (со скидкой 44990 руб)
3. "Начинающий+" (4 курса) - 29990 руб (со скидкой 24990 руб)

ЦЕЛЕВЫЕ АУДИТОРИИ:
1. Начинающие или "всё забыл" - утратившие навыки
2. Продолжающие - играют уверенно, но без полного образования
3. С музыкальным образованием по фортепиано
4. С музыкальным образованием по другим инструментам

ЦЕЛИ ОБУЧЕНИЯ:
- Свободная игра (подбор, аккомпанемент, импровизация, сочинение)
- Игра только по нотам (классика, современные произведения)
- Комбинированное обучение (ноты + свободная игра)

ПРЕИМУЩЕСТВА:
- Авторские методики, отточенные за 13 лет
- Личная поддержка от автора школы
- Гибкие условия продления (1000 руб/год)
- Платформа работает без VPN
- Возможность рассрочки от банков
- Система скидок для клиентов
"""
                logging.info("Информация о продуктах загружена из prompt.txt")
            else:
                self.products_info = "Информация о продуктах недоступна - prompt.txt не найден"
                logging.warning("Файл prompt.txt не найден")
                
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
        """Анализ карточки клиента с помощью AI"""
        try:
            # Подготовка данных для промпта
            client_data_str = json.dumps(client_data, ensure_ascii=False, indent=2, default=str)
            
            # Формирование промпта
            prompt = CARD_ANALYSIS_PROMPT.format(
                products_info=self.products_info,
                client_data=client_data_str
            )
            
            # Запрос к AI
            logging.info(f"Отправка запроса к Gemini для анализа клиента {client_data.get('conv_id')}")
            response = self.model.generate_content(prompt)
            
            # Парсинг JSON ответа
            response_text = response.text.strip()
            logging.info(f"Сырой ответ от Gemini (первые 500 символов): {response_text[:500]}")
            
            # Извлечение JSON из ответа (может быть в markdown блоке)
            import re
            json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1).strip()
                logging.info("JSON найден в markdown блоке")
            else:
                # Ищем JSON структуру в тексте
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start != -1 and json_end > json_start:
                    json_str = response_text[json_start:json_end].strip()
                    logging.info(f"JSON найден в тексте, позиции: {json_start}-{json_end}")
                else:
                    json_str = response_text.strip()
                    logging.warning("JSON структура не найдена, используем весь текст")
            
            logging.info(f"Извлеченный JSON (первые 200 символов): {json_str[:200]}")
            
            try:
                analysis_result = json.loads(json_str)
            except json.JSONDecodeError as parse_error:
                logging.error(f"Ошибка парсинга JSON: {parse_error}")
                logging.error(f"Проблемная строка: {json_str[:100]}")
                
                # Попытка очистки JSON строки
                cleaned_json = json_str.replace('\n', '').replace('\r', '').strip()
                if cleaned_json.startswith('"') and not cleaned_json.startswith('{"'):
                    # Возможно, JSON завернут в кавычки
                    try:
                        cleaned_json = cleaned_json[1:-1]  # убираем внешние кавычки
                        analysis_result = json.loads(cleaned_json)
                        logging.info("JSON успешно парсен после очистки от внешних кавычек")
                    except json.JSONDecodeError:
                        raise parse_error
                else:
                    raise parse_error
            
            logging.info(f"Анализ клиента {client_data.get('conv_id')} завершен успешно")
            return analysis_result
            
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка парсинга JSON ответа: {e}")
            logging.error(f"Сырой ответ: {response_text}")
            raise
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
    
    def load_from_json(self, json_file_path: str) -> Dict[str, Any]:
        """Загрузка данных клиента из JSON файла"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                client_data = json.load(f)
            
            logging.info(f"Данные клиента загружены из файла {json_file_path}")
            return client_data
            
        except Exception as e:
            logging.error(f"Ошибка загрузки данных из JSON: {e}")
            raise

    def analyze_client(self, conv_id: int, save_to_file: bool = True) -> Dict[str, Any]:
        """Полный анализ клиента с обновлением БД"""
        try:
            logging.info(f"Начинаем полный анализ клиента {conv_id}")
            
            # 1. Загружаем данные клиента из БД
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
    import sys
    
    try:
        # Инициализация анализатора
        analyzer = ClientCardAnalyzer()
        
        if len(sys.argv) > 1:
            # Режим работы с JSON файлом
            json_file_path = sys.argv[1]
            print(f"Анализируем клиента из файла: {json_file_path}")
            
            # Загружаем данные из JSON
            client_data = analyzer.load_from_json(json_file_path)
            client_id = client_data.get('client_id')
            
            print(f"Запускаем AI анализ клиента {client_id}...")
            
            # Проводим анализ
            analysis_result = analyzer.analyze_client_card(client_data)
            
            # Сохраняем результат
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_file = f"analysis_results/strategic_analysis_{client_id}_{timestamp}.json"
            
            os.makedirs("analysis_results", exist_ok=True)
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"Анализ карточки клиента завершен. Результат сохранен в: {result_file}")
            
            print("\n=== РЕЗУЛЬТАТ АНАЛИЗА ===")
            print(json.dumps(analysis_result, ensure_ascii=False, indent=2, default=str))
            
            return result_file
        else:
            # Старый режим работы с conv_id из БД
            test_conv_id = 123456789
            print(f"Анализируем клиента {test_conv_id}...")
            result = analyzer.analyze_client(test_conv_id)
            
            print("\n=== РЕЗУЛЬТАТ АНАЛИЗА ===")
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        
    except Exception as e:
        logging.error(f"Ошибка в main: {e}")
        print(f"Ошибка: {e}")
        return None

if __name__ == "__main__":
    main()