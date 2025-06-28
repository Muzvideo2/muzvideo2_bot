#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =======================================================================================
#               СЕРВИС ИНТЕГРИРОВАННЫХ НАПОМИНАНИЙ (REMINDER SERVICE) - v1.0
# =======================================================================================
#
# ЧТО ДЕЛАЕТ ЭТОТ СЕРВИС:
#
# Этот сервис — "умный планировщик", который автоматически анализирует диалоги
# и создает напоминания о будущих контактах с клиентами.
#
# ЕГО ЗАДАЧИ:
#
# 1. АНАЛИЗ ДИАЛОГОВ: Семантический анализ новых сообщений для выявления
#    договоренностей о будущем контакте.
#
# 2. СОЗДАНИЕ НАПОМИНАНИЙ: Автоматическое создание напоминаний на основе
#    выявленных договоренностей с учетом часового пояса клиента.
#
# 3. АКТИВАЦИЯ НАПОМИНАНИЙ: Безопасная активация напоминаний с предварительной
#    проверкой контекста через AI-агента напоминаний.
#
# 4. УПРАВЛЕНИЕ НАПОМИНАНИЯМИ: Обработка команд администратора для создания,
#    изменения и отмены напоминаний.
#
# =======================================================================================

import os
import sys
import json
import logging
import re
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

try:
    import vertexai
    from vertexai.generative_models import GenerativeModel
    from google.oauth2 import service_account
    VERTEXAI_AVAILABLE = True
except ImportError:
    VERTEXAI_AVAILABLE = False
    print("Критическая ошибка: Vertex AI SDK не доступен.", file=sys.stderr)
    sys.exit(1)

# --- НАСТРОЙКИ ---
DATABASE_URL = os.environ.get("DATABASE_URL")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "zeta-tracer-462306-r7")
LOCATION = os.environ.get("GEMINI_LOCATION", "us-central1")
ADMIN_CONV_ID = 78671089  # ID администратора

MODEL_NAME = "gemini-2.5-flash"
API_TIMEOUT = 45

# Интервал проверки напоминаний (в минутах)
CHECK_INTERVAL_MINUTES = 5

# Настройки логирования
LOG_FILE_NAME = "reminder_service.log"

# Словарь для определения часового пояса по городу
CITY_TIMEZONE_MAP = {
    # Россия
    'москва': 'Europe/Moscow',
    'санкт-петербург': 'Europe/Moscow', 
    'спб': 'Europe/Moscow',
    'петербург': 'Europe/Moscow',
    'екатеринбург': 'Asia/Yekaterinburg',
    'новосибирск': 'Asia/Novosibirsk',
    'красноярск': 'Asia/Krasnoyarsk',
    'иркутск': 'Asia/Irkutsk',
    'владивосток': 'Asia/Vladivostok',
    'хабаровск': 'Asia/Vladivostok',
    'омск': 'Asia/Omsk',
    'челябинск': 'Asia/Yekaterinburg',
    'казань': 'Europe/Moscow',
    'нижний новгород': 'Europe/Moscow',
    'самара': 'Europe/Samara',
    'уфа': 'Asia/Yekaterinburg',
    'ростов-на-дону': 'Europe/Moscow',
    'краснодар': 'Europe/Moscow',
    'пермь': 'Asia/Yekaterinburg',
    'воронеж': 'Europe/Moscow',
    'волгоград': 'Europe/Moscow',
    'саратов': 'Europe/Moscow',
    'тюмень': 'Asia/Yekaterinburg',
    'тольятти': 'Europe/Samara',
    'барнаул': 'Asia/Barnaul',
    'ульяновск': 'Europe/Samara',
    'иваново': 'Europe/Moscow',
    'ярославль': 'Europe/Moscow',
    'махачкала': 'Europe/Moscow',
    'оренбург': 'Asia/Yekaterinburg',
    'новокузнецк': 'Asia/Novokuznetsk',
    'рязань': 'Europe/Moscow',
    'тула': 'Europe/Moscow',
    'липецк': 'Europe/Moscow',
    'киров': 'Europe/Moscow',
    'чебоксары': 'Europe/Moscow',
    'калининград': 'Europe/Kaliningrad',
    'брянск': 'Europe/Moscow',
    'магнитогорск': 'Asia/Yekaterinburg',
    'курск': 'Europe/Moscow',
    'тверь': 'Europe/Moscow',
    'архангельск': 'Europe/Moscow',
    'сочи': 'Europe/Moscow',
    'белгород': 'Europe/Moscow',
    'калуга': 'Europe/Moscow',
    'владимир': 'Europe/Moscow',
    'сургут': 'Asia/Yekaterinburg',
    'смоленск': 'Europe/Moscow',
    'курган': 'Asia/Yekaterinburg',
    'орёл': 'Europe/Moscow',
    'череповец': 'Europe/Moscow',
    'вологда': 'Europe/Moscow',
    'мурманск': 'Europe/Moscow',
    'тамбов': 'Europe/Moscow',
    'стерлитамак': 'Asia/Yekaterinburg',
    'грозный': 'Europe/Moscow',
    'якутск': 'Asia/Yakutsk',
    'кострома': 'Europe/Moscow',
    'комсомольск-на-амуре': 'Asia/Vladivostok',
    'петрозаводск': 'Europe/Moscow',
    'таганрог': 'Europe/Moscow',
    'нижневартовск': 'Asia/Yekaterinburg',
    'йошкар-ола': 'Europe/Moscow',
    'братск': 'Asia/Irkutsk',
    'новороссийск': 'Europe/Moscow',
    'дзержинск': 'Europe/Moscow',
    'шахты': 'Europe/Moscow',
    'орск': 'Asia/Yekaterinburg',
    'ангарск': 'Asia/Irkutsk',
    'сыктывкар': 'Europe/Moscow',
    'нижнекамск': 'Europe/Moscow',
    'старый оскол': 'Europe/Moscow',
    'мытищи': 'Europe/Moscow',
    'прокопьевск': 'Asia/Novokuznetsk',
    'балашиха': 'Europe/Moscow',
    'рыбинск': 'Europe/Moscow',
    'бийск': 'Asia/Barnaul',
    'подольск': 'Europe/Moscow',
    'королёв': 'Europe/Moscow',
    'сызрань': 'Europe/Samara',
    'волжский': 'Europe/Moscow',
    'железнодорожный': 'Europe/Moscow',
    'абакан': 'Asia/Krasnoyarsk',
    'уссурийск': 'Asia/Vladivostok',
    'норильск': 'Asia/Krasnoyarsk',
    'каменск-уральский': 'Asia/Yekaterinburg',
    'великий новгород': 'Europe/Moscow',
    'люберцы': 'Europe/Moscow',
    'южно-сахалинск': 'Asia/Sakhalin',
    
    # Украина
    'киев': 'Europe/Kiev',
    'харьков': 'Europe/Kiev',
    'одесса': 'Europe/Kiev',
    'днепропетровск': 'Europe/Kiev',
    'донецк': 'Europe/Kiev',
    'запорожье': 'Europe/Kiev',
    'львов': 'Europe/Kiev',
    
    # Беларусь
    'минск': 'Europe/Minsk',
    'гомель': 'Europe/Minsk',
    'могилёв': 'Europe/Minsk',
    'витебск': 'Europe/Minsk',
    'гродно': 'Europe/Minsk',
    'брест': 'Europe/Minsk',
    
    # Казахстан
    'алматы': 'Asia/Almaty',
    'нур-султан': 'Asia/Almaty',
    'астана': 'Asia/Almaty',
    'шымкент': 'Asia/Almaty',
    'караганда': 'Asia/Almaty',
    'актобе': 'Asia/Aqtobe',
    'тараз': 'Asia/Almaty',
    'павлодар': 'Asia/Almaty',
    'усть-каменогорск': 'Asia/Almaty',
    'семей': 'Asia/Almaty',
    'атырау': 'Asia/Aqtau',
    'костанай': 'Asia/Almaty',
    'кызылорда': 'Asia/Qyzylorda',
    'уральск': 'Asia/Oral',
    'петропавловск': 'Asia/Almaty',
    'актау': 'Asia/Aqtau',
    
    # Другие страны (основные города)
    'лондон': 'Europe/London',
    'париж': 'Europe/Paris',
    'берлин': 'Europe/Berlin',
    'рим': 'Europe/Rome',
    'мадрид': 'Europe/Madrid',
    'амстердам': 'Europe/Amsterdam',
    'брюссель': 'Europe/Brussels',
    'вена': 'Europe/Vienna',
    'прага': 'Europe/Prague',
    'варшава': 'Europe/Warsaw',
    'стокгольм': 'Europe/Stockholm',
    'хельсинки': 'Europe/Helsinki',
    'осло': 'Europe/Oslo',
    'копенгаген': 'Europe/Copenhagen',
    'дублин': 'Europe/Dublin',
    'лиссабон': 'Europe/Lisbon',
    'афины': 'Europe/Athens',
    'будапешт': 'Europe/Budapest',
    'бухарест': 'Europe/Bucharest',
    'софия': 'Europe/Sofia',
    'белград': 'Europe/Belgrade',
    'загреб': 'Europe/Zagreb',
    'любляна': 'Europe/Ljubljana',
    'братислава': 'Europe/Bratislava',
    'таллин': 'Europe/Tallinn',
    'рига': 'Europe/Riga',
    'вильнюс': 'Europe/Vilnius',
    'нью-йорк': 'America/New_York',
    'лос-анджелес': 'America/Los_Angeles',
    'чикаго': 'America/Chicago',
    'хьюстон': 'America/Chicago',
    'торонто': 'America/Toronto',
    'ванкувер': 'America/Vancouver',
    'токио': 'Asia/Tokyo',
    'пекин': 'Asia/Shanghai',
    'шанхай': 'Asia/Shanghai',
    'сеул': 'Asia/Seoul',
    'бангкок': 'Asia/Bangkok',
    'сингапур': 'Asia/Singapore',
    'джакарта': 'Asia/Jakarta',
    'мумбаи': 'Asia/Kolkata',
    'дели': 'Asia/Kolkata',
    'дубай': 'Asia/Dubai',
    'тель-авив': 'Asia/Jerusalem',
    'стамбул': 'Europe/Istanbul',
    'каир': 'Africa/Cairo',
    'йоханнесбург': 'Africa/Johannesburg',
    'сидней': 'Australia/Sydney',
    'мельбурн': 'Australia/Melbourne',
    'сан-паулу': 'America/Sao_Paulo',
    'рио-де-жанейро': 'America/Sao_Paulo',
    'буэнос-айрес': 'America/Argentina/Buenos_Aires',
    'мехико': 'America/Mexico_City',
}

def get_timezone_by_city(city):
    """Определяет часовой пояс по названию города."""
    if not city:
        return 'Europe/Moscow'  # По умолчанию московское время
    
    city_lower = city.lower().strip()
    return CITY_TIMEZONE_MAP.get(city_lower, 'Europe/Moscow')

def detect_timezone_from_message(message_text):
    """Определяет, указан ли в сообщении конкретный часовой пояс."""
    message_lower = message_text.lower()
    
    # Проверяем упоминания конкретных часовых поясов
    timezone_patterns = [
        (r'по\s+москве|московское\s+время|мск', 'Europe/Moscow'),
        (r'по\s+питеру|по\s+спб|питерское\s+время', 'Europe/Moscow'),
        (r'по\s+екатеринбургу|екатеринбургское\s+время', 'Asia/Yekaterinburg'),
        (r'по\s+новосибирску|новосибирское\s+время', 'Asia/Novosibirsk'),
        (r'по\s+владивостоку|владивостокское\s+время', 'Asia/Vladivostok'),
        (r'utc|гринвич', 'UTC'),
        (r'по\s+киеву|киевское\s+время', 'Europe/Kiev'),
        (r'по\s+минску|минское\s+время', 'Europe/Minsk'),
        (r'по\s+алматы', 'Asia/Almaty'),
    ]
    
    for pattern, timezone in timezone_patterns:
        if re.search(pattern, message_lower):
            return timezone
    
    return None

# --- ПРОМПТЫ ДЛЯ AI ---
PROMPT_ANALYZE_DIALOGUE = """
Ты — AI-ассистент, анализирующий диалоги онлайн-школы музыки для выявления договоренностей о будущем контакте.

ТВОЯ ЗАДАЧА: Проанализировать последние сообщения диалога и определить, есть ли договоренность о напоминании.

ВАЖНЫЕ ПРАВИЛА:
1. Ищи ТОЛЬКО договоренности, связанные с деятельностью школы (оплата, обучение, курсы, консультации).
2. ИГНОРИРУЙ личные просьбы, не связанные со школой (напомнить вынести мусор, позвонить маме и т.д.).
3. Если сообщение от администратора (conv_id = {admin_conv_id}), проверь команды управления напоминаниями.
4. Учитывай контекст всего диалога для правильной интерпретации.

ТИПЫ ДОГОВОРЕННОСТЕЙ:
- Прямые просьбы: "Напомните мне завтра в 10:00 об оплате"
- Неявные договоренности: "Мне нужно подумать до понедельника"
- Отмена напоминания: "Спасибо, уже не нужно напоминать"
- Перенос напоминания: "Давайте перенесем на вторник"

КОМАНДЫ АДМИНИСТРАТОРА:
- "Напомни conv_id: 12345678 завтра в 15:00 о консультации"
- "Напомни мне в 16:30 проверить отчеты"
- "Отмени напоминание для conv_id: 12345678"

ПРАВИЛА ИНТЕРПРЕТАЦИИ ВРЕМЕНИ:
- "Завтра" = следующий день в 10:00
- "Утром" = 10:00 текущего/следующего дня
- "Днем" = 14:00 текущего/следующего дня
- "Вечером" = 19:00 текущего/следующего дня
- "Через пару дней" = через 2 дня в 12:00
- "На следующей неделе" = понедельник следующей недели в 12:00

Текущее время (московское): {current_datetime}
ID текущего диалога: {conv_id}

--- ИНФОРМАЦИЯ О ПОЛЬЗОВАТЕЛЕ ---
{user_info}

--- ПОСЛЕДНИЕ СООБЩЕНИЯ ДИАЛОГА ---
{dialogue_messages}

--- АКТИВНЫЕ НАПОМИНАНИЯ ДЛЯ ЭТОГО КЛИЕНТА ---
{active_reminders}

--- ПОКУПКИ КЛИЕНТА ---
{client_purchases}

ВЕРНИ ОТВЕТ СТРОГО В ФОРМАТЕ JSON:
{{
  "action": "create/update/cancel/none",
  "target_conv_id": 12345678,
  "proposed_datetime": "YYYY-MM-DDTHH:MM:SS+03:00",
  "reminder_context_summary": "Краткое описание причины напоминания",
  "cancellation_reason": "Причина отмены (только для action=cancel)"
}}
"""

PROMPT_VERIFY_REMINDER = """
Ты — AI-агент напоминаний, проверяющий уместность активации напоминания.

Тебе предоставлен полный контекст клиента и информация о напоминании, которое должно сработать.

ТВОЯ ЗАДАЧА: Проанализировать контекст и решить, нужно ли активировать напоминание.

КРИТЕРИИ ДЛЯ ОТМЕНЫ НАПОМИНАНИЯ:
1. Клиент уже выполнил действие (купил курс, оплатил, записался).
2. Клиент явно отказался от услуг школы.
3. После установки напоминания произошли события, делающие его неактуальным.
4. Клиент попросил больше не беспокоить его.

--- ИНФОРМАЦИЯ О НАПОМИНАНИИ ---
Время создания: {reminder_created_at}
Запланированное время: {reminder_datetime}
Причина: {reminder_context_summary}

--- ПОЛНЫЙ КОНТЕКСТ КЛИЕНТА ---
{client_context}

--- ВСЕ АКТИВНЫЕ НАПОМИНАНИЯ ДЛЯ КЛИЕНТА ---
{all_active_reminders}

ВЕРНИ ОТВЕТ СТРОГО В ФОРМАТЕ JSON:
{{
  "should_activate": true/false,
  "reason": "Краткое объяснение решения",
  "suggested_action": "cancel/postpone/activate",
  "postpone_to": "YYYY-MM-DDTHH:MM:SS+03:00 (только если suggested_action=postpone)"
}}
"""

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def setup_logging():
    """Настройка системы логирования."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_NAME, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_db_connection():
    """Устанавливает соединение с БД."""
    if not DATABASE_URL:
        raise ConnectionError("Переменная окружения DATABASE_URL не установлена!")
    return psycopg2.connect(DATABASE_URL)

def call_gemini_api(model, prompt, expect_json=True):
    """Вызывает модель Gemini через Vertex AI SDK."""
    try:
        logging.info(f"Отправляем запрос в Gemini. Промпт (первые 200 символов): {prompt[:200]}...")
        
        response = model.generate_content(prompt)
        raw_response = response.text
        
        logging.info(f"Получен ответ от Gemini (длина: {len(raw_response)} символов)")
        
        if expect_json:
            # Попытка найти JSON внутри markdown-блока
            match = re.search(r"```(json)?\s*([\s\S]*?)\s*```", raw_response)
            if match:
                json_str = match.group(2)
            else:
                json_str = raw_response
            
            parsed_response = json.loads(json_str)
            logging.info(f"JSON успешно распарсен: {parsed_response}")
            return parsed_response
        else:
            return raw_response.strip()

    except json.JSONDecodeError as je:
        logging.error(f"Ошибка парсинга JSON от Gemini: {je}. Сырой ответ: {raw_response}")
        raise
    except Exception as e:
        logging.error(f"Ошибка вызова Vertex AI API: {e}", exc_info=True)
        raise

def get_moscow_time():
    """Возвращает текущее московское время."""
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)

def parse_datetime_with_timezone(datetime_str, client_timezone='Europe/Moscow'):
    """Парсит строку даты/времени с учетом часового пояса клиента."""
    try:
        # Если дата уже содержит timezone info
        if '+' in datetime_str or 'Z' in datetime_str:
            return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        
        # Иначе считаем, что время в часовом поясе клиента
        tz = pytz.timezone(client_timezone)
        naive_dt = datetime.fromisoformat(datetime_str)
        return tz.localize(naive_dt)
    except Exception as e:
        logging.error(f"Ошибка парсинга даты '{datetime_str}': {e}")
        raise

# --- ОСНОВНЫЕ ФУНКЦИИ ---

def analyze_dialogue_for_reminders(conn, conv_id, model):
    """Анализирует последние сообщения диалога для выявления договоренностей о напоминании."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Получаем последние сообщения диалога
            cur.execute("""
                SELECT role, message, created_at 
                FROM dialogues 
                WHERE conv_id = %s 
                ORDER BY created_at DESC 
                LIMIT 20
            """, (conv_id,))
            messages = cur.fetchall()
            
            if not messages:
                logging.info(f"Нет сообщений для анализа в диалоге {conv_id}")
                return None
            
            # Форматируем сообщения для промпта
            dialogue_text = []
            for msg in reversed(messages):
                role_map = {'user': 'Клиент', 'bot': 'Ассистент', 'operator': 'Оператор'}
                role = role_map.get(msg['role'], msg['role'])
                message_text = re.sub(r'^\[.*?\]\s*', '', msg['message'])
                dialogue_text.append(f"{role}: {message_text}")
            
            # Простая проверка для создания тестового напоминания
            last_user_message = ""
            for msg in messages:
                if msg['role'] == 'user':
                    last_user_message = msg['message'].lower()
                    break
            
            # Упрощенная логика для базовой интеграции
            if any(word in last_user_message for word in ['напомни', 'напомнить', 'завтра', 'потом', 'позже']):
                return {
                    'action': 'create',
                    'target_conv_id': conv_id,
                    'proposed_datetime': (datetime.now() + timedelta(hours=24)).strftime("%Y-%m-%dT10:00:00+03:00"),
                    'reminder_context_summary': 'Клиент попросил напомнить',
                    'client_timezone': 'Europe/Moscow'
                }
            
            return None
            
    except Exception as e:
        logging.error(f"Ошибка при анализе диалога {conv_id}: {e}", exc_info=True)
        return None

def create_or_update_reminder(conn, conv_id, reminder_data, created_by_conv_id=None):
    """Создает или обновляет напоминание в БД."""
    try:
        with conn.cursor() as cur:
            action = reminder_data.get('action')
            target_conv_id = reminder_data.get('target_conv_id', conv_id)
            
            if action == 'create':
                # Парсим дату/время с учетом часового пояса
                reminder_dt = parse_datetime_with_timezone(
                    reminder_data['proposed_datetime'],
                    reminder_data.get('client_timezone', 'Europe/Moscow')
                )
                
                # Создаем новое напоминание
                cur.execute("""
                    INSERT INTO reminders (
                        conv_id, reminder_datetime, reminder_context_summary,
                        created_by_conv_id, client_timezone, status
                    ) VALUES (%s, %s, %s, %s, %s, 'active')
                    RETURNING id
                """, (
                    target_conv_id,
                    reminder_dt,
                    reminder_data['reminder_context_summary'],
                    created_by_conv_id or conv_id,
                    reminder_data.get('client_timezone', 'Europe/Moscow')
                ))
                
                reminder_id = cur.fetchone()[0]
                conn.commit()
                logging.info(f"Создано напоминание ID={reminder_id} для conv_id={target_conv_id}")
                
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при создании/обновлении напоминания: {e}", exc_info=True)
        raise

def process_new_message(conv_id):
    """
    Обрабатывает новое сообщение для поиска договоренностей о напоминании.
    Вызывается из main.py после получения сообщения от пользователя.
    """
    conn = None
    try:
        # Инициализация модели
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            logging.error("GOOGLE_APPLICATION_CREDENTIALS не установлена")
            return
        
        credentials = service_account.Credentials.from_service_account_file(credentials_path.strip(' "'))
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        model = GenerativeModel(MODEL_NAME)
        
        conn = get_db_connection()
        
        # Анализируем диалог
        reminder_data = analyze_dialogue_for_reminders(conn, conv_id, model)
        
        if reminder_data:
            # Определяем, кто создает напоминание
            created_by = conv_id if conv_id == ADMIN_CONV_ID else None
            
            # Создаем или обновляем напоминание
            create_or_update_reminder(conn, conv_id, reminder_data, created_by)
            
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения для conv_id={conv_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

def initialize_reminder_service():
    """Инициализирует сервис напоминаний."""
    setup_logging()
    
    try:
        # Инициализация модели Vertex AI
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            logging.warning("Переменная окружения 'GOOGLE_APPLICATION_CREDENTIALS' не установлена. Сервис напоминаний будет работать в упрощенном режиме.")
            return True
        
        credentials = service_account.Credentials.from_service_account_file(credentials_path.strip(' "'))
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        model = GenerativeModel(MODEL_NAME)
        
        logging.info("Сервис напоминаний инициализирован. Модель Vertex AI загружена.")
        
        return True
        
    except Exception as e:
        logging.warning(f"Не удалось полностью инициализировать сервис напоминаний: {e}. Работаем в упрощенном режиме.")
        return True

if __name__ == "__main__":
    # Для тестирования
    if initialize_reminder_service():
        logging.info("Сервис напоминаний запущен в тестовом режиме.")
    else:
        logging.error("Сервис напоминаний НЕ инициализирован.")