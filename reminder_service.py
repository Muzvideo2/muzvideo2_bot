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
import requests
from itertools import groupby
from operator import itemgetter

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

# Увеличенный таймаут для активации напоминаний
ACTIVATION_TIMEOUT = 120  # 2 минуты

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
    'хабаровск': 'Asia/Vladivostok',
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
    'иваново': 'Europe/Moscow',
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
2. ИГНОРИРУЙ личные просьбы, не связанные со школой (напомнить вынести мусор, позвонить маме и т.д.), кроме личных просьб от администратора.
3. КРИТИЧЕСКИ ВАЖНО: Администратор имеет conv_id = {admin_conv_id}. 
   - Если администратор просит поставить напоминание для себя, используй его conv_id.
   - Если он просит поставить напоминание для другого человека, он ЯВНО указывает conv_id этого человека.
   - НЕ СОЗДАВАЙ напоминания по старым или неактуальным сообщениям администратора.
4. Учитывай контекст всего диалога для правильной интерпретации.
5. АНАЛИЗИРУЙ ТОЛЬКО ПОСЛЕДНИЕ 3-5 СООБЩЕНИЙ для поиска новых договоренностей.

КРИТЕРИИ ДЛЯ АДМИНИСТРАТОРА:
- Если администратор жалуется на ошибки бота или говорит о проблемах - НЕ создавай напоминания
- Если администратор говорит "поставились напоминания", "бот запутался" - НЕ создавай новых напоминаний
- Создавай напоминания ТОЛЬКО при ЯВНЫХ новых просьбах типа "поставь напоминание"

ТИПЫ ДОГОВОРЕННОСТЕЙ:
- Прямые просьбы: "Напомните мне завтра в 10:00 об оплате"
- Неявные договоренности: "Мне нужно подумать до понедельника"
- Отмена напоминания: "Спасибо, уже не нужно напоминать"
- Перенос напоминания: "Давайте перенесем на вторник"

ПРИМЕРЫ ЕСТЕСТВЕННЫХ ПРОСЬБ:
- Клиент: "Напомните мне завтра в 15:00 об оплате курса"
- Администратор: "Поставь мне напоминание на 16:30 проверить отчеты"  
- Администратор: "Поставь напоминание conv_id: 90123456 завтра о консультации"

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

--- ПОСЛЕДНИЕ СООБЩЕНИЯ ДИАЛОГА (АНАЛИЗИРУЙ ТОЛЬКО ПОСЛЕДНИЕ 3-5) ---
{dialogue_messages}

--- АКТИВНЫЕ НАПОМИНАНИЯ ДЛЯ ЭТОГО КЛИЕНТА ---
{active_reminders}

--- ПОКУПКИ КЛИЕНТА ---
{client_purchases}

ВЕРНИ ОТВЕТ СТРОГО В ФОРМАТЕ JSON, содержащий СПИСОК всех найденных действий.
Если действий нет, верни: {{"reminders": []}}.
[
  {{
    "action": "create/update/cancel/none",
    "target_conv_id": 12345678,
    "proposed_datetime": "YYYY-MM-DDTHH:MM:SS+03:00",
    "reminder_context_summary": "Краткое описание причины напоминания",
    "cancellation_reason": "Причина отмены (только для action=cancel)"
  }}
]
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

{special_rules}

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
  "postpone_to": "YYYY-MM-DDTHH:MM:SS (в часовом поясе клиента, который равен {client_timezone})"
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
        logging.info(f"Сырой ответ от Gemini: {raw_response}")
        
        if expect_json:
            # Улучшенная логика для извлечения JSON из ответа
            # Сначала ищем JSON в markdown-блоке
            match = re.search(r"```(json)?\s*([\s\S]*?)\s*```", raw_response)
            if match:
                json_str = match.group(2)
            else:
                # Если markdown-блока нет, ищем первый символ '{' или '['
                start_index = -1
                for i, char in enumerate(raw_response):
                    if char in ('{', '['):
                        start_index = i
                        break
                if start_index != -1:
                    json_str = raw_response[start_index:]
                else:
                    json_str = raw_response
            
            # Удаляем управляющие символы в начале строки, если они есть
            json_str = json_str.strip()

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
    """
    Анализирует последние сообщения диалога для выявления договоренностей о напоминании.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Получаем последние сообщения диалога
            # Для администратора берем только последние 5 сообщений для избежания анализа старых команд
            message_limit = 5 if conv_id == ADMIN_CONV_ID else 20
            
            cur.execute("""
                SELECT role, message, created_at 
                FROM dialogues 
                WHERE conv_id = %s 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (conv_id, message_limit))
            messages = cur.fetchall()
            
            if not messages:
                logging.info(f"Нет сообщений для анализа в диалоге {conv_id}")
                return None
            
            # Для администратора дополнительно фильтруем только действительно свежие сообщения
            if conv_id == ADMIN_CONV_ID:
                # Берем только сообщения за последние 10 минут
                cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
                
                # Исправляем проблему сравнения datetime с разными timezone
                filtered_messages = []
                for msg in messages:
                    msg_time = msg['created_at']
                    # Если время из БД без timezone, считаем его UTC
                    if msg_time.tzinfo is None:
                        msg_time = msg_time.replace(tzinfo=timezone.utc)
                    
                    if msg_time > cutoff_time:
                        filtered_messages.append(msg)
                
                messages = filtered_messages
                
                if not messages:
                    logging.info(f"Нет свежих сообщений (за последние 10 минут) от администратора {conv_id}")
                    return None
                
                logging.info(f"Анализ администратора: найдено {len(messages)} свежих сообщений за последние 10 минут")
            
            # Форматируем сообщения для промпта
            dialogue_text = []
            for msg in reversed(messages):
                role_map = {'user': 'Клиент', 'bot': 'Ассистент', 'operator': 'Оператор'}
                role = role_map.get(msg['role'], msg['role'])
                message_text = re.sub(r'^\[.*?\]\s*', '', msg['message'])
                dialogue_text.append(f"{role}: {message_text}")
            
            # Получаем активные напоминания для клиента
            cur.execute("""
                SELECT reminder_datetime, reminder_context_summary 
                FROM reminders 
                WHERE conv_id = %s AND status = 'active'
                ORDER BY reminder_datetime
            """, (conv_id,))
            active_reminders = cur.fetchall()
            
            reminders_text = []
            for rem in active_reminders:
                reminders_text.append(f"- {rem['reminder_datetime']}: {rem['reminder_context_summary']}")
            
            # Получаем информацию о клиенте и определяем часовой пояс
            cur.execute("""
                SELECT first_name, last_name, city 
                FROM user_profiles 
                WHERE conv_id = %s
            """, (conv_id,))
            profile_result = cur.fetchone()
            
            # Определяем часовой пояс клиента
            client_timezone = 'Europe/Moscow'  # По умолчанию московское время
            if profile_result and profile_result['city']:
                client_timezone = get_timezone_by_city(profile_result['city'])
            
            # Проверяем, указан ли в сообщениях конкретный часовой пояс
            for msg in messages:
                if msg['role'] == 'user':  # Только сообщения клиента
                    explicit_timezone = detect_timezone_from_message(msg['message'])
                    if explicit_timezone:
                        client_timezone = explicit_timezone
                        break  # Используем последний явно указанный часовой пояс
            
            # Формируем информацию о текущем пользователе для промпта
            user_info = ""
            if profile_result:
                full_name = f"{profile_result['first_name']} {profile_result['last_name']}".strip()
                city = profile_result.get('city', '')
                user_info = f"Информация о текущем пользователе: conv_id={conv_id}, имя='{full_name}'"
                if city:
                    user_info += f", город='{city}'"
                user_info += f", часовой пояс={client_timezone}"
                if conv_id == ADMIN_CONV_ID:
                    user_info += " (АДМИНИСТРАТОР)"
                    logging.info(f"=== АНАЛИЗ СООБЩЕНИЙ АДМИНИСТРАТОРА ===")
                    logging.info(f"Обрабатываем сообщения от администратора (conv_id={ADMIN_CONV_ID})")
                    # Логируем последнее сообщение от пользователя детально
                    user_messages = [msg for msg in messages if msg['role'] == 'user']
                    if user_messages:
                        last_user_msg = user_messages[0]  # Самое последнее
                        logging.info(f"Последнее сообщение пользователя: '{last_user_msg['message']}'")
                    logging.info("=== КОНЕЦ АНАЛИЗА АДМИНИСТРАТОРА ===")
            else:
                user_info = f"conv_id={conv_id}, часовой пояс={client_timezone}"
            
            # Получаем информацию о покупках клиента
            cur.execute("""
                SELECT product_name, purchase_date, amount
                FROM client_purchases 
                WHERE conv_id = %s
                ORDER BY purchase_date DESC
                LIMIT 5
            """, (conv_id,))
            
            purchases = cur.fetchall()
            purchases_text = []
            if purchases:
                for purchase in purchases:
                    date_str = purchase['purchase_date'].strftime('%Y-%m-%d %H:%M') if purchase['purchase_date'] else 'неизвестно'
                    amount_str = f" на сумму {purchase['amount']}" if purchase.get('amount') else ""
                    purchases_text.append(f"- {purchase['product_name']} (дата: {date_str}{amount_str})")
                
                # Проверяем недавние покупки
                current_analysis_time = datetime.now(timezone.utc)
                recent_purchases = []
                for p in purchases:
                    if p['purchase_date']:
                        purchase_time = p['purchase_date']
                        # Если время из БД без timezone, считаем его UTC
                        if purchase_time.tzinfo is None:
                            purchase_time = purchase_time.replace(tzinfo=timezone.utc)
                        
                        time_diff = (current_analysis_time - purchase_time).total_seconds()
                        if time_diff < 86400:  # 24 часа
                            recent_purchases.append(p)
                if recent_purchases:
                    purchases_text.append("\n⚠️ ВНИМАНИЕ: Есть покупки за последние 24 часа!")

            # Формируем промпт
            prompt = PROMPT_ANALYZE_DIALOGUE.format(
                admin_conv_id=ADMIN_CONV_ID,
                current_datetime=get_moscow_time().strftime("%Y-%m-%d %H:%M:%S"),
                conv_id=conv_id,
                user_info=user_info,
                dialogue_messages="\n".join(dialogue_text),
                active_reminders="\n".join(reminders_text) if reminders_text else "Нет активных напоминаний",
                client_purchases="\n".join(purchases_text) if purchases_text else "Нет покупок"
            )
            
            # Логируем полный промпт для диагностики
            # logging.info(f"=== ПОЛНЫЙ ПРОМПТ ДЛЯ АНАЛИЗА ДИАЛОГА {conv_id} ===")
            # logging.info(prompt)
            # logging.info("=== КОНЕЦ ПРОМПТА ===")
            
            # Вызываем AI для анализа
            result = call_gemini_api(model, prompt, expect_json=True)
            
            # Обрабатываем результат, который может быть списком или словарем
            reminders_to_process = []
            if isinstance(result, list):
                reminders_to_process = result
            elif isinstance(result, dict):
                reminders_to_process = result.get('reminders', [])

            logging.info(f"АНАЛИЗ ДИАЛОГА {conv_id}: AI вернул {len(reminders_to_process)} потенциальных напоминаний")

            if reminders_to_process:
                processed_reminders = []
                for i, reminder_data in enumerate(reminders_to_process):
                    logging.info(f"АНАЛИЗ ДИАЛОГА {conv_id}: Обрабатываю напоминание {i+1}: {reminder_data}")
                    
                    if reminder_data.get('action') != 'none':
                        # Проверяем, не создаем ли мы дублирующее напоминание
                        if reminder_data.get('action') == 'create' and active_reminders:
                            new_summary = reminder_data.get('reminder_context_summary', '').lower()
                            is_duplicate = False
                            
                            logging.info(f"ПРОВЕРКА ДУБЛИКАТОВ {conv_id}: Новое напоминание '{new_summary}' против {len(active_reminders)} существующих")
                            
                            for existing in active_reminders:
                                existing_summary = existing['reminder_context_summary'].lower()
                                # Простая проверка на схожесть (более 50% общих слов)
                                new_words = set(new_summary.split())
                                existing_words = set(existing_summary.split())
                                intersection = new_words & existing_words
                                
                                if new_words and existing_words:
                                    similarity = len(intersection) / len(new_words)
                                    logging.info(f"СРАВНЕНИЕ ДУБЛИКАТОВ {conv_id}: '{new_summary}' vs '{existing_summary}' - схожесть {similarity:.2f}")
                                    
                                    if similarity > 0.5:
                                        logging.info(f"ДУБЛИКАТ НАЙДЕН {conv_id}: Пропускаем создание дублирующего напоминания '{new_summary}'")
                                        is_duplicate = True
                                        break
                                        
                            if is_duplicate:
                                continue
                        
                        # Специальная проверка для администратора
                        if conv_id == ADMIN_CONV_ID:
                            summary = reminder_data.get('reminder_context_summary', '').lower()
                            # Отклоняем напоминания по жалобам на ошибки
                            if any(word in summary for word in ['ошибк', 'запутал', 'поставил', 'проблем', 'баг']):
                                logging.info(f"ФИЛЬТР АДМИНИСТРАТОРА {conv_id}: Пропускаю напоминание по жалобе/ошибке: '{summary}'")
                                continue
                        
                        reminder_data['client_timezone'] = client_timezone
                        logging.info(f"ПРИНЯТО НАПОМИНАНИЕ {conv_id}: {reminder_data}")
                        processed_reminders.append(reminder_data)
                    else:
                        logging.info(f"ПРОПУЩЕНО НАПОМИНАНИЕ {conv_id}: action='none'")

                logging.info(f"ИТОГ АНАЛИЗА {conv_id}: Принято {len(processed_reminders)} из {len(reminders_to_process)} напоминаний")
                return processed_reminders if processed_reminders else None
            
            logging.info(f"АНАЛИЗ ДИАЛОГА {conv_id}: Напоминания не найдены")
            return None
            
    except Exception as e:
        logging.error(f"Ошибка при анализе диалога {conv_id}: {e}", exc_info=True)
        return None

def create_or_update_reminder(conn, conv_id, reminder_data, created_by_conv_id=None):
    """
    Создает или обновляет напоминание в БД.
    """
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
                
                # Проверка на прошедшее время
                if reminder_dt < datetime.now(timezone.utc):
                    logging.warning(f"Попытка создать напоминание на прошедшее время ({reminder_dt}) для conv_id={target_conv_id}. Напоминание не будет создано.")
                    return
                
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
                
            elif action == 'cancel':
                # Отменяем активные напоминания
                cancellation_source = 'admin' if conv_id == ADMIN_CONV_ID else 'user'
                
                cur.execute("""
                    UPDATE reminders 
                    SET status = %s, cancellation_reason = %s
                    WHERE conv_id = %s AND status = 'active'
                """, (
                    f'cancelled_by_{cancellation_source}',
                    reminder_data.get('cancellation_reason', 'Отменено пользователем'),
                    target_conv_id
                ))
                
                affected = cur.rowcount
                conn.commit()
                logging.info(f"Отменено {affected} напоминаний для conv_id={target_conv_id}")
                
            elif action == 'update':
                # Обновляем существующее напоминание
                reminder_dt = parse_datetime_with_timezone(
                    reminder_data['proposed_datetime'],
                    reminder_data.get('client_timezone', 'Europe/Moscow')
                )
                
                # Проверка на прошедшее время
                if reminder_dt < datetime.now(timezone.utc):
                    logging.warning(f"Попытка обновить напоминание на прошедшее время ({reminder_dt}) для conv_id={target_conv_id}. Напоминание не будет обновлено.")
                    return
                
                cur.execute("""
                    UPDATE reminders 
                    SET reminder_datetime = %s, reminder_context_summary = %s
                    WHERE id = (
                        SELECT id FROM reminders
                        WHERE conv_id = %s AND status = 'active'
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                """, (
                    reminder_dt,
                    reminder_data['reminder_context_summary'],
                    target_conv_id
                ))
                
                conn.commit()
                logging.info(f"Обновлено напоминание для conv_id={target_conv_id}")
                
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при создании/обновлении напоминания: {e}", exc_info=True)
        raise

def process_reminder_batch(reminders, model):
    """
    Обрабатывает пачку напоминаний для одного пользователя.
    """
    if not reminders:
        return

    conv_id = reminders[0]['conv_id']
    logging.info(f"Обработка пачки из {len(reminders)} напоминаний для conv_id={conv_id}")

    activated_contexts = []
    activated_ids = []
    cancelled_ids = []

    # Этап 1: Проверяем каждое напоминание на уместность активации
    for reminder in reminders:
        try:
            # Передаем другие напоминания из пачки в контекст для корректной проверки
            other_reminders_in_batch = [r for r in reminders if r['id'] != reminder['id']]
            result = process_single_reminder(reminder, model, activate_immediately=False, batch_context=other_reminders_in_batch)
            
            if result and result.get('status') == 'should_activate':
                activated_contexts.append(result['context'])
                activated_ids.append(result['id'])
            elif result and result.get('status') == 'cancelled':
                cancelled_ids.append(result['id'])
                
        except Exception as e:
            logging.error(f"ПАКЕТНАЯ ОБРАБОТКА для conv_id={conv_id}: Ошибка при проверке напоминания ID={reminder['id']}: {e}")
            # При ошибке проверки не добавляем в список активации
            continue

    # Этап 2: Обновляем статусы отмененных напоминаний
    if cancelled_ids:
        conn = None
        try:
            conn = get_db_connection()
            logging.info(f"ПАКЕТНАЯ ОБРАБОТКА для conv_id={conv_id}: Обновляю статусы отмененных напоминаний {cancelled_ids}")
            # Статусы уже обновлены в process_single_reminder, просто логируем
        except Exception as e:
            logging.error(f"ПАКЕТНАЯ ОБРАБОТКА для conv_id={conv_id}: Ошибка при работе с отмененными напоминаниями: {e}")
        finally:
            if conn:
                conn.close()

    if not activated_contexts:
        logging.info(f"В пачке для conv_id={conv_id} нет напоминаний для активации.")
        return

    # Этап 3: Активируем напоминания асинхронно
    logging.info(f"ПАКЕТНАЯ АКТИВАЦИЯ для conv_id={conv_id}: Запускаю асинхронную активацию для {len(activated_ids)} напоминаний")
    
    # Используем отдельный поток для активации, чтобы не блокировать планировщик
    activation_thread = threading.Thread(
        target=_activate_reminders_async,
        args=(conv_id, activated_contexts, activated_ids),
        daemon=True
    )
    activation_thread.start()

def _activate_reminders_async(conv_id, activated_contexts, activated_ids):
    """
    Асинхронная активация напоминаний в отдельном потоке.
    """
    conn = None
    try:
        conn = get_db_connection()
        combined_context = f"У вас несколько сработавших напоминаний:\n\n" + "\n".join([f"- {ctx}" for ctx in activated_contexts])

        port = os.environ.get("PORT", 8080)
        activate_url = f"http://127.0.0.1:{port}/activate_reminder"
        payload = {"conv_id": conv_id, "reminder_context_summary": combined_context}
        
        logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id} (ID: {activated_ids}): Отправляю запрос на {activate_url}")
        
        # Увеличенный таймаут для избежания worker timeout
        response = requests.post(activate_url, json=payload, timeout=ACTIVATION_TIMEOUT)
        response.raise_for_status()
        
        response_text = response.text
        logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: Получен ответ. Статус: {response.status_code}, Тело: '{response_text}'")

        with conn.cursor() as cur:
            logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id} (ID: {activated_ids}): Обновляю статусы в БД на 'done'.")
            cur.execute("UPDATE reminders SET status = 'done' WHERE id = ANY(%s::int[])", (activated_ids,))
            conn.commit()
            logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: Успешно обновлены статусы для {len(activated_ids)} напоминаний.")
            
    except requests.exceptions.Timeout as e:
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: ТАЙМАУТ. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}")
        _revert_reminder_statuses(activated_ids, "timeout")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: ОШИБКА HTTP. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}")
        _revert_reminder_statuses(activated_ids, f"http_error: {e}")
        
    except Exception as e:
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: КРИТИЧЕСКАЯ ОШИБКА. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}", exc_info=True)
        _revert_reminder_statuses(activated_ids, f"critical_error: {e}")
        
    finally:
        if conn:
            conn.close()

def _revert_reminder_statuses(reminder_ids, reason):
    """
    Возвращает статусы напоминаний обратно в 'active' при ошибке активации.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reminders 
                SET status = 'active', 
                    cancellation_reason = %s 
                WHERE id = ANY(%s::int[]) AND status = 'in_progress'
                """,
                (f"Активация не удалась: {reason}", reminder_ids)
            )
            affected = cur.rowcount
            conn.commit()
            logging.info(f"Статус {affected} напоминаний {reminder_ids} возвращен в 'active' из-за: {reason}")
    except Exception as e_revert:
        logging.error(f"Не удалось вернуть статус напоминаниям {reminder_ids}: {e_revert}")
    finally:
        if conn:
            conn.close()

def check_and_activate_reminders(model):
    """
    Проверяет и активирует созревшие напоминания, группируя их по пользователям.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Получаем созревшие напоминания
            cur.execute("""
                SELECT id, conv_id, reminder_datetime, reminder_context_summary,
                       created_at, client_timezone
                FROM reminders 
                WHERE status = 'active' AND reminder_datetime <= NOW()
                ORDER BY conv_id, reminder_datetime
            """)
            
            reminders = cur.fetchall()
            
            if not reminders:
                logging.debug("Нет созревших напоминаний")
                return
            
            logging.info(f"Найдено {len(reminders)} созревших напоминаний. Группируем по пользователям.")

            # Группируем напоминания по conv_id
            reminders_by_user = {k: list(g) for k, g in groupby(reminders, itemgetter('conv_id'))}

            for conv_id, user_reminders in reminders_by_user.items():
                try:
                    # Блокируем все напоминания для этого пользователя
                    reminder_ids = [r['id'] for r in user_reminders]
                    cur.execute(
                        "UPDATE reminders SET status = 'in_progress' WHERE id = ANY(%s::int[])",
                        (reminder_ids,)
                    )
                    conn.commit()
                    
                    # Выбираем, как обрабатывать: по одному или пачкой
                    if len(user_reminders) == 1:
                        target_func = process_single_reminder
                        args = (user_reminders[0], model)
                    else:
                        target_func = process_reminder_batch
                        args = (user_reminders, model)
                    
                    # Обрабатываем в отдельном потоке
                    thread = threading.Thread(target=target_func, args=args, daemon=True)
                    thread.start()
                    
                except Exception as e:
                    logging.error(f"Ошибка при обработке пачки напоминаний для conv_id={conv_id}: {e}")
                    conn.rollback()
                    
    except Exception as e:
        logging.error(f"Ошибка при проверке напоминаний: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

def process_single_reminder(reminder, model, activate_immediately=True, batch_context=None):
    """
    Обрабатывает одно напоминание.
    Если activate_immediately=False, возвращает результат для пакетной обработки.
    batch_context - другие напоминания, обрабатываемые в той же пачке.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        # Собираем контекст клиента (аналогично context_builder.py)
        context = collect_client_context(conn, reminder['conv_id'])
        
        # Получаем все активные напоминания для клиента из БД
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT reminder_datetime, reminder_context_summary 
                FROM reminders 
                WHERE conv_id = %s AND status = 'active' AND id != %s
                ORDER BY reminder_datetime
            """, (reminder['conv_id'], reminder['id']))
            
            other_reminders_from_db = cur.fetchall()
            
        # Добавляем напоминания из текущей пачки для полного контекста
        all_other_reminders = other_reminders_from_db
        if batch_context:
            all_other_reminders.extend(batch_context)
        
        # Формируем промпт для проверки
        reminders_text = []
        for rem in all_other_reminders:
            reminders_text.append(f"- {rem['reminder_datetime']}: {rem['reminder_context_summary']}")
        
        # Определяем, является ли напоминание для администратора
        is_admin_reminder = reminder['conv_id'] == ADMIN_CONV_ID
        special_rules_text = ""
        if is_admin_reminder:
            special_rules_text = (
                "ОСОБЫЕ ПРАВИЛА:\\n"
                "1. Это напоминание для администратора.\\n"
                "2. ЗАПРЕЩЕНО отменять или переносить напоминания для администратора, если он ЯВНО не попросил об этом в диалоге.\\n"
                "3. Сообщения о 'тестировании', 'отладке' или любые другие технические обсуждения НЕ ЯВЛЯЮТСЯ причиной для отмены. Активируй напоминание в любом случае, если нет прямой команды на отмену."
            )

        verify_prompt = PROMPT_VERIFY_REMINDER.format(
            special_rules=special_rules_text,
            reminder_created_at=reminder['created_at'],
            reminder_datetime=reminder['reminder_datetime'],
            reminder_context_summary=reminder['reminder_context_summary'],
            client_context=context,
            all_active_reminders="\\n".join(reminders_text) if reminders_text else "Нет других активных напоминаний",
            client_timezone=reminder.get('client_timezone', 'Europe/Moscow')
        )
        
        # Проверяем уместность напоминания
        verification = call_gemini_api(model, verify_prompt, expect_json=True)
        
        with conn.cursor() as cur:
            if verification['should_activate']:
                logging.info(f"АКТИВАЦИЯ ID={reminder['id']}: Напоминание прошло проверку. Начинаю процесс активации.")
                # Для пакетной обработки просто возвращаем результат
                if not activate_immediately:
                    logging.info(f"АКТИВАЦИЯ ID={reminder['id']}: Готово к пакетной активации.")
                    return {'status': 'should_activate', 'context': reminder['reminder_context_summary'], 'id': reminder['id']}
                
                # Активируем напоминание
                try:
                    # Формируем внутренний URL для вызова внутри того же контейнера
                    port = os.environ.get("PORT", 8080)
                    activate_url = f"http://127.0.0.1:{port}/activate_reminder"
                    
                    payload = {
                        "conv_id": reminder['conv_id'],
                        "reminder_context_summary": reminder['reminder_context_summary']
                    }
                    logging.info(f"АКТИВАЦИЯ ID={reminder['id']}: Отправляю запрос на {activate_url}")
                    
                    response = requests.post(activate_url, json=payload, timeout=60)
                    response.raise_for_status()
                    
                    response_text = response.text
                    logging.info(f"АКТИВАЦИЯ ID={reminder['id']}: Получен ответ. Статус: {response.status_code}, Тело: '{response_text}'")
                    
                    # Помечаем как выполненное
                    logging.info(f"АКТИВАЦИЯ ID={reminder['id']}: Обновляю статус в БД на 'done'.")
                    cur.execute(
                        "UPDATE reminders SET status = 'done' WHERE id = %s",
                        (reminder['id'],)
                    )

                except requests.exceptions.RequestException as e:
                    logging.error(f"АКТИВАЦИЯ ID={reminder['id']}: ОШИБКА. Не удалось вызвать эндпоинт активации. Напоминание НЕ будет активировано в этот раз. Ошибка: {e}")
                    # При ошибке напоминание останется в статусе 'in_progress', 
                    # и блок except выше вернет его в 'active'
                    raise  # Передаем исключение выше, чтобы сработал rollback и возврат статуса
                
            else:
                # Отменяем или переносим напоминание
                suggested_action = verification.get('suggested_action')
                reason = verification.get('reason', 'Причина не указана AI')

                if suggested_action == 'cancel':
                    logging.info(f"ОТМЕНА ID={reminder['id']}: Обновляю статус на 'cancelled_by_reminder'. Причина: {reason}")
                    cur.execute("""
                        UPDATE reminders 
                        SET status = 'cancelled_by_reminder', 
                            cancellation_reason = %s
                        WHERE id = %s
                    """, (reason, reminder['id']))
                    return_status = 'cancelled'
                    
                elif suggested_action == 'postpone':
                    postpone_to_str = verification.get('postpone_to')
                    if not postpone_to_str:
                        logging.warning(f"ПЕРЕНОС ID={reminder['id']}: AI предложил перенос, но не указал время. Напоминание будет отменено.")
                        cur.execute("""
                            UPDATE reminders SET status = 'cancelled_by_reminder', cancellation_reason = %s WHERE id = %s
                        """, (f"Ошибка переноса: не указано время. Исходная причина: {reason}", reminder['id']))
                        return_status = 'cancelled'
                    else:
                        new_datetime = parse_datetime_with_timezone(
                            postpone_to_str,
                            reminder.get('client_timezone') or 'Europe/Moscow'
                        )
                        
                        cur.execute("""
                            UPDATE reminders 
                            SET reminder_datetime = %s, 
                                status = 'active',
                                cancellation_reason = %s
                            WHERE id = %s
                        """, (new_datetime, f"Перенесено: {reason}", reminder['id']))
                        
                        logging.info(f"ПЕРЕНОС ID={reminder['id']}: Напоминание перенесено на {new_datetime}")
                        return_status = 'postponed'
                else:
                    logging.warning(f"НЕИЗВЕСТНОЕ ДЕЙСТВИЕ ID={reminder['id']}: AI предложил '{suggested_action}'. Напоминание будет отменено для безопасности.")
                    cur.execute("""
                        UPDATE reminders SET status = 'cancelled_by_reminder', cancellation_reason = %s WHERE id = %s
                    """, (f"Неизвестное действие от AI: {suggested_action}. Причина: {reason}", reminder['id']))
                    return_status = 'cancelled'

            conn.commit()
            logging.info(f"ID={reminder['id']}: Транзакция успешно завершена.")
            return {'status': return_status, 'id': reminder['id']} # Возвращаем статус для пакетной обработки
            
    except Exception as e:
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА ID={reminder['id']}: Произошла непредвиденная ошибка. Откатываю транзакцию. Статус будет возвращен в 'active'. Ошибка: {e}", exc_info=True)
        if conn:
            conn.rollback()
            # Возвращаем статус обратно в active при ошибке
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE reminders SET status = 'active' WHERE id = %s",
                        (reminder['id'],)
                    )
                    conn.commit()
            except:
                pass
        return None # При ошибке
    finally:
        if conn:
            conn.close()

def collect_client_context(conn, conv_id):
    """
    Собирает полный контекст клиента (аналогично context_builder.py).
    """
    context_parts = []
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Профиль пользователя
            cur.execute("SELECT * FROM user_profiles WHERE conv_id = %s", (conv_id,))
            profile = cur.fetchone()
            
            if profile:
                context_parts.append("--- КАРТОЧКА КЛИЕНТА ---")
                if profile.get('first_name') or profile.get('last_name'):
                    context_parts.append(f"Имя: {profile.get('first_name', '')} {profile.get('last_name', '')}".strip())
                if profile.get('dialogue_summary'):
                    context_parts.append(f"\nКраткое саммари диалога:\n{profile['dialogue_summary']}")
            
            # История диалога
            cur.execute("""
                SELECT role, message, created_at 
                FROM dialogues 
                WHERE conv_id = %s 
                ORDER BY created_at DESC 
                LIMIT 30
            """, (conv_id,))
            
            messages = cur.fetchall()
            if messages:
                context_parts.append("\n--- ПОСЛЕДНЯЯ ИСТОРИЯ ДИАЛОГА ---")
                for msg in reversed(messages):
                    role_map = {'user': 'Клиент', 'bot': 'Ассистент', 'operator': 'Оператор'}
                    role = role_map.get(msg['role'], msg['role'])
                    message_text = re.sub(r'^\[.*?\]\s*', '', msg['message'])
                    context_parts.append(f"{role}: {message_text}")
            
            # Покупки (включая недавние)
            cur.execute("""
                SELECT product_name, purchase_date, amount
                FROM client_purchases 
                WHERE conv_id = %s
                ORDER BY purchase_date DESC
                LIMIT 10
            """, (conv_id,))
            
            purchases = cur.fetchall()
            if purchases:
                context_parts.append("\n--- ПОКУПКИ КЛИЕНТА ---")
                for purchase in purchases:
                    date_str = purchase['purchase_date'].strftime('%Y-%m-%d %H:%M') if purchase['purchase_date'] else 'неизвестно'
                    amount_str = f" на сумму {purchase['amount']}" if purchase.get('amount') else ""
                    context_parts.append(f"- {purchase['product_name']} (дата: {date_str}{amount_str})")
                
                # Проверяем недавние покупки (за последние 24 часа)
                current_time = datetime.now(timezone.utc)
                recent_purchases = []
                for p in purchases:
                    if p['purchase_date']:
                        purchase_time = p['purchase_date']
                        # Если время из БД без timezone, считаем его UTC
                        if purchase_time.tzinfo is None:
                            purchase_time = purchase_time.replace(tzinfo=timezone.utc)
                        
                        time_diff = (current_time - purchase_time).total_seconds()
                        if time_diff < 86400:  # 24 часа
                            recent_purchases.append(p)
                if recent_purchases:
                    context_parts.append("\n⚠️ ВНИМАНИЕ: Есть покупки за последние 24 часа!")
                    for purchase in recent_purchases:
                        purchase_time = purchase['purchase_date']
                        if purchase_time.tzinfo is None:
                            purchase_time = purchase_time.replace(tzinfo=timezone.utc)
                        
                        hours_ago = int((current_time - purchase_time).total_seconds() / 3600)
                        context_parts.append(f"  • {purchase['product_name']} ({hours_ago} часов назад)")
    
    except Exception as e:
        logging.error(f"Ошибка при сборе контекста для conv_id={conv_id}: {e}")
    
    return "\n".join(context_parts)

# --- ПЛАНИРОВЩИК ---

scheduler = None

def start_scheduler(model):
    """Запускает планировщик для периодической проверки напоминаний."""
    global scheduler
    
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # Добавляем задачу проверки напоминаний
    scheduler.add_job(
        func=lambda: check_and_activate_reminders(model),
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        id='check_reminders',
        replace_existing=True
    )
    
    scheduler.start()
    logging.info(f"Планировщик запущен. Проверка напоминаний каждые {CHECK_INTERVAL_MINUTES} минут.")

    # Немедленный первый запуск проверки в отдельном потоке
    logging.info("Запускаем немедленную проверку напоминаний при старте сервиса...")
    initial_check_thread = threading.Thread(target=check_and_activate_reminders, args=(model,), daemon=True)
    initial_check_thread.start()

def stop_scheduler():
    """Останавливает планировщик."""
    global scheduler
    if scheduler:
        scheduler.shutdown()
        logging.info("Планировщик остановлен.")

# --- ФУНКЦИЯ ДЛЯ ВЫЗОВА ИЗ MAIN.PY ---

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
        
        # Анализируем диалог. Теперь reminders_data - это список.
        reminders_data = analyze_dialogue_for_reminders(conn, conv_id, model)
        
        if reminders_data:
            # Определяем, кто создает напоминание
            created_by = conv_id if conv_id == ADMIN_CONV_ID else None
            
            # Обрабатываем каждое напоминание из списка
            for reminder_item in reminders_data:
                try:
                    create_or_update_reminder(conn, conv_id, reminder_item, created_by)
                except Exception as e_item:
                    logging.error(f"Ошибка при обработке одного из напоминаний для conv_id={conv_id}: {reminder_item}. Ошибка: {e_item}")

    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения для conv_id={conv_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# --- ИНИЦИАЛИЗАЦИЯ ПРИ ИМПОРТЕ ---

def initialize_reminder_service():
    """Инициализирует сервис напоминаний."""
    setup_logging()
    
    try:
        # Логируем используемый порт для диагностики
        port = os.environ.get("PORT", 8080)
        logging.info(f"Сервис напоминаний будет использовать порт {port} для внутренних запросов активации.")
        
        # Инициализация модели Vertex AI
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            raise RuntimeError("Переменная окружения 'GOOGLE_APPLICATION_CREDENTIALS' не установлена.")
        
        credentials = service_account.Credentials.from_service_account_file(credentials_path.strip(' "'))
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        model = GenerativeModel(MODEL_NAME)
        
        logging.info("Сервис напоминаний инициализирован. Модель Vertex AI загружена.")
        
        # Запускаем планировщик
        start_scheduler(model)
        
        return True
        
    except Exception as e:
        logging.critical(f"Не удалось инициализировать сервис напоминаний: {e}")
        return False

if __name__ == "__main__":
    # Для тестирования
    if initialize_reminder_service():
        logging.info("Сервис напоминаний запущен в тестовом режиме.")
        try:
            # Держим процесс активным
            import time
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            stop_scheduler()
            logging.info("Сервис напоминаний остановлен.")