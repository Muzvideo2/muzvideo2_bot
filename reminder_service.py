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
3. Администратор имеет conv_id = {admin_conv_id}. Если администратор просит поставить напоминание для себя, используй его conv_id. Если он просит поставить напоминание для другого человека, он обычно указывает conv_id этого человека и понятную просьбу о постановке напоминания.
4. Учитывай контекст всего диалога для правильной интерпретации.

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
        logging.info(f"Сырой ответ от Gemini: {raw_response}")
        
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
    """
    Анализирует последние сообщения диалога для выявления договоренностей о напоминании.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Получаем последние сообщения диалога (больше контекста для лучшего анализа)
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
            
            # Логируем сырые сообщения для диагностики
            logging.info(f"=== СООБЩЕНИЯ ДИАЛОГА {conv_id} ДЛЯ АНАЛИЗА ===")
            for i, msg in enumerate(reversed(messages)):
                logging.info(f"Сообщение {i+1}: role={msg['role']}, message='{msg['message']}'")
            logging.info("=== КОНЕЦ СООБЩЕНИЙ ===")
            
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
                recent_purchases = [p for p in purchases if p['purchase_date'] and 
                                  (datetime.now() - p['purchase_date']).total_seconds() < 86400]
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
            logging.info(f"=== ПОЛНЫЙ ПРОМПТ ДЛЯ АНАЛИЗА ДИАЛОГА {conv_id} ===")
            logging.info(prompt)
            logging.info("=== КОНЕЦ ПРОМПТА ===")
            
            # Вызываем AI для анализа
            result = call_gemini_api(model, prompt, expect_json=True)
            
            # Обрабатываем результат
            if result.get('action') != 'none':
                # Проверяем, не создаем ли мы дублирующее напоминание
                if result.get('action') == 'create' and active_reminders:
                    # Если есть активные напоминания с похожим контекстом, не создаем новое
                    new_summary = result.get('reminder_context_summary', '').lower()
                    for existing in active_reminders:
                        existing_summary = existing['reminder_context_summary'].lower()
                        # Простая проверка на схожесть (более 50% общих слов)
                        new_words = set(new_summary.split())
                        existing_words = set(existing_summary.split())
                        if len(new_words & existing_words) > len(new_words) * 0.5:
                            logging.info(f"Пропускаем создание дублирующего напоминания для {conv_id}")
                            return None
                
                result['client_timezone'] = client_timezone
                logging.info(f"Выявлена договоренность в диалоге {conv_id}: {result}")
                return result
            
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

def check_and_activate_reminders(model):
    """
    Проверяет и активирует созревшие напоминания.
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
                ORDER BY reminder_datetime
            """)
            
            reminders = cur.fetchall()
            
            if not reminders:
                logging.debug("Нет созревших напоминаний")
                return
            
            logging.info(f"Найдено {len(reminders)} созревших напоминаний")
            
            for reminder in reminders:
                try:
                    # Блокируем напоминание
                    cur.execute(
                        "UPDATE reminders SET status = 'in_progress' WHERE id = %s",
                        (reminder['id'],)
                    )
                    conn.commit()
                    
                    # Обрабатываем напоминание в отдельном потоке
                    thread = threading.Thread(
                        target=process_single_reminder,
                        args=(reminder, model)
                    )
                    thread.daemon = True
                    thread.start()
                    
                except Exception as e:
                    logging.error(f"Ошибка при обработке напоминания ID={reminder['id']}: {e}")
                    conn.rollback()
                    
    except Exception as e:
        logging.error(f"Ошибка при проверке напоминаний: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

def process_single_reminder(reminder, model):
    """
    Обрабатывает одно напоминание.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        # Собираем контекст клиента (аналогично context_builder.py)
        context = collect_client_context(conn, reminder['conv_id'])
        
        # Получаем все активные напоминания для клиента
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT reminder_datetime, reminder_context_summary 
                FROM reminders 
                WHERE conv_id = %s AND status = 'active' AND id != %s
                ORDER BY reminder_datetime
            """, (reminder['conv_id'], reminder['id']))
            
            other_reminders = cur.fetchall()
            
        # Формируем промпт для проверки
        reminders_text = []
        for rem in other_reminders:
            reminders_text.append(f"- {rem['reminder_datetime']}: {rem['reminder_context_summary']}")
        
        verify_prompt = PROMPT_VERIFY_REMINDER.format(
            reminder_created_at=reminder['created_at'],
            reminder_datetime=reminder['reminder_datetime'],
            reminder_context_summary=reminder['reminder_context_summary'],
            client_context=context,
            all_active_reminders="\n".join(reminders_text) if reminders_text else "Нет других активных напоминаний"
        )
        
        # Проверяем уместность напоминания
        verification = call_gemini_api(model, verify_prompt, expect_json=True)
        
        with conn.cursor() as cur:
            if verification['should_activate']:
                # Активируем напоминание
                logging.info(f"Напоминание ID={reminder['id']} прошло проверку и будет активировано")
                
                # Здесь должен быть вызов основного AI-коммуникатора
                # Для этого нужна интеграция с main.py
                # Пока просто помечаем как выполненное
                cur.execute(
                    "UPDATE reminders SET status = 'done' WHERE id = %s",
                    (reminder['id'],)
                )
                
                # TODO: Вызвать generate_and_send_response из main.py
                # с инъекцией контекста напоминания
                
            else:
                # Отменяем или переносим напоминание
                if verification['suggested_action'] == 'cancel':
                    cur.execute("""
                        UPDATE reminders 
                        SET status = 'cancelled_by_reminder', 
                            cancellation_reason = %s
                        WHERE id = %s
                    """, (verification['reason'], reminder['id']))
                    
                    logging.info(f"Напоминание ID={reminder['id']} отменено: {verification['reason']}")
                    
                elif verification['suggested_action'] == 'postpone':
                    new_datetime = parse_datetime_with_timezone(
                        verification['postpone_to'],
                        reminder['client_timezone'] or 'Europe/Moscow'
                    )
                    
                    cur.execute("""
                        UPDATE reminders 
                        SET reminder_datetime = %s, 
                            status = 'active',
                            cancellation_reason = %s
                        WHERE id = %s
                    """, (new_datetime, f"Перенесено: {verification['reason']}", reminder['id']))
                    
                    logging.info(f"Напоминание ID={reminder['id']} перенесено на {new_datetime}")
            
            conn.commit()
            
    except Exception as e:
        logging.error(f"Ошибка при обработке напоминания ID={reminder['id']}: {e}", exc_info=True)
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
                recent_purchases = [p for p in purchases if p['purchase_date'] and 
                                  (datetime.now() - p['purchase_date']).total_seconds() < 86400]
                if recent_purchases:
                    context_parts.append("\n⚠️ ВНИМАНИЕ: Есть покупки за последние 24 часа!")
                    for purchase in recent_purchases:
                        hours_ago = int((datetime.now() - purchase['purchase_date']).total_seconds() / 3600)
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

# --- ИНИЦИАЛИЗАЦИЯ ПРИ ИМПОРТЕ ---

def initialize_reminder_service():
    """Инициализирует сервис напоминаний."""
    setup_logging()
    
    try:
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