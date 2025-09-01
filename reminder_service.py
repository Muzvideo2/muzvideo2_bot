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

# Словарь блокировок для предотвращения конкурентного создания напоминаний
reminder_creation_locks = {}

# Словарь для отслеживания неудачных попыток активации напоминаний
# Формат: {reminder_id: {"attempts": count, "last_error": "error_message", "first_attempt": datetime}}
failed_activation_attempts = {}

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
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "hardy-technique-470816-f2")
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

# Настройки для уведомлений в Telegram
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
MAX_RETRY_ATTEMPTS = 3  # Максимальное количество попыток перед отправкой уведомления

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

ТВОЯ РОЛЬ В СИСТЕМЕ:
Ты — специализированный AI-агент, который работает за кулисами. Твоя активация происходит ПОСЛЕ того, как Ассистент-коммуникатор ответил клиенту.
- Ассистент-коммуникатор (в диалоге 'Ассистент') НЕ УМЕЕТ ставить напоминания. Он только вежливо общается с клиентом.
- Если Ассистент говорит "хорошо, напомню", "я поставил напоминание" — это сигнал для ТЕБЯ создать это напоминание. Это НЕ значит, что оно уже создано.
- Только ты можешь создавать, изменять или отменять напоминания. Твои решения — финальные.

ПРАВИЛА ОБРАБОТКИ ВРЕМЕНИ:
1.  ПРИОРИТЕТ ЧАСОВОГО ПОЯСА КЛИЕНТА: Вся интерпретация времени должна происходить в часовом поясе клиента: {client_timezone} (UTC{client_timezone_offset}).
2.  ИСКЛЮЧЕНИЕ: Если в сообщении явно указан другой часовой пояс (например, "по Москве", "по МСК", "UTC"), используй его для расчета, но в итоговом JSON формате `proposed_datetime` время ВСЕГДА должно быть приведено к часовому поясу клиента.
3.  ЕСЛИ ВРЕМЯ НЕ УКАЗАНО: Используй стандартное время по умолчанию в часовом поясе клиента (например, "завтра" = 10:00 по времени клиента).
4.  СОХРАНЕНИЕ КОНТЕКСТА: Если в просьбе упоминается особый часовой пояс ("напомни в 12 по Москве"), эта деталь ДОЛЖНА БЫТЬ включена в `reminder_context_summary`. Это критически важно для других агентов, чтобы они понимали логику.

ВАЖНЫЕ ПРАВИЛА:
1. Ищи ТОЛЬКО договоренности, связанные с деятельностью школы (оплата, обучение, курсы, консультации).
2. ИГНОРИРУЙ личные просьбы, не связанные со школой (напомнить вынести мусор, позвонить маме и т.д.), кроме личных просьб от администратора.
3. КРИТИЧЕСКИ ВАЖНО: Администратор имеет conv_id = {admin_conv_id}. 
   - Если администратор просит поставить напоминание для себя, используй его conv_id.
   - Если он просит поставить напоминание для другого человека, он ЯВНО указывает conv_id этого человека.
   - НЕ СОЗДАВАЙ напоминания по старым или неактуальным сообщениям администратора.
4. Учитывай контекст всего диалога для правильной интерпретации.

⚠️ КРИТИЧЕСКИ ВАЖНО - ПРАВИЛА ПРОТИВ ДУБЛИРОВАНИЯ:
1. НА ОДНУ ПРОСЬБУ СОЗДАВАЙ ТОЛЬКО ОДНО НАПОМИНАНИЕ!
2. Если администратор говорит "поставь МНЕ напоминание" → target_conv_id = {admin_conv_id}
3. Если администратор говорит "поставь Сергею/клиенту напоминание" → target_conv_id = conv_id этого клиента
4. НИКОГДА не создавай два напоминания с одинаковым содержанием для разных людей!

🎯 СПЕЦИАЛЬНЫЕ ПРАВИЛА ДЛЯ УПОМИНАНИЙ "СЕРГЕЯ" КЛИЕНТАМИ:

КОГДА КЛИЕНТ УПОМИНАЕТ "СЕРГЕЯ" - СОЗДАВАЙ НАПОМИНАНИЕ ДЛЯ АДМИНИСТРАТОРА:
✅ Клиент просит связаться с Сергеем:
   - "Вы можете передать Сергею, чтобы он написал мне?"
   - "Скажите Сергею, что я готова к занятию"
   - "Попросите Сергея связаться со мной"
   - "Сергей говорил, что напишет мне сегодня"
   → target_conv_id = {admin_conv_id}, напоминание "Клиент [имя] просит связаться"

КОГДА НЕ СОЗДАВАТЬ НАПОМИНАНИЕ ДЛЯ АДМИНИСТРАТОРА:
❌ Простое упоминание без просьбы о контакте:
   - "Я не получала от Сергея письма" (констатация факта)
   - "Сергей мне вчера говорил..." (рассказ о прошлом)
   - "Сергей автор курса" (информация)

КОГДА КЛИЕНТ ПРОСИТ НАПОМИНАНИЕ ДЛЯ СЕБЯ - НЕ ДУБЛИРУЙ ДЛЯ АДМИНИСТРАТОРА:
✅ Клиент просит напомнить ему:
   - "Напомните мне завтра об оплате"
   - "Я сегодня занята, напишите завтра" 
   - "Можете написать мне через час?"
   → target_conv_id = conv_id клиента (НЕ администратора!)

ОБЯЗАТЕЛЬНЫЙ ФОРМАТ ОПИСАНИЙ НАПОМИНАНИЙ:

🔹 ДЛЯ КЛИЕНТОВ: "[Действие] для [Имя Клиента] (conv_id: [ID]) - [Причина]"
   Пример: "Написать Юлии Благодаровой (conv_id: 553547455) о консультации завтра"

🔹 ДЛЯ АДМИНИСТРАТОРА: "[Действие для admin] - [Причина] от клиента [Имя] (conv_id: [ID клиента])"
   Пример: "Связаться с администратором - клиент Мария Иванова (conv_id: 123456) просит консультацию"

ПРИМЕРЫ ПРАВИЛЬНОГО ОПРЕДЕЛЕНИЯ target_conv_id:

✅ Администратор: "Поставь мне напоминание завтра проверить отчеты"
   → target_conv_id: {admin_conv_id} (ТОЛЬКО одно напоминание для администратора)
   → reminder_context_summary: "Проверить отчеты по задаче от {admin_conv_id}"

✅ Администратор: "Поставь напоминание conv_id: 90123456 завтра о консультации"  
   → target_conv_id: 90123456 (ТОЛЬКО одно напоминание для указанного клиента)
   → reminder_context_summary: "Консультация для клиента (conv_id: 90123456) по запросу admin"

✅ Клиент: "Передайте Сергею, чтобы он мне написал"
   → target_conv_id: {admin_conv_id} (ТОЛЬКО одно напоминание для администратора)
   → reminder_context_summary: "Связаться с администратором - клиент [Имя] (conv_id: [conv_id клиента]) просит контакт"

✅ Клиент: "Напомните мне завтра об оплате"
   → target_conv_id: [conv_id клиента] (ТОЛЬКО одно напоминание для клиента)
   → reminder_context_summary: "Напомнить [Имя] (conv_id: [conv_id]) об оплате"

❌ Администратор: "Поставь Сергею Какорину напоминание завтра"
   → НЕ СОЗДАВАЙ - нет conv_id для Сергея Какорина!

❌ Клиент: "Я не получала от Сергея письма"
   → НЕ СОЗДАВАЙ - нет просьбы о контакте!

ТИПЫ ДОГОВОРЕННОСТЕЙ:
- Прямые просьбы: "Напомните мне завтра в 10:00 об оплате"
- Прямые просьбы с двойным временем: "Напомните мне завтра в 10 утра о том, чтобы я мог оплатить вечером в 18:00"
- Неявные договоренности: "Мне нужно подумать до понедельника"
- Просьбы связаться с администратором: "Передайте Сергею, чтобы он написал"
- Отмена напоминания: "Спасибо, уже не нужно напоминать"
- Перенос напоминания: "Давайте перенесем на вторник"

📋 СПЕЦИАЛЬНЫЕ СЦЕНАРИИ - СОГЛАСИЕ/ОТКАЗ НА ПРЕДЛОЖЕНИЯ АССИСТЕНТА:

✅ СОГЛАСИЕ НА ПРЕДЛОЖЕНИЕ АССИСТЕНТА О НАПОМИНАНИИ:
Контекст: Ассистент предлагает напоминание в предыдущем сообщении
Клиент соглашается:
- "Да, конечно"
- "Хорошо"
- "Да, давайте"
- "Согласна"
- "Можно"
→ Создай напоминание на время, которое Ассистент предложил в ПРЕДЫДУЩЕМ сообщении

Пример диалога:
Ассистент: "Я могу Вам мягко напомнить через неделю о нашем разговоре?"
Клиент: "Да, конечно"
→ proposed_datetime: через 7 дней в 19:00
→ reminder_context_summary: "Мягко напомнить о разговоре"

❌ ОТКАЗ ОТ НАПОМИНАНИЯ:
Клиент отказывается:
- "Нет, не пишите"
- "Не нужно"
- "Не надо напоминать"
- "Пока не пишите"
→ action: "cancel" - отменить все активные напоминания для этого клиента

⚠️ СОГЛАСИЕ НА АЛЬТЕРНАТИВНОЕ ПРЕДЛОЖЕНИЕ:
Контекст: Ассистент предлагает другой срок напоминания после отказа
Клиент соглашается:
- "Да, хорошо"
- "Через месяц можно"
- "Согласна на месяц"
→ Создай напоминание на новый срок, предложенный Ассистентом

Пример диалога:
Клиент: "Нет, не пишите"
Ассистент: "А что если я напишу Вам через месяц?"
Клиент: "Да, хорошо"
→ proposed_datetime: через 30 дней в 19:00
→ reminder_context_summary: "Мягко напомнить о разговоре"

🔍 КАК ОПРЕДЕЛИТЬ КОНТЕКСТ ИЗ ПРЕДЫДУЩИХ СООБЩЕНИЙ:
- Ищи в последних 3-5 сообщениях предложения Ассистента со словами: "напомню", "напишу", "свяжусь"
- Извлекай временные интервалы: "через неделю", "через месяц", "завтра", "через 3 дня"
- Если клиент соглашается БЕЗ контекста предложения Ассистента - НЕ создавай напоминание

ВАЖНО: Эти правила работают ТОЛЬКО если в диалоге есть предложение от Ассистента!

ПРИМЕРЫ ЕСТЕСТВЕННЫХ ПРОСЬБ:
- Клиент: "Напомните мне завтра в 15:00 об оплате курса"
- Клиент: "Передайте Сергею, что я готова к консультации" → напоминание для администратора
- Администратор: "Поставь мне напоминание на 16:30 проверить отчеты"  
- Администратор: "Поставь напоминание conv_id: 90123456 завтра о консультации"

КРИТИЧЕСКИ ВАЖНО: РАЗЛИЧАЙ ВРЕМЯ НАПОМИНАНИЯ И ВРЕМЯ СОБЫТИЯ!

ВРЕМЯ НАПОМИНАНИЯ (proposed_datetime) = когда должно сработать напоминание
ВРЕМЯ СОБЫТИЯ (reminder_context_summary) = о чем напомнить

ПРИМЕРЫ ПРАВИЛЬНОЙ ИНТЕРПРЕТАЦИИ:
✅ "Напомни мне завтра в 10 утра о том, что в 13:00 замена масла"
   → proposed_datetime: завтра 10:00
   → reminder_context_summary: "Замена масла в 13:00"

✅ "Напомни мне завтра в 12:00 по Москве, что пора в больницу"
   → proposed_datetime: [время, соответствующее 12:00 МСК, но в часовом поясе клиента, т.е. 15:00 по Омску]
   → reminder_context_summary: "Пора в больницу (напоминание было установлено на 12:00 по Москве)"

✅ "Передайте Сергею, чтобы он написал мне завтра"
   → proposed_datetime: завтра 10:00
   → target_conv_id: {admin_conv_id}
   → reminder_context_summary: "Клиент [имя], conv_id: [conv_id клиента] просит связаться завтра"

✅ "Напомни мне в 15:00 об оплате курса"
   → proposed_datetime: сегодня/завтра 15:00
   → reminder_context_summary: "Об оплате курса"

✅ "Напомни мне вечером проверить почту"
   → proposed_datetime: сегодня 19:00
   → reminder_context_summary: "Проверить почту"

✅ "Напомни мне за час до встречи в 14:00"
   → proposed_datetime: сегодня 13:00
   → reminder_context_summary: "Встреча в 14:00"

📋 ПРИМЕРЫ ИНТЕРПРЕТАЦИИ:

✅ Ассистент: "Я могу Вам мягко напомнить через неделю о нашем разговоре?"
   → proposed_datetime: через 7 дней в 19:00
   → target_conv_id: [conv_id клиента]
   → reminder_context_summary: "Мягко напомнить о разговоре"

✅ Ассистент: "Хорошо, я поставлю себе напоминание и через неделю мягко напомню о себе"
   → proposed_datetime: через 7 дней в 19:00
   → target_conv_id: [conv_id клиента]  
   → reminder_context_summary: "Мягко напомнить о разговоре"

❌ Клиент: "Нет, не пишите"
   → action: "cancel"
   → target_conv_id: [conv_id клиента]
   → cancellation_reason: "Клиент попросил не писать"

✅ Диалог (после отказа):
Ассистент: "А что если я напишу Вам через месяц?"
Клиент: "Да, хорошо"
Ассистент: "Хорошо, я поставлю себе напоминание"
   → proposed_datetime: через 30 дней в 19:00
   → target_conv_id: [conv_id клиента]
   → reminder_context_summary: "Мягко напомнить о разговоре"

ПРАВИЛА ИНТЕРПРЕТАЦИИ ВРЕМЕНИ НАПОМИНАНИЯ:
- "Завтра" = следующий день в 10:00
- "Утром" = 10:00 текущего/следующего дня
- "Днем" = 14:00 текущего/следующего дня
- "Вечером" = 19:00 текущего/следующего дня
- "Через пару дней" = через 2 дня в 12:00
- "На следующей неделе" = понедельник следующей недели в 12:00
- Конкретное время ("в 10:00", "в 15:30") = точное указанное время
- Если время не указано для просьбы связаться с Сергеем = сегодня 19:00

📋 ПРАВИЛА ДЛЯ СОГЛАСИЙ НА ПРЕДЛОЖЕНИЯ АССИСТЕНТА:
- "через неделю" (из предложения Ассистента) = через 7 дней в 19:00
- "через месяц" (из предложения Ассистента) = через 30 дней в 19:00  
- "через 3 дня" (из предложения Ассистента) = через 3 дня в 19:00
- Если Ассистент предложил конкретное время - используй его
- Если время не указано в предложении Ассистента = сегодня 19:00

Текущее время (в часовом поясе клиента, {client_timezone}): {current_client_time}
ID текущего диалога: {conv_id}

========================================
+--- ИНФОРМАЦИЯ О ПОЛЬЗОВАТЕЛЕ ---
========================================
{user_info}
========================================

========================================
--- ПОСЛЕДНИЕ СООБЩЕНИЯ ДИАЛОГА ---
========================================
⏰ ФОРМАТ: Каждое сообщение содержит временную метку [YYYY-MM-DD HH:MM:SS]
📝 ИСПОЛЬЗУЙ МЕТКИ для понимания последовательности событий
{dialogue_messages}
========================================

========================================
--- АКТИВНЫЕ НАПОМИНАНИЯ ДЛЯ ЭТОГО КЛИЕНТА ---
========================================
{active_reminders}
========================================

💡 ГЛАВНОЕ ПРАВИЛО: ОДНА ПРОСЬБА — ОДНО НАПОМИНАНИЕ.

🚫 НЕ СОЗДАВАЙ НОВОЕ НАПОМИНАНИЕ, ЕСЛИ:
1. ✅ В списке "АКТИВНЫЕ НАПОМИНАНИЯ" уже есть точно такое же или очень похожее по смыслу и времени. Это твой главный источник правды.
2. ✅ Клиент просто продолжает обсуждать детали уже согласованного напоминания, не создавая новой просьбы.
3. ✅ ОБЯЗАТЕЛЬНО проверяй каждое предлагаемое напоминание против списка активных. Даже небольшая схожесть (>50% общих слов) - повод НЕ создавать дубликат.
4. ✅ При любых сомнениях - лучше НЕ создавать новое напоминание, чем создать дубликат.

✅ ОБЯЗАТЕЛЬНО СОЗДАЙ НАПОМИНАНИЕ, ЕСЛИ:
1. ✅ Клиент попросил напомнить, а Ассистент в ответ пообещал это сделать ("хорошо, напомню", "поставил напоминание"). Твоя задача — выполнить это обещание! Ответ Ассистента — это подтверждение необходимости создания напоминания, а не признак того, что оно уже существует.

🔍 ПРИМЕРЫ АНАЛИЗА:

Сценарий 1:
[14:30] Клиент: напишите завтра
[14:31] Ассистент: хорошо, напомню
→ РЕШЕНИЕ: ✅ Создавай напоминание! Ответ ассистента — это твоя команда к действию.

Сценарий 2: (Агент вызывается снова после следующего сообщения клиента)
[14:30] Клиент: напишите завтра
[14:31] Ассистент: хорошо, напомню
[14:32] Клиент: я буду дома
[14:33] Ассистент: да, я поставил напоминание
[Список активных напоминаний]: "Напомнить завтра"
→ РЕШЕНИЕ: 🚫 НЕ создавай новое! Напоминание уже есть в активных. Сообщение клиента — лишь дополнение к существующей договоренности.

Сценарий 3:
[14:30] Клиент: напишите завтра  
[14:31] Ассистент: хорошо, напомню
[14:35] Клиент: и еще напомните в понедельник ← НОВАЯ просьба
[14:37] Ассистент: хорошо
→ РЕШЕНИЕ: ✅ Создавай новое напоминание на понедельник. Это отдельная, новая просьба.

========================================
--- ПОКУПКИ КЛИЕНТА ---
========================================
{client_purchases}

ВЕРНИ ОТВЕТ СТРОГО В ФОРМАТЕ JSON, содержащий СПИСОК всех найденных действий.
Если действий нет, верни: {{"reminders": []}}.
ПОМНИ: НА ОДНУ ПРОСЬБУ - ТОЛЬКО ОДНО НАПОМИНАНИЕ!
[
  {{
    "action": "create/update/cancel/none",
    "target_conv_id": 12345678,
    "proposed_datetime": "YYYY-MM-DDTHH:MM:SS{client_timezone_offset}",
    "reminder_context_summary": "Краткое описание причины напоминания",
    "cancellation_reason": "Причина отмены (только для action=cancel)",
    "none_reason": "Причина отсутствия действия (для action=none, например: 'напоминание уже установлено')"
  }}
]
"""

# ПРОМПТ ДЛЯ ПРОВЕРКИ АКТУАЛЬНОСТИ УДАЛЕН
# Теперь все напоминания активируются автоматически без ИИ-проверки

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
        # logging.info(f"Отправляем запрос в Gemini. Промпт (первые 200 символов): {prompt[:200]}...")
        
        response = model.generate_content(prompt)
        raw_response = response.text
        
        # logging.info(f"Получен ответ от Gemini (длина: {len(raw_response)} символов)")
        # logging.info(f"Сырой ответ от Gemini: {raw_response}")
        
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
            # logging.info(f"JSON успешно распарсен: {parsed_response}")
            return parsed_response
        else:
            return raw_response.strip()

    except json.JSONDecodeError as je:
        logging.error(f"Ошибка парсинга JSON от Gemini: {je}. Сырой ответ: {raw_response}")
        raise
    except Exception as e:
        logging.error(f"Ошибка вызова Vertex AI API: {e}", exc_info=True)
        raise

def get_timezone_offset_str(tz_name):
    """Возвращает строковое представление смещения UTC для часового пояса."""
    try:
        tz = pytz.timezone(tz_name)
        offset = datetime.now(tz).utcoffset()
        offset_hours = offset.total_seconds() / 3600
        return f"{offset_hours:+03.0f}:00".replace('.0', '')
    except pytz.UnknownTimeZoneError:
        return "+03:00" # По умолчанию Москва
        
def get_moscow_time():
    """Возвращает текущее московское время."""
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)

def send_telegram_notification(message):
    """
    Отправляет уведомление в Telegram администратору.
    """
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        logging.warning("TELEGRAM УВЕДОМЛЕНИЕ: Не настроены TELEGRAM_TOKEN или ADMIN_CHAT_ID")
        return False
    
    try:
        telegram_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        
        payload = {
            "chat_id": ADMIN_CHAT_ID,
            "text": f"🚨 ОШИБКА СЕРВИСА НАПОМИНАНИЙ\n\n{message}",
            "parse_mode": "HTML"
        }
        
        response = requests.post(telegram_url, json=payload, timeout=10)
        response.raise_for_status()
        
        logging.info(f"TELEGRAM УВЕДОМЛЕНИЕ: Успешно отправлено сообщение в чат {ADMIN_CHAT_ID}")
        return True
        
    except Exception as e:
        logging.error(f"TELEGRAM УВЕДОМЛЕНИЕ: Ошибка отправки в Telegram: {e}")
        return False

def track_activation_failure(reminder_id, error_message):
    """
    Отслеживает неудачные попытки активации напоминания.
    Отправляет уведомление в Telegram после MAX_RETRY_ATTEMPTS неудачных попыток.
    """
    global failed_activation_attempts
    
    current_time = datetime.now(timezone.utc)
    
    if reminder_id not in failed_activation_attempts:
        failed_activation_attempts[reminder_id] = {
            "attempts": 1,
            "last_error": error_message,
            "first_attempt": current_time
        }
        logging.info(f"МОНИТОРИНГ ОШИБОК: Первая неудачная попытка активации для reminder_id={reminder_id}")
    else:
        failed_activation_attempts[reminder_id]["attempts"] += 1
        failed_activation_attempts[reminder_id]["last_error"] = error_message
        
        attempts = failed_activation_attempts[reminder_id]["attempts"]
        logging.warning(f"МОНИТОРИНГ ОШИБОК: Попытка #{attempts} активации для reminder_id={reminder_id} неудачна")
        
        # Отправляем уведомление после MAX_RETRY_ATTEMPTS попыток
        if attempts >= MAX_RETRY_ATTEMPTS:
            first_attempt = failed_activation_attempts[reminder_id]["first_attempt"]
            duration = current_time - first_attempt
            
            message = (
                f"<b>Напоминание ID {reminder_id} не удается активировать</b>\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"• Количество попыток: {attempts}\n"
                f"• Первая попытка: {first_attempt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                f"• Длительность проблемы: {duration}\n\n"
                f"❌ <b>Последняя ошибка:</b>\n"
                f"<code>{error_message}</code>\n\n"
                f"⚠️ <b>Действие:</b> Требуется ручная проверка сервиса напоминаний"
            )
            
            if send_telegram_notification(message):
                logging.info(f"МОНИТОРИНГ ОШИБОК: Отправлено уведомление в Telegram для reminder_id={reminder_id}")
                # Удаляем из отслеживания после отправки уведомления
                del failed_activation_attempts[reminder_id]
            else:
                logging.error(f"МОНИТОРИНГ ОШИБОК: Не удалось отправить уведомление в Telegram для reminder_id={reminder_id}")

def clear_activation_success(reminder_id):
    """
    Очищает счетчик неудачных попыток при успешной активации напоминания.
    """
    global failed_activation_attempts
    
    if reminder_id in failed_activation_attempts:
        attempts = failed_activation_attempts[reminder_id]["attempts"]
        logging.info(f"МОНИТОРИНГ ОШИБОК: Напоминание reminder_id={reminder_id} успешно активировано после {attempts} неудачных попыток")
        del failed_activation_attempts[reminder_id]

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
        logging.info(f"Начат анализ на необходимость установки напоминаний для conv_id={conv_id}")
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Получаем последние сообщения диалога (унифицировано 10 сообщений для всех пользователей)
            message_limit = 10  # Унифицировано для всех пользователей
            
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
                
                # КРИТИЧЕСКИ ВАЖНО: Добавляем временные метки для понимания хронологии (синхронизация с агентом активации)
                timestamp = msg['created_at'].strftime('%Y-%m-%d %H:%M:%S') if msg['created_at'] else 'неизвестно'
                dialogue_text.append(f"[{timestamp}] {role}: {message_text}")
            
            # Получаем активные напоминания для клиента (ВСЕГДА СВЕЖИЕ ДАННЫЕ)
            # КРИТИЧЕСКИ ВАЖНО: Выполняем запрос БЕЗ КЕШИРОВАНИЯ для актуального состояния
            cur.execute("""
                SELECT id, reminder_datetime, reminder_context_summary, created_at
                FROM reminders 
                WHERE conv_id = %s AND status = 'active'
                ORDER BY reminder_datetime
            """, (conv_id,))
            active_reminders = cur.fetchall()
            
            logging.info(f"ПРОВЕРКА СУЩЕСТВУЮЩИХ НАПОМИНАНИЙ: Найдено {len(active_reminders)} активных напоминаний для conv_id={conv_id}")
            
            reminders_text = []
            for rem in active_reminders:
                # Добавляем ID напоминания для лучшей отслеживаемости
                created_time = rem['created_at'].strftime('%Y-%m-%d %H:%M:%S') if rem['created_at'] else 'неизвестно'
                reminders_text.append(f"- [ID:{rem['id']}] {rem['reminder_datetime']}: {rem['reminder_context_summary']} (создано: {created_time})")
            
            # Получаем информацию о клиенте и определяем часовой пояс
            cur.execute("""
                SELECT first_name, last_name, city 
                FROM user_profiles 
                WHERE conv_id = %s
            """, (conv_id,))
            profile_result = cur.fetchone()
            
            # Определяем часовой пояс клиента и его смещение
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
            
            client_timezone_offset = get_timezone_offset_str(client_timezone)

            # Формируем информацию о текущем пользователе для промпта
            user_info = ""
            if profile_result:
                full_name = f"{profile_result['first_name']} {profile_result['last_name']}".strip()
                city = profile_result.get('city', '')
                user_info = f"Информация о текущем пользователе: conv_id={conv_id}, имя='{full_name}'"
                if city:
                    user_info += f", город='{city}'"
                user_info += f", часовой пояс={client_timezone} (UTC{client_timezone_offset})"
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
                user_info = f"conv_id={conv_id}, часовой пояс={client_timezone} (UTC{client_timezone_offset})"
            
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

            # Формируем промпт безопасным способом в несколько этапов
            prompt = PROMPT_ANALYZE_DIALOGUE.replace("{user_info}", user_info)
            prompt = prompt.replace("{dialogue_messages}", "\n".join(dialogue_text))
            prompt = prompt.replace("{active_reminders}", "\n".join(reminders_text) if reminders_text else "Нет активных напоминаний")
            prompt = prompt.replace("{client_purchases}", "\n".join(purchases_text) if purchases_text else "Нет покупок")
            
            # Теперь используем .format() для оставшихся, безопасных переменных
            current_client_time_str = datetime.now(pytz.timezone(client_timezone)).strftime("%Y-%m-%d %H:%M:%S")
            
            prompt = prompt.format(
                admin_conv_id=ADMIN_CONV_ID,
                current_client_time=current_client_time_str,
                conv_id=conv_id,
                client_timezone=client_timezone,
                client_timezone_offset=client_timezone_offset
            )
            
            # Логируем полный промпт для диагностики
            logging.info(f"=== ПОЛНЫЙ ПРОМПТ ДЛЯ АНАЛИЗА ДИАЛОГА {conv_id} ===")
            logging.info(prompt)
            logging.info("=== КОНЕЦ ПРОМПТА ===")
            
            # Вызываем AI для анализа
            result = call_gemini_api(model, prompt, expect_json=True)
            
            # ===== ДОБАВЛЯЕМ ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ =====
            logging.info(f"=== ПОЛНЫЙ ОТВЕТ AI ДЛЯ ДИАЛОГА {conv_id} ===")
            logging.info(f"Сырой ответ AI: {json.dumps(result, ensure_ascii=False, indent=2)}")
            logging.info("=== КОНЕЦ ОТВЕТА AI ===")
            
            # Обрабатываем результат, который может быть списком или словарем
            reminders_to_process = []
            if isinstance(result, list):
                reminders_to_process = result
            elif isinstance(result, dict):
                reminders_to_process = result.get('reminders', [])

            logging.info(f"АНАЛИЗ ДИАЛОГА {conv_id}: AI вернул {len(reminders_to_process)} потенциальных напоминаний")

            if reminders_to_process:
                processed_reminders = []
                
                # ===== ДОБАВЛЯЕМ ПРОВЕРКУ НА ДУБЛИКАТЫ ВНУТРИ ОДНОГО ОТВЕТА =====
                unique_reminders = {}  # ключ: (target_conv_id, summary_hash), значение: reminder_data
                
                for i, reminder_data in enumerate(reminders_to_process):
                    logging.info(f"АНАЛИЗ ДИАЛОГА {conv_id}: Обрабатываю напоминание {i+1}: {reminder_data}")
                    
                    if reminder_data.get('action') != 'none':
                        # Проверяем валидность target_conv_id
                        target_conv_id = reminder_data.get('target_conv_id')
                        if not target_conv_id:
                            logging.warning(f"ОТКЛОНЕНО НАПОМИНАНИЕ {conv_id}: Отсутствует target_conv_id: {reminder_data}")
                            continue
                        
                        # Создаем ключ для проверки дубликатов внутри одного ответа AI
                        summary = reminder_data.get('reminder_context_summary', '').lower().strip()
                        summary_hash = hash(summary)
                        duplicate_key = (target_conv_id, summary_hash)
                        
                        if duplicate_key in unique_reminders:
                            logging.warning(f"ДУБЛИКАТ В ОТВЕТЕ AI {conv_id}: Обнаружен дубликат напоминания для target_conv_id={target_conv_id}, summary='{summary}'. Пропускаю.")
                            continue
                        
                        unique_reminders[duplicate_key] = reminder_data
                        
                        # Проверяем, не создаем ли мы дублирующее напоминание с существующими
                        if reminder_data.get('action') == 'create' and active_reminders:
                            new_summary = summary
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
                        none_reason = reminder_data.get('none_reason', 'причина не указана')
                        logging.info(f"ПРОПУЩЕНО НАПОМИНАНИЕ {conv_id}: action='none' - {none_reason}")

                if processed_reminders:
                    logging.info(f"ИТОГ АНАЛИЗА {conv_id}: Принято {len(processed_reminders)} из {len(reminders_to_process)} напоминаний после проверки дубликатов")
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
            
            # ===== КРИТИЧЕСКАЯ ЗАЩИТА ОТ ПУТАНИЦЫ КОНТЕКСТОВ =====
            if action == 'create':
                # 1. Проверяем валидность target_conv_id
                if not target_conv_id or target_conv_id == 0:
                    logging.error(f"ОТКЛОНЕНО СОЗДАНИЕ НАПОМИНАНИЯ {conv_id}: Некорректный target_conv_id={target_conv_id}")
                    return
                
                # 2. ЗАЩИТА ОТ ПУТАНИЦЫ: Разрешаем создавать напоминания только для того же пользователя или администратора
                if target_conv_id != conv_id and target_conv_id != ADMIN_CONV_ID:
                    logging.error(f"БЛОКИРОВКА ПУТАНИЦЫ КОНТЕКСТОВ: Пользователь conv_id={conv_id} пытается создать напоминание для target_conv_id={target_conv_id}. Это запрещено для безопасности.")
                    return
                
                # 3. Если администратор создаёт напоминание не для себя, проверяем explicit разрешение
                if conv_id == ADMIN_CONV_ID and target_conv_id != ADMIN_CONV_ID:
                    # Проверяем, что в описании есть явное указание conv_id клиента
                    reminder_summary = reminder_data.get('reminder_context_summary', '').lower()
                    if f"conv_id: {target_conv_id}" not in reminder_summary and f"conv_id:{target_conv_id}" not in reminder_summary:
                        logging.error(f"БЛОКИРОВКА АДМИНИСТРАТОРА: Admin conv_id={conv_id} пытается создать напоминание для target_conv_id={target_conv_id}, но в описании нет явного 'conv_id: {target_conv_id}'. Блокируем для безопасности.")
                        return
                
                # Логируем детали создания
                logging.info(f"СОЗДАНИЕ НАПОМИНАНИЯ: conv_id={conv_id}, target_conv_id={target_conv_id}, created_by={created_by_conv_id}")
                logging.info(f"ДЕТАЛИ НАПОМИНАНИЯ: {reminder_data}")
                
                # ===== УСИЛЕННАЯ ДЕДУПЛИКАЦИЯ НАПОМИНАНИЙ =====
                summary = reminder_data.get('reminder_context_summary', '').strip()
                if summary:
                    # Проверяем на точные дубликаты по времени и описанию
                    proposed_time = parse_datetime_with_timezone(
                        reminder_data['proposed_datetime'],
                        reminder_data.get('client_timezone', 'Europe/Moscow')
                    )
                    
                    cur.execute("""
                        SELECT id, reminder_context_summary, reminder_datetime
                        FROM reminders 
                        WHERE conv_id = %s AND status = 'active'
                        AND ABS(EXTRACT(EPOCH FROM (reminder_datetime - %s))) < 3600
                        ORDER BY created_at DESC
                        LIMIT 5
                    """, (target_conv_id, proposed_time))
                    
                    similar_by_time = cur.fetchall()
                    if similar_by_time:
                        for existing in similar_by_time:
                            existing_summary = existing[1].lower().strip()
                            new_summary = summary.lower().strip()
                            
                            # Проверяем схожесть по ключевым словам
                            new_words = set(new_summary.split())
                            existing_words = set(existing_summary.split())
                            intersection = new_words & existing_words
                            
                            if new_words and existing_words:
                                similarity = len(intersection) / len(new_words | existing_words)  # Жаккар индекс
                                logging.info(f"ДЕДУПЛИКАЦИЯ: Сравнение '{new_summary}' vs '{existing_summary}' - схожесть {similarity:.2f}")
                                
                                # Более строгая проверка: блокируем при схожести > 60%
                                if similarity > 0.6:
                                    logging.warning(f"ДУБЛИРУЮЩЕЕ НАПОМИНАНИЕ ЗАБЛОКИРОВАНО: '{new_summary}' слишком похоже на существующее '{existing_summary}' (схожесть {similarity:.2f})")
                                    return
                    # ВРЕМЕННО ОТКЛЮЧЕНО ИЗ-ЗА ОШИБКИ ОТСУТСТВИЯ РАСШИРЕНИЯ pg_trgm
                    # cur.execute("""
                    #     SELECT id, reminder_context_summary, reminder_datetime
                    #     FROM reminders 
                    #     WHERE conv_id = %s AND status = 'active'
                    #     AND similarity(reminder_context_summary, %s) > 0.6
                    #     ORDER BY created_at DESC
                    #     LIMIT 3
                    # """, (target_conv_id, summary))
                    
                    # similar_reminders = cur.fetchall()
                    # if similar_reminders:
                    #     logging.warning(f"ПРЕДУПРЕЖДЕНИЕ {conv_id}: Найдены похожие активные напоминания для target_conv_id={target_conv_id}:")
                    #     for similar in similar_reminders:
                    #         logging.warning(f"  - ID={similar[0]}: '{similar[1]}' на {similar[2]}")
                        
                    # Можно добавить более строгую проверку, если нужно
                    # return  # Раскомментировать для блокировки создания похожих напоминаний
                    pass # Установлена заглушка после комментирования
                
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
                logging.info(f"✅ СОЗДАНО НАПОМИНАНИЕ ID={reminder_id} для target_conv_id={target_conv_id} (создал conv_id={created_by_conv_id or conv_id})")
                
            elif action == 'cancel':
                # ===== ЗАЩИТА ОТ НЕПРАВОМЕРНОЙ ОТМЕНЫ =====
                # Проверяем права на отмену напоминаний
                if target_conv_id != conv_id and target_conv_id != ADMIN_CONV_ID:
                    logging.error(f"БЛОКИРОВКА ОТМЕНЫ: Пользователь conv_id={conv_id} пытается отменить напоминания для target_conv_id={target_conv_id}. Это запрещено.")
                    return
                
                # Если администратор отменяет чужие напоминания, требуем explicit разрешение
                if conv_id == ADMIN_CONV_ID and target_conv_id != ADMIN_CONV_ID:
                    cancellation_reason = reminder_data.get('cancellation_reason', '')
                    if f"conv_id: {target_conv_id}" not in cancellation_reason and f"conv_id:{target_conv_id}" not in cancellation_reason:
                        logging.error(f"БЛОКИРОВКА ОТМЕНЫ АДМИНОМ: Admin conv_id={conv_id} пытается отменить напоминания для target_conv_id={target_conv_id}, но в причине нет явного 'conv_id: {target_conv_id}'.")
                        return
                
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
                if affected > 0:
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

def process_reminder_batch(reminders):
    """
    Обрабатывает пачку напоминаний для одного пользователя.
    Активирует все напоминания без дополнительной ИИ-проверки.
    """
    if not reminders:
        return

    conv_id = reminders[0]['conv_id']
    logging.info(f"Обработка пачки из {len(reminders)} напоминаний для conv_id={conv_id}")

    # Формируем контексты для всех напоминаний
    activated_contexts = []
    activated_ids = []

    for reminder in reminders:
        activated_contexts.append(reminder['reminder_context_summary'])
        activated_ids.append(reminder['id'])
        logging.info(f"Готово к активации: ID={reminder['id']}, context={reminder['reminder_context_summary']}")

    # Активируем напоминания асинхронно
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
        
        # === УМНАЯ ДЕДУПЛИКАЦИЯ: Отменяем все похожие напоминания ===
        additional_cancelled_ids = _cancel_similar_reminders(conn, conv_id, activated_contexts)
        if additional_cancelled_ids:
            activated_ids.extend(additional_cancelled_ids)
            logging.info(f"ДЕДУПЛИКАЦИЯ ПРИ АКТИВАЦИИ: Дополнительно отменено {len(additional_cancelled_ids)} похожих напоминаний для conv_id={conv_id}")
        
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
            
            # Очищаем счетчики ошибок при успешной активации
            for reminder_id in activated_ids:
                clear_activation_success(reminder_id)
            
        # ВАЖНО: После успешной активации очищаем все просроченные напоминания
        logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: Запускаю очистку просроченных напоминаний...")
        cleanup_expired_reminders()
            
    except requests.exceptions.Timeout as e:
        error_message = f"Таймаут HTTP запроса: {e}"
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: ТАЙМАУТ. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}")
        
        # Отслеживаем ошибки для каждого напоминания
        for reminder_id in activated_ids:
            track_activation_failure(reminder_id, error_message)
            
        _revert_reminder_statuses(activated_ids, "timeout")
        
    except requests.exceptions.RequestException as e:
        error_message = f"Ошибка HTTP запроса: {e}"
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: ОШИБКА HTTP. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}")
        
        # Отслеживаем ошибки для каждого напоминания
        for reminder_id in activated_ids:
            track_activation_failure(reminder_id, error_message)
            
        _revert_reminder_statuses(activated_ids, f"http_error: {e}")
        
    except Exception as e:
        error_message = f"Критическая ошибка асинхронной активации: {e}"
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ для conv_id={conv_id}: КРИТИЧЕСКАЯ ОШИБКА. Напоминания {activated_ids} будут возвращены в 'active'. Ошибка: {e}", exc_info=True)
        
        # Отслеживаем ошибки для каждого напоминания
        for reminder_id in activated_ids:
            track_activation_failure(reminder_id, error_message)
            
        _revert_reminder_statuses(activated_ids, f"critical_error: {e}")
        
    finally:
        if conn:
            conn.close()

def _cancel_similar_reminders(conn, conv_id, activated_contexts):
    """
    Находит и отменяет похожие активные напоминания для предотвращения дублирования.
    Возвращает список ID отмененных напоминаний.
    """
    cancelled_ids = []
    try:
        # Получаем все активные напоминания для этого пользователя
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, reminder_context_summary, reminder_datetime
                FROM reminders 
                WHERE conv_id = %s AND status = 'active'
                ORDER BY reminder_datetime
            """, (conv_id,))
            
            active_reminders = cur.fetchall()
            
            # Для каждого активированного контекста ищем похожие
            for activated_context in activated_contexts:
                activated_words = set(activated_context.lower().split())
                
                for reminder in active_reminders:
                    if reminder['id'] in cancelled_ids:
                        continue  # Уже отменено
                    
                    reminder_words = set(reminder['reminder_context_summary'].lower().split())
                    
                    # Проверяем схожесть (используем тот же алгоритм, что и при создании)
                    if activated_words and reminder_words:
                        similarity = len(activated_words & reminder_words) / len(activated_words | reminder_words)
                        
                        if similarity > 0.6:  # Если схожесть > 60%
                            # Отменяем похожее напоминание
                            cur.execute("""
                                UPDATE reminders 
                                SET status = 'cancelled_by_deduplication', 
                                    cancellation_reason = %s
                                WHERE id = %s
                            """, (
                                f"Автоотмена похожего напоминания при активации. Схожесть: {similarity:.2f}",
                                reminder['id']
                            ))
                            cancelled_ids.append(reminder['id'])
                            logging.info(f"ДЕДУПЛИКАЦИЯ: Отменено похожее напоминание ID={reminder['id']} (схожесть {similarity:.2f})")
            
            conn.commit()
            
    except Exception as e:
        logging.error(f"Ошибка при поиске похожих напоминаний для conv_id={conv_id}: {e}")
        conn.rollback()
    
    return cancelled_ids

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

def check_and_activate_reminders():
    """
    Проверяет и активирует созревшие напоминания, группируя их по пользователям.
    Активирует все напоминания без дополнительной ИИ-проверки.
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
                        args = (user_reminders[0],)
                    else:
                        target_func = process_reminder_batch
                        args = (user_reminders,)
                    
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

def process_single_reminder(reminder):
    """
    Обрабатывает одно напоминание - активирует его напрямую без ИИ-проверки.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        logging.info(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: Начинаю процесс активации без ИИ-проверки.")
        
        with conn.cursor() as cur:
            # Активируем напоминание напрямую
            try:
                # Формируем внутренний URL для вызова внутри того же контейнера
                port = os.environ.get("PORT", 8080)
                activate_url = f"http://127.0.0.1:{port}/activate_reminder"
                
                payload = {
                    "conv_id": reminder['conv_id'],
                    "reminder_context_summary": reminder['reminder_context_summary']
                }
                logging.info(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: Отправляю запрос на {activate_url}")
                
                response = requests.post(activate_url, json=payload, timeout=60)
                response.raise_for_status()
                
                response_text = response.text
                logging.info(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: Получен ответ. Статус: {response.status_code}, Тело: '{response_text}'")
                
                # Помечаем как выполненное
                logging.info(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: Обновляю статус в БД на 'done'.")
                cur.execute(
                    "UPDATE reminders SET status = 'done' WHERE id = %s",
                    (reminder['id'],)
                )
                
                # Очищаем счетчик ошибок при успешной активации
                clear_activation_success(reminder['id'])

            except requests.exceptions.RequestException as e:
                error_message = f"Ошибка HTTP запроса: {e}"
                logging.error(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: ОШИБКА. Не удалось вызвать эндпоинт активации. Напоминание НЕ будет активировано в этот раз. Ошибка: {e}")
                
                # Отслеживаем неудачную попытку активации
                track_activation_failure(reminder['id'], error_message)
                
                # При ошибке напоминание останется в статусе 'in_progress', 
                # и блок except выше вернет его в 'active'
                raise  # Передаем исключение выше, чтобы сработал rollback и возврат статуса

            conn.commit()
            logging.info(f"ID={reminder['id']}: Транзакция успешно завершена.")
            
            # ВАЖНО: После успешной активации очищаем все просроченные напоминания  
            logging.info(f"ПРЯМАЯ АКТИВАЦИЯ ID={reminder['id']}: Запускаю очистку просроченных напоминаний...")
            cleanup_expired_reminders()
            
    except Exception as e:
        error_message = f"Критическая ошибка: {e}"
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА ID={reminder['id']}: Произошла непредвиденная ошибка. Откатываю транзакцию. Статус будет возвращен в 'active'. Ошибка: {e}", exc_info=True)
        
        # Отслеживаем неудачную попытку активации
        track_activation_failure(reminder['id'], error_message)
        
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

def cleanup_expired_reminders():
    """
    Очищает просроченные напоминания - переводит из статуса 'active' в 'done'
    все напоминания, время которых уже прошло.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        with conn.cursor() as cur:
            # Находим все активные напоминания, время которых уже прошло
            cur.execute("""
                UPDATE reminders 
                SET status = 'done', 
                    cancellation_reason = 'Автоматически завершено как просроченное'
                WHERE status = 'active' AND reminder_datetime <= NOW()
                RETURNING id, conv_id, reminder_datetime, reminder_context_summary
            """)
            
            updated_reminders = cur.fetchall()
            conn.commit()
            
            if updated_reminders:
                logging.info(f"ОЧИСТКА ПРОСРОЧЕННЫХ: Переведено в статус 'done' {len(updated_reminders)} просроченных напоминаний:")
                for reminder in updated_reminders:
                    logging.info(f"  - ID={reminder[0]}, conv_id={reminder[1]}, время={reminder[2]}, описание='{reminder[3]}'")
            else:
                logging.debug("ОЧИСТКА ПРОСРОЧЕННЫХ: Просроченных активных напоминаний не найдено")
                
    except Exception as e:
        logging.error(f"Ошибка при очистке просроченных напоминаний: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Функция collect_client_context удалена, так как больше не нужна без ИИ-проверки актуальности

# --- ПЛАНИРОВЩИК ---

scheduler = None

def start_scheduler():
    """Запускает планировщик для периодической проверки напоминаний."""
    global scheduler
    
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # Добавляем задачу проверки напоминаний
    scheduler.add_job(
        func=check_and_activate_reminders,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        id='check_reminders',
        replace_existing=True
    )
    
    scheduler.start()
    logging.info(f"Планировщик запущен. Проверка напоминаний каждые {CHECK_INTERVAL_MINUTES} минут.")

    # Немедленный первый запуск проверки в отдельном потоке
    logging.info("Запускаем немедленную проверку напоминаний при старте сервиса...")
    initial_check_thread = threading.Thread(target=check_and_activate_reminders, daemon=True)
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
    # === ЗАЩИТА ОТ КОНКУРЕНТНЫХ ВЫЗОВОВ ===
    if conv_id in reminder_creation_locks:
        logging.info(f"БЛОКИРОВКА КОНКУРЕНТНОГО ВЫЗОВА: Обработка напоминаний для conv_id={conv_id} уже выполняется. Пропускаем.")
        return
    
    # Устанавливаем блокировку
    reminder_creation_locks[conv_id] = True
    
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
        # Снимаем блокировку
        if conv_id in reminder_creation_locks:
            del reminder_creation_locks[conv_id]
            logging.debug(f"СНЯТИЕ БЛОКИРОВКИ: Обработка напоминаний для conv_id={conv_id} завершена.")

# --- ИНИЦИАЛИЗАЦИЯ ПРИ ИМПОРТЕ ---

def initialize_reminder_service():
    """Инициализирует сервис напоминаний."""
    setup_logging()
    
    try:
        # Логируем используемый порт для диагностики
        port = os.environ.get("PORT", 8080)
        logging.info(f"Сервис напоминаний будет использовать порт {port} для внутренних запросов активации.")
        
        logging.info("Сервис напоминаний инициализирован. Работает без ИИ-проверки актуальности.")
        
        # Запускаем планировщик
        start_scheduler()
        
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