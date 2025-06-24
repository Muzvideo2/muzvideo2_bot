#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =======================================================================================
#               СЕРВИС СБОРКИ КОНТЕКСТА КЛИЕНТА (CONTEXT BUILDER) - v2.0
# =======================================================================================
#
# ВЕРСИЯ 2.0:
# - Принимает СЫРОЙ JSON от main.py.
# - Делает запрос к VK API для получения полной информации о пользователе.
# - Создает или ОБНОВЛЯЕТ карточку клиента в таблице user_profiles.
# - Выполняет остальные функции по сбору контекста из БД.
#
# =======================================================================================

import sys
import os
import json
import re
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timezone

# --- НАСТРОЙКИ ---
DATABASE_URL = os.environ.get("DATABASE_URL")
VK_COMMUNITY_TOKEN = os.environ.get("VK_COMMUNITY_TOKEN", "") # !!! НУЖНО ДОБАВИТЬ ЭТУ ПЕРЕМЕННУЮ ОКРУЖЕНИЯ
VK_API_VERSION = "5.131"

# Таблицы, которые нужно исключить из автоматического поиска
EXCLUDED_TABLES = ['operator_activity']
# Лимит на количество последних сообщений для истории диалога
DIALOGUES_LIMIT = 30
# Регулярное выражение для поиска email
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_db_connection():
    """Устанавливает соединение с БД."""
    if not DATABASE_URL:
        raise ConnectionError("Переменная окружения DATABASE_URL не установлена!")
    return psycopg2.connect(DATABASE_URL)

def default_serializer(obj):
    """Сериализатор для JSON, обрабатывающий datetime."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Тип {type(obj)} не сериализуется в JSON")

# --- НОВЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С VK API ---

def fetch_and_update_vk_profile(conn, conv_id):
    """
    Получает данные из VK API и обновляет/создает профиль пользователя в БД.
    """
    if not VK_COMMUNITY_TOKEN:
        print("WARNING: VK_COMMUNITY_TOKEN не установлен. Пропуск обновления профиля из VK.", file=sys.stderr)
        return

    # Список полей, которые мы хотим получить из VK API
    fields_to_request = "first_name,last_name,screen_name,sex,city,bdate"
    
    params = {
        'user_ids': conv_id,
        'fields': fields_to_request,
        'access_token': VK_COMMUNITY_TOKEN,
        'v': VK_API_VERSION
    }
    
    try:
        response = requests.get("https://api.vk.com/method/users.get", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'error' in data or not data.get('response'):
            print(f"ERROR: Ошибка VK API: {data.get('error', 'Нет ответа')}", file=sys.stderr)
            return

        user_data = data['response'][0]

        # Подготовка данных для записи в БД
        profile = {
            'conv_id': user_data.get('id'),
            'first_name': user_data.get('first_name'),
            'last_name': user_data.get('last_name'),
            'screen_name': user_data.get('screen_name'),
            'sex': {1: 'Женский', 2: 'Мужской', 0: 'Не указан'}.get(user_data.get('sex')),
            'city': user_data.get('city', {}).get('title'),
            'birth_day': None,
            'birth_month': None,
            'last_updated': datetime.now(timezone.utc)
        }

        # Парсинг даты рождения
        if 'bdate' in user_data:
            bdate_parts = user_data['bdate'].split('.')
            if len(bdate_parts) >= 2:
                profile['birth_day'] = int(bdate_parts[0])
                profile['birth_month'] = int(bdate_parts[1])

        # Используем INSERT ... ON CONFLICT (UPSERT) для атомарного создания/обновления
        upsert_query = """
        INSERT INTO user_profiles (conv_id, first_name, last_name, screen_name, sex, city, birth_day, birth_month, last_updated)
        VALUES (%(conv_id)s, %(first_name)s, %(last_name)s, %(screen_name)s, %(sex)s, %(city)s, %(birth_day)s, %(birth_month)s, %(last_updated)s)
        ON CONFLICT (conv_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            screen_name = EXCLUDED.screen_name,
            sex = EXCLUDED.sex,
            city = EXCLUDED.city,
            birth_day = EXCLUDED.birth_day,
            birth_month = EXCLUDED.birth_month,
            last_updated = EXCLUDED.last_updated;
        """
        
        with conn.cursor() as cur:
            cur.execute(upsert_query, profile)
            conn.commit()
            print(f"INFO: Профиль для conv_id {conv_id} успешно создан/обновлен из VK API.", file=sys.stderr)

    except requests.RequestException as e:
        print(f"ERROR: Ошибка сети при запросе к VK API: {e}", file=sys.stderr)
    except (KeyError, IndexError) as e:
        print(f"ERROR: Ошибка при парсинге ответа от VK API: {e}", file=sys.stderr)
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR: Ошибка БД при обновлении профиля: {e}", file=sys.stderr)


def update_conv_id_by_email(conn, conv_id, text):
    """Ищет email в тексте и обновляет conv_id в таблице client_purchases."""
    emails = re.findall(EMAIL_REGEX, text)
    if not emails:
        return

    email_to_update = emails[0].lower()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE client_purchases
                SET conv_id = %s
                WHERE lower(email) = %s AND conv_id IS NULL;
                """,
                (conv_id, email_to_update)
            )
            updated_rows = cur.rowcount
            if updated_rows > 0:
                print(f"INFO: Связано {updated_rows} покупок с conv_id {conv_id} по email {email_to_update}", file=sys.stderr)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR: Ошибка при обновлении conv_id по email: {e}", file=sys.stderr)


def find_user_data_tables(conn):
    """Динамически находит все таблицы в схеме 'public' с колонкой 'conv_id'."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.columns
                WHERE column_name = 'conv_id' AND table_schema = 'public';
            """)
            tables = [row[0] for row in cur.fetchall() if row[0] not in EXCLUDED_TABLES]
            return tables
    except psycopg2.Error as e:
        print(f"ERROR: Не удалось получить список таблиц из БД: {e}", file=sys.stderr)
        return []

def fetch_data_from_table(conn, table_name, conv_id):
    """Извлекает все строки для данного conv_id из указанной таблицы."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if table_name == 'dialogues':
                query = "SELECT * FROM dialogues WHERE conv_id = %s ORDER BY created_at DESC LIMIT %s;"
                cur.execute(query, (conv_id, DIALOGUES_LIMIT))
            else:
                query = f"SELECT * FROM {psycopg2.extensions.AsIs(table_name)} WHERE conv_id = %s;"
                cur.execute(query, (conv_id,))

            rows = [dict(row) for row in cur.fetchall()]
            return rows
    except psycopg2.Error as e:
        print(f"ERROR: Ошибка при извлечении данных из таблицы '{table_name}': {e}", file=sys.stderr)
        return []

# --- ФУНКЦИИ ФОРМАТИРОВАНИЯ (без изменений) ---

def format_user_profile(rows):
    if not rows: return ""
    profile = rows[0]
    lines = [f"--- КАРТОЧКА КЛИЕНТА ---"]
    if profile.get('first_name') or profile.get('last_name'):
        lines.append(f"Имя: {profile.get('first_name', '')} {profile.get('last_name', '')}".strip())
    # Добавлены новые поля из VK
    if profile.get('screen_name'):
        lines.append(f"Профиль VK: https://vk.com/{profile['screen_name']}")
    if profile.get('city'):
        lines.append(f"Город: {profile['city']}")
    if profile.get('birth_day') and profile.get('birth_month'):
        lines.append(f"Дата рождения: {profile['birth_day']}.{profile['birth_month']}")
    if profile.get('lead_qualification'):
        lines.append(f"Квалификация лида: {profile['lead_qualification']}")
    if profile.get('funnel_stage'):
        lines.append(f"Этап воронки: {profile['funnel_stage']}")
    if profile.get('client_level'):
        lines.append(f"Уровень клиента: {', '.join(profile['client_level'])}")
    if profile.get('learning_goals'):
        lines.append(f"Цели обучения: {', '.join(profile['learning_goals'])}")
    if profile.get('client_pains'):
        lines.append(f"Боли клиента: {', '.join(profile['client_pains'])}")
    if profile.get('dialogue_summary'):
        lines.append(f"\nКраткое саммари диалога:\n{profile['dialogue_summary']}")
    return "\n".join(lines)

def format_client_purchases(rows):
    if not rows: return ""
    lines = ["--- ПОДТВЕРЖДЕННЫЕ ПОКУПКИ (из платежной системы) ---"]
    for row in rows:
        purchase_date = row.get('purchase_date').strftime('%Y-%m-%d') if row.get('purchase_date') else 'неизвестно'
        lines.append(f"- Продукт: {row.get('product_name')}, Дата: {purchase_date}")
    return "\n".join(lines)

def format_purchased_products(rows):
    if not rows: return ""
    lines = ["--- УПОМЯНУТЫЕ ПОКУПКИ (со слов клиента) ---"]
    for row in rows:
        lines.append(f"- {row.get('product_name')}")
    return "\n".join(lines)

def format_dialogues(rows):
    if not rows: return ""
    lines = [f"--- ПОСЛЕДНЯЯ ИСТОРИЯ ДИАЛОГА (до {DIALOGUES_LIMIT} сообщений) ---"]
    for row in reversed(rows):
        role_map = {'user': 'Пользователь', 'bot': 'Модель', 'operator': 'Оператор'}
        role = role_map.get(row.get('role', 'unknown'), 'Неизвестно')
        message_text = row.get('message', '')
        clean_message = re.sub(r'^\[\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\]\s*', '', message_text)
        lines.append(f"{role}: {clean_message}")
    return "\n".join(lines)

def format_generic(rows, table_name):
    if not rows: return ""
    lines = [f'--- ДАННЫЕ ИЗ ТАБЛИЦЫ "{table_name}" ---']
    for i, row in enumerate(rows):
        row_str = json.dumps(row, ensure_ascii=False, indent=None, default=default_serializer)
        lines.append(f"- Запись {i+1}: {row_str}")
    return "\n".join(lines)

# --- ОСНОВНОЙ ПРОЦЕСС ---

def main():
    """Главная функция-оркестратор."""
    try:
        # 1. Чтение и парсинг входных данных
        input_json = sys.stdin.read()
        if not input_json:
            raise ValueError("Входные данные (stdin) пусты.")
        
        # Теперь парсим сырой JSON от VK
        data = json.loads(input_json)
        
        # Извлекаем ID пользователя (from_id) из новой структуры
        message_data = data.get("object", {}).get("message", {})
        conv_id = message_data.get("from_id")
        message_text = message_data.get("text", "")

        if not conv_id:
            # Проверяем старый формат на всякий случай
            conv_id = data.get("conv_id")
            if not conv_id:
                 raise ValueError("Не найден 'from_id' или 'conv_id' во входных данных.")

        output_blocks = []

        # 2. Работа с базой данных
        with get_db_connection() as conn:
            # === ШАГ 1: ОБНОВИТЬ ПРОФИЛЬ ИЗ VK API ===
            fetch_and_update_vk_profile(conn, conv_id)
            
            # === ШАГ 2: Связать покупки по email (side-effect) ===
            update_conv_id_by_email(conn, conv_id, message_text)

            # === ШАГ 3: Собрать все данные для контекста ===
            tables_to_scan = find_user_data_tables(conn)

            preferred_order = ['user_profiles', 'client_purchases', 'purchased_products', 'dialogues']
            ordered_tables = [t for t in preferred_order if t in tables_to_scan]
            ordered_tables.extend([t for t in tables_to_scan if t not in preferred_order])

            formatters = {
                'user_profiles': format_user_profile,
                'client_purchases': format_client_purchases,
                'purchased_products': format_purchased_products,
                'dialogues': format_dialogues
            }

            for table in ordered_tables:
                rows = fetch_data_from_table(conn, table, conv_id)
                if rows:
                    formatter_func = formatters.get(table, format_generic)
                    if formatter_func == format_generic:
                        formatted_block = formatter_func(rows, table)
                    else:
                        formatted_block = formatter_func(rows)

                    if formatted_block:
                        output_blocks.append(formatted_block)

        # 3. Формирование и вывод итогового результата
        final_context = "\n\n".join(output_blocks)
        print(final_context)

    except Exception as e:
        print(f"FATAL ERROR in context_builder.py: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()