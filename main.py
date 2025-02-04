import os
import re
import time
import json
import requests
import psycopg2
from datetime import datetime, timedelta
import threading
import vk_api
from flask import Flask, request, jsonify
from urllib.parse import quote
import openpyxl
import logging

# ==============================
# Читаем переменные окружения (секретные данные)
# ==============================
TELEGRAM_TOKEN       = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID       = os.environ.get("ADMIN_CHAT_ID", "")
GEMINI_API_KEY       = os.environ.get("GEMINI_API_KEY", "")
VK_COMMUNITY_TOKEN = os.environ.get("VK_COMMUNITY_TOKEN", "")
YANDEX_DISK_TOKEN  = os.environ.get("YANDEX_DISK_TOKEN", "")
VK_SECRET_KEY       = os.environ.get("VK_SECRET_KEY", "")
VK_CONFIRMATION_TOKEN = os.environ.get("VK_CONFIRMATION_TOKEN", "")
print(VK_CONFIRMATION_TOKEN)
# Параметры PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")

# ID сообщества (нужно для формирования ссылки формата https://vk.com/gim<community_id>?sel=<user_id>)
# Например, если сообщество имеет адрес https://vk.com/club48116621, то его ID = 48116621
VK_COMMUNITY_ID = "48116621"

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================
# Пути к файлам
# ==============================
knowledge_base_path = "knowledge_base.json"
prompt_path          = "prompt.txt"
logs_directory      = "dialog_logs"

# ==============================
# Прочитаем базу знаний и промпт
# ==============================
if not os.path.exists(logs_directory):
    os.makedirs(logs_directory, exist_ok=True)

with open(knowledge_base_path, "r", encoding="utf-8") as f:
    knowledge_base = json.load(f)

with open(prompt_path, "r", encoding="utf-8") as f:
    custom_prompt = f.read().strip()

# ==============================
# Сервисные переменные
# ==============================
gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

# Для хранения истории диалогов (по user_id)
dialog_history_dict = {}

# Для хранения (user_id -> (first_name, last_name)) и (user_id -> путь к лог-файлу)
user_names = {}
user_log_files = {}

# Лог-файл по умолчанию (используем, пока не знаем имени пользователя)
log_file_path = os.path.join(
    logs_directory,
    f"dialog_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_unknown_user.txt"
)

# =========================================================================
# 1. ФУНКЦИЯ ПОИСКА ПОКУПОК КЛИЕНТОВ, ЕСЛИ В ЗАПРОСЕ ЕСТЬ ЕМЕЙЛ ИЛИ ТЕЛЕФОН
# =========================================================================

def get_client_info(user_question, user_id):
    """
    Анализирует текст user_question на предмет email или номера телефона.
    Если они найдены, ищет информацию о клиенте в Excel-файле "clients.xlsx".
    Возвращает строку со всеми найденными данными или сообщает, что ничего не найдено.
    Учитывает, что у клиента может быть несколько покупок (строк).
    """
    
    client_info = ""  # Очищаем перед каждым новым запросом
    
    # Рег. выражения для email и телефона
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    phone_regex = r"(?:\+7|7|8)?[\s\-]?\(?(\d{3})\)?[\s\-]?(\d{3})[\s\-]?(\d{2})[\s\-]?(\d{2})"

    # Ищем все email и телефоны в сообщении
    emails = re.findall(email_regex, user_question)
    phones = re.findall(phone_regex, user_question)

    logging.info(f"Пользователь {user_id}: Запрошен поиск в таблице.")

    # Открываем Excel
    try:
        workbook = openpyxl.load_workbook("clients.xlsx")
        sheet = workbook.active   # Или workbook["НазваниеЛиста"], если нужен конкретный лист
    except FileNotFoundError:
        logging.error("Файл clients.xlsx не найден.")
        return ""

    # --- Поиск по email ---
    for email in emails:
        email_lower = email.lower().strip()
        logging.info(f"Пользователь {user_id}: Ищем данные по e-mail {email_lower}.")
        
        # Сделаем заголовок (для наглядности, если несколько емейлов)
        client_info += f"\n=== Результат поиска по e-mail: {email_lower} ===\n"
        email_found = False

        for row_index, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            # row — это кортеж вида (A, B, C, D, E, F, ...)
            # Предположим, что E=4 (A=0,B=1,C=2,D=3,E=4,F=5).
            # Если колонка E - это email, берём row[4], проверяем:
            
            # Пустая ячейка будет None, поэтому:
            cell_value = (row[4] or "") if len(row) > 4 else ""
            cell_value_lower = str(cell_value).lower().strip()

            # Дополнительно можно вывести отладку, чтобы видеть, что читаем
            # logging.debug(f"Строка {row_index}: {row}")  
            # (закомментируйте или включите по необходимости)

            if cell_value_lower == email_lower:
                # Формируем строку со всеми данными, убирая None
                client_data = ", ".join(str(x) for x in row if x is not None)
                client_info += f"- {client_data}\n"
                email_found = True
        
        if not email_found:
            client_info += "  Ничего не найдено\n"
            logging.warning(f"Пользователь {user_id}: Не найдены данные по e-mail {email_lower}.")

    # --- Поиск по телефону ---
    for phone_tuple in phones:
        # phone_tuple — это кортеж из рег. выражения (три группы по 3,2,2 цифры)
        # Например, ("905", "787", "89", "61")
        digits_only = "".join(filter(str.isdigit, "".join(phone_tuple)))
        logging.info(f"Пользователь {user_id}: Ищем данные по телефону (цифры): {digits_only}.")

        client_info += f"\n=== Результат поиска по телефону: {digits_only} ===\n"
        phone_found = False

        for row_index, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            # Аналогично проверяем столбец F = row[5]
            phone_cell = (row[5] or "") if len(row) > 5 else ""
            phone_digits = "".join(filter(str.isdigit, str(phone_cell)))

            # Сравниваем последние 10 цифр
            if phone_digits.endswith(digits_only[-10:]):
                client_data = ", ".join(str(x) for x in row if x is not None)
                client_info += f"- {client_data}\n"
                phone_found = True
        
        if not phone_found:
            client_info += "  Ничего не найдено\n"
            logging.warning(f"Пользователь {user_id}: Не найдены данные по телефону {digits_only}.")

    # Если в тексте не было ни email, ни телефона, client_info останется пустым
    return client_info.strip()

# =======================================
# 2. ФУНКЦИЯ ЗАПРОСА ИМЕНИ КЛИЕНТА ВКОНТАКТЕ
# =======================================

def get_vk_user_full_name(user_id):
    """
    Получает имя и фамилию пользователя ВКонтакте по user_id через API.
    """
    if user_id in user_names:
        logging.info(f"Пользователь {user_id} уже есть в кеше, имя: {user_names[user_id]}")
        return user_names[user_id]

    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    try:
        response = vk.users.get(user_ids=user_id, fields="first_name,last_name")
        if response and isinstance(response, list) and len(response) > 0:
            user_data = response[0]
            if "deactivated" in user_data:
                logging.warning(f"Пользователь {user_id} удален или заблокирован.")
                return "", ""

            first_name = user_data.get("first_name", "")
            last_name = user_data.get("last_name", "")
            user_names[user_id] = (first_name, last_name)  # Кешируем имя и фамилию
            return first_name, last_name
    except vk_api.ApiError as e:
        logging.error(f"Ошибка VK API при получении имени пользователя {user_id}: {e}")
    except Exception as e:
        logging.error(f"Неизвестная ошибка при получении имени пользователя {user_id}: {e}")

    return "", ""  # Если API не отвечает или имя не найдено — возвращаем пустые строки


# ==============================
# 3. ФУНКЦИИ УВЕДОМЛЕНИЙ В ТЕЛЕГРАМ
# ==============================
def send_telegram_notification(user_question, dialog_id, first_name="", last_name=""):
    """
    Уведомление в телеграм при первом сообщении пользователя или при запросе "оператор".
    Диалог-ссылка вида https://vk.com/gim<community_id>?sel=<user_id>.
    """
    # Формируем ссылку на диалог внутри сообщества:
    vk_dialog_link = f"https://vk.com/gim{VK_COMMUNITY_ID}?sel={dialog_id}"

    # Убираем дату/время (Телеграм сам показывает) и добавляем имя/фамилию
    message = f"""
👤 Пользователь: {first_name} {last_name}
Стартовый вопрос: {user_question}
🔗 Ссылка на диалог: {vk_dialog_link}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": ADMIN_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)


def send_operator_notification(dialog_id, initial_question, dialog_summary, reason_guess, first_name="", last_name=""):
    """
    Уведомление, если пользователь запросил оператора в процессе диалога.
    """
    vk_dialog_link = f"https://vk.com/gim{VK_COMMUNITY_ID}?sel={dialog_id}"

    message = f"""
🆘 Запрос оператора!
👤 Пользователь: {first_name} {last_name}
Изначальный вопрос клиента: {initial_question}
Сводка обсуждения: {dialog_summary}
Предполагаемая причина: {reason_guess}
🔗 Ссылка на диалог: {vk_dialog_link}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": ADMIN_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)


# ==============================================
# 4. РАБОТА С ЯНДЕКС.ДИСКОМ: ЗАГРУЗКА ЛОГ-ФАЙЛОВ
# ==============================================

def upload_log_to_yandex_disk(log_file_path):
    # Проверяем, существует ли папка на Яндекс.Диске
    create_dir_url = "https://cloud-api.yandex.net/v1/disk/resources"
    headers = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"}
    params = {"path": "disk:/app-logs"}
    requests.put(create_dir_url, headers=headers, params=params)

    if not os.path.exists(log_file_path):
        return

    if not YANDEX_DISK_TOKEN:
        print("YANDEX_DISK_TOKEN не задан. Пропускаем загрузку логов.")
        return

    file_name = os.path.basename(log_file_path)
    ya_path = f"disk:/app-logs/{file_name}"

    get_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
    params = {
        "path": ya_path,
        "overwrite": "true"
    }
    headers = {
        "Authorization": f"OAuth {YANDEX_DISK_TOKEN}"
    }
    r = requests.get(get_url, headers=headers, params=params)
    if r.status_code != 200:
        print("Ошибка при получении URL для загрузки на Яндекс.Диск:", r.text)
        return

    href = r.json().get("href", "")
    if not href:
        print("Не нашли 'href' в ответе Яндекс.Диска:", r.text)
        return

    with open(log_file_path, "rb") as f:
        upload_resp = requests.put(href, files={"file": f})
        if upload_resp.status_code == 201:
            print(f"Лог-файл {file_name} успешно загружен на Яндекс.Диск.")
        else:
            print("Ошибка загрузки на Яндекс.Диск:", upload_resp.text)

# ================================================================================
# 5. ФУНКЦИЯ ЗАПИСИ СООБЩЕНИЯ ОТ ОПЕРАТОРА В JSON-файл и сохранения на Яндекс.Диск
# ================================================================================

# Создадим папку, где будут сохраняться дампы callback:
CALLBACK_LOGS_DIR = "callback_logs"
if not os.path.exists(CALLBACK_LOGS_DIR):
    os.makedirs(CALLBACK_LOGS_DIR, exist_ok=True)


def save_callback_payload(data):
    """
    Сохраняет весь JSON, полученный от ВКонтакте, в локальный файл
    и загружает этот файл на Яндекс.Диск.

    data: dict — полный JSON из request.json
    """
    # Генерируем имя файла вида callback_2025-02-16_13-59-59.json
    timestamp_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"callback_{timestamp_str}.json"
    file_path = os.path.join(CALLBACK_LOGS_DIR, file_name)

    # Записываем JSON в файл (в UTF-8, с отступами для удобства)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Загружаем файл на Яндекс.Диск (предполагается, что функция уже определена)
    upload_log_to_yandex_disk(file_path)

    print(f"Сохранён колбэк JSON: {file_name}")

# =================================
# 6. СОХРАНЕНИЕ ДИАЛОГОВ В POSTGRES
# =================================

def store_dialog_in_db(user_id, role, message, client_info=""):
    """
    Сохраняет одно сообщение (от пользователя, бота или оператора) в базу PostgreSQL.
    Поле role должно принимать значение "user", "bot" или "operator".
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Создание таблицы с новым полем role, если её нет
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dialogues (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                role TEXT,
                message TEXT,
                client_info TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Вставка записи
        cur.execute(
            """INSERT INTO dialogues (user_id, role, message, client_info)
               VALUES (%s, %s, %s, %s)""",
            (user_id, role, message, client_info)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при сохранении диалога в БД: {e}")


def load_dialog_from_db(user_id):
    """
    Подгружает из БД всю историю сообщений для указанного user_id.
    Каждая запись возвращается в виде словаря с ключами, соответствующими роли сообщения,
    и дополнительным полем "client_info" (если оно было сохранено).
    Пример: {"user": "текст", "client_info": "..."} или {"operator": "текст", "client_info": "..."}
    """
    dialog_history = []
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT role, message, client_info
            FROM dialogues
            WHERE user_id = %s
            ORDER BY id ASC
        """, (user_id,))
        rows = cur.fetchall()
        for row in rows:
            role, message, client_info = row
            dialog_history.append({role: message, "client_info": client_info})
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при загрузке диалога из БД для user_id={user_id}: {e}")
    return dialog_history



# ==============================
# 7. ЛОГИРОВАНИЕ
# ==============================
def log_dialog(user_question, bot_response, relevant_titles, relevant_answers, user_id, full_name="", client_info=""):
    """
    Логирует в локальный файл и сохраняет каждое сообщение в базу данных.
    Сообщения от пользователя и от бота сохраняются по отдельности.
    """
    # Сохраняем сообщение пользователя и ответа бота в БД
    store_dialog_in_db(user_id, "user", user_question, client_info)
    store_dialog_in_db(user_id, "bot", bot_response, client_info)

    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")

    # Определяем лог-файл для пользователя
    if user_id in user_log_files:
        local_log_file = user_log_files[user_id]
    else:
        local_log_file = log_file_path

    # Записываем данные в лог-файл
    with open(local_log_file, "a", encoding="utf-8") as log_file:
        log_file.write(f"[{formatted_time}] {full_name}: {user_question}\n")
        if relevant_titles and relevant_answers:
            for title, answer in zip(relevant_titles, relevant_answers):
                log_file.write(f"[{formatted_time}] Найдено в базе знаний: {title} -> {answer}\n")
        if client_info:  # Добавляем информацию по клиенту, если она есть
            log_file.write(f"[{formatted_time}] Информация по клиенту: {client_info}\n")
        log_file.write(f"[{formatted_time}] Модель: {bot_response}\n\n")

    print(f"Содержимое лога:\n{open(local_log_file, 'r', encoding='utf-8').read()}")

    # Загружаем лог-файл на Яндекс.Диск
    upload_log_to_yandex_disk(local_log_file)


# ==============================
# 8. ИНТЕГРАЦИЯ С GEMINI
# ==============================
def find_relevant_titles_with_gemini(user_question):
    titles = list(knowledge_base.keys())
    prompt_text = f"""
Вот список вопросов-ключей:
{', '.join(titles)}

Найди три наиболее релевантных вопроса к запросу: "{user_question}".
Верни только сами вопросы, без пояснений и изменений.
    """.strip()

    data = {
        "contents": [{"parts": [{"text": prompt_text}]}]
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(5):
        resp = requests.post(gemini_url, headers=headers, json=data)
        if resp.status_code == 200:
            try:
                result = resp.json()
                text_raw = result['candidates'][0]['content']['parts'][0]['text']
                lines = text_raw.strip().split("\n")
                return [ln.strip() for ln in lines if ln.strip()]
            except KeyError:
                return []
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return []
    return []


def generate_response(user_question, client_data, dialog_history, custom_prompt, first_name, relevant_answers=None, relevant_titles=None):
    """
    Генерирует ответ от модели (Gemini) с учётом истории (включая оператора), клиентской информации и базы знаний.
    Теперь каждый участник (Ольга / Сергей (оператор) / Модель) выводится в хронологическом порядке, без дублирования.
    """

    # 1. Формируем историю диалога, чтобы передать в prompt
    #    Здесь пользователь будет писаться по имени (если есть), оператор – "Сергей (оператор)", бот – "Модель".
    history_lines = []
    user_display_name = first_name if first_name else "Пользователь"  # Например, "Ольга"

    for turn in dialog_history:
        if "operator" in turn:
            # Сообщение оператора
            history_lines.append(f"Сергей (оператор): {turn['operator']}")
        elif "user" in turn:
            # Сообщение пользователя
            # Предположим, мы где-то записали имя. Сейчас возьмём user_display_name
            history_lines.append(f"{user_display_name}: {turn['user']}")
        elif "bot" in turn:
            # Ответ модели
            history_lines.append(f"Модель: {turn['bot']}")

    history_text = "\n\n".join(history_lines)

    # 2. Если последнее сообщение в истории содержит client_info, передаём его
    last_client_info = ""
    if dialog_history and "client_info" in dialog_history[-1]:
        last_client_info = dialog_history[-1]["client_info"] or ""

    # 3. Формируем "подсказки из базы знаний" (если есть)
    knowledge_hint = ""
    if relevant_answers:
        knowledge_hint = "Подсказки из базы знаний:\n" + "\n".join(relevant_answers)

    # 4. Собираем всё в единый prompt
    #    Уберём отдельную фразу "Обращайся к пользователю по имени...", чтобы не захламлять
    #    Просто используем в самом диалоге имя (Ольга) вместо слова "Пользователь"
    parts = []
    parts.append(custom_prompt)
    parts.append(f"Контекст диалога (все сообщения подряд):\n{history_text}")
    if last_client_info.strip():
        parts.append(f"Информация о клиенте:\n{last_client_info}")
    if knowledge_hint:
        parts.append(knowledge_hint)
    parts.append(f"Текущий запрос от {user_display_name}: {user_question}")
    if client_data.strip():
        parts.append(f"Результат поиска по таблице (email/телефон):\n{client_data}")
    parts.append("Модель:")

    full_prompt = "\n\n".join(parts)

    # 5. Сохраним полный промпт в отдельный файл
    now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    prompt_filename = f"prompt_{now_str}.txt"
    prompt_file_path = os.path.join(logs_directory, prompt_filename)
    try:
        with open(prompt_file_path, "w", encoding="utf-8") as pf:
            pf.write(full_prompt)
        upload_log_to_yandex_disk(prompt_file_path)
        logging.info(f"Полный промпт, отправленный в модель, сохранён: {prompt_filename}")
    except Exception as e:
        logging.error(f"Ошибка записи промпта: {e}")

    # 6. Логируем короткую версию (лишь последние несколько сообщений)
    short_history = history_lines[-4:] if len(history_lines) > 4 else history_lines
    short_history_text = "\n".join(short_history)
    short_knowledge = ", ".join(relevant_titles) if relevant_titles else "нет"

    logging.info(
        f"\nЗапрос к модели:\n"
        f"Промпт: (Сокращённая версия)\n"
        f"Последние 4 сообщения:\n{short_history_text}\n\n"
        f"Подсказки из базы знаний (ключи): {short_knowledge}\n"
        f"Текущий запрос: {user_question}\n"
        f"client_data: {client_data}\n"
    )

    # 7. Отправляем запрос в Gemini
    data = {
        "contents": [
            {
                "parts": [
                    {"text": full_prompt}
                ]
            }
        ]
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(5):
        resp = requests.post(gemini_url, headers=headers, json=data)
        if resp.status_code == 200:
            try:
                result = resp.json()
                return result['candidates'][0]['content']['parts'][0]['text']
            except KeyError:
                return "Извините, произошла ошибка при обработке ответа модели."
        elif resp.status_code == 503:
            return (
                "Ой! Извините, я сейчас перегружен. Как только смогу, обязательно отвечу "
                "или подключится оператор. Если срочно, напишите 'оператор'."
            )
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return f"Ошибка: {resp.status_code}. {resp.text}"

    return "Извините, я сейчас не могу ответить. Попробуйте чуть позже."





def generate_summary_and_reason(dialog_history):
    history_text = " | ".join([
        f"Пользователь: {turn.get('user', 'Неизвестно')} -> Модель: {turn.get('bot', 'Нет ответа')}"
        for turn in dialog_history[-10:]
    ])
    prompt_text = f"""
Сводка диалога: {history_text}

На основе предоставленного диалога:
1. Сформируй сводку обсуждения.
2. Предположи причину, почему пользователь запросил оператора.
    """.strip()
    data = {
        "contents": [{"parts": [{"text": prompt_text}]}]
    }
    headers = {"Content-Type": "application/json"}

    for attempt in range(5):
        resp = requests.post(gemini_url, headers=headers, json=data)
        if resp.status_code == 200:
            try:
                result = resp.json()
                output = result['candidates'][0]['content']['parts'][0]['text'].split("\n", 1)
                dialog_summary = output[0].strip() if len(output) > 0 else "Сводка не сформирована"
                reason_guess   = output[1].strip() if len(output) > 1 else "Причина не определена"
                return dialog_summary, reason_guess
            except (KeyError, IndexError):
                return "Сводка не сформирована", "Причина не определена"
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return "Ошибка API", "Ошибка API"
    return "Не удалось связаться с сервисом", "Не удалось связаться с сервисом"


# ==========================================
# 9. 60-секундная задержка и буфер сообщений
# ==========================================
user_buffers = {}
user_timers  = {}
last_questions = {}

DELAY_SECONDS = 60

# =====================================
# 10. ПАУЗА ДЛЯ КОНКРЕТНОГО ПОЛЬЗОВАТЕЛЯ
# =====================================

def is_user_paused(full_name):
    try:
        # Приводим имя к нижнему регистру (на всякий случай)
        full_name_lower = full_name.lower()
        response = requests.get(f"http://telegram-bot.railway.internal/is_paused/{quote(full_name_lower)}", timeout=5)
        if response.status_code == 200:
            paused_status = response.json().get("paused", False)
            print(f"Статус паузы для {full_name_lower}: {paused_status}") # Логируем имя в нижнем регистре
            return paused_status
        else:
            print(f"Ошибка API: {response.status_code}, {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Ошибка подключения к Telegram API: {e}")
        return False

# =====================================
# 11. ОБРАБОТКА ПОСТУПИВШЕГО СООБЩЕНИЯ
# =====================================

def handle_new_message(user_id, text, vk, is_outgoing=False):
    """
    Обрабатывает новое входящее или исходящее сообщение.
    - Если is_outgoing = True, проверяем, совпадает ли текст с последним bot-сообщением в dialog_history:
      * если совпадает — считаем это "бот" (уже записано), пропускаем;
      * иначе — считаем "оператор".
    - Если is_outgoing = False — пользователь.
    """

    text_stripped = text.strip()

    # 1. Если в памяти ещё нет истории, подгружаем из БД
    if user_id not in dialog_history_dict:
        existing_history = load_dialog_from_db(user_id)
        dialog_history_dict[user_id] = existing_history

    dialog_history = dialog_history_dict[user_id]

    # 2. Если это исходящее сообщение (т.е. от сообщества к пользователю)
    if is_outgoing:
        # Ищем последнее сообщение в диалоге с ключом "bot"
        last_bot_message = None
        for turn in reversed(dialog_history):
            if "bot" in turn:
                last_bot_message = (turn["bot"] or "").strip()
                break

        if last_bot_message and last_bot_message == text_stripped:
            # Совпадает с последним сообщением бота => это бот
            logging.info(f"[handle_new_message] Исходящее сообщение совпадает с последним bot: пропускаем.")
            return
        else:
            # Иначе считаем, что это оператор
            logging.info(f"[handle_new_message] Исходящее сообщение НЕ совпадает с bot => оператор.")
            dialog_history.append({"operator": text})
            store_dialog_in_db(user_id, "operator", text)

            # Локальное логирование
            current_time = datetime.utcnow() + timedelta(hours=6)
            fmt_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")

            if user_id not in user_log_files:
                f_name, l_name = user_names.get(user_id, ("unknown", ""))
                full_name = f"{f_name}_{l_name}".strip("_")
                now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
                fname = f"dialog_{now_str}_{full_name}.txt"
                user_log_files[user_id] = os.path.join(logs_directory, fname)

            operator_log = user_log_files[user_id]
            with open(operator_log, "a", encoding="utf-8") as f:
                f.write(f"[{fmt_time}] user_id={user_id}, Оператор: {text}\n\n")
            upload_log_to_yandex_disk(operator_log)
            return

    else:
        # ========== Входящее сообщение от пользователя ==========
        logging.info(f"[handle_new_message] Входящее сообщение от пользователя user_id={user_id}: {text}")

        # Запоминаем имя (если ещё нет)
        if user_id not in user_names:
            first_name, last_name = get_vk_user_full_name(user_id)
            user_names[user_id] = (first_name, last_name)

        f_name, l_name = user_names[user_id]
        full_name = f"{f_name}_{l_name}".strip("_")

        # Если совсем первое сообщение в истории
        if len(dialog_history) == 0:
            # Создаём лог-файл
            now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
            fname = f"dialog_{now_str}_{full_name}.txt"
            user_log_files[user_id] = os.path.join(logs_directory, fname)

            # Уведомляем в ТГ, если нет слова "оператор"
            if "оператор" not in text_stripped.lower():
                send_telegram_notification(text, user_id, f_name, l_name)
        else:
            # Если пользователь просит оператора
            if "оператор" in text_stripped.lower():
                summary, reason = generate_summary_and_reason(dialog_history)
                init_q = last_questions.get(user_id, "")
                send_operator_notification(
                    user_id, init_q, summary, reason,
                    first_name=f_name, last_name=l_name
                )

        # Проверяем паузу
        if is_user_paused(full_name):
            logging.info(f"[handle_new_message] Пользователь {full_name} на паузе, пропускаем.")
            return

        # Кладём в буфер
        user_buffers.setdefault(user_id, []).append(text)
        last_questions[user_id] = text

        # Сбрасываем/перезапускаем таймер
        if user_id in user_timers:
            user_timers[user_id].cancel()

        timer = threading.Timer(DELAY_SECONDS, generate_and_send_response, args=(user_id, vk))
        user_timers[user_id] = timer
        timer.start()


def generate_and_send_response(user_id, vk):
    """
    По истечении DELAY_SECONDS формируем единый текст (из user_buffers), генерируем ответ,
    добавляем обе реплики (user + bot) в БД/память, а затем отправляем сообщение через VK.
    """

    # Если в буфере нет сообщений - ничего не делаем
    msgs = user_buffers.get(user_id, [])
    if not msgs:
        return

    # Собираем их в единый текст
    combined_text = "\n".join(msgs).strip()
    user_buffers[user_id] = []  # очищаем буфер

    dialog_history = dialog_history_dict[user_id]

    # Определяем имя пользователя
    f_name, l_name = user_names.get(user_id, ("", ""))
    full_name = f"{f_name}_{l_name}".strip("_")

    # ========== 1. Сначала сохраняем сообщение пользователя как "user" ==========
    # (В БД и в локальном кеш)
    # - Если нужно, перед этим вы можете анализировать e-mail/телефон и т.п.
    #   (client_data = get_client_info(combined_text, user_id)) - если требуется
    client_data = ""

    store_dialog_in_db(user_id, "user", combined_text, client_data)
    dialog_history.append({"user": combined_text, "client_info": client_data})

    # ========== 2. Генерируем ответ модели (используя вашу функцию generate_response) ==========
    relevant_titles = find_relevant_titles_with_gemini(combined_text)
    relevant_answers = [knowledge_base[t] for t in relevant_titles if t in knowledge_base]

    model_response = generate_response(
        user_question=combined_text,
        client_data=client_data,
        dialog_history=dialog_history,
        custom_prompt=custom_prompt,
        first_name=f_name,
        relevant_answers=relevant_answers,
        relevant_titles=relevant_titles
    )

    # ========== 3. Теперь СРАЗУ добавляем ответ бота в БД и в кеш как "bot" ==========
    store_dialog_in_db(user_id, "bot", model_response, client_data)
    dialog_history.append({"bot": model_response, "client_info": client_data})

    # ========== 4. Локальное логирование (если нужно) ==========
    # (Можете использовать ваш log_dialog, но обязательно до vk.messages.send)
    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")

    if user_id not in user_log_files:
        now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f"dialog_{now_str}_{full_name}.txt"
        user_log_files[user_id] = os.path.join(logs_directory, file_name)

    local_log_path = user_log_files[user_id]
    with open(local_log_path, "a", encoding="utf-8") as log_file:
        # Логируем сообщение пользователя
        log_file.write(f"[{formatted_time}] {f_name}: {combined_text}\n")
        # ... если нужно, логируем find_relevant_titles_with_gemini
        # Логируем ответ модели
        log_file.write(f"[{formatted_time}] Модель: {model_response}\n\n")

    upload_log_to_yandex_disk(local_log_path)

    # ========== 5. Наконец отправляем сообщение через VK (с бот-ответом) ==========
    if vk:
        vk.messages.send(
            user_id=user_id,
            message=model_response,
            random_id=int(time.time() * 1000)
        )
    else:
        logging.warning("Объект vk не передан, не могу отправить сообщение.")



# ==============================
# 12. ОСНОВНОЙ ЦИКЛ
# ==============================

# Flask-приложение
app = Flask(__name__)

@app.route("/clear_context/<full_name>", methods=["POST"])
def clear_context(full_name):
    """
    Удаляет контекст пользователя из базы данных и локального кеша.
    """
    user_id = None

    # Найти user_id по имени и фамилии
    for uid, (first, last) in user_names.items():
        if f"{first}_{last}" == full_name:
            user_id = uid
            break

    if not user_id:
        return "Пользователь не найден", 404

    try:
        # Удаление из базы данных
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("DELETE FROM dialogues WHERE user_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()

        # Удаление из кеша
        dialog_history_dict.pop(user_id, None)
        user_buffers.pop(user_id, None)
        user_timers.pop(user_id, None)
        last_questions.pop(user_id, None)

        return "Контекст успешно очищен", 200
    except Exception as e:
        print(f"Ошибка при очистке контекста для {full_name}: {e}")
        return "Ошибка сервера", 500

@app.route("/callback", methods=["POST"])
def callback():
    # 1. Получаем сырые данные, сохраняем их при необходимости:
    data = request.json
    # Если у вас есть функция save_callback_payload(data), можете раскомментировать:
    save_callback_payload(data)

    # 2. Обрабатываем confirmation в первую очередь (если вы не отключили это в настройках ВК)
    if data.get("type") == "confirmation":
        return VK_CONFIRMATION_TOKEN

    # 3. Проверяем secret, если используется
    if VK_SECRET_KEY and data.get("secret") != VK_SECRET_KEY:
        return "Invalid secret", 403

    # 4. Смотрим на тип события (message_new, message_reply, message_edit и т.п.)
    event_type = data.get("type")
    if event_type not in ("message_new", "message_reply", "message_edit"):
        logging.info(f"Пропускаем событие type={event_type}")
        return "ok"

    # 5. Достаём ключ "object"
    vk_object = data.get("object", {})
    if not isinstance(vk_object, dict):
        logging.warning("Неправильный формат 'object' в колбэке.")
        return "ok"

    # 6. Пытаемся вытащить поля from_id, text и out
    msg = {}
    if "message" in vk_object:
        # Обычно при "message_new"
        inner = vk_object["message"]
        msg["from_id"] = inner.get("from_id")
        msg["text"] = inner.get("text", "")
        msg["out"] = inner.get("out", 0)
    else:
        # При "message_reply" часто нужные поля лежат прямо в object
        msg["from_id"] = vk_object.get("from_id")
        msg["text"] = vk_object.get("text", "")
        msg["out"] = vk_object.get("out", 0)

    if "admin_author_id" in vk_object:
        # Сообщение от администратора (оператор), но не пропускаем
        logging.info(f"admin_author_id={vk_object['admin_author_id']} => сообщение оператора (вероятно)")

    # 7. Если нет from_id или text, пропускаем
    if not msg.get("from_id") or "text" not in msg:
        logging.warning(f"Не удалось извлечь from_id/text из события {event_type}: {data}")
        return "ok"

    # 8. Определяем, это исходящее (out=1) или входящее
    is_outgoing = (msg["out"] == 1)
    user_id = msg["from_id"]
    text = msg["text"]

    # 9. Подключаемся к VK API (создаём vk здесь, 
    #    чтобы handle_new_message мог его использовать)
    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    # 10. Передаём в функцию handle_new_message
    handle_new_message(
        user_id=user_id,
        text=text,
        vk=vk,
        is_outgoing=is_outgoing
    )

    # 11. Возвращаем "ok"
    return "ok"


@app.route('/ping', methods=['GET'])
def ping():
    return "Pong!", 200

def process_message(user_id, text):
    """
    Логика обработки сообщений (пример, если бы нужно было).
    """
    send_message(user_id, f"Вы написали: {text}")


def send_message(user_id, message):
    """
    Отправляет сообщение через API ВКонтакте.
    """
    url = "https://api.vk.com/method/messages.send"
    params = {
        "access_token": VK_COMMUNITY_TOKEN,
        "user_id": user_id,
        "message": message,
        "random_id": 0,
        "v": "5.131"
    }
    requests.post(url, params=params)


if __name__ == "__main__":
    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)