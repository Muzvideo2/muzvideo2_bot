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

#Некоторые глобальные переменные:

operator_timers = {}  # Для хранения таймеров оператора по conv_id
client_timers = {}    # Для хранения таймеров клиента по conv_id
dialog_history_dict = {} # {conv_id: [...]}
user_names = {} # {(conv_id: (first_name, last_name))}
user_log_files = {} # {conv_id: "путь к лог-файлу"}
user_buffers = {}
user_timers = {}
last_questions = {}

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

# Лог-файл по умолчанию (используем, пока не знаем имени пользователя)
log_file_path = os.path.join(
    logs_directory,
    f"dialog_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_unknown_user.txt"
)

#============================================================================
# Функция, убирающая любую «хвостовую» пунктуацию или спецсимволы из ключа БЗ 
#============================================================================

def remove_trailing_punctuation(text: str) -> str:
    pattern = r'[^\w\s]+$'
    # Это значит: «одна или несколько НЕ-буквенных и НЕ-пробельных подряд в конце строки»
    return re.sub(pattern, '', text).strip()

#========================================================
# Функция, которая пытается найти ключ в knowledge_base 
#========================================================

# ключ, у которого "очищенная" версия совпадает с "очищенной" версией user_key.
def match_kb_key_ignoring_trailing_punc(user_key: str, knowledge_base: dict) -> str | None:
    user_clean = remove_trailing_punctuation(user_key)
    for kb_key in knowledge_base:
        kb_clean = remove_trailing_punctuation(kb_key)
        if kb_clean == user_clean:
            return kb_key  # Возвращаем реальный ключ из базы (как записан в JSON)
    return None


# =========================================================================
# 1. ФУНКЦИЯ ПОИСКА ПОКУПОК КЛИЕНТОВ, ЕСЛИ В ЗАПРОСЕ ЕСТЬ ЕМЕЙЛ ИЛИ ТЕЛЕФОН
# =========================================================================

def get_client_info(user_question, conv_id):
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

    logging.info(f"Пользователь {conv_id}: Запрошен поиск в таблице.")

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
        logging.info(f"Пользователь {conv_id}: Ищем данные по e-mail {email_lower}.")

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

            if cell_value_lower == email_lower:
                # Формируем строку со всеми данными, убирая None
                client_data = ", ".join(str(x) for x in row if x is not None)
                client_info += f"- {client_data}\n"
                email_found = True

        if not email_found:
            client_info += "  Ничего не найдено\n"
            logging.warning(f"Пользователь {conv_id}: Не найдены данные по e-mail {email_lower}.")

    # --- Поиск по телефону ---
    for phone_tuple in phones:
        # phone_tuple — это кортеж из рег. выражения (три группы по 3,2,2 цифры)
        # Например, ("905", "787", "89", "61")
        digits_only = "".join(filter(str.isdigit, "".join(phone_tuple)))
        logging.info(f"Пользователь {conv_id}: Ищем данные по телефону (цифры): {digits_only}.")

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
            logging.warning(f"Пользователь {conv_id}: Не найдены данные по телефону {digits_only}.")

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
    """
    Загружает файл log_file_path на Яндекс.Диск, если YANDEX_DISK_TOKEN задан.
    Теперь с обработкой исключений и таймаутом, чтобы при зависании запроса
    не падал весь сервер (worker timeout).
    """
    # Проверяем, существует ли папка на Яндекс.Диске
    create_dir_url = "https://cloud-api.yandex.net/v1/disk/resources"
    headers = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"}
    params = {"path": "disk:/app-logs"}

    # Если нет токена, выходим
    if not YANDEX_DISK_TOKEN:
        logging.warning("YANDEX_DISK_TOKEN не задан. Пропускаем загрузку логов.")
        return

    # Если файла нет, тоже выходим
    if not os.path.exists(log_file_path):
        logging.warning(f"Файл '{log_file_path}' не найден. Пропускаем загрузку на Я.Диск.")
        return

    # Сначала пытаемся создать папку "app-logs"
    try:
        requests.put(create_dir_url, headers=headers, params=params, timeout=10)
        # Ошибки здесь не критичны, если папка уже существует, всё ок
    except requests.Timeout:
        logging.error("Timeout при создании папки /app-logs на Яндекс.Диске.")
        return
    except requests.RequestException as e:
        logging.error(f"Ошибка при создании папки на Яндекс.Диске: {e}")
        return

    file_name = os.path.basename(log_file_path)
    ya_path = f"disk:/app-logs/{file_name}"

    get_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
    params = {
        "path": ya_path,
        "overwrite": "true"
    }

    # 1. Получаем ссылку для загрузки
    try:
        r = requests.get(get_url, headers=headers, params=params, timeout=10)
    except requests.Timeout:
        logging.error(f"Timeout при получении URL для загрузки на Яндекс.Диск: {file_name}")
        return
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе URL для загрузки на Яндекс.Диск: {e}")
        return

    if r.status_code != 200:
        logging.error(f"Ошибка {r.status_code} при получении URL для загрузки на Яндекс.Диск: {r.text}")
        return

    href = r.json().get("href", "")
    if not href:
        logging.error(f"Не нашли 'href' в ответе Яндекс.Диска при получении ссылки загрузки: {r.text}")
        return

    # 2. Загружаем файл
    try:
        with open(log_file_path, "rb") as f:
            upload_resp = requests.put(href, files={"file": f}, timeout=15)  # 15с на выгрузку
    except requests.Timeout:
        logging.error(f"Timeout при отправке файла '{file_name}' на Яндекс.Диск.")
        return
    except requests.RequestException as e:
        logging.error(f"Ошибка запроса при загрузке файла '{file_name}' на Я.Диск: {e}")
        return

    if upload_resp.status_code == 201:
        logging.info(f"Лог-файл '{file_name}' успешно загружен на Яндекс.Диск.")
    else:
        logging.error(f"Ошибка {upload_resp.status_code} при загрузке '{file_name}' на Яндекс.Диск: {upload_resp.text}")


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

def store_dialog_in_db(conv_id, role, message_with_timestamp, client_info=""):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Создание таблицы с новым полем role, если её нет
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dialogues (
                id SERIAL PRIMARY KEY,
                conv_id BIGINT,
                role TEXT,
                message TEXT,          -- Текст сообщения (включая timestamp)
                client_info TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Вставка записи
        cur.execute(
            """INSERT INTO dialogues (conv_id, role, message, client_info)
               VALUES (%s, %s, %s, %s)""",
            (conv_id, role, message_with_timestamp, client_info)  # Используем message_with_timestamp
        )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при сохранении диалога в БД: {e}")


def load_dialog_from_db(conv_id):
    """
    Подгружает из БД всю историю сообщений для указанного conv_id.
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
            WHERE conv_id = %s
            ORDER BY id ASC
        """, (conv_id,))
        rows = cur.fetchall()
        for row in rows:
            role, message_with_timestamp, client_info = row
            dialog_history.append({role: message_with_timestamp, "client_info": client_info}) # Используем строку целиком
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при загрузке диалога из БД для conv_id={conv_id}: {e}")
    return dialog_history



# ==============================
# 7. ЛОГИРОВАНИЕ
# ==============================
def log_dialog(user_question, bot_response, relevant_titles, relevant_answers, conv_id, full_name="", client_info="", timestamp=""):
    """
    Логирует в локальный файл и сохраняет каждое сообщение в базу данных.
    Сообщения от пользователя и от бота сохраняются по отдельности.
    """
    # Сохраняем сообщение пользователя и ответа бота в БД
    store_dialog_in_db(conv_id, "user", user_question, client_info, timestamp)
    store_dialog_in_db(conv_id, "bot", bot_response, client_info, timestamp)

    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")

    # Определяем лог-файл для пользователя
    if conv_id in user_log_files:
        local_log_file = user_log_files[conv_id]
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
Вот список вопросов-ключей (каждый вопрос - уникальный элемент):
{', '.join(titles)}

Выбери ИЗ ЭТОГО СПИСКА три наиболее релевантных вопроса к запросу: "{user_question}".
Ни в коем случае не выдумывай новых вопросов и не меняй формулировки!
Верни вопросы-ключи СТРОГО по одному в каждой строке, без пояснений и без номеров.
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
    Генерирует ответ от модели с учётом истории диалога и подсказок из базы знаний.

    Формируется контекст диалога без дублирования последовательных реплик.
    Каждая реплика имеет формат:
         "<Имя отправителя>: <сообщение>"
    Подсказки из knowledge_base включаются в формате "ключ -> значение".
    """
    history_lines = []
    last_line = None  # Для отслеживания последнего сообщения
    for turn in dialog_history:
        if "user" in turn:
            line = f"{first_name}: {turn['user'].strip()}"
        elif "operator" in turn:
            line = f"Оператор: {turn['operator'].strip()}"
        elif "bot" in turn:
            line = f"Модель: {turn['bot'].strip()}"
        else:
            continue

        if line == last_line:  # Проверяем на дубликат
            continue

        history_lines.append(line)
        last_line = line  # Запоминаем текущее сообщение

    history_text = "\n\n".join(history_lines)

    # Формируем подсказки из базы знаний: для каждого ключа из relevant_titles, если он есть в knowledge_base,
    # добавляем строку "ключ -> значение"
    knowledge_hint = ""
    if relevant_titles:
        kb_lines = []
        for key in relevant_titles:
            k = key.strip()  # удаляем лишние пробелы и символы перевода строки
            logging.info(f"Обрабатывается ключ: '{k}'") # Логируем ключ
            
            # Вызываем нашу функцию match_kb_key_ignoring_trailing_punc:
            matched_key = match_kb_key_ignoring_trailing_punc(k, knowledge_base)
            if matched_key is not None:
                # Значит база содержит такой ключ (игнорируя хвостовую пунктуацию)
                value = str(knowledge_base[matched_key]).strip()
                kb_lines.append(f"{k} -> {value}")
                logging.info(f"Добавлено в kb_lines: '{k} -> {value}'")
            else:
                # Ни один ключ не подошёл
                logging.warning(f"Ключ '{k}' отсутствует в knowledge_base (даже игнорируя пунктуацию).")
            
        if kb_lines:
            knowledge_hint = "Подсказки из базы знаний:\n" + "\n".join(kb_lines)

    # Собираем полный промпт для модели
    prompt_parts = [custom_prompt,
                    f"Контекст диалога:\n{history_text}"]
    if client_data.strip():
        prompt_parts.append(client_data)
    if knowledge_hint:
        prompt_parts.append(knowledge_hint)
    prompt_parts.append(f"Текущий запрос от {first_name}: {user_question}")
    prompt_parts.append("Модель:")
    full_prompt = "\n\n".join(prompt_parts)

    # Сохраняем полный промпт в файл и обновляем на Яндекс.Диске
    now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    prompt_filename = f"prompt_{now_str}.txt"
    prompt_file_path = os.path.join(logs_directory, prompt_filename)
    try:
        with open(prompt_file_path, "w", encoding="utf-8") as pf:
            pf.write(full_prompt)
        upload_log_to_yandex_disk(prompt_file_path)
        logging.info(f"Полный промпт, отправленный в модель, сохранён: {prompt_filename}")
    except Exception as e:
        logging.error(f"Ошибка при записи промпта в файл: {e}")

    # Логируем сокращённую версию контекста: последние 4 сообщения
    if len(history_lines) > 4:
        short_context = "\n".join(history_lines[-4:])
    else:
        short_context = "\n".join(history_lines)
    log_knowledge = ", ".join(relevant_titles) if relevant_titles else "нет"
    logging.info(
        f"\nЗапрос к модели:\nПромпт: (Сокращённая версия)\nКонтекст (последние 4 сообщения):\n{short_context}\nПодсказки (ключи): {log_knowledge}\nТекущий запрос от {first_name}: {user_question}\nИнформация о клиенте: {client_data}\n"
    )

    # Отправляем запрос модели (Gemini)
    data = {"contents": [{"parts": [{"text": full_prompt}]}]}
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
            return "Ой! Извините, я сейчас перегружен. Если срочно, напишите 'оператор'."
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return f"Ошибка: {resp.status_code}. {resp.text}"
    return "Извините, я сейчас не могу ответить. Попробуйте позже."



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

def clear_operator_timer(conv_id):
    """
    Вызывается по истечении 15 минут после последнего сообщения оператора.
    Просто удаляет запись об операторском таймере для данного диалога,
    позволяя клиентскому таймеру запускать генерацию ответа.
    """
    if conv_id in operator_timers:
        del operator_timers[conv_id]
        logging.info(f"Операторский таймер для диалога {conv_id} истёк, сбрасываем.")

def handle_new_message(user_id, text, vk, is_outgoing=False, conv_id=None):
    """
    Обрабатывает новое сообщение в диалоге.

    Параметры:
      - user_id: ID отправителя (из поля from_id).
      - text: текст сообщения.
      - vk: объект VK API.
      - is_outgoing: True, если сообщение исходящее (от сообщества или оператора).
      - conv_id: идентификатор диалога. Для входящих — msg["from_id"],
                для исходящих — msg.get("peer_id", msg["from_id"]). Все реплики одного диалога записываются в один лог‑файл.

    """
    OPERATOR_ID = 78671089

    # Используем conv_id, если передан, иначе user_id
    if conv_id is None:
        conv_id = user_id

    # Определяем роль
    if is_outgoing:
        if int(user_id) < 0:
            logging.info(f"[handle_new_message] Исходящее сообщение от бота (community), пропускаем. user_id={user_id}")
            return
        elif int(user_id) == OPERATOR_ID:
            role = "operator"
        else:
            role = "operator"  # Если is_outgoing, но не от бота – считаем операторским.
    else:
        role = "user"

    # Загружаем историю диалога по conv_id
    if conv_id not in dialog_history_dict:
        existing_history = load_dialog_from_db(conv_id)
        dialog_history_dict[conv_id] = existing_history
    dialog_history = dialog_history_dict[conv_id]

    # Получаем имя пользователя по conv_id. Если уже есть – используем кеш.
    if conv_id in user_names:
        first_name, last_name = user_names[conv_id]
    else:
        first_name, last_name = get_vk_user_full_name(conv_id)
        user_names[conv_id] = (first_name, last_name)
    # Формируем полное имя: если last_name присутствует, объединяем, иначе просто first_name.
    if first_name and last_name:
        display_name = f"{first_name} {last_name}".strip()
    elif first_name:
        display_name = first_name
    else:
        display_name = "unknown"

    # Для входящих сообщений: если последнее сообщение с ролью "user" совпадает (после strip) с новым, пропускаем
    if role == "user" and dialog_history:
        last_entry = dialog_history[-1]
        if "user" in last_entry and last_entry["user"].strip() == text.strip():
            logging.info("[handle_new_message] Дублирующееся входящее сообщение от пользователя, пропускаем.")
            return

    # Если это первое входящее сообщение (история пуста) и текст не содержит "оператор", отправляем уведомление в Telegram
    if role == "user" and len(dialog_history) == 0 and "оператор" not in text.lower():
        send_telegram_notification(user_question=text, dialog_id=conv_id,
                                   first_name=first_name, last_name=last_name)

    # Определяем лог-файл для диалога по conv_id (все сообщения одного диалога в один файл)
    if conv_id in user_log_files:
        log_file_path = user_log_files[conv_id]
    else:
        now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        log_file_name = f"dialog_{now_str}_{display_name.replace(' ', '_')}.txt"
        log_file_path = os.path.join(logs_directory, log_file_name)
        user_log_files[conv_id] = log_file_path

    # Формируем строку для записи в лог-файл
    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")
    message_with_timestamp = f"[{formatted_time}] {text}"
    
    # Сохраняем сообщение с timestamp в БД и в dialog_history
    store_dialog_in_db(conv_id, role, message_with_timestamp, client_info="")  # Используем message_with_timestamp
    dialog_history.append({role: message_with_timestamp}) # Добавляем message_with_timestamp
    
    if role == "user":
        log_entry = f"[{formatted_time}] {display_name}: {text}\n"
    elif role == "operator":
        log_entry = f"[{formatted_time}] Оператор: {text}\n"
    else:
        log_entry = f"[{formatted_time}] {role}: {text}\n"

    try:
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(log_entry)
    except Exception as e:
        logging.error(f"Ошибка записи в лог-файл {log_file_path}: {e}")

    # Обновляем лог-файл на Яндекс.Диске
    try:
        upload_log_to_yandex_disk(log_file_path)
    except Exception as e:
        logging.error(f"Ошибка загрузки лог-файла {log_file_path} на Яндекс.Диск: {e}")

    # Настройка таймеров задержки после сообщений от оператора или клиента
    # Всегда добавляем сообщение в буфер и обновляем last_questions
    user_buffers.setdefault(conv_id, []).append(text)
    last_questions[conv_id] = text

    if role == "operator":
        # Обновляем только операторский таймер на 15 минут.
        if conv_id in operator_timers:
            operator_timers[conv_id].cancel()
        op_timer = threading.Timer(15 * 60, clear_operator_timer, args=(conv_id,))
        operator_timers[conv_id] = op_timer
        op_timer.start()
    # Проверяем, не моё ли это сообщение
    if role == "user" and user_id == OPERATOR_ID:
        logging.info("[handle_new_message] Сообщение от владельца бота, пропускаем таймер.")
        generate_and_send_response(conv_id, vk)
    elif role == "user":
        # Обновляем только клиентский таймер на 60 секунд.
        if conv_id in client_timers:
            client_timers[conv_id].cancel()
        cl_timer = threading.Timer(60, generate_and_send_response, args=(conv_id, vk))
        client_timers[conv_id] = cl_timer
        cl_timer.start()

    # Если входящее сообщение содержит слово "оператор" и это не первое сообщение, отправляем уведомление оператору
    if role == "user" and "оператор" in text.lower() and len(dialog_history) > 1:
        summary, reason = generate_summary_and_reason(dialog_history)
        init_q = last_questions.get(conv_id, "")
        send_operator_notification(conv_id, init_q, summary, reason,
                                   first_name=first_name, last_name=last_name)


def generate_and_send_response(conv_id, vk):
    """
    По истечении DELAY_SECONDS формируем единый текст (из user_buffers), генерируем ответ,
    добавляем обе реплики (user + bot) в БД/память, а затем отправляем сообщение через VK.
    """

    # Если в буфере нет сообщений - ничего не делаем
    # Если операторский таймер ещё активен, значит оператор писал недавно – не отвечаем.
    if conv_id in operator_timers:
        logging.info(f"Операторский таймер активен для диалога {conv_id}. Не генерируем ответ.")
        user_buffers[conv_id] = []  # очищаем буфер
        return

    msgs = user_buffers.get(conv_id, [])
    if not msgs:
        return
    # Берём все пользовательские сообщения (накопленные за DELAY_SECONDS)
    combined_text = "\n".join(msgs).strip()
    user_buffers[conv_id] = []

    # Определяем имя пользователя
    f_name, l_name = user_names.get(conv_id, ("", ""))
    full_name = f"{f_name}_{l_name}".strip("_")

    # -- Вот ЗДЕСЬ получаем данные по email / телефону --
    found_data = get_client_info(combined_text, conv_id)
    
    # Загружаем историю диалога из dialog_history ( *уже* без client_info )
    dialog_history = dialog_history_dict[conv_id]

    # Генерируем ответ модели (используя функцию generate_response)
    relevant_titles = find_relevant_titles_with_gemini(combined_text)
    relevant_answers = [knowledge_base[t] for t in relevant_titles if t in knowledge_base]

    # Передаём found_data только в промпт модели
    model_response = generate_response(
        user_question=combined_text,
        client_data=found_data,  # <--- только в промпт
        dialog_history=dialog_history,
        custom_prompt=custom_prompt,
        first_name=f_name,
        relevant_answers=relevant_answers,
        relevant_titles=relevant_titles
    )
    
    # ========== 1. Теперь сохраняем сообщение пользователя в БД и кеш, но БЕЗ found_data ==========
    store_dialog_in_db(conv_id, "user", combined_text, client_info="") 
    dialog_history.append({"user": combined_text})  # без поля "client_info"

    # ========== 2. Добавляем ответ бота в БД и кеш как "bot", тоже БЕЗ found_data ==========
    store_dialog_in_db(conv_id, "bot", model_response, client_info="")
    dialog_history.append({"bot": model_response})  # без поля "client_info"
    
    # ========== 3. Локальное логирование ==========
    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")

    if conv_id not in user_log_files:
        now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f"dialog_{now_str}_{full_name}.txt"
        user_log_files[conv_id] = os.path.join(logs_directory, file_name)

    local_log_path = user_log_files[conv_id]
    with open(local_log_path, "a", encoding="utf-8") as log_file:
        # Логируем сообщение пользователя
        log_file.write(f"[{formatted_time}] {f_name}: {combined_text}\n")
        # ... если нужно, логируем find_relevant_titles_with_gemini
        # Логируем ответ модели
        log_file.write(f"[{formatted_time}] Модель: {model_response}\n\n")

    upload_log_to_yandex_disk(local_log_path)

    # ========== 4. Наконец отправляем сообщение через VK (с бот-ответом) ==========
    if vk:
        vk.messages.send(
            user_id=conv_id,
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
    conv_id = None

    # Найти conv_id по имени и фамилии
    for cid, (first, last) in user_names.items():
        if f"{first}_{last}" == full_name:
            conv_id = cid
            break

    if not conv_id:
        return "Пользователь не найден", 404

    try:
        # Удаление из базы данных
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("DELETE FROM dialogues WHERE conv_id = %s", (conv_id,))
        conn.commit()
        cur.close()
        conn.close()

        # Удаление из кеша
        dialog_history_dict.pop(conv_id, None)
        user_buffers.pop(conv_id, None)
        user_timers.pop(conv_id, None)
        last_questions.pop(conv_id, None)

        return "Контекст успешно очищен", 200
    except Exception as e:
        print(f"Ошибка при очистке контекста для {full_name}: {e}")
        return "Ошибка сервера", 500

# Глобальная память для недавних event_id
recent_event_ids = {}  # event_id -> float( time.time() )
EVENT_ID_TTL = 30       # Сколько секунд хранить event_id

@app.route("/callback", methods=["POST"])
def callback():
    # 1. Получаем данные из запроса
    data = request.json

    # Если хотите сохранять JSON, раскомментируйте следующую строку:
    # save_callback_payload(data)

    # 2. Обработка confirmation-запроса
    if data.get("type") == "confirmation":
        return VK_CONFIRMATION_TOKEN

    # 3. Проверяем секрет (если используется)
    if VK_SECRET_KEY and data.get("secret") != VK_SECRET_KEY:
        return "Invalid secret", 403

    # 4. Определяем тип события и event_id
    event_type = data.get("type")
    event_id = data.get("event_id", "no_event_id")

    # 5. Дедубликация: очищаем старые event_id и пропускаем дубликаты
    now_ts = time.time()
    for eid in list(recent_event_ids.keys()):
        if now_ts - recent_event_ids[eid] > EVENT_ID_TTL:
            del recent_event_ids[eid]
    if event_id in recent_event_ids:
        logging.info(f"Дублирующийся колбэк event_id={event_id} (type={event_type}), пропускаем.")
        return "ok"
    else:
        recent_event_ids[event_id] = now_ts

    # 6. Если тип события не интересует нас, выходим
    if event_type not in ("message_new", "message_reply", "message_edit"):
        logging.info(f"Пропускаем событие type={event_type}")
        return "ok"

    # 7. Извлекаем объект события
    vk_object = data.get("object", {})
    if not isinstance(vk_object, dict):
        logging.warning("Неправильный формат 'object' в колбэке.")
        return "ok"

    # 8. Извлекаем необходимые поля: from_id, text, out.
    msg = {}
    if "message" in vk_object:
        inner = vk_object["message"]
        msg["from_id"] = inner.get("from_id")
        msg["text"] = inner.get("text", "")
        msg["out"] = inner.get("out", 0)
    else:
        msg["from_id"] = vk_object.get("from_id")
        msg["text"] = vk_object.get("text", "")
        msg["out"] = vk_object.get("out", 0)

    if "admin_author_id" in vk_object:
        logging.info(f"admin_author_id={vk_object['admin_author_id']} => сообщение от оператора")

    if not msg.get("from_id") or "text" not in msg:
        logging.warning(f"Не удалось извлечь from_id/text из события {event_type}: {data}")
        return "ok"

    # 9. Определяем, исходящее ли сообщение (out=1) или входящее (out=0)
    is_outgoing = (msg["out"] == 1)
    # Всегда используем conv_id = from_id, чтобы все сообщения одного диалога записывались в один файл
    conv_id = msg["from_id"]

    user_id = msg["from_id"]
    text = msg["text"]

    # 10. Создаем объект vk (локально)
    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    # 11. Вызываем функцию handle_new_message, передавая conv_id
    handle_new_message(user_id=msg["from_id"], text=msg["text"], vk=vk, is_outgoing=is_outgoing, conv_id=conv_id)

    # 12. Возвращаем "ok"
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