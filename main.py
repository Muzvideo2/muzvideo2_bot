import os
import re
import time
import json
import requests
import psycopg2
from datetime import datetime, timedelta
import threading
import vk_api
from flask import Flask, request, jsonify # Убедимся, что jsonify импортирован
from urllib.parse import quote
import openpyxl # Если все еще используется для get_client_info
import logging

# ==============================
# Читаем переменные окружения (секретные данные)
# ==============================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
VK_COMMUNITY_TOKEN = os.environ.get("VK_COMMUNITY_TOKEN", "")
YANDEX_DISK_TOKEN = os.environ.get("YANDEX_DISK_TOKEN", "")
VK_SECRET_KEY = os.environ.get("VK_SECRET_KEY", "") # Используется для проверки callback
VK_CONFIRMATION_TOKEN = os.environ.get("VK_CONFIRMATION_TOKEN", "") # Для подтверждения callback сервера

# Параметры PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")

# ID сообщества (нужно для формирования ссылки формата https://vk.com/gim<community_id>?sel=<user_id>)
# Например, если сообщество имеет адрес https://vk.com/club48116621, то его ID = 48116621
VK_COMMUNITY_ID = os.environ.get("VK_COMMUNITY_ID", "ВАШ_ID_СООБЩЕСТВА") # Лучше тоже из env

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Некоторые глобальные переменные:
# Таймеры для предотвращения ответа бота, если оператор недавно писал
operator_timers = {}  # {conv_id: threading.Timer}
# Таймеры для задержки ответа клиенту (сбор сообщений в буфер)
client_timers = {}    # {conv_id: threading.Timer}
# Словарь для хранения истории диалогов в памяти {conv_id: [сообщения]}
dialog_history_dict = {}
# Кеш имен пользователей {conv_id: (first_name, last_name)}
user_names = {}
# Пути к лог-файлам диалогов {conv_id: "путь_к_файлу.txt"}
user_log_files = {}
# Буферы для сбора сообщений от пользователя перед обработкой
user_buffers = {} # {conv_id: [сообщение1, сообщение2]}
# Не используется напрямую в новой логике, но может быть полезно для отладки
last_questions = {} # {conv_id: "последний обработанный вопрос"}

# Константа для задержки ответа клиенту (в секундах)
USER_MESSAGE_BUFFERING_DELAY = 60 # Было DELAY_SECONDS

# ==============================
# Пути к файлам
# ==============================
KNOWLEDGE_BASE_PATH = "knowledge_base.json" # Константа для пути
PROMPT_PATH = "prompt.txt" # Константа для пути
LOGS_DIRECTORY = "dialog_logs" # Константа для папки логов

# ==============================
# Прочитаем базу знаний и промпт
# ==============================
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY, exist_ok=True)

try:
    with open(KNOWLEDGE_BASE_PATH, "r", encoding="utf-8") as f:
        knowledge_base = json.load(f)
except FileNotFoundError:
    logging.error(f"Файл базы знаний '{KNOWLEDGE_BASE_PATH}' не найден. Работа будет продолжена без нее.")
    knowledge_base = {} # Работаем с пустой БЗ, если файл не найден
except json.JSONDecodeError:
    logging.error(f"Ошибка декодирования JSON в файле базы знаний '{KNOWLEDGE_BASE_PATH}'.")
    knowledge_base = {}

try:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        custom_prompt = f.read().strip()
except FileNotFoundError:
    logging.error(f"Файл промпта '{PROMPT_PATH}' не найден. Будет использован пустой промпт.")
    custom_prompt = "Ты — полезный ассистент." # Запасной промпт

# ==============================
# Сервисные переменные
# ==============================
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
# Переименовал gemini_url в GEMINI_API_URL для ясности

# ID оператора (владельца бота), чьи сообщения из VK не должны запускать стандартную логику пользователя
# Это значение должно быть числом, если оно используется для сравнения с user_id из VK
try:
    OPERATOR_VK_ID = int(os.environ.get("OPERATOR_VK_ID", 0)) # Установите реальный VK ID оператора/владельца
except ValueError:
    logging.warning("Переменная окружения OPERATOR_VK_ID не является числом. Установлено значение 0.")
    OPERATOR_VK_ID = 0


#============================================================================
# Функция, убирающая любую «хвостовую» пунктуацию или спецсимволы из ключа БЗ
#============================================================================
def remove_trailing_punctuation(text: str) -> str:
    # Паттерн: один или несколько НЕ-буквенно-цифровых символов и НЕ-пробелов в конце строки
    pattern = r'[^\w\s]+$'
    return re.sub(pattern, '', text).strip()

#=============================================================================
# Функция, которая пытается найти ключ в knowledge_base, игнорируя пунктуацию
#=============================================================================
def match_kb_key_ignoring_trailing_punc(user_key: str, kb: dict) -> str | None:
    user_clean = remove_trailing_punctuation(user_key)
    for kb_key in kb:
        kb_clean = remove_trailing_punctuation(kb_key)
        if kb_clean.lower() == user_clean.lower(): # Сравнение без учета регистра
            return kb_key  # Возвращаем реальный ключ из базы (как записан в JSON)
    return None

# =========================================================================
# 1. ФУНКЦИЯ ПОИСКА ПОКУПОК КЛИЕНТОВ, ЕСЛИ В ЗАПРОСЕ ЕСТЬ ЕМЕЙЛ ИЛИ ТЕЛЕФОН
# (Оставляем как есть, если она все еще нужна)
# =========================================================================
def get_client_info(user_question, conv_id):
    """
    Анализирует текст user_question на предмет email или номера телефона.
    Если они найдены, ищет информацию о клиенте в Excel-файле "clients.xlsx".
    Возвращает строку со всеми найденными данными или сообщает, что ничего не найдено.
    Учитывает, что у клиента может быть несколько покупок (строк).
    """
    client_info_parts = [] # Собираем информацию в список

    # Рег. выражения для email и телефона
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    # Улучшенный regex для телефонов, чтобы лучше захватывать разные форматы
    phone_regex = r"(?:\+7|7|8)?[\s\-(]*(\d{3})[\s\-)]*(\d{3})[\s\-]*(\d{2})[\s\-]*(\d{2})"

    emails_found = re.findall(email_regex, user_question)
    phones_found_tuples = re.findall(phone_regex, user_question)

    if not emails_found and not phones_found_tuples:
        return "" # Если нет ни email, ни телефона, ничего не ищем

    logging.info(f"Пользователь {conv_id}: Запрошен поиск в таблице клиентов по данным: emails={emails_found}, phones={phones_found_tuples}.")

    try:
        # Убедитесь, что файл 'clients.xlsx' находится в корневой директории проекта или укажите полный путь
        workbook = openpyxl.load_workbook("clients.xlsx")
        sheet = workbook.active
    except FileNotFoundError:
        logging.error("Файл 'clients.xlsx' не найден. Информация о клиентах не будет загружена.")
        return "" # Возвращаем пустую строку, если файл не найден

    header = [cell.value for cell in sheet[1]] # Заголовки столбцов

    # --- Поиск по email ---
    for email in emails_found:
        email_lower = email.lower().strip()
        email_search_results = []
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            # Предположим, что email находится в 5-й колонке (индекс 4)
            if len(row) > 4 and row[4] and isinstance(row[4], str) and row[4].lower().strip() == email_lower:
                client_data = []
                for col_idx, cell_value in enumerate(row):
                    if cell_value is not None: # Добавляем только непустые ячейки
                        col_name = header[col_idx] if col_idx < len(header) else f"Колонка {col_idx+1}"
                        client_data.append(f"{col_name}: {cell_value}")
                if client_data:
                    email_search_results.append("; ".join(client_data))

        if email_search_results:
            client_info_parts.append(f"Данные по e-mail ({email_lower}):\n- " + "\n- ".join(email_search_results))
        else:
            logging.info(f"Пользователь {conv_id}: Не найдены данные по e-mail {email_lower}.")

    # --- Поиск по телефону ---
    for phone_tuple in phones_found_tuples:
        # Собираем цифры телефона из кортежа групп
        digits_only_query = "".join(phone_tuple) # Уже содержит только цифры из групп
        phone_search_results = []
        
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            # Предположим, что телефон находится в 6-й колонке (индекс 5)
            if len(row) > 5 and row[5]:
                phone_cell_value = str(row[5])
                # Очищаем телефон из ячейки от всего, кроме цифр
                phone_digits_in_cell = "".join(filter(str.isdigit, phone_cell_value))
                
                # Сравниваем последние 10 цифр (стандартная длина российского номера без +7/8)
                if phone_digits_in_cell.endswith(digits_only_query[-10:]):
                    client_data = []
                    for col_idx, cell_value in enumerate(row):
                        if cell_value is not None:
                            col_name = header[col_idx] if col_idx < len(header) else f"Колонка {col_idx+1}"
                            client_data.append(f"{col_name}: {cell_value}")
                    if client_data:
                        phone_search_results.append("; ".join(client_data))
        
        if phone_search_results:
            client_info_parts.append(f"Данные по телефону ({digits_only_query}):\n- " + "\n- ".join(phone_search_results))
        else:
            logging.info(f"Пользователь {conv_id}: Не найдены данные по телефону {digits_only_query}.")

    if not client_info_parts:
        logging.info(f"Пользователь {conv_id}: В таблице клиентов ничего не найдено по запросу '{user_question}'.")
        return ""
        
    return "\n\n".join(client_info_parts).strip()


# =======================================
# 2. ФУНКЦИЯ ЗАПРОСА ИМЕНИ КЛИЕНТА ВКОНТАКТЕ
# =======================================
def get_vk_user_full_name(user_id_to_fetch): # Переименовал user_id во избежание конфликтов
    """
    Получает имя и фамилию пользователя ВКонтакте по user_id через API.
    Использует кеширование в user_names.
    """
    try:
        # VK API ожидает user_id как строку, но для ключей словаря лучше использовать int
        user_id_int = int(user_id_to_fetch)
    except ValueError:
        logging.error(f"Некорректный user_id '{user_id_to_fetch}' для запроса имени VK.")
        return "Пользователь", "VK" # Заглушка

    if user_id_int in user_names:
        logging.debug(f"Имя для user_id {user_id_int} взято из кеша: {user_names[user_id_int]}")
        return user_names[user_id_int]

    if not VK_COMMUNITY_TOKEN:
        logging.warning("VK_COMMUNITY_TOKEN отсутствует. Невозможно получить имя пользователя VK.")
        user_names[user_id_int] = (f"User_{user_id_int}", "") # Кешируем заглушку
        return f"User_{user_id_int}", ""

    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    try:
        response = vk.users.get(user_ids=str(user_id_int), fields="first_name,last_name") # API ожидает строку
        if response and isinstance(response, list) and len(response) > 0:
            user_data = response[0]
            if "deactivated" in user_data:
                logging.warning(f"Пользователь VK {user_id_int} удален или заблокирован: {user_data.get('deactivated')}")
                first_name = f"User_{user_id_int}"
                last_name = "(деактивирован)"
            else:
                first_name = user_data.get("first_name", f"User_{user_id_int}")
                last_name = user_data.get("last_name", "")
            
            user_names[user_id_int] = (first_name, last_name)
            logging.info(f"Получено имя для user_id {user_id_int}: {first_name} {last_name}")
            return first_name, last_name
    except vk_api.ApiError as e:
        logging.error(f"Ошибка VK API при получении имени пользователя {user_id_int}: {e}")
    except Exception as e: # Более общая обработка других исключений (сеть и т.д.)
        logging.error(f"Неизвестная ошибка при получении имени пользователя VK {user_id_int}: {e}")
    
    # Если произошла ошибка или имя не найдено, кешируем и возвращаем заглушку
    user_names[user_id_int] = (f"User_{user_id_int}", "(ошибка API)")
    return f"User_{user_id_int}", "(ошибка API)"


# ==============================
# 3. ФУНКЦИИ УВЕДОМЛЕНИЙ В ТЕЛЕГРАМ
# ==============================
def send_telegram_notification(user_question_text, dialog_id, first_name="", last_name=""):
    """
    Уведомление в телеграм при первом сообщении пользователя или при запросе "оператор".
    Диалог-ссылка вида https://vk.com/gim<community_id>?sel=<user_id>.
    """
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        logging.warning("Токен Telegram или ID чата администратора не настроены. Уведомление не отправлено.")
        return

    vk_dialog_link = f"https://vk.com/gim{VK_COMMUNITY_ID}?sel={dialog_id}"
    user_full_name = f"{first_name} {last_name}".strip()
    if not user_full_name: # Если имя пустое
        user_full_name = f"Пользователь ID {dialog_id}"

    message_text = f"""
👤 Пользователь: {user_full_name}
💬 Стартовый вопрос: {user_question_text}
🔗 Ссылка на диалог: {vk_dialog_link}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = { # Переименовал data в payload для ясности
        "chat_id": ADMIN_CHAT_ID,
        "text": message_text,
        "parse_mode": "Markdown", # MarkdownV2 более современный, но Markdown тоже работает
        "disable_web_page_preview": True # Чтобы ссылка не разворачивалась в большое превью
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status() # Проверка на HTTP ошибки
        logging.info(f"Уведомление о новом диалоге ({dialog_id}) отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отправке уведомления в Telegram для диалога {dialog_id}: {e}")


def send_operator_request_notification(dialog_id, initial_question, dialog_summary, reason_guess, first_name="", last_name=""):
    """
    Уведомление, если пользователь запросил оператора в процессе диалога.
    Переименовал send_operator_notification для ясности.
    """
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        logging.warning("Токен Telegram или ID чата администратора не настроены. Уведомление о запросе оператора не отправлено.")
        return

    vk_dialog_link = f"https://vk.com/gim{VK_COMMUNITY_ID}?sel={dialog_id}"
    user_full_name = f"{first_name} {last_name}".strip()
    if not user_full_name:
        user_full_name = f"Пользователь ID {dialog_id}"

    message_text = f"""
🆘 Запрос оператора!
👤 Пользователь: {user_full_name}
❓ Изначальный вопрос клиента: {initial_question}
📝 Сводка обсуждения: {dialog_summary}
🤔 Предполагаемая причина: {reason_guess}
🔗 Ссылка на диалог: {vk_dialog_link}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": ADMIN_CHAT_ID,
        "text": message_text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Уведомление о запросе оператора ({dialog_id}) отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отправке уведомления о запросе оператора ({dialog_id}) в Telegram: {e}")

# ==============================================
# 4. РАБОТА С ЯНДЕКС.ДИСКОМ: ЗАГРУЗКА ЛОГ-ФАЙЛОВ
# ==============================================
# (эта функция остается без изменений по логике, но убедитесь, что она корректно работает)
def upload_log_to_yandex_disk(log_file_path_to_upload): # Переименовал log_file_path
    """
    Загружает файл log_file_path_to_upload на Яндекс.Диск, если YANDEX_DISK_TOKEN задан.
    С обработкой исключений и таймаутом.
    """
    if not YANDEX_DISK_TOKEN:
        logging.warning("YANDEX_DISK_TOKEN не задан. Пропускаем загрузку логов на Яндекс.Диск.")
        return

    if not os.path.exists(log_file_path_to_upload):
        logging.warning(f"Файл '{log_file_path_to_upload}' не найден. Пропускаем загрузку на Яндекс.Диск.")
        return

    # Создаем папку "app-logs" на Яндекс.Диске, если ее нет
    create_dir_url = "https://cloud-api.yandex.net/v1/disk/resources"
    headers_ya = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"} # Переименовал headers
    params_ya_create_dir = {"path": "disk:/app-logs"} # Переименовал params

    try:
        # PUT-запрос для создания папки. Яндекс API вернет ошибку, если папка есть, но это не страшно.
        response_create_dir = requests.put(create_dir_url, headers=headers_ya, params=params_ya_create_dir, timeout=10)
        if response_create_dir.status_code == 201:
            logging.info("Папка 'app-logs' успешно создана на Яндекс.Диске.")
        elif response_create_dir.status_code == 409: # 409 Conflict - папка уже существует
            logging.info("Папка 'app-logs' уже существует на Яндекс.Диске.")
        else:
            # Логируем, если код ответа не 201 (создано) и не 409 (уже есть)
            logging.warning(f"Не удалось создать/проверить папку на Яндекс.Диске. Статус: {response_create_dir.status_code}, Ответ: {response_create_dir.text}")
    except requests.Timeout:
        logging.error("Тайм-аут при создании/проверке папки /app-logs на Яндекс.Диске.")
        return # Не продолжаем, если не можем создать/проверить папку
    except requests.RequestException as e:
        logging.error(f"Ошибка при создании/проверке папки на Яндекс.Диске: {e}")
        return

    file_name_to_upload = os.path.basename(log_file_path_to_upload) # Переименовал file_name
    ya_disk_path = f"disk:/app-logs/{file_name_to_upload}" # Переименовал ya_path

    get_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload" # Переименовал get_url
    params_get_url = {"path": ya_disk_path, "overwrite": "true"} # Переименовал params

    try:
        # 1. Получаем ссылку для загрузки
        response_get_link = requests.get(get_upload_url, headers=headers_ya, params=params_get_url, timeout=10)
        response_get_link.raise_for_status() # Проверит HTTP ошибки
        
        href_upload_link = response_get_link.json().get("href") # Переименовал href
        if not href_upload_link:
            logging.error(f"Не найден 'href' в ответе Яндекс.Диска при получении ссылки для загрузки файла '{file_name_to_upload}': {response_get_link.text}")
            return

        # 2. Загружаем файл
        with open(log_file_path_to_upload, "rb") as f_log: # Переименовал f
            upload_response = requests.put(href_upload_link, files={"file": f_log}, timeout=30) # Увеличил таймаут на загрузку
        
        if upload_response.status_code == 201: # 201 Created - успешно загружено
            logging.info(f"Лог-файл '{file_name_to_upload}' успешно загружен на Яндекс.Диск.")
        else:
            logging.error(f"Ошибка {upload_response.status_code} при загрузке '{file_name_to_upload}' на Яндекс.Диск: {upload_response.text}")

    except requests.Timeout:
        logging.error(f"Тайм-аут при работе с Яндекс.Диском для файла '{file_name_to_upload}'.")
    except requests.RequestException as e:
        logging.error(f"Ошибка запроса при работе с Яндекс.Диском для файла '{file_name_to_upload}': {e}")
    except KeyError: # Если .json() не вернул 'href'
        logging.error(f"Ошибка извлечения 'href' из ответа Яндекс.Диска для файла '{file_name_to_upload}'.")
    except Exception as e: # Прочие непредвиденные ошибки
        logging.error(f"Непредвиденная ошибка при загрузке файла '{file_name_to_upload}' на Яндекс.Диск: {e}")

# ================================================================================
# 5. ФУНКЦИЯ ЗАПИСИ ДАННЫХ CALLBACK ОТ VK В JSON-файл (для отладки)
# ================================================================================
CALLBACK_LOGS_DIR = "callback_logs"
if not os.path.exists(CALLBACK_LOGS_DIR):
    os.makedirs(CALLBACK_LOGS_DIR, exist_ok=True)

def save_callback_payload(data_payload): # Переименовал data в data_payload
    """
    Сохраняет весь JSON, полученный от ВКонтакте, в локальный файл
    и опционально загружает этот файл на Яндекс.Диск.
    Используется в основном для отладки příchoщих callback-запросов.

    data_payload: dict — полный JSON из request.json
    """
    timestamp_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S_%f") # Добавил микросекунды для уникальности
    file_name = f"callback_{timestamp_str}.json"
    file_path = os.path.join(CALLBACK_LOGS_DIR, file_name)

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_payload, f, ensure_ascii=False, indent=2)
        logging.info(f"Сохранён callback JSON: {file_name}")
        
        # Опционально: загрузка на Яндекс.Диск (если это нужно для callback-логов)
        # upload_log_to_yandex_disk(file_path) 
    except Exception as e:
        logging.error(f"Ошибка при сохранении callback payload в файл '{file_path}': {e}")


# =================================
# 6. СОХРАНЕНИЕ ДИАЛОГОВ В POSTGRES
# =================================
def store_dialog_in_db(conv_id, role, message_text_with_timestamp, client_info=""):
    """
    Сохраняет одно сообщение (пользователя, бота или оператора) в базу данных.
    Время сообщения (created_at) будет установлено базой данных как CURRENT_TIMESTAMP.
    message_text_with_timestamp - это текст сообщения, уже содержащий метку времени [гггг-мм-дд_чч-мм-сс].
    """
    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. Сообщение не будет сохранено в БД.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Таблица должна уже существовать. Если нет, ее нужно создать один раз вручную или миграцией.
        # CREATE TABLE IF NOT EXISTS dialogues (
        #     id SERIAL PRIMARY KEY,
        #     conv_id BIGINT NOT NULL,
        #     role TEXT NOT NULL,
        #     message TEXT,
        #     client_info TEXT,
        #     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        # );
        # CREATE INDEX IF NOT EXISTS idx_dialogues_conv_id_created_at ON dialogues (conv_id, created_at DESC);


        cur.execute(
            """INSERT INTO dialogues (conv_id, role, message, client_info)
               VALUES (%s, %s, %s, %s)""",
            (conv_id, role, message_text_with_timestamp, client_info)
        )
        conn.commit()
        logging.info(f"Сообщение для conv_id {conv_id} (роль: {role}) сохранено в БД.")
    except psycopg2.Error as e: # Более специфичная обработка ошибок psycopg2
        logging.error(f"Ошибка PostgreSQL при сохранении диалога для conv_id {conv_id}: {e}")
        if conn: # conn может быть None, если psycopg2.connect выбросил исключение
            conn.rollback() # Откатываем транзакцию при ошибке
    except Exception as e:
        logging.error(f"Неизвестная ошибка при сохранении диалога в БД для conv_id {conv_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()


def load_dialog_from_db(conv_id_to_load): # Переименовал conv_id
    """
    Подгружает из БД всю историю сообщений для указанного conv_id_to_load.
    Возвращает список словарей, где каждый словарь представляет сообщение.
    Пример: [{'user': "текст", "client_info": "..."}, {'bot': "текст"}]
    Сообщения уже содержат метку времени в своем тексте.
    """
    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. История диалога не будет загружена из БД.")
        return []

    dialog_history_from_db = []
    try:
        conn = psycopg2.connect(DATABASE_URL)
        # psycopg2.extras.DictCursor позволяет обращаться к колонкам по имени
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT role, message, client_info
            FROM dialogues
            WHERE conv_id = %s
            ORDER BY created_at ASC 
        """, (conv_id_to_load,)) # created_at (или id) важен для порядка
        
        rows = cur.fetchall()
        for row in rows:
            # row['message'] это текст сообщения, который уже содержит [timestamp]
            # row['client_info'] может быть None, поэтому .get('', '')
            entry = {row['role']: row['message']}
            if row['client_info']: # Добавляем client_info только если оно есть
                entry['client_info'] = row['client_info']
            dialog_history_from_db.append(entry)
            
        logging.info(f"Загружено {len(rows)} сообщений из БД для conv_id {conv_id_to_load}.")
    except psycopg2.Error as e:
        logging.error(f"Ошибка PostgreSQL при загрузке диалога из БД для conv_id {conv_id_to_load}: {e}")
    except Exception as e:
        logging.error(f"Неизвестная ошибка при загрузке диалога из БД для conv_id {conv_id_to_load}: {e}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
    return dialog_history_from_db

# ==============================
# Flask App и его эндпоинты
# ==============================
app = Flask(__name__)

@app.route('/ping_main_bot', methods=['GET'])
def ping_main_bot():
    return "Pong from Main Bot!", 200

@app.route("/clear_context/<int:user_conv_id>", methods=["POST"]) # Изменил <full_name> на <int:user_conv_id>
def clear_context(user_conv_id):
    """
    Удаляет контекст пользователя (историю диалога) из базы данных и локального кеша по conv_id.
    Вызывается извне, например, из Telegram-бота администратора.
    """
    logging.info(f"Запрос на очистку контекста для conv_id: {user_conv_id}")

    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. Контекст не может быть очищен из БД.")
        return jsonify({"status": "error", "message": "DATABASE_URL не настроен"}), 500

    try:
        # Удаление из базы данных
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("DELETE FROM dialogues WHERE conv_id = %s", (user_conv_id,))
        deleted_rows = cur.rowcount # Количество удаленных строк
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Удалено {deleted_rows} записей из БД для conv_id {user_conv_id}.")

        # Удаление из локального кеша в памяти
        if user_conv_id in dialog_history_dict:
            del dialog_history_dict[user_conv_id]
        if user_conv_id in user_buffers:
            del user_buffers[user_conv_id]
        if user_conv_id in client_timers: # Отменяем и удаляем таймер клиента
            client_timers[user_conv_id].cancel()
            del client_timers[user_conv_id]
        if user_conv_id in operator_timers: # Отменяем и удаляем таймер оператора
            operator_timers[user_conv_id].cancel()
            del operator_timers[user_conv_id]
        if user_conv_id in last_questions:
            del last_questions[user_conv_id]
        # user_names и user_log_files можно не удалять, т.к. они могут быть полезны при новом диалоге
        # или удалить, если требуется полная "забывчивость":
        # if user_conv_id in user_names: del user_names[user_conv_id]
        # if user_conv_id in user_log_files: del user_log_files[user_conv_id]


        logging.info(f"Локальный кеш для conv_id {user_conv_id} очищен.")
        return jsonify({"status": "success", "message": f"Контекст для conv_id {user_conv_id} успешно очищен. Удалено записей из БД: {deleted_rows}."}), 200
    
    except psycopg2.Error as db_err:
        logging.error(f"Ошибка PostgreSQL при очистке контекста для conv_id {user_conv_id}: {db_err}")
        return jsonify({"status": "error", "message": "Ошибка базы данных при очистке контекста"}), 500
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при очистке контекста для conv_id {user_conv_id}: {e}")
        return jsonify({"status": "error", "message": "Внутренняя ошибка сервера при очистке контекста"}), 500


# НОВЫЙ ЭНДПОИНТ для уведомлений от веб-интерфейса
@app.route("/operator_message_sent", methods=["POST"])
def operator_message_sent():
    """
    Эндпоинт, вызываемый веб-интерфейсом оператора после того, как оператор отправил сообщение.
    Ставит основного бота на "паузу" для данного диалога.
    """
    data = request.json
    conv_id_from_request = data.get("conv_id")

    if conv_id_from_request is None: # Проверяем, что conv_id вообще есть
        logging.warning("Получен запрос /operator_message_sent без conv_id в теле JSON.")
        return jsonify({"status": "error", "message": "conv_id is required in JSON body"}), 400

    try:
        conv_id = int(conv_id_from_request) # Преобразуем в int для согласованности
    except ValueError:
        logging.warning(f"Получен некорректный conv_id в /operator_message_sent: '{conv_id_from_request}'. Не является числом.")
        return jsonify({"status": "error", "message": "Invalid conv_id format, must be an integer"}), 400
    
    logging.info(f"Получено уведомление от веб-интерфейса: оператор отправил сообщение в диалог {conv_id}")

    # 1. Отменяем существующий таймер ожидания ответа клиента (если есть)
    if conv_id in client_timers:
        client_timers[conv_id].cancel()
        # del client_timers[conv_id] # Удаляем, чтобы не было ссылок на отмененный таймер
        logging.info(f"Клиентский таймер для диалога {conv_id} отменен из-за активности оператора.")
    
    # 2. Очищаем буфер сообщений пользователя для этого диалога
    if conv_id in user_buffers:
        user_buffers[conv_id] = []
        logging.info(f"Буфер сообщений пользователя для диалога {conv_id} очищен из-за активности оператора.")

    # 3. Устанавливаем/обновляем операторский таймер на 15 минут
    if conv_id in operator_timers:
        operator_timers[conv_id].cancel() # Отменяем предыдущий операторский таймер, если он был

    # Функция clear_operator_timer должна быть определена где-то выше
    # (она просто удаляет таймер из словаря operator_timers)
    op_timer = threading.Timer(15 * 60, clear_operator_timer, args=(conv_id,))
    operator_timers[conv_id] = op_timer
    op_timer.start()
    logging.info(f"Операторский таймер на 15 минут установлен/обновлен для диалога {conv_id}.")

    return jsonify({"status": "success", "message": f"Operator activity processed for conv_id {conv_id}"}), 200


def clear_operator_timer(conv_id_for_timer): # Переименовал conv_id
    """
    Вызывается по истечении 15 минут после последнего сообщения оператора (или уведомления от веб-интерфейса).
    Удаляет запись об операторском таймере для данного диалога,
    позволяя боту снова отвечать, если клиент напишет после этого.
    """
    if conv_id_for_timer in operator_timers:
        del operator_timers[conv_id_for_timer]
        logging.info(f"Операторский таймер для диалога {conv_id_for_timer} истёк и был удален.")
    # else: # Это сообщение может быть излишним, если таймер был отменен и удален ранее
        # logging.info(f"Попытка удалить истекший операторский таймер для диалога {conv_id_for_timer}, но он уже не существует.")


# Глобальная память для недавних event_id (для дедупликации callback-ов VK)
recent_event_ids = {}  # {event_id: float(time.time())}
EVENT_ID_TTL = 30       # Сколько секунд хранить event_id (в секундах)

# ==============================
# 8. ИНТЕГРАЦИЯ С GEMINI
# ==============================
def find_relevant_titles_with_gemini(user_question_text): # Переименовал user_question
    """
    Использует Gemini для выбора до трех наиболее релевантных заголовков
    из базы знаний к вопросу пользователя.
    """
    if not GEMINI_API_KEY:
        logging.warning("GEMINI_API_KEY не настроен. Поиск релевантных заголовков через Gemini невозможен.")
        return []
    
    if not knowledge_base: # Если база знаний пуста
        logging.info("База знаний пуста. Поиск релевантных заголовков не выполняется.")
        return []

    titles = list(knowledge_base.keys())
    # Формируем промпт для Gemini
    prompt_text = f"""
Ты — ассистент, помогающий найти наиболее подходящие вопросы из предоставленного списка к запросу пользователя.
Вот список доступных вопросов-ключей (каждый вопрос - уникальный элемент):
--- НАЧАЛО СПИСКА ВОПРОСОВ ---
{', '.join(titles)}
--- КОНЕЦ СПИСКА ВОПРОСОВ ---

Проанализируй следующий запрос пользователя:
"{user_question_text}"

Твоя задача: выбрать из СПИСКА ВОПРОСОВ не более трех (3) наиболее релевантных вопросов-ключей к этому запросу.
Крайне важно:
1.  Ты должен возвращать ТОЛЬКО вопросы-ключи ИЗ ПРЕДОСТАВЛЕННОГО СПИСКА.
2.  Не выдумывай новые вопросы и не изменяй формулировки существующих.
3.  Если подходящих вопросов нет, верни пустой ответ.
4.  Если нашел подходящие вопросы, верни их СТРОГО по одному в каждой строке, без нумерации, без пояснений, без каких-либо дополнительных слов или символов.

Пример ответа, если найдено два вопроса:
Вопрос-ключ из списка 1
Вопрос-ключ из списка 2

Ответ:
    """.strip()

    payload = {"contents": [{"parts": [{"text": prompt_text}]}]} # Переименовал data в payload
    headers_gemini = {"Content-Type": "application/json"} # Переименовал headers

    # Попытки запроса к Gemini с простой задержкой
    for attempt in range(3): # Уменьшил количество попыток до 3
        try:
            response = requests.post(GEMINI_API_URL, headers=headers_gemini, json=payload, timeout=20) # Увеличил таймаут
            response.raise_for_status() # Проверка на HTTP ошибки

            result = response.json()
            if 'candidates' in result and result['candidates'][0]['content']['parts'][0]['text']:
                text_raw = result['candidates'][0]['content']['parts'][0]['text']
                lines = text_raw.strip().split("\n")
                # Фильтруем, чтобы убедиться, что возвращенные строки действительно являются ключами из БЗ
                relevant_titles_found = [ln.strip() for ln in lines if ln.strip() and ln.strip() in knowledge_base]
                logging.info(f"Gemini нашел релевантные заголовки: {relevant_titles_found} для вопроса: '{user_question_text}'")
                return relevant_titles_found[:3] # Возвращаем не более трех
            else:
                # Если Gemini вернул пустой текст или некорректную структуру
                logging.warning(f"Gemini вернул пустой или некорректный ответ для поиска заголовков: {result}")
                return []
        except requests.Timeout:
            logging.warning(f"Тайм-аут при запросе к Gemini для поиска заголовков (попытка {attempt + 1}).")
            if attempt < 2: time.sleep(5) # Пауза перед следующей попыткой
        except requests.RequestException as e:
            logging.error(f"Ошибка запроса к Gemini для поиска заголовков (попытка {attempt + 1}): {e}")
            if attempt < 2: time.sleep(5)
        except (KeyError, IndexError) as e:
            logging.error(f"Ошибка обработки ответа от Gemini при поиске заголовков: {e}. Ответ: {result if 'result' in locals() else 'Нет ответа'}")
            return [] # Не повторяем попытку при ошибке парсинга
        except Exception as e: # Обработка других неожиданных ошибок
            logging.error(f"Непредвиденная ошибка при поиске заголовков через Gemini: {e}")
            return []
            
    logging.warning("Не удалось получить релевантные заголовки от Gemini после нескольких попыток.")
    return []


def generate_response(user_question_text, client_data_text, dialog_history_list, current_custom_prompt, user_first_name, relevant_kb_titles=None):
    """
    Генерирует ответ от модели Gemini с учётом истории диалога и подсказок из базы знаний.
    Переименовал аргументы для ясности.
    """
    if not GEMINI_API_KEY:
        logging.warning("GEMINI_API_KEY не настроен. Генерация ответа через Gemini невозможна.")
        return "Извините, я временно не могу обработать ваш запрос (отсутствует API-ключ)."

    history_lines_for_prompt = []
    last_sender_role = None # Для отслеживания последнего отправителя, чтобы не дублировать роль
    
    for turn in dialog_history_list: # dialog_history_list - это список словарей
        role = list(turn.keys())[0] # 'user', 'bot', 'operator'
        message_content = turn[role] # Текст сообщения (уже с меткой времени)
        
        # Извлекаем чистый текст сообщения без [timestamp] для промпта, если это нужно
        # В данном случае, оставляем timestamp, т.к. он может нести доп. информацию о времени сообщения
        # message_clean = remove_trailing_punctuation(message_content.split("]", 1)[-1].strip() if "]" in message_content else message_content)

        sender_name_for_prompt = ""
        if role == "user":
            sender_name_for_prompt = user_first_name if user_first_name else "Пользователь"
        elif role == "bot":
            sender_name_for_prompt = "Модель"
        elif role == "operator":
            sender_name_for_prompt = "Оператор"
        else: # Пропускаем неизвестные роли
            continue
        
        # Добавляем роль, только если она сменилась или это первое сообщение
        # if last_sender_role != sender_name_for_prompt:
        #    history_lines_for_prompt.append(f"{sender_name_for_prompt}:")
        # history_lines_for_prompt.append(f"  {message_content.strip()}") # Отступ для сообщений
        
        # Более простой вариант: просто "Роль: Сообщение"
        history_lines_for_prompt.append(f"{sender_name_for_prompt}: {message_content.strip()}")
        last_sender_role = sender_name_for_prompt

    history_text_for_prompt = "\n".join(history_lines_for_prompt)

    knowledge_hint_text = ""
    if relevant_kb_titles and knowledge_base:
        kb_lines = []
        for key_title in relevant_kb_titles: # key_title - это уже реальный ключ из БЗ
            # matched_key = match_kb_key_ignoring_trailing_punc(key_title, knowledge_base) # Эта проверка уже не нужна, если relevant_kb_titles содержит реальные ключи
            if key_title in knowledge_base: # Убедимся, что ключ все еще есть
                value = str(knowledge_base[key_title]).strip()
                kb_lines.append(f"- {key_title}: {value}") # Форматируем как элемент списка
        if kb_lines:
            knowledge_hint_text = "Контекст из базы знаний:\n" + "\n".join(kb_lines)

    # Собираем полный промпт для модели
    prompt_parts = [current_custom_prompt]
    if history_text_for_prompt:
        prompt_parts.append(f"История диалога:\n{history_text_for_prompt}")
    if client_data_text.strip(): # client_data_text - это строка с информацией о клиенте
        prompt_parts.append(f"Дополнительная информация о клиенте:\n{client_data_text.strip()}")
    if knowledge_hint_text:
        prompt_parts.append(knowledge_hint_text)
    
    prompt_parts.append(f"Текущий вопрос от {user_first_name if user_first_name else 'Пользователя'}: {user_question_text}")
    prompt_parts.append("Твой ответ (Модель):") # Явное указание, чей ответ ожидается
    
    full_prompt_text = "\n\n".join(prompt_parts)

    # Логирование полного промпта (опционально, может быть очень длинным)
    # logging.debug(f"Полный промпт для Gemini:\n{full_prompt_text}")
    # Сохранение промпта в файл для отладки
    prompt_log_filename = f"prompt_gemini_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S_%f')}.txt"
    prompt_log_filepath = os.path.join(LOGS_DIRECTORY, prompt_log_filename)
    try:
        with open(prompt_log_filepath, "w", encoding="utf-8") as pf:
            pf.write(full_prompt_text)
        # upload_log_to_yandex_disk(prompt_log_filepath) # Загрузка на Я.Диск может быть избыточной для каждого промпта
        logging.info(f"Полный промпт для Gemini сохранён в: {prompt_log_filepath}")
    except Exception as e:
        logging.error(f"Ошибка при записи промпта Gemini в файл '{prompt_log_filepath}': {e}")

    payload_gemini = {"contents": [{"parts": [{"text": full_prompt_text}]}]} # Переименовал data
    headers_gemini_req = {"Content-Type": "application/json"} # Переименовал headers

    for attempt in range(3): # Уменьшил количество попыток
        try:
            response = requests.post(GEMINI_API_URL, headers=headers_gemini_req, json=payload_gemini, timeout=30) # Увеличил таймаут
            response.raise_for_status()
            result = response.json()

            if 'candidates' in result and result['candidates'][0]['content']['parts'][0]['text']:
                model_response_text = result['candidates'][0]['content']['parts'][0]['text'].strip()
                logging.info(f"Ответ от Gemini получен: '{model_response_text[:200]}...'") # Логируем начало ответа
                return model_response_text
            else: # Некорректный ответ
                logging.error(f"Gemini вернул некорректный ответ: {result}")
                # Не возвращаем ошибку сразу, даем шанс другим попыткам, если это не последняя
                if attempt == 2 : return "Извините, произошла ошибка при обработке ответа модели (некорректный формат)."
        
        except requests.Timeout:
            logging.warning(f"Тайм-аут при запросе к Gemini (попытка {attempt + 1}).")
            if attempt < 2: time.sleep(5)
            elif attempt == 2: return "Извините, сервис временно перегружен. Пожалуйста, попробуйте позже. (Таймаут)"
        except requests.RequestException as e: # Включает HTTPError от raise_for_status
            status_code = e.response.status_code if e.response is not None else "N/A"
            logging.error(f"Ошибка запроса к Gemini (попытка {attempt + 1}), статус: {status_code}: {e}")
            if status_code == 503: # Service Unavailable
                 return "Ой! Извините, я сейчас перегружен. Если ваш вопрос срочный, пожалуйста, напишите 'оператор'."
            if attempt < 2: time.sleep(5)
            elif attempt == 2: return f"Произошла ошибка при обращении к сервису (код: {status_code}). Пожалуйста, попробуйте позже."
        except (KeyError, IndexError) as e: # Ошибка парсинга ответа
            logging.error(f"Ошибка обработки ответа от Gemini: {e}. Ответ: {result if 'result' in locals() else 'Нет ответа'}")
            return "Извините, произошла ошибка при обработке ответа модели (ошибка парсинга)." # Не повторяем
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при генерации ответа через Gemini: {e}")
            return "Произошла непредвиденная ошибка. Пожалуйста, попробуйте позже."


    logging.error("Не удалось получить ответ от Gemini после нескольких попыток.")
    return "Извините, я сейчас не могу ответить. Пожалуйста, попробуйте позже или напишите 'оператор', если вопрос срочный."


def generate_summary_and_reason(dialog_history_list_for_summary): # Переименовал dialog_history
    """
    Генерирует сводку диалога и предполагаемую причину запроса оператора с помощью Gemini.
    """
    if not GEMINI_API_KEY:
        logging.warning("GEMINI_API_KEY не настроен. Генерация сводки и причины невозможна.")
        return "Сводка не сформирована (API-ключ отсутствует)", "Причина не определена (API-ключ отсутствует)"

    # Формируем текстовое представление истории диалога для промпта
    history_text_parts = []
    for turn in dialog_history_list_for_summary[-10:]: # Берем последние 10 реплик для краткости
        role = list(turn.keys())[0]
        message_content = turn[role]
        # Можно извлечь чистый текст, если метки времени мешают
        # message_clean = remove_trailing_punctuation(message_content.split("]", 1)[-1].strip() if "]" in message_content else message_content)
        
        sender_name = "Неизвестно"
        if role == 'user': sender_name = "Пользователь"
        elif role == 'bot': sender_name = "Модель"
        elif role == 'operator': sender_name = "Оператор"
        
        history_text_parts.append(f"{sender_name}: {message_content.strip()}")
    
    history_text_for_prompt = "\n".join(history_text_parts)

    prompt_text = f"""
Проанализируй следующий диалог между Пользователем, Моделью и Оператором:
--- НАЧАЛО ДИАЛОГА ---
{history_text_for_prompt}
--- КОНЕЦ ДИАЛОГА ---

Пользователь запросил помощь оператора. Твоя задача:
1.  Сформируй ОЧЕНЬ КРАТКУЮ сводку сути обсуждения (не более 1-2 предложений).
2.  Предположи НАИБОЛЕЕ ВЕРОЯТНУЮ причину, почему пользователь запросил оператора (1 предложение).

Верни ответ СТРОГО в две строки:
Строка 1: <Краткая сводка обсуждения>
Строка 2: <Предполагаемая причина запроса оператора>

Пример:
Строка 1: Обсуждали проблему с доступом к курсу после оплаты.
Строка 2: Модель не смогла решить техническую проблему пользователя.

Ответ:
    """.strip()

    payload = {"contents": [{"parts": [{"text": prompt_text}]}]}
    headers_req = {"Content-Type": "application/json"}

    for attempt in range(2): # Достаточно 2 попыток для этой задачи
        try:
            response = requests.post(GEMINI_API_URL, headers=headers_req, json=payload, timeout=15)
            response.raise_for_status()
            result = response.json()

            if 'candidates' in result and result['candidates'][0]['content']['parts'][0]['text']:
                output_text = result['candidates'][0]['content']['parts'][0]['text'].strip()
                parts = output_text.split("\n", 1) # Разделяем на две части по первому переносу строки
                dialog_summary_text = parts[0].strip() if len(parts) > 0 else "Сводка не сформирована"
                reason_guess_text = parts[1].strip() if len(parts) > 1 else "Причина не определена"
                logging.info(f"Сводка для запроса оператора: '{dialog_summary_text}', Причина: '{reason_guess_text}'")
                return dialog_summary_text, reason_guess_text
            else:
                logging.warning(f"Gemini вернул пустой или некорректный ответ для сводки: {result}")
                if attempt == 1: break # Выходим после последней попытки

        except requests.Timeout:
            logging.warning(f"Тайм-аут при запросе к Gemini для сводки (попытка {attempt + 1}).")
            if attempt < 1: time.sleep(3)
        except requests.RequestException as e:
            logging.error(f"Ошибка запроса к Gemini для сводки (попытка {attempt + 1}): {e}")
            if attempt < 1: time.sleep(3)
        except (KeyError, IndexError): # Ошибка парсинга
             logging.error(f"Ошибка обработки ответа от Gemini при генерации сводки. Ответ: {result if 'result' in locals() else 'Нет ответа'}")
             break # Не повторяем при ошибке парсинга
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при генерации сводки через Gemini: {e}")
            break

    logging.error("Не удалось сгенерировать сводку и причину от Gemini.")
    return "Не удалось сформировать сводку (ошибка сервиса)", "Не удалось определить причину (ошибка сервиса)"


# =====================================
# 11. ОБРАБОТКА ПОСТУПИВШЕГО СООБЩЕНИЯ ИЗ VK CALLBACK
# =====================================
# Эта функция будет сильно изменена, чтобы не сохранять сырые сообщения пользователя в БД,
# а только буферизовать их.

def handle_new_message(user_id_from_vk, message_text_from_vk, vk_api_object, is_outgoing_message=False, conversation_id=None):
    """
    Обрабатывает новое сообщение, полученное через VK Callback API.
    - Сообщения от пользователя (is_outgoing_message=False) буферизуются.
    - Сообщения от оператора, отправленные через VK (is_outgoing_message=True), обрабатываются для сброса таймеров бота.
    - Сообщения от самого бота (is_outgoing_message=True, user_id_from_vk < 0) игнорируются.
    """
    
    # Если conversation_id не передан явно (например, для исходящих), используем user_id_from_vk
    # Для входящих от пользователя, conversation_id обычно равен user_id_from_vk.
    # Важно, чтобы conv_id был ID пользователя для корректного ведения диалога.
    actual_conv_id = conversation_id if conversation_id is not None else user_id_from_vk
    
    # Определяем роль и обрабатываем особые случаи
    if is_outgoing_message:
        if int(user_id_from_vk) < 0: # Сообщение от самого сообщества (бота)
            logging.info(f"[VK Callback] Исходящее сообщение от сообщества (user_id: {user_id_from_vk}), пропускаем.")
            return
        # Если сообщение исходящее и ID отправителя совпадает с OPERATOR_VK_ID (если он настроен и >0)
        # или если это просто исходящее сообщение от имени администратора/редактора группы, но не от бота.
        # Это сложный момент, т.к. `user_id_from_vk` в `message_new` с `out=1` - это ID ПОЛУЧАТЕЛЯ.
        # А `from_id` - это ID сообщества.
        # Для `message_reply` (ответ оператора из интерфейса VK), `from_id` будет ID оператора.
        # Поэтому, для исходящих, роль "operator" лучше устанавливать через прямой вызов /operator_message_sent
        # из веб-интерфейса. Здесь мы можем только предполагать.
        # Если OPERATOR_VK_ID настроен и совпадает с from_id в объекте сообщения (если доступно),
        # то это точно оператор. Но `user_id_from_vk` в аргументах это `msg['from_id']`.
        # Для исходящего `message_new` `msg['from_id']` будет ID сообщества.
        # Это означает, что через callback мы не можем надежно определить, что исходящее сообщение было от оператора,
        # если только не анализировать поле `admin_author_id` (если оно есть).
        # Проще всего эту логику не усложнять здесь, а полагаться на веб-интерфейс.
        # Здесь мы просто логируем исходящее сообщение, но не ставим операторский таймер на его основе.
        # Основной механизм паузы бота - через /operator_message_sent
        logging.info(f"[VK Callback] Зафиксировано исходящее сообщение для conv_id {actual_conv_id}. Текст: {message_text_from_vk[:50]}...")
        # Не предпринимаем активных действий по установке операторской паузы здесь,
        # так как это должно делаться через прямой вызов /operator_message_sent
        # или если оператор пишет через интерфейс VK, то `handle_new_message` для `message_reply` 
        # может попробовать определить это (но это менее надежно).
        # Просто запишем это сообщение в лог-файл, если нужно.
        # Роль будет "bot" или "unknown_sender" для простоты, т.к. это не оператор через веб-интерфейс.
        role_for_log = "bot_vk_callback" # Условная роль для такого сообщения
    else: # Входящее сообщение от пользователя
        role_for_log = "user"

    # Загружаем историю диалога из памяти или БД
    if actual_conv_id not in dialog_history_dict:
        dialog_history_dict[actual_conv_id] = load_dialog_from_db(actual_conv_id)
    
    # Получаем имя пользователя (или используем кеш)
    # user_id_from_vk должен быть ID пользователя, а не сообщества.
    # Для входящих user_id_from_vk = from_id (ID пользователя).
    # Для исходящих через message_new, user_id_from_vk = from_id (ID сообщества), peer_id = ID пользователя.
    # Поэтому, для корректного получения имени, нужен ID конечного пользователя.
    # В callback() мы передаем from_id как user_id, и from_id как conv_id. Это правильно для входящих.
    user_display_id_for_name = actual_conv_id # Используем conv_id, т.к. он должен быть ID пользователя

    first_name, last_name = get_vk_user_full_name(user_display_id_for_name)
    full_name_display = f"{first_name} {last_name}".strip()
    if not full_name_display: full_name_display = f"User_{user_display_id_for_name}"

    # Определение пути к лог-файлу для этого диалога
    if actual_conv_id not in user_log_files:
        # Создаем имя файла только один раз для каждого пользователя
        now_for_filename = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        # Очищаем имя от недопустимых символов для имени файла
        safe_display_name = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in full_name_display).replace(' ', '_')
        log_file_name = f"dialog_{now_for_filename}_{actual_conv_id}_{safe_display_name}.txt"
        user_log_files[actual_conv_id] = os.path.join(LOGS_DIRECTORY, log_file_name)
    
    current_log_file_path = user_log_files[actual_conv_id]
    
    # Формируем сообщение с временной меткой для локального лога (сырых данных)
    # Используем время сервера +6 часов (как в предыдущих версиях)
    current_server_time_adjusted = datetime.utcnow() + timedelta(hours=6)
    formatted_log_time = current_server_time_adjusted.strftime("%Y-%m-%d_%H-%M-%S")
    
    # Запись в локальный лог-файл (сырые сообщения)
    log_entry_text = ""
    if role_for_log == "user":
        log_entry_text = f"[{formatted_log_time}] {full_name_display} (raw VK): {message_text_from_vk}\n"
    elif role_for_log == "bot_vk_callback": # Исходящее сообщение, зафиксированное callback'ом
        log_entry_text = f"[{formatted_log_time}] Сообщество (исходящее VK): {message_text_from_vk}\n"
    
    if log_entry_text: # Пишем в лог, если есть что писать
        try:
            with open(current_log_file_path, "a", encoding="utf-8") as log_f:
                log_f.write(log_entry_text)
            # Загрузка на Яндекс.Диск каждого сырого сообщения может быть избыточной.
            # Лучше загружать лог после завершения сессии или периодически.
            # upload_log_to_yandex_disk(current_log_file_path) 
        except Exception as e:
            logging.error(f"Ошибка записи в локальный лог-файл '{current_log_file_path}': {e}")

    # ==================================================================
    # Логика обработки для ВХОДЯЩИХ сообщений от пользователя
    # ==================================================================
    if role_for_log == "user":
        # Если активен операторский таймер, ИГНОРИРУЕМ сообщение пользователя (не отвечаем, не буферизуем)
        # Оператор должен сам увидеть это сообщение в веб-интерфейсе.
        if actual_conv_id in operator_timers:
            logging.info(f"Сообщение от пользователя {actual_conv_id} получено, но операторский таймер активен. Сообщение не будет обработано ботом.")
            # Сообщение уже записано в сырой лог выше. Оператор увидит его в истории.
            # Ничего не делаем, чтобы бот не отвечал и не сбрасывал операторскую паузу.
            return

        # Проверяем, не является ли отправитель сообщения OPERATOR_VK_ID (если настроено)
        # Это для случая, если оператор пишет из приложения VK как обычный пользователь.
        # И мы не хотим, чтобы бот ему отвечал или обрабатывал его сообщения как клиентские.
        if OPERATOR_VK_ID > 0 and int(user_id_from_vk) == OPERATOR_VK_ID:
            logging.info(f"Сообщение от VK ID оператора ({OPERATOR_VK_ID}) в диалоге {actual_conv_id}. Игнорируется для автоматической обработки.")
            # Можно добавить логику, если оператор дает команды боту таким образом, но пока просто игнорируем.
            return

        # Добавляем сообщение пользователя в буфер
        user_buffers.setdefault(actual_conv_id, []).append(message_text_from_vk)
        logging.info(f"Сообщение от {full_name_display} (conv_id: {actual_conv_id}) добавлено в буфер. Буфер: {user_buffers[actual_conv_id]}")

        # Отправка уведомления в Telegram о ПЕРВОМ сообщении в диалоге (если история пуста)
        # Проверяем, действительно ли это первое сообщение, посмотрев в dialog_history_dict
        # (которое загружается из БД и должно быть пустым для нового диалога)
        if not dialog_history_dict.get(actual_conv_id): # Если для этого conv_id еще нет истории в памяти
            is_truly_first_message = True
            # Дополнительная проверка в БД, если есть сомнения (хотя load_dialog_from_db уже должен был это сделать)
            # try:
            #     conn_check = psycopg2.connect(DATABASE_URL)
            #     cur_check = conn_check.cursor()
            #     cur_check.execute("SELECT 1 FROM dialogues WHERE conv_id = %s LIMIT 1", (actual_conv_id,))
            #     if cur_check.fetchone(): is_truly_first_message = False
            #     cur_check.close()
            #     conn_check.close()
            # except Exception as e_db_check: logging.error(f"Ошибка проверки БД на первое сообщение: {e_db_check}")

            if is_truly_first_message and "оператор" not in message_text_from_vk.lower():
                 send_telegram_notification(
                     user_question_text=message_text_from_vk, 
                     dialog_id=actual_conv_id, 
                     first_name=first_name, 
                     last_name=last_name
                 )
        
        # Если в тексте сообщения есть "оператор" (даже если это первое сообщение)
        if "оператор" in message_text_from_vk.lower():
            # Используем текущую историю из dialog_history_dict (которая была загружена из БД)
            # и добавляем к ней текущее сообщение из буфера для полноты картины
            temp_history_for_summary = list(dialog_history_dict.get(actual_conv_id, []))
            # Добавляем текущее (еще не обработанное) сообщение в историю для сводки
            temp_history_for_summary.append({'user': message_text_from_vk})

            summary, reason = generate_summary_and_reason(temp_history_for_summary)
            
            # В качестве "изначального вопроса" можно взять первое сообщение из буфера
            initial_q_for_op_notify = user_buffers[actual_conv_id][0] if user_buffers[actual_conv_id] else message_text_from_vk
            
            send_operator_request_notification(
                dialog_id=actual_conv_id, 
                initial_question=initial_q_for_op_notify, 
                dialog_summary=summary, 
                reason_guess=reason, 
                first_name=first_name, 
                last_name=last_name
            )
            # Важно: после запроса оператора, мы НЕ ставим операторский таймер здесь.
            # Оператор должен сам взять диалог в работу через веб-интерфейс,
            # и только тогда веб-интерфейс вызовет /operator_message_sent.
            # Бот продолжит работать в обычном режиме (ответит после задержки),
            # если оператор не вмешается. Это дает шанс боту все же ответить,
            # если запрос "оператор" был не очень настойчивым.

        # Перезапускаем таймер ожидания (буферизации) для пользователя
        if actual_conv_id in client_timers:
            client_timers[actual_conv_id].cancel()
        
        # vk_api_object передается в generate_and_send_response, чтобы он мог отправить ответ
        client_timer_thread = threading.Timer(
            USER_MESSAGE_BUFFERING_DELAY, 
            generate_and_send_response, 
            args=(actual_conv_id, vk_api_object) # Передаем объект VK API
        )
        client_timers[actual_conv_id] = client_timer_thread
        client_timer_thread.start()
        logging.info(f"Клиентский таймер на {USER_MESSAGE_BUFFERING_DELAY}с для диалога {actual_conv_id} установлен/перезапущен.")

# =====================================
# 12. ФОРМИРОВАНИЕ И ОТПРАВКА ОТВЕТА БОТА ПОСЛЕ ЗАДЕРЖКИ (из generate_and_send_response)
# =====================================
def generate_and_send_response(conv_id_to_respond, vk_api_for_sending): # Переименовал аргументы
    """
    Вызывается по истечении USER_MESSAGE_BUFFERING_DELAY.
    Формирует единый текст из буфера user_buffers, получает ответ от Gemini,
    сохраняет ОБЪЕДИНЕННОЕ сообщение пользователя и ответ бота в БД и в dialog_history_dict,
    а затем отправляет ответ пользователю через VK API.
    """
    # Проверяем, не активен ли операторский таймер. Если да, бот не должен отвечать.
    if conv_id_to_respond in operator_timers:
        logging.info(f"Ответ для conv_id {conv_id_to_respond} не будет сгенерирован: операторский таймер активен.")
        # Важно: НЕ очищаем user_buffers здесь, так как оператор может еще не видеть эти сообщения.
        # Если операторский таймер истечет, и пользователь напишет снова, буфер обработается.
        # Если оператор напишет через веб-интерфейс, тот вызовет /operator_message_sent, который очистит буфер.
        return

    # Получаем накопленные сообщения из буфера
    buffered_messages = user_buffers.get(conv_id_to_respond, [])
    if not buffered_messages:
        logging.info(f"Нет сообщений в буфере для conv_id {conv_id_to_respond}. Ответ не генерируется.")
        return

    # Объединяем сообщения из буфера в один текст
    combined_user_text = "\n".join(buffered_messages).strip()
    # Очищаем буфер ПОСЛЕ того, как сообщения из него извлечены
    user_buffers[conv_id_to_respond] = []
    logging.info(f"Сообщения для conv_id {conv_id_to_respond} извлечены из буфера. Объединенный текст: '{combined_user_text[:100]}...'")

    # Получаем имя пользователя для использования в промпте и логах
    first_name, last_name = get_vk_user_full_name(conv_id_to_respond)
    user_display_name = f"{first_name} {last_name}".strip() if first_name or last_name else f"User_{conv_id_to_respond}"


    # Получаем информацию о клиенте из Excel (если функция используется)
    client_data_from_excel = get_client_info(combined_user_text, conv_id_to_respond)
    
    # Загружаем текущую историю диалога из dialog_history_dict (которая синхронизируется с БД)
    # Эта история НЕ будет содержать текущий combined_user_text, его мы добавим ниже.
    current_dialog_history = list(dialog_history_dict.get(conv_id_to_respond, []))

    # Ищем релевантные заголовки из базы знаний для объединенного текста
    relevant_titles_from_kb = find_relevant_titles_with_gemini(combined_user_text)

    # Генерируем ответ модели
    bot_response_text = generate_response(
        user_question_text=combined_user_text,
        client_data_text=client_data_from_excel,
        dialog_history_list=current_dialog_history, # Передаем историю ДО текущего вопроса пользователя
        current_custom_prompt=custom_prompt,
        user_first_name=first_name,
        relevant_kb_titles=relevant_titles_from_kb
    )
    
    # Формируем временные метки для записи в БД и логи (единые для пары "вопрос-ответ")
    # Используем время UTC для БД, и локальное (сервер+6ч) для метки в тексте сообщения
    timestamp_utc_for_db = datetime.utcnow()
    # Для метки в тексте сообщения, как это было сделано в handle_new_message и web_interface
    timestamp_in_message_text = (timestamp_utc_for_db + timedelta(hours=6)).strftime("%Y-%m-%d_%H-%M-%S")

    # 1. Сохраняем ОБЪЕДИНЕННОЕ сообщение пользователя в БД и в dialog_history_dict
    user_message_with_ts_for_storage = f"[{timestamp_in_message_text}] {combined_user_text}"
    store_dialog_in_db(
        conv_id=conv_id_to_respond, 
        role="user", 
        message_text_with_timestamp=user_message_with_ts_for_storage,
        client_info=client_data_from_excel # Сохраняем client_info вместе с сообщением пользователя
    )
    dialog_history_dict.setdefault(conv_id_to_respond, []).append(
        {"user": user_message_with_ts_for_storage, "client_info": client_data_from_excel}
    )

    # 2. Сохраняем ответ БОТА в БД и в dialog_history_dict
    bot_message_with_ts_for_storage = f"[{timestamp_in_message_text}] {bot_response_text}"
    store_dialog_in_db(
        conv_id=conv_id_to_respond, 
        role="bot", 
        message_text_with_timestamp=bot_message_with_ts_for_storage,
        client_info="" # Ответ бота обычно не имеет своего client_info
    )
    dialog_history_dict.setdefault(conv_id_to_respond, []).append(
        {"bot": bot_message_with_ts_for_storage}
    )
    
    # 3. Локальное логирование в файл (дополняем тот же файл, куда писались raw сообщения)
    log_file_path_for_processed = user_log_files.get(conv_id_to_respond)
    if log_file_path_for_processed: # Если путь к файлу известен
        try:
            with open(log_file_path_for_processed, "a", encoding="utf-8") as log_f:
                log_f.write(f"[{timestamp_in_message_text}] {user_display_name} (processed): {combined_user_text}\n")
                if client_data_from_excel:
                    log_f.write(f"[{timestamp_in_message_text}] Информация по клиенту (для processed): {client_data_from_excel}\n")
                if relevant_titles_from_kb:
                    log_f.write(f"[{timestamp_in_message_text}] Найденные ключи БЗ (для processed): {', '.join(relevant_titles_from_kb)}\n")
                log_f.write(f"[{timestamp_in_message_text}] Модель: {bot_response_text}\n\n")
            
            # Загружаем обновленный лог-файл на Яндекс.Диск
            upload_log_to_yandex_disk(log_file_path_for_processed)
        except Exception as e:
            logging.error(f"Ошибка записи в локальный лог-файл (processed) '{log_file_path_for_processed}': {e}")
    else:
        logging.warning(f"Путь к лог-файлу для conv_id {conv_id_to_respond} не найден. Логирование обработанных сообщений пропущено.")

    # 4. Отправляем ответ пользователю через VK API
    if vk_api_for_sending:
        try:
            vk_api_for_sending.messages.send(
                user_id=conv_id_to_respond,
                message=bot_response_text,
                random_id=int(time.time() * 1000000) # Увеличиваем для большей уникальности
            )
            logging.info(f"Ответ бота успешно отправлен пользователю {conv_id_to_respond}.")
        except vk_api.ApiError as e:
            logging.error(f"VK API Ошибка при отправке сообщения пользователю {conv_id_to_respond}: {e}")
        except Exception as e: # Другие ошибки, например, сетевые
            logging.error(f"Неизвестная ошибка при отправке сообщения VK пользователю {conv_id_to_respond}: {e}")
    else:
        logging.warning(f"Объект VK API не передан в generate_and_send_response для conv_id {conv_id_to_respond}. Сообщение не отправлено.")


# ==============================
# 13. ОБРАБОТЧИК CALLBACK ОТ VK И ЗАПУСК ПРИЛОЖЕНИЯ
# ==============================
@app.route("/callback", methods=["POST"])
def callback_handler(): # Переименовал callback в callback_handler
    data_from_vk = request.json

    # Опционально: сохраняем весь payload для отладки
    # save_callback_payload(data_from_vk)

    # 1. Проверка типа события и секрета (если используется)
    event_type = data_from_vk.get("type")
    if VK_SECRET_KEY and data_from_vk.get("secret") != VK_SECRET_KEY:
        logging.warning("Callback: Неверный секретный ключ.")
        return "forbidden", 403 # Явный ответ об ошибке

    # 2. Обработка confirmation-запроса от VK
    if event_type == "confirmation":
        if not VK_CONFIRMATION_TOKEN:
            logging.error("Callback: VK_CONFIRMATION_TOKEN не установлен!")
            return "error", 500 # Не можем подтвердить без токена
        logging.info("Callback: получен confirmation запрос, отправляем токен подтверждения.")
        return VK_CONFIRMATION_TOKEN, 200

    # 3. Дедупликация событий по event_id
    event_id = data_from_vk.get("event_id")
    if event_id: # Если event_id есть
        current_time_ts = time.time()
        # Очищаем старые event_id из кеша
        for eid in list(recent_event_ids.keys()):
            if current_time_ts - recent_event_ids[eid] > EVENT_ID_TTL:
                del recent_event_ids[eid]
        
        if event_id in recent_event_ids:
            logging.info(f"Callback: Дублирующийся event_id={event_id} (type={event_type}), пропускаем.")
            return "ok", 200 # Подтверждаем получение, но не обрабатываем
        else:
            recent_event_ids[event_id] = current_time_ts # Сохраняем новый event_id
    else: # Если event_id нет (маловероятно для message_new, но для подстраховки)
        logging.warning(f"Callback: отсутствует event_id в событии типа {event_type}.")


    # 4. Обрабатываем только нужные типы событий (message_new, message_reply)
    # message_edit можно добавить, если нужна обработка отредактированных сообщений
    if event_type not in ("message_new", "message_reply"):
        logging.info(f"Callback: Пропускаем событие типа '{event_type}'.")
        return "ok", 200

    # 5. Извлечение данных из объекта сообщения
    vk_message_object = data_from_vk.get("object")
    if not vk_message_object or not isinstance(vk_message_object, dict):
        # Для message_new/message_reply 'object' это само сообщение, если версия API < 5.107
        # Если версия API >= 5.107, то 'object' содержит 'message', который является объектом сообщения
        if 'message' in vk_message_object and isinstance(vk_message_object['message'], dict):
            msg_data = vk_message_object['message']
        else: # Старый формат или неожиданная структура
            msg_data = vk_message_object 
        
        if not msg_data or not isinstance(msg_data, dict):
             logging.warning(f"Callback: Неправильный формат 'object' или 'object.message' в событии {event_type}: {data_from_vk}")
             return "ok", 200
    else: # Если 'object' это напрямую объект сообщения (старые версии API callback)
        msg_data = vk_message_object


    message_text = msg_data.get("text", "")
    from_id = msg_data.get("from_id")  # ID отправителя (пользователь или админ при ответе)
    peer_id = msg_data.get("peer_id")  # ID диалога (куда было отправлено сообщение)
    # 'out': 1 для исходящего, 0 для входящего. Поле есть только у 'message_new'.
    # 'message_reply' не имеет поля 'out', это всегда ответ от имени сообщества (или админа).
    is_outgoing = True if msg_data.get("out") == 1 else False 
    
    # Для message_reply, from_id это ID администратора, peer_id это ID пользователя, кому ответили.
    # is_outgoing будет False для message_reply по логике выше (нет поля 'out').
    # Нужно скорректировать определение is_outgoing для message_reply.
    if event_type == "message_reply":
        is_outgoing = True # Ответ оператора из интерфейса VK это всегда "исходящее" с точки зрения логики бота
        # В message_reply, from_id - это ID админа, а peer_id - это ID пользователя.
        # Для единообразия, conv_id должен быть ID пользователя.
        if peer_id:
            conversation_id_for_handler = peer_id
            # user_id_for_handler = from_id # Это ID оператора, а не пользователя
        else: # Не должно быть для message_reply
            logging.warning(f"Callback (message_reply): отсутствует peer_id. from_id={from_id}")
            return "ok", 200
    elif event_type == "message_new":
        if is_outgoing: # Исходящее от имени бота (например, через API messages.send)
            # from_id будет ID сообщества, peer_id - ID пользователя
            conversation_id_for_handler = peer_id
            # user_id_for_handler = from_id # ID сообщества
        else: # Входящее от пользователя
            # from_id - ID пользователя, peer_id - тоже ID пользователя (или ID чата, если это беседа)
            conversation_id_for_handler = from_id # или peer_id, они должны совпадать для ЛС
            # user_id_for_handler = from_id
    else: # Не должно случиться из-за проверки event_type выше
        return "ok", 200

    if not from_id or not conversation_id_for_handler:
        logging.warning(f"Callback: Не удалось извлечь from_id или определить conversation_id из события {event_type}: {msg_data}")
        return "ok", 200
    
    # Пропускаем сообщения без текста (например, стикеры без текстового сопровождения, аудио и т.д.)
    if not message_text.strip() and not msg_data.get("attachments"): # Если нет текста и нет вложений
        logging.info(f"Callback: Получено пустое сообщение (без текста и вложений) от from_id {from_id} в conv_id {conversation_id_for_handler}. Пропускаем.")
        return "ok", 200
    elif not message_text.strip() and msg_data.get("attachments"):
        # Если есть вложения, но нет текста, можно заменить текст на плейсхолдер
        message_text = "[Вложение без текста]" # Или более специфично, если анализировать тип вложения
        logging.info(f"Callback: Сообщение от from_id {from_id} с вложением, но без текста. Установлен плейсхолдер.")


    # Создаем объект VK API для передачи в handle_new_message, если он потребуется для ответа
    # Это нужно только если handle_new_message или generate_and_send_response будут слать через него.
    # В нашем случае generate_and_send_response использует его.
    vk_session_for_handler = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk_api_local = vk_session_for_handler.get_api()

    # Вызываем основную функцию обработки сообщения
    # user_id_from_vk = from_id (кто инициировал событие)
    # conversation_id = ID пользователя, с которым идет диалог
    # is_outgoing = флаг исходящего сообщения
    handle_new_message(
        user_id_from_vk=from_id, 
        message_text_from_vk=message_text, 
        vk_api_object=vk_api_local, 
        is_outgoing_message=is_outgoing, 
        conversation_id=conversation_id_for_handler
    )

    return "ok", 200


if __name__ == "__main__":
    if not DATABASE_URL:
        logging.critical("Переменная окружения DATABASE_URL не установлена. Приложение не может запуститься.")
        exit(1) # Выход, если нет подключения к БД
    if not VK_COMMUNITY_TOKEN or not VK_CONFIRMATION_TOKEN:
        logging.critical("Переменные окружения VK_COMMUNITY_TOKEN или VK_CONFIRMATION_TOKEN не установлены. Приложение не может корректно обрабатывать запросы VK.")
        # Можно разрешить запуск для локальной отладки без VK, но для продакшена это критично.
        # exit(1) 
    if not GEMINI_API_KEY:
        logging.warning("Переменная окружения GEMINI_API_KEY не установлена. Функционал Gemini будет недоступен.")
    
    logging.info("Запуск Flask-приложения основного бота...")
    # Railway предоставляет переменную PORT. Используем ее.
    # Для локального запуска можно установить значение по умолчанию, например, 5000.
    server_port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=server_port, debug=False) # debug=False для продакшена