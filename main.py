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
# ФУНКЦИЯ ЗАПРОСА ИМЕНИ КЛИЕНТА ВКОНТАКТЕ
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
# 2. ФУНКЦИИ УВЕДОМЛЕНИЙ В ТЕЛЕГРАМ
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


# ==============================
# 3. РАБОТА С ЯНДЕКС.ДИСКОМ: ЗАГРУЗКА ЛОГ-ФАЙЛОВ
# ==============================
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


# ==============================
# 4. СОХРАНЕНИЕ ДИАЛОГОВ В POSTGRES
# ==============================
def store_dialog_in_db(user_id, user_message, bot_message, client_info=""):
    """
    Сохраняем каждую пару (user_message + bot_message) в базу PostgreSQL.
    Добавлено сохранение информации о клиенте.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Создание таблицы, если её нет
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dialogues (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_message TEXT,
                bot_message TEXT,
                client_info TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Вставка записи
        cur.execute(
            """INSERT INTO dialogues (user_id, user_message, bot_message, client_info)
                VALUES (%s, %s, %s, %s)""",
            (user_id, user_message, bot_message, client_info)
        )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при сохранении диалога в БД: {e}")


def load_dialog_from_db(user_id):
    """
    Подгрузить из БД всю историю сообщений для указанного user_id.
    Возвращает список словарей вида: [{"user": "...", "bot": "...", "client_info": "..."}, ...].
    """
    dialog_history = []
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Берём все сообщения по user_id, сортируем по id (или по created_at)
        cur.execute("""
            SELECT user_message, bot_message, client_info
            FROM dialogues
            WHERE user_id = %s
            ORDER BY id ASC
        """, (user_id,))

        rows = cur.fetchall()
        for row in rows:
            user_m = row[0]
            bot_m = row[1]
            client_info = row[2]
            dialog_history.append({"user": user_m, "bot": bot_m, "client_info": client_info})

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Ошибка при загрузке диалога из БД для user_id={user_id}:", e)

    return dialog_history


# ==============================
# 5. ЛОГИРОВАНИЕ
# ==============================
def log_dialog(user_question, bot_response, relevant_titles, relevant_answers, user_id, full_name="", client_info=""): 
    """Логируем в локальный файл + отправляем пару (user_message, bot_message) в PostgreSQL. 
    Без подсчёта токенов. 
    """ 
    # Сохраняем в базу данных 
    store_dialog_in_db(user_id, user_question, bot_response, client_info)

    current_time = datetime.utcnow() + timedelta(hours=6) 
    formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S") 

    # Определяем лог-файл для пользователя 
    if user_id in user_log_files: 
        local_log_file = user_log_files[user_id] 
    else: 
        local_log_file = log_file_path 

    # Пишем данные в лог-файл 
    with open(local_log_file, "a", encoding="utf-8") as log_file: 
        log_file.write(f"[{formatted_time}] {full_name}: {user_question}\n") 
        if relevant_titles and relevant_answers: 
            for title, answer in zip(relevant_titles, relevant_answers): 
                log_file.write(f"[{formatted_time}] Найдено в базе знаний: {title} -> {answer}\n") 
        if client_info: # Добавляем информацию по клиенту в лог-файл
            log_file.write(f"[{formatted_time}] Информация по клиенту: {client_info}\n")
        log_file.write(f"[{formatted_time}] Модель: {bot_response}\n\n") 

    print(f"Содержимое лога:\n{open(local_log_file, 'r', encoding='utf-8').read()}") 

    # Загружаем лог-файл в Яндекс.Диск 
    upload_log_to_yandex_disk(local_log_file)


# ==============================
# 6. ИНТЕГРАЦИЯ С GEMINI
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


def generate_response(user_question, client_data, dialog_history, custom_prompt, first_name, relevant_answers=None):

    # Формируем историю в текстовом виде
    history_lines = []
    for turn in dialog_history:
        if "operator" in turn:
            # Добавляем сообщение оператора, можно явно указать, что это Сергей
            history_lines.append(f"Учти, что сообщения от Сергея (оператора) являются корректной информацией и должны иметь приоритет. Сергей (оператор) пишет: {turn['operator']}")
        else:
            # Обычное сообщение пользователя и ответа бота
            user_msg = turn.get("user", "Неизвестно")
            bot_msg = turn.get("bot", "Нет ответа")
            history_lines.append(f"Пользователь: {user_msg}")
            history_lines.append(f"Модель: {bot_msg}")
    history_text = "\n".join(history_lines)

    # Добавляем информацию о клиенте в историю, если она есть
    client_info_history = ""
    if dialog_history:
        last_client_info = dialog_history[-1].get("client_info", "")
        if last_client_info:
            client_info_history = f"Информация о клиенте: {last_client_info}\n"

    knowledge_hint = (
        f"Подсказки из базы знаний: {relevant_answers}"
        if relevant_answers else ""
    )
    
    if not first_name:
        full_prompt = (
            f"{custom_prompt}\n\n"
            f"Контекст диалога:\n{history_text}\n\n"
            f"{client_info_history}"
            f"{knowledge_hint}\n\n"
            f"Текущий запрос пользователя: {user_question}\n"
            f"Информация о клиенте: {client_data}\n"
            f"Модель:"
        )
    else:
        full_prompt = (
            f"{custom_prompt}\n\n"
            f"Контекст диалога:\n{history_text}\n\n"
            f"{client_info_history}"
            f"{knowledge_hint}\n\n"
            f"Обращайся к пользователю по имени: {first_name}\n"
            f"Текущий запрос пользователя: {user_question}\n"
            f"Информация о клиенте: {client_data}\n"
            f"Модель:"
        )

    data = {
        "contents": [{"parts": [{"text": full_prompt}]}]
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
            # Возвращаем кастомное сообщение при перегрузке модели
            return "Ой! Извините. Я - нейробот онлайн-школы фортепиано и у меня прямо сейчас возникла какая-то проблема. Как только будет возможность, отвечу либо я, либо Сергей. Чтобы ускорить этот процесс напишите слово 'оператор' и тогда возможно помощь придёт быстрее."
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


# ==============================
# 7. 30-секундная задержка и буфер сообщений
# ==============================
user_buffers = {}
user_timers  = {}
last_questions = {}

DELAY_SECONDS = 60

# ==============================
# 8. ПАУЗА ДЛЯ КОНКРЕТНОГО ПОЛЬЗОВАТЕЛЯ
# ==============================

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

def handle_new_message(user_id, text, vk, is_outgoing=False):

    first_name, last_name = get_vk_user_full_name(user_id)
    full_name = f"{first_name}_{last_name}"

    lower_text = text.lower()

    # Если сообщение исходящее (оператор пишет боту)
    if is_outgoing:
        dialog_history = dialog_history_dict.setdefault(user_id, [])
        dialog_history.append({"operator": text})

        # Логирование оператора
        current_time = datetime.utcnow() + timedelta(hours=6)
        formatted_time = current_time.strftime("%Y-%m-%d_%H-%M-%S")
        if user_id in user_log_files:
            op_log_path = user_log_files[user_id]
        else:
            op_log_path = log_file_path
        with open(op_log_path, "a", encoding="utf-8") as f:
            f.write(f"[{formatted_time}] user_id={user_id}, Оператор: {text}\n\n")
        return

    # 1. Если пользователя ещё нет в диалоге, подгружаем историю из БД
    if user_id not in dialog_history_dict:
        existing_history = load_dialog_from_db(user_id)
        dialog_history_dict[user_id] = existing_history

    dialog_history = dialog_history_dict[user_id]

    # 2. При первом сообщении вообще (то есть если в БД и памяти пусто) получаем имя/фамилию
    if len(dialog_history) == 0:
        user_names[user_id] = (first_name, last_name)

        # Формируем отдельный log_file_path c именем/фамилией
        now_str = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        custom_file_name = f"dialog_{now_str}_{first_name}_{last_name}.txt"
        custom_log_path  = os.path.join(logs_directory, custom_file_name)
        user_log_files[user_id] = custom_log_path

        # Уведомляем в Телеграм (если нет слова "оператор")
        if "оператор" not in lower_text:
            send_telegram_notification(
                user_question=text,
                dialog_id=user_id,
                first_name=first_name,
                last_name=last_name
            )
    else:
        # Если пользователь упомянул "оператор"
        if "оператор" in lower_text:
            # Проверяем, является ли это первое сообщение в диалоге
            if len(dialog_history) == 0:
                first_name, last_name = user_names.get(user_id, ("", ""))
                first_name = first_name or ""
                last_name = last_name or ""
                send_telegram_notification(
                    user_question=text,
                    dialog_id=user_id,
                    first_name=first_name,
                    last_name=last_name
                )
            else:
                # Отправляем уведомление в Телеграм с детализированным содержимым
                summary, reason = generate_summary_and_reason(dialog_history)
                initial_q = last_questions.get(user_id, "")
                first_name, last_name = user_names.get(user_id, ("", ""))
                first_name = first_name or ""
                last_name = last_name or ""
                send_operator_notification(
                    user_id,
                    initial_q,
                    summary,
                    reason,
                    first_name=first_name,
                    last_name=last_name
                )

    # 3. Проверяем, находится ли пользователь в paused_names
    if is_user_paused(full_name):
        print(f"Пользователь {full_name} находится на паузе. Пропускаем сообщение.")
        return  # Не отвечаем пользователю

    # 4. Добавляем сообщение в буфер
    user_buffers.setdefault(user_id, []).append(text)
    last_questions[user_id] = text

    # 5. Сбрасываем/перезапускаем таймер
    if user_id in user_timers:
        user_timers[user_id].cancel()
    timer = threading.Timer(DELAY_SECONDS, generate_and_send_response, args=(user_id, vk))
    user_timers[user_id] = timer
    timer.start()

def generate_and_send_response(user_id, vk):

    first_name, last_name = get_vk_user_full_name(user_id)

    if vk is None:
        print("Ошибка: объект vk не передан!")
        return

    msgs = user_buffers.get(user_id, [])
    if not msgs:
        return

    # Проверяем, находится ли пользователь в paused_names перед генерацией ответа

    full_name = f"{first_name}_{last_name}"
    if is_user_paused(full_name):
        print(f"Пользователь {full_name} находится на паузе. Пропускаем генерацию ответа.")
        user_buffers[user_id] = []
        return

    combined_text = "\n".join(msgs)
    user_buffers[user_id] = []

    dialog_history = dialog_history_dict[user_id]

    # Извлекаем client_info из последнего сообщения в истории
    last_client_info = ""
    if dialog_history:
        last_client_info = dialog_history[-1].get("client_info", "")

    # Проверяем, есть ли в запросе емейл или телефон
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    emails = re.findall(email_regex, combined_text)
    phone_regex = r"(?:\+7|7|8)?[\s\-]?\(?(\d{3})\)?[\s\-]?(\d{3})[\s\-]?(\d{2})[\s\-]?(\d{2})"
    phones = re.findall(phone_regex, combined_text)

    # Вызываем get_client_info, если есть емейл или телефон
    if emails or phones:
        client_data = get_client_info(combined_text, user_id)
        logging.info(f"Пользователь {user_id}: запрошена информация о клиенте из таблицы.")

        # Логируем результат поиска
        if client_data:
            logging.info(f"Пользователь {user_id}: найдена информация о клиенте: {client_data}")
        else:
            logging.info(f"Пользователь {user_id}: информация о клиенте не найдена.")

    else:
        # Если нет ни емейла, ни телефона, не ищем
        client_data = ""
        logging.info(f"Пользователь {user_id}: нет емейла или телефона в запросе.")

    relevant_titles = find_relevant_titles_with_gemini(combined_text)
    relevant_answers = [knowledge_base[t] for t in relevant_titles if t in knowledge_base]

    # Добавляем client_data в запрос модели
    model_response = generate_response(combined_text, client_data, dialog_history, custom_prompt, first_name, relevant_answers)

    # Логируем
    log_dialog(combined_text, model_response, relevant_titles, relevant_answers, user_id, full_name=full_name, client_info=client_data)

    # Обновляем диалог в памяти
    dialog_history.append({"user": combined_text, "bot": model_response, "client_info": client_data})

    # Отправляем ответ в ВК
    vk.messages.send(
        user_id=user_id,
        message=model_response,
        random_id=int(time.time() * 1000)
    )



# ==============================
# 8. ОСНОВНОЙ ЦИКЛ
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
    data = request.json

    # Проверяем, что объект получен
    if not isinstance(data, dict) or "object" not in data:
        logging.error("Ошибка: Нет ключа 'object' в данных от ВКонтакте.")
        return "Bad request", 400

    # Проверяем тип запроса (подтверждение Callback API)
    if data.get("type") == "confirmation":        
        return VK_CONFIRMATION_TOKEN

    # Проверяем секретный ключ (если он установлен)
    if VK_SECRET_KEY and data.get("secret") != VK_SECRET_KEY:
        return "Invalid secret", 403

    # Проверяем, что объект содержит `message`
    vk_object = data["object"]
    if not isinstance(vk_object, dict) or "message" not in vk_object:
        logging.warning(f"Игнорируем событие без 'message': {data}")
        return "ok"  # Возвращаем "ok", чтобы ВКонтакте не заблокировал API

    # Обрабатываем сообщение
    msg = data["object"]["message"]

    # Проверяем наличие обязательных полей в сообщении
    if "from_id" not in msg or "text" not in msg:
        logging.error("Ошибка: Сообщение не содержит 'from_id' или 'text'.")
        return "Bad request", 400

    user_id = msg["from_id"]
    text = msg["text"]

    # Проверяем, является ли сообщение исходящим (от оператора)
    out_flag = msg.get("out", 0)
    is_outgoing = (out_flag == 1)

    # Подключаемся к API ВКонтакте
    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    # Обрабатываем новое сообщение
    handle_new_message(user_id, text, vk, is_outgoing=is_outgoing)

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