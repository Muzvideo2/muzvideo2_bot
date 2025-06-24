import os
import re
import time
import json
import requests
import psycopg2
import psycopg2.extras
import subprocess
from datetime import datetime, timedelta, timezone
import threading
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
from flask import Flask, request, jsonify
from urllib.parse import quote
import openpyxl
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from google.cloud import aiplatform
import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account

# ====
# Читаем переменные окружения (секретные данные)
# ====
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
VK_COMMUNITY_TOKEN = os.environ.get("VK_COMMUNITY_TOKEN", "")
YANDEX_DISK_TOKEN = os.environ.get("YANDEX_DISK_TOKEN", "")
VK_SECRET_KEY = os.environ.get("VK_SECRET_KEY", "")
VK_CONFIRMATION_TOKEN = os.environ.get("VK_CONFIRMATION_TOKEN", "")

# Параметры PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")

# ID сообщества
VK_COMMUNITY_ID = 48116621

# Настройки для Vertex AI
PROJECT_ID = "zeta-tracer-462306-r7"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash"

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Глобальные переменные:
operator_timers = {}
client_timers = {}
dialog_history_dict = {}
user_names = {}
user_log_files = {}
user_buffers = {}
last_questions = {}

# Константа для задержки ответа клиенту
USER_MESSAGE_BUFFERING_DELAY = 60

# ====
# Пути к файлам и внешним сервисам
# ====
KNOWLEDGE_BASE_PATH = "knowledge_base.json"
PROMPT_PATH = "prompt.txt"
LOGS_DIRECTORY = "dialog_logs"

CONTEXT_BUILDER_PATH = "context_builder.py"
SUMMARY_UPDATER_PATH = "summary_updater.py"

# ====
# Прочитаем базу знаний и промпт
# ====
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY, exist_ok=True)

try:
    with open(KNOWLEDGE_BASE_PATH, "r", encoding="utf-8") as f:
        knowledge_base = json.load(f)
except FileNotFoundError:
    logging.error(f"Файл базы знаний '{KNOWLEDGE_BASE_PATH}' не найден. Работа будет продолжена без нее.")
    knowledge_base = {}
except json.JSONDecodeError:
    logging.error(f"Ошибка декодирования JSON в файле базы знаний '{KNOWLEDGE_BASE_PATH}'.")
    knowledge_base = {}

try:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        custom_prompt = f.read().strip()
except FileNotFoundError:
    logging.error(f"Файл промпта '{PROMPT_PATH}' не найден. Будет использован пустой промпт.")
    custom_prompt = "Ты — полезный ассистент."

# ID оператора (владельца бота)
try:
    OPERATOR_VK_ID = int(os.environ.get("OPERATOR_VK_ID", 0))
except ValueError:
    logging.warning("Переменная окружения OPERATOR_VK_ID не является числом. Установлено значение 0.")
    OPERATOR_VK_ID = 0

# ====
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ПОДКЛЮЧЕНИЯ К БД
# ====
def get_main_db_connection():
    """Устанавливает новое соединение с базой данных, используя DATABASE_URL."""
    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. Невозможно подключиться к базе данных.")
        raise ValueError("DATABASE_URL не настроен.")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logging.debug("Соединение с базой данных успешно установлено.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Не удалось подключиться к базе данных: {e}")
        raise

# ====
# 1. ФУНКЦИЯ ЗАПРОСА ИМЕНИ КЛИЕНТА ИЗ БАЗЫ ДАННЫХ
# ====
def get_user_name_from_db(user_id_to_fetch):
    """
    Получает имя и фамилию пользователя из таблицы user_profiles в БД.
    Использует кеширование в user_names для минимизации запросов к БД.
    """
    try:
        user_id_int = int(user_id_to_fetch)
    except ValueError:
        logging.error(f"Некорректный user_id '{user_id_to_fetch}' для запроса имени из БД.")
        return "Пользователь", "VK"

    if user_id_int in user_names:
        logging.debug(f"Имя для user_id {user_id_int} взято из кеша: {user_names[user_id_int]}")
        return user_names[user_id_int]

    conn = None
    try:
        conn = get_main_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT first_name, last_name FROM user_profiles WHERE conv_id = %s",
                (user_id_int,)
            )
            result = cur.fetchone()

            if result:
                first_name, last_name = result
                user_names[user_id_int] = (first_name, last_name)
                logging.info(f"Получено имя для user_id {user_id_int} из БД: {first_name} {last_name}")
                return first_name, last_name
            else:
                logging.warning(f"Профиль для user_id {user_id_int} еще не найден в БД. Используется временное имя.")
                user_names[user_id_int] = (f"User_{user_id_int}", "")
                return f"User_{user_id_int}", ""

    except psycopg2.Error as e:
        logging.error(f"Ошибка БД при получении имени для user_id {user_id_int}: {e}")
        return f"User_{user_id_int}", "(ошибка БД)"
    finally:
        if conn:
            conn.close()

# ====
# 2. ФУНКЦИИ УВЕДОМЛЕНИЙ В ТЕЛЕГРАМ
# ====
def send_telegram_notification(user_question_text, dialog_id, first_name="", last_name=""):
    """
    Уведомление в телеграм при первом сообщении пользователя или при запросе "оператор".
    """
    if not TELEGRAM_TOKEN or not ADMIN_CHAT_ID:
        logging.warning("Токен Telegram или ID чата администратора не настроены. Уведомление не отправлено.")
        return

    vk_dialog_link = f"https://vk.com/gim{VK_COMMUNITY_ID}?sel={dialog_id}"
    user_full_name = f"{first_name} {last_name}".strip()
    if not user_full_name:
        user_full_name = f"Пользователь ID {dialog_id}"

    message_text = f"""
👤 Пользователь: {user_full_name}
💬 Стартовый вопрос: {user_question_text}
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
        logging.info(f"Уведомление о новом диалоге ({dialog_id}) отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отправке уведомления в Telegram для диалога {dialog_id}: {e}")


def send_operator_request_notification(dialog_id, initial_question, dialog_summary, reason_guess, first_name="", last_name=""):
    """
    Уведомление, если пользователь запросил оператора в процессе диалога.
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

# ====
# 3. РАБОТА С ЯНДЕКС.ДИСКОМ: ЗАГРУЗКА ЛОГ-ФАЙЛОВ
# ====
def upload_log_to_yandex_disk(log_file_path_to_upload):
    """
    Загружает файл log_file_path_to_upload на Яндекс.Диск, если YANDEX_DISK_TOKEN задан.
    """
    if not YANDEX_DISK_TOKEN:
        logging.warning("YANDEX_DISK_TOKEN не задан. Пропускаем загрузку логов на Яндекс.Диск.")
        return

    if not os.path.exists(log_file_path_to_upload):
        logging.warning(f"Файл '{log_file_path_to_upload}' не найден. Пропускаем загрузку на Яндекс.Диск.")
        return

    create_dir_url = "https://cloud-api.yandex.net/v1/disk/resources"
    headers_ya = {"Authorization": f"OAuth {YANDEX_DISK_TOKEN}"}
    params_ya_create_dir = {"path": "disk:/app-logs"}

    try:
        response_create_dir = requests.put(create_dir_url, headers=headers_ya, params=params_ya_create_dir, timeout=10)
        if response_create_dir.status_code == 201:
            logging.info("Папка 'app-logs' успешно создана на Яндекс.Диске.")
        elif response_create_dir.status_code == 409:
            logging.info("Папка 'app-logs' уже существует на Яндекс.Диске.")
        else:
            logging.warning(f"Не удалось создать/проверить папку на Яндекс.Диске. Статус: {response_create_dir.status_code}, Ответ: {response_create_dir.text}")
    except requests.Timeout:
        logging.error("Тайм-аут при создании/проверке папки /app-logs на Яндекс.Диске.")
        return
    except requests.RequestException as e:
        logging.error(f"Ошибка при создании/проверке папки на Яндекс.Диске: {e}")
        return

    file_name_to_upload = os.path.basename(log_file_path_to_upload)
    ya_disk_path = f"disk:/app-logs/{file_name_to_upload}"

    get_upload_url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
    params_get_url = {"path": ya_disk_path, "overwrite": "true"}

    try:
        response_get_link = requests.get(get_upload_url, headers=headers_ya, params=params_get_url, timeout=10)
        response_get_link.raise_for_status()

        href_upload_link = response_get_link.json().get("href")
        if not href_upload_link:
            logging.error(f"Не найден 'href' в ответе Яндекс.Диска при получении ссылки для загрузки файла '{file_name_to_upload}': {response_get_link.text}")
            return

        with open(log_file_path_to_upload, "rb") as f_log:
            upload_response = requests.put(href_upload_link, files={"file": f_log}, timeout=30)

        if upload_response.status_code == 201:
            logging.info(f"Лог-файл '{file_name_to_upload}' успешно загружен на Яндекс.Диск.")
        else:
            logging.error(f"Ошибка {upload_response.status_code} при загрузке '{file_name_to_upload}' на Яндекс.Диск: {upload_response.text}")

    except requests.Timeout:
        logging.error(f"Тайм-аут при работе с Яндекс.Диском для файла '{file_name_to_upload}'.")
    except requests.RequestException as e:
        logging.error(f"Ошибка запроса при работе с Яндекс.Диском для файла '{file_name_to_upload}': {e}")
    except KeyError:
        logging.error(f"Ошибка извлечения 'href' из ответа Яндекс.Диска для файла '{file_name_to_upload}'.")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при загрузке файла '{file_name_to_upload}' на Яндекс.Диск: {e}")

# ====
# 4. ФУНКЦИЯ ЗАПИСИ ДАННЫХ CALLBACK ОТ VK В JSON-файл
# ====
CALLBACK_LOGS_DIR = "callback_logs"
if not os.path.exists(CALLBACK_LOGS_DIR):
    os.makedirs(CALLBACK_LOGS_DIR, exist_ok=True)

def save_callback_payload(data_payload):
    """
    Сохраняет весь JSON, полученный от ВКонтакте, в локальный файл для отладки.
    """
    timestamp_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S_%f")
    file_name = f"callback_{timestamp_str}.json"
    file_path = os.path.join(CALLBACK_LOGS_DIR, file_name)

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_payload, f, ensure_ascii=False, indent=2)
        logging.info(f"Сохранён callback JSON: {file_name}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении callback payload в файл '{file_path}': {e}")


# ====
# 5. СОХРАНЕНИЕ ДИАЛОГОВ В POSTGRES
# ====
def store_dialog_in_db(conv_id, role, message_text_with_timestamp, client_info=""):
    """
    Сохраняет одно сообщение в базу данных.
    """
    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. Сообщение не будет сохранено в БД.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO dialogues (conv_id, role, message, client_info)
            VALUES (%s, %s, %s, %s)""",
            (conv_id, role, message_text_with_timestamp, client_info)
        )
        conn.commit()
        logging.info(f"Сообщение для conv_id {conv_id} (роль: {role}) сохранено в БД.")
    except psycopg2.Error as e:
        logging.error(f"Ошибка PostgreSQL при сохранении диалога для conv_id {conv_id}: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"Неизвестная ошибка при сохранении диалога в БД для conv_id {conv_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()


# ====
# 6. ФУНКЦИИ РАБОТЫ С ВНЕШНИМИ СЕРВИСАМИ
# ====
def call_context_builder(vk_callback_data):
    """
    Вызывает внешний сервис context_builder.py для сбора контекста пользователя.
    """
    try:
        process = subprocess.run(
            ["python", CONTEXT_BUILDER_PATH],
            input=json.dumps(vk_callback_data, ensure_ascii=False),
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=30
        )

        if process.returncode != 0:
            error_msg = f"Context Builder завершился с кодом ошибки {process.returncode}. stderr: {process.stderr}"
            logging.error(error_msg)
            raise Exception(error_msg)

        context_text = process.stdout.strip()
        if not context_text:
            logging.warning("Context Builder вернул пустой результат")
            return ""

        logging.info(f"Context Builder успешно вернул контекст (длина: {len(context_text)} символов)")
        return context_text

    except subprocess.TimeoutExpired:
        error_msg = "Context Builder превысил таймаут выполнения (30 секунд)"
        logging.error(error_msg)
        raise Exception(error_msg)
    except FileNotFoundError:
        error_msg = f"Файл {CONTEXT_BUILDER_PATH} не найден"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Ошибка при вызове Context Builder: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)


def call_summary_updater_async(conv_id):
    """
    Асинхронно вызывает внешний сервис summary_updater.py для обновления саммари диалога.
    """
    def run_summary_updater():
        try:
            process = subprocess.run(
                ["python", SUMMARY_UPDATER_PATH],
                input=str(conv_id),
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=60
            )

            if process.returncode != 0:
                logging.error(f"Summary Updater завершился с кодом ошибки {process.returncode} для conv_id {conv_id}. stderr: {process.stderr}")
            else:
                logging.info(f"Summary Updater успешно обработал conv_id {conv_id}")

        except subprocess.TimeoutExpired:
            logging.error(f"Summary Updater превысил таймаут выполнения для conv_id {conv_id}")
        except FileNotFoundError:
            logging.error(f"Файл {SUMMARY_UPDATER_PATH} не найден")
        except Exception as e:
            logging.error(f"Ошибка при асинхронном вызове Summary Updater для conv_id {conv_id}: {e}")

    threading.Thread(target=run_summary_updater, daemon=True).start()
    logging.info(f"Summary Updater запущен асинхронно для conv_id {conv_id}")


# ====
# Flask App и его эндпоинты
# ====
app = Flask(__name__)

@app.route('/ping_main_bot', methods=['GET'])
def ping_main_bot():
    return "Pong from Main Bot!", 200

@app.route("/clear_context/<int:user_conv_id>", methods=["POST"])
def clear_context(user_conv_id):
    """
    Удаляет контекст пользователя (историю диалога) из базы данных и локального кеша.
    """
    logging.info(f"Запрос на очистку контекста для conv_id: {user_conv_id}")

    if not DATABASE_URL:
        logging.error("DATABASE_URL не настроен. Контекст не может быть очищен из БД.")
        return jsonify({"status": "error", "message": "DATABASE_URL не настроен"}), 500

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("DELETE FROM dialogues WHERE conv_id = %s", (user_conv_id,))
        deleted_rows = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Удалено {deleted_rows} записей из БД для conv_id {user_conv_id}.")

        if user_conv_id in dialog_history_dict:
            del dialog_history_dict[user_conv_id]
        if user_conv_id in user_buffers:
            del user_buffers[user_conv_id]
        if user_conv_id in client_timers:
            client_timers[user_conv_id].cancel()
            del client_timers[user_conv_id]
        if user_conv_id in operator_timers:
            operator_timers[user_conv_id].cancel()
            del operator_timers[user_conv_id]
        if user_conv_id in last_questions:
            del last_questions[user_conv_id]

        logging.info(f"Локальный кеш для conv_id {user_conv_id} очищен.")
        return jsonify({"status": "success", "message": f"Контекст для conv_id {user_conv_id} успешно очищен. Удалено записей из БД: {deleted_rows}."}), 200

    except psycopg2.Error as db_err:
        logging.error(f"Ошибка PostgreSQL при очистке контекста для conv_id {user_conv_id}: {db_err}")
        return jsonify({"status": "error", "message": "Ошибка базы данных при очистке контекста"}), 500
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при очистке контекста для conv_id {user_conv_id}: {e}")
        return jsonify({"status": "error", "message": "Внутренняя ошибка сервера при очистке контекста"}), 500


@app.route("/operator_message_sent", methods=["POST"])
def operator_message_sent():
    """
    Эндпоинт, вызываемый веб-интерфейсом оператора после отправки сообщения.
    """
    data = request.json
    conv_id_from_request = data.get("conv_id")

    if conv_id_from_request is None:
        logging.warning("Получен запрос /operator_message_sent без conv_id в теле JSON.")
        return jsonify({"status": "error", "message": "conv_id is required in JSON body"}), 400

    try:
        conv_id = int(conv_id_from_request)
    except ValueError:
        logging.warning(f"Получен некорректный conv_id в /operator_message_sent: '{conv_id_from_request}'. Не является числом.")
        return jsonify({"status": "error", "message": "Invalid conv_id format, must be an integer"}), 400

    logging.info(f"Получено уведомление от веб-интерфейса: оператор отправил сообщение в диалог {conv_id}")

    if conv_id in client_timers:
        client_timers[conv_id].cancel()
        logging.info(f"Клиентский таймер для диалога {conv_id} отменен из-за активности оператора.")

    if conv_id in user_buffers:
        user_buffers[conv_id] = []
        logging.info(f"Буфер сообщений пользователя для диалога {conv_id} очищен из-за активности оператора.")

    if conv_id in operator_timers:
        operator_timers[conv_id].cancel()

    op_timer = threading.Timer(15 * 60, clear_operator_timer, args=(conv_id,))
    operator_timers[conv_id] = op_timer
    op_timer.start()
    logging.info(f"Операторский таймер на 15 минут установлен/обновлен для диалога {conv_id}.")

    return jsonify({"status": "success", "message": f"Operator activity processed for conv_id {conv_id}"}), 200


def clear_operator_timer(conv_id_for_timer):
    """
    Вызывается по истечении 15 минут после последнего сообщения оператора.
    """
    if conv_id_for_timer in operator_timers:
        del operator_timers[conv_id_for_timer]
        logging.info(f"Операторский таймер для диалога {conv_id_for_timer} истёк и был удален.")


# Глобальная память для недавних event_id
recent_event_ids = {}
EVENT_ID_TTL = 30

# ====
# 7. ИНТЕГРАЦИЯ С GEMINI (VERTEX AI)
# ====
def find_relevant_titles_with_gemini(user_question_text, model):
    """
    Использует Gemini через Vertex AI SDK для выбора релевантных заголовков.
    """
    if not knowledge_base:
        logging.info("База знаний пуста. Поиск релевантных заголовков не выполняется.")
        return []

    titles = list(knowledge_base.keys())
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

    for attempt in range(3):
        try:
            response = model.generate_content(prompt_text, request_options={'timeout': 20})
            
            text_raw = response.text
            lines = text_raw.strip().split("\n")
            relevant_titles_found = [ln.strip() for ln in lines if ln.strip() and ln.strip() in knowledge_base]
            logging.info(f"Gemini (Vertex AI) нашел релевантные заголовки: {relevant_titles_found} для вопроса: '{user_question_text}'")
            return relevant_titles_found[:3]

        except Exception as e:
            logging.error(f"Ошибка Vertex AI при поиске заголовков (попытка {attempt + 1}): {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                logging.warning("Не удалось получить релевантные заголовки от Gemini (Vertex AI) после нескольких попыток.")
                return []
    return []


def generate_response(user_question_text, context_from_builder, current_custom_prompt, user_first_name, model, relevant_kb_titles=None):
    """
    Генерирует ответ от модели Gemini с учётом контекста от Context Builder и подсказок из базы знаний.
    """
    knowledge_hint_text = ""
    if relevant_kb_titles and knowledge_base:
        kb_lines = []
        for key_title in relevant_kb_titles:
            if key_title in knowledge_base:
                value = str(knowledge_base[key_title]).strip()
                kb_lines.append(f"- {key_title}: {value}")
        if kb_lines:
            knowledge_hint_text = "Контекст из базы знаний:\n" + "\n".join(kb_lines)

    prompt_parts = [current_custom_prompt]
    if context_from_builder.strip():
        prompt_parts.append(f"Информация о клиенте и история диалога:\n{context_from_builder.strip()}")
    if knowledge_hint_text:
        prompt_parts.append(knowledge_hint_text)

    prompt_parts.append(f"Текущий вопрос от {user_first_name if user_first_name else 'Пользователя'}: {user_question_text}")
    prompt_parts.append("Твой ответ (Модель):")

    full_prompt_text = "\n\n".join(prompt_parts)

    prompt_log_filename = f"prompt_gemini_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S_%f')}.txt"
    prompt_log_filepath = os.path.join(LOGS_DIRECTORY, prompt_log_filename)
    try:
        with open(prompt_log_filepath, "w", encoding="utf-8") as pf:
            pf.write(full_prompt_text)
        logging.info(f"Полный промпт для Gemini сохранён в: {prompt_log_filepath}")
    except Exception as e:
        logging.error(f"Ошибка при записи промпта Gemini в файл '{prompt_log_filepath}': {e}")

    for attempt in range(3):
        try:
            response = model.generate_content(full_prompt_text, request_options={'timeout': 30})
            model_response_text = response.text.strip()
            logging.info(f"Ответ от Gemini (Vertex AI) получен: '{model_response_text[:200]}...'")
            return model_response_text
            
        except Exception as e:
            logging.error(f"Ошибка Vertex AI при генерации ответа (попытка {attempt + 1}): {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                return "Извините, сервис временно перегружен. Пожалуйста, попробуйте позже. (Ошибка Vertex AI)"
    
    logging.error("Не удалось получить ответ от Gemini (Vertex AI) после нескольких попыток.")
    return "Извините, я сейчас не могу ответить. Пожалуйста, попробуйте позже или напишите 'оператор', если вопрос срочный."


def generate_summary_and_reason(dialog_history_list_for_summary, model):
    """
    Генерирует сводку диалога и предполагаемую причину запроса оператора с помощью Gemini (Vertex AI).
    """
    history_text_parts = []
    for turn in dialog_history_list_for_summary[-10:]:
        role = list(turn.keys())[0]
        message_content = turn[role]
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

    for attempt in range(2):
        try:
            response = model.generate_content(prompt_text, request_options={'timeout': 15})
            output_text = response.text.strip()
            parts = output_text.split("\n", 1)
            dialog_summary_text = parts[0].strip() if len(parts) > 0 else "Сводка не сформирована"
            reason_guess_text = parts[1].strip() if len(parts) > 1 else "Причина не определена"
            logging.info(f"Сводка для запроса оператора (Vertex AI): '{dialog_summary_text}', Причина: '{reason_guess_text}'")
            return dialog_summary_text, reason_guess_text
        except Exception as e:
            logging.error(f"Ошибка Vertex AI при генерации сводки (попытка {attempt + 1}): {e}")
            if attempt < 1:
                time.sleep(3)
    
    logging.error("Не удалось сгенерировать сводку и причину от Gemini (Vertex AI).")
    return "Не удалось сформировать сводку (ошибка сервиса)", "Не удалось определить причину (ошибка сервиса)"

# ====
# 8. ПРОВЕРКА АКТИВНОСТИ ОПЕРАТОРА
# ====
def check_operator_activity_and_cleanup(conv_id):
    """
    Проверяет, был ли оператор недавно активен для данного conv_id.
    """
    logging.info(f"[ПроверкаОператора] Начало проверки активности оператора для conv_id: {conv_id}")
    conn = None
    cur = None
    try:
        conn = get_main_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 

        cur.execute("SELECT last_operator_activity_at FROM operator_activity WHERE conv_id = %s", (conv_id,))
        record = cur.fetchone()

        if record:
            last_active_time = record['last_operator_activity_at']
            if last_active_time.tzinfo is None:
                last_active_time = last_active_time.replace(tzinfo=timezone.utc)
                logging.warning(f"[ПроверкаОператора] Время last_operator_activity_at для conv_id {conv_id} было без tzinfo, принято как UTC.")

            current_time = datetime.now(timezone.utc)
            time_since_operator_activity = current_time - last_active_time

            logging.debug(f"[ПроверкаОператора] conv_id: {conv_id}, Последняя активность оператора: {last_active_time}, Текущее время: {current_time}, Разница: {time_since_operator_activity}")

            if time_since_operator_activity <= timedelta(minutes=15):
                logging.info(f"[ПроверкаОператора] Оператор был недавно активен для conv_id: {conv_id} (в {last_active_time}). Бот сделает ПАУЗУ.")
                return True
            else:
                logging.info(f"[ПроверкаОператора] Активность оператора для conv_id: {conv_id} старше 15 минут ({last_active_time}). Бот может отвечать. Удаление старой записи.")
                try:
                    cur.execute("DELETE FROM operator_activity WHERE conv_id = %s", (conv_id,))
                    conn.commit()
                    logging.info(f"[ПроверкаОператора] Успешно удалена старая запись об активности оператора для conv_id: {conv_id}.")
                except psycopg2.Error as e_delete:
                    conn.rollback()
                    logging.error(f"[ПроверкаОператора] Не удалось удалить старую запись об активности оператора для conv_id: {conv_id}. Ошибка: {e_delete}")
                return False
        else:
            logging.info(f"[ПроверкаОператора] Запись об активности оператора для conv_id: {conv_id} не найдена. Бот может отвечать.")
            return False

    except psycopg2.Error as e_db:
        logging.error(f"[ПроверкаОператора] Ошибка базы данных при проверке активности оператора для conv_id {conv_id}: {e_db}")
        logging.warning(f"[ПроверкаОператора] Бот сделает ПАУЗУ для conv_id {conv_id} из-за ошибки БД при проверке активности.")
        return True 
    except Exception as e_generic:
        logging.error(f"[ПроверкаОператора] Общая ошибка при проверке активности оператора для conv_id {conv_id}: {e_generic}")
        logging.warning(f"[ПроверкаОператора] Бот сделает ПАУЗУ для conv_id {conv_id} из-за общей ошибки при проверке активности.")
        return True
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.debug(f"[ПроверкаОператора] Соединение с БД для проверки активности оператора (conv_id {conv_id}) закрыто.")

# ====
# 9. ОБРАБОТКА ПОСТУПИВШЕГО СООБЩЕНИЯ ИЗ VK CALLBACK
# ====
def handle_new_message(user_id_from_vk, message_text_from_vk, vk_api_object, vk_callback_data, is_outgoing_message=False, conversation_id=None):
    """
    Обрабатывает новое сообщение, полученное через VK Callback API.
    """
    actual_conv_id = conversation_id if conversation_id is not None else user_id_from_vk

    if is_outgoing_message:
        if int(user_id_from_vk) < 0:
            logging.info(f"[VK Callback] Исходящее сообщение от сообщества (user_id: {user_id_from_vk}) в диалоге {actual_conv_id}, пропускаем.")
        else:
            logging.info(f"[VK Callback] Зафиксировано исходящее сообщение от администратора {user_id_from_vk} в диалоге {actual_conv_id}. Не обрабатывается.")
        return

    first_name, last_name = get_user_name_from_db(actual_conv_id)
    full_name_display = f"{first_name} {last_name}".strip() or f"User_{actual_conv_id}"

    if actual_conv_id not in user_log_files:
        now_for_filename = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        safe_display_name = "".join(c for c in full_name_display if c.isalnum() or c in (' ', '_')).replace(' ', '_')
        log_file_name = f"dialog_{now_for_filename}_{actual_conv_id}_{safe_display_name}.txt"
        user_log_files[actual_conv_id] = os.path.join(LOGS_DIRECTORY, log_file_name)

    try:
        log_entry_text = f"[{datetime.utcnow() + timedelta(hours=6):%Y-%m-%d_%H-%M-%S}] {full_name_display} (raw VK): {message_text_from_vk}\n"
        with open(user_log_files[actual_conv_id], "a", encoding="utf-8") as log_f:
            log_f.write(log_entry_text)
    except Exception as e:
        logging.error(f"Ошибка записи в локальный лог-файл для conv_id {actual_conv_id}: {e}")

    if OPERATOR_VK_ID > 0 and int(user_id_from_vk) == OPERATOR_VK_ID:
        logging.info(f"Сообщение от VK ID оператора ({OPERATOR_VK_ID}) в диалоге {actual_conv_id}. Игнорируется для автоматической обработки.")
        return

    try:
        is_operator_active = check_operator_activity_and_cleanup(actual_conv_id)
    except Exception as e_check:
        logging.error(f"[handle_new_message] Ошибка при проверке активности оператора в БД для conv_id {actual_conv_id}: {e_check}")
        is_operator_active = True

    if is_operator_active:
        logging.info(f"[Оператор активен] Сообщение от пользователя {actual_conv_id}. Немедленно сохраняем в БД, бот не отвечает.")
        timestamp_in_message = f"[{datetime.utcnow() + timedelta(hours=6):%Y-%m-%d_%H-%M-%S}]"
        message_to_store = f"{timestamp_in_message} {message_text_from_vk}"
        store_dialog_in_db(
            conv_id=actual_conv_id, role="user", message_text_with_timestamp=message_to_store, client_info=""
        )
        dialog_history_dict.setdefault(actual_conv_id, []).append({"user": message_to_store, "client_info": ""})
        return
    else:
        logging.info(f"[Оператор неактивен] Сообщение от пользователя {actual_conv_id} будет обработано ботом после задержки.")
        user_buffers.setdefault(actual_conv_id, []).append(message_text_from_vk)
        logging.info(f"Сообщение от {full_name_display} (conv_id: {actual_conv_id}) добавлено в буфер. Буфер: {user_buffers[actual_conv_id]}")

        if not dialog_history_dict.get(actual_conv_id):
            if "оператор" not in message_text_from_vk.lower():
                send_telegram_notification(user_question_text=message_text_from_vk, dialog_id=actual_conv_id, first_name=first_name, last_name=last_name)

        if "оператор" in message_text_from_vk.lower():
            temp_history_for_summary = list(dialog_history_dict.get(actual_conv_id, []))
            temp_history_for_summary.append({'user': message_text_from_vk})
            summary, reason = generate_summary_and_reason(temp_history_for_summary, app.model)
            initial_q_for_op_notify = user_buffers[actual_conv_id][0] if user_buffers[actual_conv_id] else message_text_from_vk
            send_operator_request_notification(dialog_id=actual_conv_id, initial_question=initial_q_for_op_notify, dialog_summary=summary, reason_guess=reason, first_name=first_name, last_name=last_name)

        if actual_conv_id in client_timers:
            client_timers[actual_conv_id].cancel()
        
        client_timer_thread = threading.Timer(USER_MESSAGE_BUFFERING_DELAY, generate_and_send_response, args=(actual_conv_id, vk_api_object, vk_callback_data, app.model))
        client_timers[actual_conv_id] = client_timer_thread
        client_timer_thread.start()
        logging.info(f"Клиентский таймер на {USER_MESSAGE_BUFFERING_DELAY}с для диалога {actual_conv_id} установлен/перезапущен.")

# ====
# 10. ФОРМИРОВАНИЕ И ОТПРАВКА ОТВЕТА БОТА ПОСЛЕ ЗАДЕРЖКИ
# ====
def generate_and_send_response(conv_id_to_respond, vk_api_for_sending, vk_callback_data, model):
    """
    Вызывается по истечении USER_MESSAGE_BUFFERING_DELAY.
    """
    logging.info(f"Вызвана функция generate_and_send_response для conv_id: {conv_id_to_respond}")

    try:
        if check_operator_activity_and_cleanup(conv_id_to_respond):
            logging.info(f"Ответ бота для conv_id {conv_id_to_respond} ПОДАВЛЕН на основе проверки активности оператора в БД.")
            return
    except Exception as e_op_check:
        logging.critical(f"Критическая ошибка во время вызова проверки активности оператора для conv_id {conv_id_to_respond}: {e_op_check}. Бот НЕ будет отвечать в целях предосторожности.")
        return

    if conv_id_to_respond in operator_timers:
        logging.info(f"Ответ для conv_id {conv_id_to_respond} не будет сгенерирован: активен локальный таймер оператора (operator_timers).")
        return

    buffered_messages = user_buffers.get(conv_id_to_respond, [])
    if not buffered_messages:
        logging.info(f"Нет сообщений в буфере для conv_id {conv_id_to_respond}. Ответ не генерируется.")
        if conv_id_to_respond in client_timers:
            del client_timers[conv_id_to_respond]
            logging.debug(f"Клиентский таймер для conv_id {conv_id_to_respond} удален (пустой буфер).")
        return

    combined_user_text = "\n".join(buffered_messages).strip()
    logging.info(f"Сообщения для conv_id {conv_id_to_respond} извлечены из буфера. Объединенный текст: '{combined_user_text[:100]}...'")
    user_buffers[conv_id_to_respond] = []

    if conv_id_to_respond in client_timers:
        del client_timers[conv_id_to_respond]
        logging.debug(f"Клиентский таймер для conv_id {conv_id_to_respond} удален после выполнения.")

    first_name, last_name = get_user_name_from_db(conv_id_to_respond)
    user_display_name = f"{first_name} {last_name}".strip() if first_name or last_name else f"User_{conv_id_to_respond}"

    try:
        context_from_builder = call_context_builder(vk_callback_data)
        logging.info(f"Context Builder успешно вернул контекст для conv_id {conv_id_to_respond}")

    except Exception as e:
        logging.error(f"Ошибка вызова Context Builder для conv_id {conv_id_to_respond}: {e}")
        logging.error(f"Обработка запроса для conv_id {conv_id_to_respond} прекращена из-за ошибки Context Builder")
        return

    relevant_titles_from_kb = find_relevant_titles_with_gemini(combined_user_text, model)

    bot_response_text = generate_response(
        user_question_text=combined_user_text,
        context_from_builder=context_from_builder,
        current_custom_prompt=custom_prompt,
        user_first_name=first_name,
        model=model,
        relevant_kb_titles=relevant_titles_from_kb
    )

    timestamp_utc_for_db = datetime.utcnow()
    timestamp_in_message_text = (timestamp_utc_for_db + timedelta(hours=6)).strftime("%Y-%m-%d_%H-%M-%S")

    user_message_with_ts_for_storage = f"[{timestamp_in_message_text}] {combined_user_text}"
    store_dialog_in_db(
        conv_id=conv_id_to_respond, 
        role="user", 
        message_text_with_timestamp=user_message_with_ts_for_storage,
        client_info=""
    )
    dialog_history_dict.setdefault(conv_id_to_respond, []).append(
        {"user": user_message_with_ts_for_storage, "client_info": ""}
    )

    bot_message_with_ts_for_storage = f"[{timestamp_in_message_text}] {bot_response_text}"
    store_dialog_in_db(
        conv_id=conv_id_to_respond, 
        role="bot", 
        message_text_with_timestamp=bot_message_with_ts_for_storage,
        client_info=""
    )
    dialog_history_dict.setdefault(conv_id_to_respond, []).append(
        {"bot": bot_message_with_ts_for_storage}
    )

    log_file_path_for_processed = user_log_files.get(conv_id_to_respond)
    if log_file_path_for_processed:
        try:
            with open(log_file_path_for_processed, "a", encoding="utf-8") as log_f:
                log_f.write(f"[{timestamp_in_message_text}] {user_display_name} (processed): {combined_user_text}\n")
                if relevant_titles_from_kb:
                    log_f.write(f"[{timestamp_in_message_text}] Найденные ключи БЗ (для processed): {', '.join(relevant_titles_from_kb)}\n")
                log_f.write(f"[{timestamp_in_message_text}] Context Builder: Context retrieved successfully\n")
                log_f.write(f"[{timestamp_in_message_text}] Модель: {bot_response_text}\n\n")
            upload_log_to_yandex_disk(log_file_path_for_processed)
        except Exception as e:
            logging.error(f"Ошибка записи в локальный лог-файл (processed) '{log_file_path_for_processed}': {e}")
    else:
        logging.warning(f"Путь к лог-файлу для conv_id {conv_id_to_respond} не найден. Логирование обработанных сообщений пропущено.")

    if vk_api_for_sending:
        try:
            vk_api_for_sending.messages.send(
                user_id=conv_id_to_respond,
                message=bot_response_text,
                random_id=int(time.time() * 10000)
            )
            logging.info(f"Ответ бота успешно отправлен пользователю {conv_id_to_respond}.")
        except vk_api.ApiError as e:
            logging.error(f"VK API Ошибка при отправке сообщения пользователю {conv_id_to_respond}: {e}")
        except Exception as e:
            logging.error(f"Неизвестная ошибка при отправке сообщения VK пользователю {conv_id_to_respond}: {e}")
    else:
        logging.warning(f"Объект VK API не передан в generate_and_send_response для conv_id {conv_id_to_respond}. Сообщение не отправлено.")

    try:
        call_summary_updater_async(conv_id_to_respond)
    except Exception as e:
        logging.error(f"Ошибка при запуске Summary Updater для conv_id {conv_id_to_respond}: {e}")


# ====
# 11. ОБРАБОТЧИК CALLBACK ОТ VK И ЗАПУСК ПРИЛОЖЕНИЯ
# ====
@app.route("/callback", methods=["POST"])
def callback_handler():
    data_from_vk = request.json

    event_type = data_from_vk.get("type")
    if VK_SECRET_KEY and data_from_vk.get("secret") != VK_SECRET_KEY:
        logging.warning("Callback: Неверный секретный ключ.")
        return "forbidden", 403

    if event_type == "confirmation":
        if not VK_CONFIRMATION_TOKEN:
            logging.error("Callback: VK_CONFIRMATION_TOKEN не установлен!")
            return "error", 500
        logging.info("Callback: получен confirmation запрос, отправляем токен подтверждения.")
        return VK_CONFIRMATION_TOKEN, 200

    event_id = data_from_vk.get("event_id")
    if event_id:
        current_time_ts = time.time()
        for eid in list(recent_event_ids.keys()):
            if current_time_ts - recent_event_ids[eid] > EVENT_ID_TTL:
                del recent_event_ids[eid]
        if event_id in recent_event_ids:
            logging.info(f"Callback: Дублирующийся event_id={event_id} (type={event_type}), пропускаем.")
            return "ok", 200
        else:
            recent_event_ids[event_id] = current_time_ts
    else:
        logging.warning(f"Callback: отсутствует event_id в событии типа {event_type}.")

    if event_type not in ("message_new", "message_reply"):
        logging.info(f"Callback: Пропускаем событие типа '{event_type}'.")
        return "ok", 200

    vk_event_object = data_from_vk.get("object")
    actual_message_payload = None

    if isinstance(vk_event_object, dict):
        if 'message' in vk_event_object and isinstance(vk_event_object.get('message'), dict):
            actual_message_payload = vk_event_object.get('message')
        else:
            actual_message_payload = vk_event_object
    else:
        logging.warning(f"Callback: 'object' отсутствует или не является словарем в событии {event_type}: {data_from_vk}")
        return "ok", 200

    if not isinstance(actual_message_payload, dict):
        logging.warning(f"Callback: Не удалось извлечь корректный словарь сообщения. Получено: {actual_message_payload}")
        return "ok", 200

    message_text = actual_message_payload.get("text", "")
    from_id = actual_message_payload.get("from_id")
    peer_id = actual_message_payload.get("peer_id")
    is_outgoing = True if actual_message_payload.get("out") == 1 else False
    
    conversation_id_for_handler = None
    if event_type == "message_reply":
        is_outgoing = True
        if peer_id:
            conversation_id_for_handler = peer_id
        else: 
            logging.warning(f"Callback (message_reply): отсутствует peer_id. from_id={from_id}, actual_payload={actual_message_payload}")
            return "ok", 200
    elif event_type == "message_new":
        conversation_id_for_handler = peer_id if is_outgoing else from_id
    else:
        return "ok", 200

    if not from_id or not conversation_id_for_handler:
        logging.warning(f"Callback: Не удалось извлечь from_id или определить conversation_id. from_id={from_id}, conv_id={conversation_id_for_handler}, payload={actual_message_payload}")
        return "ok", 200

    if not message_text.strip() and not actual_message_payload.get("attachments"):
        logging.info(f"Callback: Получено пустое сообщение (без текста и вложений) от from_id {from_id} в conv_id {conversation_id_for_handler}. Пропускаем.")
        return "ok", 200
    elif not message_text.strip() and actual_message_payload.get("attachments"):
        message_text = "[Вложение без текста]"
        logging.info(f"Callback: Сообщение от from_id {from_id} с вложением, но без текста. Установлен плейсхолдер.")

    vk_session_for_handler = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk_api_local = vk_session_for_handler.get_api()

    handle_new_message(
        user_id_from_vk=from_id,
        message_text_from_vk=message_text, 
        vk_api_object=vk_api_local, 
        is_outgoing_message=is_outgoing, 
        conversation_id=conversation_id_for_handler,
        vk_callback_data=data_from_vk
    )

    return "ok", 200


if __name__ == "__main__":
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        logging.critical("КРИТИЧЕСКАЯ ОШИБКА: Переменная окружения 'GOOGLE_APPLICATION_CREDENTIALS' не установлена.")
        exit(1)
    
    credentials_path = credentials_path.strip(' "')
    if not os.path.exists(credentials_path):
        logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Файл с учетными данными не найден по пути: {credentials_path}")
        exit(1)

    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        app.model = GenerativeModel(MODEL_NAME)
        logging.info("Учетные данные Vertex AI успешно загружены. Модель инициализирована.")
    except Exception as e:
        logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось инициализировать Vertex AI. Ошибка: {e}")
        exit(1)

    if not DATABASE_URL:
        logging.critical("Переменная окружения DATABASE_URL не установлена. Приложение не может запуститься.")
        exit(1)
    if not VK_COMMUNITY_TOKEN or not VK_CONFIRMATION_TOKEN:
        logging.critical("Переменные окружения VK_COMMUNITY_TOKEN или VK_CONFIRMATION_TOKEN не установлены. Приложение не может корректно обрабатывать запросы VK.")
    
    logging.info("Запуск Flask-приложения основного бота...")
    server_port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=server_port, debug=False)