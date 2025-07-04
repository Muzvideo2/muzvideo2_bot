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
from concurrent.futures import ThreadPoolExecutor  # Для асинхронного context builder
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

# Импорт сервиса напоминаний
from reminder_service import process_new_message as process_reminder_message, initialize_reminder_service

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
MODEL_NAME = "gemini-2.5-pro"
SEARCH_MODEL_NAME = "gemini-2.0-flash-exp"  # Быстрая модель для поиска в базе знаний

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

# ThreadPoolExecutor для асинхронной обработки context builder
context_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="ContextBuilder")

# Константы
USER_MESSAGE_BUFFERING_DELAY = 60
EVENT_ID_TTL = 300  # Время жизни event_id в секундах (5 минут)

# Словарь для отслеживания event_id
recent_event_ids = {}

# ====
# Пути к файлам и внешним сервисам
# ====
KNOWLEDGE_BASE_PATH = "knowledge_base.json"
PROMPT_PATH = "prompt.txt"
LOGS_DIRECTORY = "dialog_logs"

# CONTEXT_BUILDER_PATH = "context_builder.py"  # БОЛЬШЕ НЕ ИСПОЛЬЗУЕТСЯ - логика перенесена в main.py
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
    УСТАРЕВШАЯ ФУНКЦИЯ - НЕ ИСПОЛЬЗУЕТСЯ!
    Заменена на call_context_builder_async для лучшей производительности.
    """
    logging.warning("Вызов устаревшей функции call_context_builder! Используйте call_context_builder_async")
    return call_context_builder_async(vk_callback_data)


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
                timeout=180
            )

            if process.returncode != 0:
                logging.error(f"Summary Updater завершился с кодом ошибки {process.returncode} для conv_id {conv_id}. stderr: {process.stderr}. stdout: {process.stdout}")
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

# ====
# Инициализация Vertex AI при старте приложения
# ====
try:
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise RuntimeError("Переменная окружения 'GOOGLE_APPLICATION_CREDENTIALS' не установлена.")
    
    credentials_path = credentials_path.strip(' "')
    if not os.path.exists(credentials_path):
        raise RuntimeError(f"Файл с учетными данными не найден по пути: {credentials_path}")

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
    app.model = GenerativeModel(MODEL_NAME)
    logging.info("Учетные данные Vertex AI успешно загружены. Модель инициализирована.")
    
    # Инициализация быстрой модели для поиска в базе знаний
    app.search_model = GenerativeModel(SEARCH_MODEL_NAME)
    logging.info(f"Модель поиска {SEARCH_MODEL_NAME} инициализирована.")
    
    # Инициализация сервиса напоминаний
    try:
        initialize_reminder_service()
        logging.info("Сервис напоминаний успешно инициализирован.")
    except Exception as e:
        logging.error(f"Ошибка инициализации сервиса напоминаний: {e}")
except Exception as e:
    logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось инициализировать Vertex AI. Приложение не сможет работать. Ошибка: {e}")
    # В проде приложение может продолжить работать без модели, но будет выдавать ошибки.
    # Для критического функционала можно использовать exit(1)
    app.model = None # Явно указываем, что модель не создана

@app.route('/ping_main_bot', methods=['GET'])
def ping_main_bot():
    return "Pong from Main Bot!", 200

@app.route("/activate_reminder", methods=["POST"])
def activate_reminder():
    """
    Эндпоинт для активации напоминания из reminder_service.
    Оптимизированная версия для избежания worker timeout.
    """
    data = request.json
    conv_id = data.get("conv_id")
    reminder_context = data.get("reminder_context_summary")
    
    if not conv_id or not reminder_context:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400
    
    try:
        # Получаем VK API
        vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
        vk_api_local = vk_session.get_api()
        
        # Запускаем активацию в отдельном потоке для избежания блокировки
        activation_thread = threading.Thread(
            target=_activate_reminder_async,
            args=(conv_id, reminder_context, vk_api_local),
            daemon=True
        )
        activation_thread.start()
        
        # Быстро возвращаем успешный ответ
        return jsonify({"status": "success", "message": "Activation started"}), 200
        
    except Exception as e:
        logging.error(f"Ошибка при запуске активации напоминания: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def _activate_reminder_async(conv_id, reminder_context, vk_api_object):
    """
    Асинхронная активация напоминания в отдельном потоке.
    """
    try:
        logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ НАПОМИНАНИЯ: Начинаю обработку для conv_id={conv_id}")
        
        # Создаем минимальный vk_callback_data
        mock_callback_data = {
            "object": {
                "message": {
                    "from_id": conv_id,
                    "peer_id": conv_id,
                    "text": ""  # Пустой текст для напоминания
                }
            },
            "group_id": VK_COMMUNITY_ID
        }

        # Генерируем и отправляем ответ с контекстом напоминания
        generate_and_send_response(
            conv_id_to_respond=conv_id,
            vk_api_for_sending=vk_api_object,
            vk_callback_data=mock_callback_data,
            model=app.model,
            reminder_context=reminder_context
        )
        
        logging.info(f"АСИНХРОННАЯ АКТИВАЦИЯ НАПОМИНАНИЯ: Успешно завершена для conv_id={conv_id}")
        
    except Exception as e:
        logging.error(f"АСИНХРОННАЯ АКТИВАЦИЯ НАПОМИНАНИЯ: Ошибка для conv_id={conv_id}: {e}", exc_info=True)

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
    """Сбрасывает таймер оператора для указанного диалога."""
    if conv_id_for_timer in operator_timers:
        operator_timers[conv_id_for_timer].cancel()
        del operator_timers[conv_id_for_timer]
        logging.info(f"Таймер ответа оператора для диалога {conv_id_for_timer} сброшен.")


def get_last_n_messages(conv_id, n=2):
    """Извлекает последние n сообщений из диалога. Убирает таймштампы из сообщений."""
    conn = None
    messages = []
    try:
        conn = get_main_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT role, message
                FROM dialogues
                WHERE conv_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (conv_id, n)
            )
            rows = cur.fetchall()
            # Убираем временные метки из сообщений для чистоты
            messages = list(reversed([
                {"role": row["role"], "message": re.sub(r'^\\[.*?\\]\\s*', '', row["message"])}
                for row in rows
            ]))
    except psycopg2.Error as e:
        logging.error(f"Ошибка БД при получении последних сообщений для conv_id {conv_id}: {e}")
    finally:
        if conn:
            conn.close()
    return messages

def find_relevant_titles_with_gemini(dialog_snippet, model=None):
    """
    Анализирует фрагмент диалога (последние сообщения) и находит наиболее релевантные заголовки
    в базе знаний с помощью Gemini.
    """
    search_model_to_use = app.search_model if model is None else model
    
    if not isinstance(search_model_to_use, GenerativeModel):
        logging.error("Модель поиска не инициализирована или некорректна.")
        return []

    if not dialog_snippet:
        logging.warning("В find_relevant_titles_with_gemini был передан пустой фрагмент диалога.")
        return []

    formatted_dialog = "\\n".join([f"- {msg['role'].capitalize()}: {msg['message']}" for msg in dialog_snippet])

    all_titles = list(knowledge_base.keys())
    if not all_titles:
        logging.warning("База знаний пуста или не загружена. Поиск релевантных заголовков невозможен.")
        return []
    
    all_titles_text = "\\n".join(f"- {title}" for title in all_titles)

    prompt = f"""
Ты — ассистент, твоя задача — найти наиболее релевантные заголовки из базы знаний на основе диалога. Эти заголовки (с полным текстом подсказки) будут использоваться для подсказки ИИ-бота при ответе клиенту.

Проанализируй последний вопрос клиента в контексте диалога.

**Диалог:**
{formatted_dialog}

**Вот список доступных заголовков из базы знаний:**
---
{all_titles_text}
---

**Твоя задача:**
1.  Внимательно изучи **ПОСЛЕДНЕЕ** сообщение от клиента. Это может быть вопрос или реакция на реплику бота. Важно оценить смысл реплики пользователя в контексте всего диалога.
2.  Определи, какие из **ДОСТУПНЫХ** заголовков могут содержать подсказку для работы ИИ-бота с клиентом, учитывая контекст диалога.
3.  Твой ответ должен быть **СТРОГО** JSON-объектом.
4.  Если ты находишь релевантные заголовки, верни их в виде списка в поле "titles". Например: {{"titles": ["Заголовок 1", "Заголовок 2"]}}.
5.  **КРИТИЧЕСКИ ВАЖНО:** Если ты не находишь **НИ ОДНОГО** подходящего заголовка из списка, или если диалог не содержит конкретного вопроса, верни пустой список в JSON. Вот так: {{"titles": []}}. Не выдумывай заголовки, которых нет в списке.

**Ответ (только JSON):**
"""
    try:
        logging.info(f"Запрос к {SEARCH_MODEL_NAME} для поиска релевантных заголовков по диалогу: {formatted_dialog}")
        response = search_model_to_use.generate_content(prompt)
        
        logging.debug(f"Получен сырой ответ от модели поиска: {response.text}")
        
        # Улучшенный парсинг JSON из ответа модели
        match = re.search(r'\\{.*\\}', response.text, re.DOTALL)
        if not match:
            logging.warning(f"Модель поиска вернула невалидный JSON (не найдена структура {{}}). Ответ: {response.text}")
            return []
            
        json_response = json.loads(match.group(0))
        relevant_titles = json_response.get("titles", [])

        if not isinstance(relevant_titles, list):
            logging.warning(f"Поле 'titles' в JSON-ответе не является списком. Ответ: {response.text}")
            return []

        logging.info(f"{SEARCH_MODEL_NAME} определил следующие релевантные заголовки: {relevant_titles}")
        
        final_titles = [title for title in relevant_titles if title in all_titles]
        if len(final_titles) != len(relevant_titles):
            logging.warning(f"Модель поиска вернула заголовки, которых нет в базе знаний. Они были отфильтрованы. Исходные: {relevant_titles}, Финальные: {final_titles}")
        
        return final_titles

    except json.JSONDecodeError:
        logging.error(f"Ошибка декодирования JSON ответа от модели поиска. Ответ: {response.text}")
        return []
    except Exception as e:
        logging.error(f"Ошибка при взаимодействии с {SEARCH_MODEL_NAME} для поиска релевантных заголовков: {e}", exc_info=True)
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

    if user_question_text:
        prompt_parts.append(f"Текущий вопрос от {user_first_name if user_first_name else 'Пользователя'}: {user_question_text}")
    
    prompt_parts.append("Твой ответ (Модель):")

    full_prompt_text = "\n\n".join(prompt_parts)

    prompt_log_filename = f"prompt_gemini_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S_%f')}.txt"
    prompt_log_filepath = os.path.join(LOGS_DIRECTORY, prompt_log_filename)
    try:
        with open(prompt_log_filepath, "w", encoding="utf-8") as pf:
            pf.write(full_prompt_text)
        logging.info(f"Полный промпт для Gemini сохранён в: {prompt_log_filepath}")
        upload_log_to_yandex_disk(prompt_log_filepath)
    except Exception as e:
        logging.error(f"Ошибка при записи промпта Gemini в файл '{prompt_log_filepath}': {e}")

    for attempt in range(3):
        try:
            response = model.generate_content(full_prompt_text)
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
            response = model.generate_content(prompt_text)
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
def generate_and_send_response(conv_id_to_respond, vk_api_for_sending, vk_callback_data, model, reminder_context=None):
    """
    Вызывается по истечении USER_MESSAGE_BUFFERING_DELAY или при активации напоминания.
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

    is_reminder_call = reminder_context is not None
    combined_user_text = ""

    if not is_reminder_call:
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
    else:
        logging.info(f"Это вызов по напоминанию для conv_id {conv_id_to_respond} (контекст: '{reminder_context}'). Буфер сообщений не используется.")
        # combined_user_text остается пустым, так как вопрос генерируется на основе контекста напоминания.

    if conv_id_to_respond in client_timers:
        del client_timers[conv_id_to_respond]
        logging.debug(f"Клиентский таймер для conv_id {conv_id_to_respond} удален после выполнения.")

    first_name, last_name = get_user_name_from_db(conv_id_to_respond)
    user_display_name = f"{first_name} {last_name}".strip() if first_name or last_name else f"User_{conv_id_to_respond}"

    try:
        context_from_builder = call_context_builder_async(vk_callback_data)
        logging.info(f"Асинхронный Context Builder успешно вернул контекст для conv_id {conv_id_to_respond}")

        # Если это вызов от напоминания, добавляем контекст в начало промпта
        if reminder_context:
            context_from_builder = f"[СИСТЕМНОЕ УВЕДОМЛЕНИЕ] Сработало напоминание. Причина: '{reminder_context}'. Проанализируй весь диалог и реши, уместно ли сейчас возобновлять общение. Если да — напиши релевантное сообщение клиенту. Если нет — верни ПУСТУЮ СТРОКУ.\\n\\n{context_from_builder}"

    except Exception as e:
        logging.error(f"Ошибка вызова Context Builder для conv_id {conv_id_to_respond}: {e}")
        logging.error(f"Обработка запроса для conv_id {conv_id_to_respond} прекращена из-за ошибки Context Builder")
        return

    # 1. Получаем последнее сообщение из БД (это должен быть ответ бота)
    last_messages_from_db = get_last_n_messages(conv_id_to_respond, n=1)
    
    # 2. Формируем новый `dialog_snippet`
    dialog_snippet = []
    if last_messages_from_db:
        dialog_snippet.extend(last_messages_from_db) # Добавляем сообщение бота
    
    # Добавляем текущее сообщение пользователя, которого еще нет в БД
    dialog_snippet.append({"role": "user", "message": combined_user_text})

    logging.info(f"Сформирован dialog_snippet для поиска заголовков: {dialog_snippet}")

    relevant_titles_from_kb = find_relevant_titles_with_gemini(dialog_snippet)

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

    # Для вызовов по напоминанию не сохраняем "пустое" сообщение пользователя
    if not is_reminder_call:
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
                if not is_reminder_call:
                    log_f.write(f"[{timestamp_in_message_text}] {user_display_name} (processed): {combined_user_text}\\n")
                    if relevant_titles_from_kb:
                        log_f.write(f"[{timestamp_in_message_text}] Найденные ключи БЗ (для processed): {', '.join(relevant_titles_from_kb)}\\n")
                else:
                    log_f.write(f"[{timestamp_in_message_text}] Активировано напоминание: {reminder_context}\\n")

                log_f.write(f"[{timestamp_in_message_text}] Context Builder: Context retrieved successfully\\n")
                log_f.write(f"[{timestamp_in_message_text}] Модель: {bot_response_text}\\n\\n")
        except Exception as e:
            logging.error(f"Ошибка записи в локальный лог-файл (processed) '{log_file_path_for_processed}': {e}")
    else:
        logging.warning(f"Путь к лог-файлу для conv_id {conv_id_to_respond} не найден. Логирование обработанных сообщений пропущено.")

    if vk_api_for_sending:
        try:
            vk_api_for_sending.messages.send(
                user_id=conv_id_to_respond,
                message=bot_response_text,
                random_id=int(time.time() * 10000),
                disable_mentions=1
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
    
    # Обработка напоминаний
    try:
        process_reminder_message(conv_id_to_respond)
        logging.info(f"Сервис напоминаний обработал сообщение для conv_id {conv_id_to_respond}")
    except Exception as e:
        logging.error(f"Ошибка при обработке напоминаний для conv_id {conv_id_to_respond}: {e}")


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

# === ПЕРЕНЕСЕННЫЕ ФУНКЦИИ ИЗ CONTEXT_BUILDER.PY ===

# Таблицы, которые нужно исключить из автоматического поиска
EXCLUDED_TABLES = ['operator_activity']
# Лимит на количество последних сообщений для истории диалога
DIALOGUES_LIMIT = 30
# Регулярное выражение для поиска email
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

def context_default_serializer(obj):
    """Сериализатор для JSON, обрабатывающий datetime."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Тип {type(obj)} не сериализуется в JSON")

def fetch_and_update_vk_profile(conn, conv_id):
    """
    Получает данные из VK API и обновляет/создает профиль пользователя в БД.
    """
    if not VK_COMMUNITY_TOKEN:
        logging.warning("VK_COMMUNITY_TOKEN не установлен. Пропуск обновления профиля из VK.")
        return

    # Список полей, которые мы хотим получить из VK API
    fields_to_request = "first_name,last_name,screen_name,sex,city,bdate"
    
    params = {
        'user_ids': conv_id,
        'fields': fields_to_request,
        'access_token': VK_COMMUNITY_TOKEN,
        'v': "5.131"
    }
    
    try:
        response = requests.get("https://api.vk.com/method/users.get", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'error' in data or not data.get('response'):
            logging.error(f"Ошибка VK API: {data.get('error', 'Нет ответа')}")
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
            logging.info(f"Профиль для conv_id {conv_id} успешно создан/обновлен из VK API.")

    except requests.RequestException as e:
        logging.error(f"Ошибка сети при запросе к VK API: {e}")
    except (KeyError, IndexError) as e:
        logging.error(f"Ошибка при парсинге ответа от VK API: {e}")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Ошибка БД при обновлении профиля: {e}")

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
                logging.info(f"Связано {updated_rows} покупок с conv_id {conv_id} по email {email_to_update}")
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Ошибка при обновлении conv_id по email: {e}")

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
        logging.error(f"Не удалось получить список таблиц из БД: {e}")
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
        logging.error(f"Ошибка при извлечении данных из таблицы '{table_name}': {e}")
        return []

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
        row_str = json.dumps(row, ensure_ascii=False, indent=None, default=context_default_serializer)
        lines.append(f"- Запись {i+1}: {row_str}")
    return "\n".join(lines)

def build_context_sync(vk_callback_data):
    """
    Синхронная версия context builder - перенесенная логика из context_builder.py
    """
    try:
        # Извлекаем ID пользователя (from_id) из структуры VK
        message_data = vk_callback_data.get("object", {}).get("message", {})
        conv_id = message_data.get("from_id")
        message_text = message_data.get("text", "")

        if not conv_id:
            # Проверяем старый формат на всякий случай
            conv_id = vk_callback_data.get("conv_id")
            if not conv_id:
                raise ValueError("Не найден 'from_id' или 'conv_id' во входных данных.")

        output_blocks = []

        # Работа с базой данных
        with get_main_db_connection() as conn:
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

        # Формирование итогового результата
        final_context = "\n\n".join(output_blocks)
        return final_context

    except Exception as e:
        logging.error(f"FATAL ERROR in build_context_sync: {e}")
        raise Exception(f"Context Builder Error: {e}")

def call_context_builder_async(vk_callback_data):
    """
    Неблокирующий вызов context builder через ThreadPoolExecutor
    """
    try:
        future = context_executor.submit(build_context_sync, vk_callback_data)
        # Ждем результат, но это не блокирует других пользователей
        # так как каждый пользователь обрабатывается в своем потоке
        context_result = future.result(timeout=45)
        logging.info(f"Context Builder успешно вернул контекст (длина: {len(context_result)} символов)")
        return context_result
    except Exception as e:
        logging.error(f"Ошибка при асинхронном вызове Context Builder: {e}")
        raise Exception(f"Context Builder Error: {e}")

# === КОНЕЦ ПЕРЕНЕСЕННЫХ ФУНКЦИЙ ===

if __name__ == "__main__":
    # Этот блок теперь используется только для локального запуска и отладки.
    # На Railway будет использоваться gunicorn, и этот код выполняться не будет.
    if not DATABASE_URL:
        logging.critical("Переменная окружения DATABASE_URL не установлена. Приложение не может запуститься.")
        exit(1)
    if not VK_COMMUNITY_TOKEN or not VK_CONFIRMATION_TOKEN:
        logging.critical("Переменные окружения VK_COMMUNITY_TOKEN или VK_CONFIRMATION_TOKEN не установлены.")
    
    # Инициализация сервиса напоминаний
    if not initialize_reminder_service():
        logging.error("Не удалось инициализировать сервис напоминаний. Продолжаем без него.")
    
    logging.info("Запуск Flask-приложения в режиме разработки...")
    server_port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=server_port, debug=False)