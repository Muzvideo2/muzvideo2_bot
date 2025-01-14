# -*- coding: utf-8 -*-
"""Код бота-продажника muzvideo2

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1nfapwF-aNiQrpn3GGGcvkShvU_Ce0-3e
"""

import os
import time
import json
import requests
from datetime import datetime, timedelta
import threading

# ====== БИБЛИОТЕКИ ДЛЯ РАБОТЫ С VK ======
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType

# ==============================
# Читаем переменные окружения (секретные данные)
# ==============================
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
ADMIN_CHAT_ID      = os.environ.get("ADMIN_CHAT_ID", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
VK_COMMUNITY_TOKEN = os.environ.get("VK_COMMUNITY_TOKEN", "")

# === Новый токен для Яндекс.Диска ===
YANDEX_DISK_TOKEN  = os.environ.get("YANDEX_DISK_TOKEN", "")

# ==============================
# Пути к файлам
# ==============================
knowledge_base_path = "knowledge_base.json"
prompt_path         = "prompt.txt"
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

# Лог-файл (один общий)
log_file_path = os.path.join(logs_directory, f"dialog_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.txt")

# ==============================
# 1. ФУНКЦИИ УВЕДОМЛЕНИЙ В ТЕЛЕГРАМ
# ==============================
def send_telegram_notification(user_question, dialog_id):
    current_time = datetime.utcnow() + timedelta(hours=6)  # Время Омска (+6 к UTC)
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    message = f"""
🕒 Дата и время (Омск): {formatted_time}
👤 Стартовый вопрос: {user_question}
🔗 Ссылка на диалог: https://vk.com/im?sel={dialog_id}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": ADMIN_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)

def send_operator_notification(dialog_id, initial_question, dialog_summary, reason_guess):
    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    message = f"""
🆘 Запрос оператора!
🕒 Дата и время (Омск): {formatted_time}
👤 Изначальный вопрос клиента: {initial_question}
📋 Обсуждение в ходе диалога: {dialog_summary}
🤔 Предполагаемая причина запроса оператора: {reason_guess}
🔗 Ссылка на диалог: https://vk.com/im?sel={dialog_id}
    """.strip()

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": ADMIN_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)

# ==============================
# 2. РАБОТА С ЯНДЕКС.ДИСКОМ: ЗАГРУЗКА ЛОГ-ФАЙЛОВ
# ==============================
def upload_log_to_yandex_disk(log_file_path):
    """
    Загрузка лог-файла на Яндекс.Диск при помощи REST API.

    Алгоритм:
    1) Выполнить GET-запрос на URL:
       https://cloud-api.yandex.net/v1/disk/resources/upload
       с параметрами: path=<путь в Я.Диске>, overwrite=true
       и заголовком Authorization: OAuth <YANDEX_DISK_TOKEN>
       Получить "href" для загрузки.
    2) Выполнить PUT-запрос на полученный "href",
       передав сам файл. Код 201 = успех.

    Нужно иметь в переменной окружения:
      YANDEX_DISK_TOKEN
    """
    if not os.path.exists(log_file_path):
        return

    if not YANDEX_DISK_TOKEN:
        print("YANDEX_DISK_TOKEN не задан. Пропускаем загрузку логов.")
        return

    file_name = os.path.basename(log_file_path)
    ya_path = f"disk:/app-logs/{file_name}"

    # Шаг 1: получаем ссылку для загрузки
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

    # Шаг 2: загружаем файл по ссылке
    with open(log_file_path, "rb") as f:
        upload_resp = requests.put(href, files={"file": f})
        if upload_resp.status_code == 201:
            print(f"Лог-файл {file_name} успешно загружен на Яндекс.Диск.")
        else:
            print("Ошибка загрузки на Яндекс.Диск:", upload_resp.text)

# ==============================
# 3. ЛОГИРОВАНИЕ
# ==============================
def log_dialog(user_question, bot_response, relevant_titles, relevant_answers, user_id):
    current_time = datetime.utcnow() + timedelta(hours=6)
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"[{formatted_time}] user_id={user_id}, Пользователь: {user_question}\n")
        if relevant_titles and relevant_answers:
            for title, answer in zip(relevant_titles, relevant_answers):
                log_file.write(f"[{formatted_time}] Найдено в базе знаний: {title} -> {answer}\n")
        log_file.write(f"[{formatted_time}] Модель: {bot_response}\n\n")

    # Загружаем в Яндекс.Диск (вместо Google Drive)
    upload_log_to_yandex_disk(log_file_path)

# ==============================
# 4. ИНТЕГРАЦИЯ С GEMINI
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

def generate_response(user_question, dialog_history, custom_prompt, relevant_answers=None):
    history_text = "\n".join([
    f"Пользователь: {turn.get('user', 'Неизвестно')}\nМодель: {turn.get('bot', 'Неизвестно')}"
    for turn in dialog_history
])

    knowledge_hint = f"Подсказки из базы знаний: {relevant_answers}" if relevant_answers else ""

    full_prompt = (
        f"{custom_prompt}\n\n"
        f"Контекст диалога:\n{history_text}\n\n"
        f"{knowledge_hint}\n\n"
        f"Пользователь: {user_question}\nМодель:"
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
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return f"Ошибка: {resp.status_code}. {resp.text}"
    return "Извините, я сейчас не могу ответить. Попробуйте позже."

def generate_summary_and_reason(dialog_history):
    history_text = " | ".join([f"Пользователь: {turn['user']} -> Модель: {turn['bot']}"
                               for turn in dialog_history[-10:]])
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
                reason_guess = output[1].strip() if len(output) > 1 else "Причина не определена"
                return dialog_summary, reason_guess
            except (KeyError, IndexError):
                return "Сводка не сформирована", "Причина не определена"
        elif resp.status_code == 500:
            time.sleep(10)
        else:
            return "Ошибка API", "Ошибка API"
    return "Не удалось связаться с сервисом", "Не удалось связаться с сервисом"

# ==============================
# 5. 30-секундная задержка и буфер сообщений
# ==============================
user_buffers = {}
user_timers  = {}
last_questions = {}

DELAY_SECONDS = 30

# ==============================
# 6. ПАУЗА ДЛЯ КОНКРЕТНОГО ПОЛЬЗОВАТЕЛЯ
# ==============================
paused_users = set()

def handle_new_message(user_id, text, vk, is_outgoing=False):
    """
    Обрабатывает новое сообщение.
    :param user_id: ID пользователя
    :param text: текст сообщения
    :param vk: объект VK API
    :param is_outgoing: True, если сообщение исходящее
    """
    lower_text = text.lower()

    # Логируем все сообщения (входящие и исходящие)
    dialog_history_dict.setdefault(user_id, []).append({
        "user" if not is_outgoing else "operator": text
    })

    if is_outgoing:
        # Реагируем только на команды "поставить на паузу" или "снять с паузы"
        if "я поставил бота на паузу" in lower_text:
            paused_users.add(user_id)
            print(f"Пользователь {user_id} поставлен на паузу. Бот не будет отвечать.")
        elif "бот снова будет отвечать" in lower_text:
            paused_users.discard(user_id)
            print(f"Пользователь {user_id} снят с паузы. Бот снова будет отвечать.")
        return  # Игнорируем остальные исходящие сообщения

    # Если пользователь на паузе, игнорируем входящее сообщение
    if user_id in paused_users:
        return

    # Обычная обработка входящих сообщений
    user_buffers.setdefault(user_id, []).append(text)
    last_questions[user_id] = text

    if user_id in user_timers:
        user_timers[user_id].cancel()

    timer = threading.Timer(DELAY_SECONDS, generate_and_send_response, args=(user_id, vk))
    user_timers[user_id] = timer
    timer.start()


def generate_and_send_response(user_id, vk):
    if user_id in paused_users:
        user_buffers[user_id] = []
        return

    msgs = user_buffers.get(user_id, [])
    if not msgs:
        return
    combined_text = "\n".join(msgs)
    user_buffers[user_id] = []

    dialog_history = dialog_history_dict[user_id]
    relevant_titles = find_relevant_titles_with_gemini(combined_text)
    relevant_answers = [knowledge_base[t] for t in relevant_titles if t in knowledge_base]

    model_response = generate_response(combined_text, dialog_history, custom_prompt, relevant_answers)

    log_dialog(combined_text, model_response, relevant_titles, relevant_answers, user_id)

    if "оператор" in combined_text.lower():
        summary, reason = generate_summary_and_reason(dialog_history)
        initial_q = last_questions.get(user_id, "")
        send_operator_notification(user_id, initial_q, summary, reason)

    dialog_history.append({"user": combined_text, "bot": model_response})

    vk.messages.send(
        user_id=user_id,
        message=model_response,
        random_id=int(time.time() * 1000)
    )

# ==============================
# 7. ОСНОВНОЙ ЦИКЛ (Long Poll)
# ==============================
def main():
    vk_session = vk_api.VkApi(token=VK_COMMUNITY_TOKEN)
    vk = vk_session.get_api()

    longpoll = VkLongPoll(vk_session)
    print("Бот запущен и слушает сообщения...")

    for event in longpoll.listen():  # Цикл перенесён внутрь main
        if event.type == VkEventType.MESSAGE_NEW:
            user_id = event.user_id
            text = event.text.strip()
            is_outgoing = not event.to_me  # Если сообщение исходящее
            if text:
                handle_new_message(user_id, text, vk, is_outgoing=is_outgoing)


if __name__ == "__main__":
    main()