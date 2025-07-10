# =======================================================================================
#    СКРИПТ СОЗДАНИЯ НЕДОСТАЮЩИХ ПРОФИЛЕЙ ПОЛЬЗОВАТЕЛЕЙ
# =======================================================================================
#
# ЧТО ОН ДЕЛАЕТ:
# 1. Находит пользователей, у которых есть диалоги в БД, но нет профиля.
# 2. Для каждого такого пользователя запрашивает актуальные данные из VK API.
# 3. Собирает историю диалога из БД.
# 4. Отправляет объединенные данные (VK профиль + диалог) в Gemini для анализа.
# 5. Создает в базе данных `user_profiles` полноценную карточку клиента.
#
# =======================================================================================
#               ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ
# =======================================================================================
#
# ШАГ 1: УСТАНОВКА ЗАВИСИМОСТЕЙ
# -----------------------------
# Откройте терминал и выполните команду:
#    pip install "google-cloud-aiplatform>=1.38" tqdm psycopg2-binary requests python-dotenv
#
# ШАG 2: НАСТРОЙКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
# ---------------------------------------------
# Убедитесь, что в файле `.env` в этой же папке есть следующие переменные:
#   - `POSTGRES_DSN`: Строка подключения к вашей базе данных.
#   - `VK_TOKEN1`: Сервисный токен VK.
#   - `GOOGLE_APPLICATION_CREDENTIALS`: Путь к вашему JSON-ключу Google.
#
# ШАГ 3: ЗАПУСК
# -----------------
#   - Для безопасного "сухого запуска" (покажет, что будет делать, но не изменит БД):
#       python create_missing_profiles.py
#
#   - Для реального создания профилей в базе данных:
#       python create_missing_profiles.py --execute
#
# =======================================================================================

import os
import json
import logging
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import DictCursor
import requests
from dotenv import load_dotenv
from google.cloud import aiplatform
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel
from tqdm import tqdm

# --- НАСТРОЙКИ ---
LOG_FILE_NAME = "create_missing_profiles_errors.log"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "zeta-tracer-462306-r7")
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.0-flash"

MAX_WORKERS = 20  # Снижаем для стабильности при работе с разными API
API_RETRY_COUNT = 3
API_RETRY_DELAY = 10

# --- Промпт для Gemini ---
SYSTEM_PROMPT = """
Ты — продвинутый CRM-аналитик, специализирующийся на анализе диалогов в сфере онлайн-образования (обучение игре на фортепиано).
Твоя задача — проанализировать предоставленную историю переписки между клиентом и сообществом и извлечь из нее ключевую информацию.
Верни ответ СТРОГО в формате JSON-объекта без каких-либо дополнительных слов, пояснений или markdown-разметки.

### СТРУКТУРА JSON-ОТВЕТА:

```json
{{
  "summary": "Подробное саммари диалога",
  "client_level": ["список", "всех", "уровней", "упомянутых", "клиентом"],
  "learning_goals": ["список", "целей", "обучения", "клиента"],
  "purchased_products": ["список", "названий", "купленных", "продуктов"],
  "client_pains": ["список", "озвученных", "проблем", "и", "болей"],
  "email": "все_найденные_email_от_клиента",
  "lead_qualification": "выбери_значение_из_списка",
  "funnel_stage": "выбери_значение_из_списка"
}}
```

### ИНСТРУКЦИИ ПО ЗАПОЛНЕНИЮ ПОЛЕЙ:

1.  **summary**: Сделай подробное, ёмкое саммари всего диалога. Опиши:
- с чего началось общение
- историю жизни клиента (если рассказывал)
- историю его музыкальной жизни (если рассказывал)
- его цели и мечты в жизни и музыке
- наличие инструмента для занятий (есть или нет, какой именно инструмент есть или хочет приобрести)
- ключевые вопросы клиента к нашей школе
- обсуждавшиеся темы
- чего хочет добиться в игре на фортепиано (словами клиента)
- какие курсы он рассматривал
- какие курсы или наборы курсов ему предлагались с нашей стороны
- что его заинтересовало из курсов и наборов
- по какой цене было сделано предложение каждого из продуктов
- реакция клиента на предложение (возражения, отказ, покупка)
- ответные действия со стороны школы
- общий настрой на покупку после обсуждений (сомнительный, воодушевлённый, высокая готовность).
Обрати внимание на опросы, которые проводились школой - если клиент участвовал в опросе, то какие курсы он отмечал как "купленные", как он называл свой уровень игры.
Выпиши все сомнения клиента его же словами, чтобы использовать это для дожима.

2.  **client_level**: Извлеки ВСЕ упоминания уровня клиента. Если клиент называл себя и "продвинутым", и "вспоминающим", включи оба.
    *   Возможные значения (но не ограничиваясь ими): 'начинающий', 'продолжающий', 'продвинутый', 'вспоминающий', 'преподаватель', 'музыкант', 'с нуля', 'ничего не умею'.
    *   Если уровень не упоминался, верни пустой список `[]`.

3.  **learning_goals**: Извлеки ВСЕ цели обучения, которые упоминал клиент.
    *   Возможные значения: 'игра по нотам', 'подбор на слух', 'импровизация' (уточни стиль, если есть, например 'импровизация (блюз)'), 'сочинение музыки', 'аранжировка', 'аккомпанемент', 'для себя', 'для преподавания'.
    *   Если цели не ясны, верни пустой список `[]`.

4.  **purchased_products**: Извлеки названия всех курсов, наборов или мастер-классов, которые клиент УЖЕ КУПИЛ или упомянул как купленные. Это могла быть реакция на вопрос сообщества, например: "какие курсы у вас есть?" и ответ "Аккорд-Мастер".
    * Если ничего не куплено, верни пустой список `[]`.
    * Список продуктов для поиска:
        - Курс "Первые шаги"
        - "Аккорд Мастер"
        - Курс "Игра по нотам с листа"
        - Курс "Мастер Гармонии"
        - Курс "Ноты прочь! Играю что хочу!"
        - Курс "Мастер Аранжировки"
        - Курс "Блюз Мастер"
        - Курс "Баллад Мастер"
        - Курс "10 шагов к музыкальной свободе"
        - Курс "Качественная запись фортепиано"
        - Курс "Виртуозная техника игры за 12 минут в день"
        - Курс "Импровизация с нуля"
        - МК "Нотатор MuseScore"
        - МК "Стиль Чарли Паркера"
        - МК "Сочинение музыки"
        - Набор "Всё включено"
        - Набор "6 шагов"
        - Набор "6 шагов (для продвинутых)"
        - Набор "Начинающий+"
        - Набор "Аккорд-Мастер+Мастер Гармонии"
        - Набор "Блюз+Баллад+Аккорды"


5.  **client_pains**: Список конкретных трудностей и "болей", озвученных клиентом.
    *   Примеры: "не хватает времени", "сложно играть двумя руками", "путаюсь в аккордах", "боюсь сцены", "не понимаю теорию".
    *   Если боли не озвучены, верни пустой список `[]`.

6.  **email**: Извлеки все валидные емейлы, который клиент написал в диалоге. Выпиши все не повторяющиеся емейлы.
    *   Если email не найден, верни пустую строку `""`.

7.  **lead_qualification**: Оцени общую "теплоту" клиента на основе всего диалога. Выбери ОДНО из следующих значений:
    *   `'холодный'`: Только подписался, общие вопросы, не проявляет интереса к покупке.
    *   `'тёплый'`: Задает вопросы о курсах, ценах, условиях, сравнивает, проявляет явный интерес.
    *   `'горячий'`: Готов купить, просит ссылку на оплату, обсуждает детали покупки.
    *   `'клиент'`: Уже совершил покупку.
    *   `'не определено'`: Невозможно сделать вывод.

8.  **funnel_stage**: Определи текущий этап воронки продаж. Выбери ОДНО из следующих значений:
    *   `'предложение по продуктам ещё не сделано'`: Обсуждение общих тем, нет конкретики по курсам.
    *   `'сделано предложение по продуктам'`: Были предложены конкретные курсы/наборы.
    *   `'клиент думает'`: Клиент взял время на раздумье после предложения.
    *   `'решение принято (ожидаем оплату)'`: Клиент согласился на покупку, ждет реквизиты.
    *   `'отказ от покупки'`: Клиент явно отказался от предложенного продукта.
    *   `'покупка совершена'`: Диалог подтверждает факт оплаты.
    *   `'не применимо'`: Диалог не связан с продажей (например, техподдержка).

### ПРОФИЛЬ КЛИЕНТА ИЗ VK (для дополнительного контекста):
{vk_user_info}

### ДИАЛОГ ДЛЯ АНАЛИЗА:
"""

MERGE_PROMPT = """Ты — редактор-аналитик. Тебе предоставлены несколько последовательных частей саммари одного длинного диалога.
Твоя задача — объединить их в одно единое, логичное и подробное итоговое саммари, устранив повторения и сохранив все ключевые детали, факты и цитаты из всех частей.
Не выдумывай информацию. Просто скомпилируй предоставленные части в качественный, целостный текст.

Вот части для анализа:
"""

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    file_handler = logging.FileHandler(LOG_FILE_NAME, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - Файл %(filename)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)

def get_db_connection():
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise ValueError("Переменная окружения POSTGRES_DSN не найдена.")
    return psycopg2.connect(dsn)

def get_orphan_conv_ids(conn) -> List[int]:
    logger = logging.getLogger(__name__)
    logger.info("Шаг 1: Поиск 'осиротевших' ID пользователей...")
    with conn.cursor() as cur:
        query = """
            SELECT DISTINCT d.conv_id
            FROM dialogues d
            LEFT JOIN user_profiles up ON d.conv_id = up.conv_id
            WHERE up.conv_id IS NULL;
        """
        cur.execute(query)
        user_ids = [row[0] for row in cur.fetchall()]
        logger.info(f"Найдено {len(user_ids)} 'осиротевших' пользователей.")
        return user_ids

def get_vk_user_info(conv_id: int, token: str) -> Optional[Dict]:
    try:
        url = "https://api.vk.com/method/users.get"
        params = {
            'user_ids': conv_id,
            'fields': 'first_name,last_name,city,sex,bdate',
            'access_token': token,
            'v': '5.131'
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if 'response' in data and data['response']:
            return data['response'][0]
        else:
            logging.warning(f"Не удалось получить информацию из VK для conv_id={conv_id}. Ответ: {data.get('error', {}).get('error_msg')}")
            return None
    except Exception as e:
        logging.error(f"Ошибка запроса к VK API для conv_id={conv_id}: {e}")
        return None

def get_dialogue_history(conn, conv_id: int) -> List[Dict]:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            "SELECT role, message FROM dialogues WHERE conv_id = %s ORDER BY created_at ASC",
            (conv_id,)
        )
        return [dict(row) for row in cur.fetchall()]

def preprocess_dialogue(dialogue_history: List[Dict]) -> str:
    if not dialogue_history:
        return ""
    history_text = []
    for message in dialogue_history:
        sender = "Клиент" if message.get('role') == 'user' else "Менеджер"
        text = message.get('message', '')
        history_text.append(f"{sender}: {text}")
    return "\n".join(history_text)

def call_gemini_with_retry(prompt, credentials):
    vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
    model = GenerativeModel(MODEL_NAME)
    for attempt in range(API_RETRY_COUNT):
        try:
            response = model.generate_content(prompt)
            cleaned_response = response.text.strip().lstrip("```json").rstrip("```").strip()
            return json.loads(cleaned_response)
        except Exception as e:
            logging.warning(f"Ошибка API (попытка {attempt + 1}/{API_RETRY_COUNT}): {e}")
            if attempt < API_RETRY_COUNT - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                raise

def call_gemini_for_text_with_retry(prompt, credentials):
    vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
    model = GenerativeModel(MODEL_NAME)
    for attempt in range(API_RETRY_COUNT):
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logging.warning(f"Ошибка слияния саммари (попытка {attempt + 1}/{API_RETRY_COUNT}): {e}")
            if attempt < API_RETRY_COUNT - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                raise
    return "Не удалось объединить части саммари из-за ошибки API."

def merge_summaries(summaries, credentials):
    final_summary = {}
    all_levels, all_goals, all_products, all_pains, all_emails = [], [], [], [], []

    for summary in summaries:
        all_levels.extend(summary.get('client_level', []))
        all_goals.extend(summary.get('learning_goals', []))
        all_products.extend(summary.get('purchased_products', []))
        all_pains.extend(summary.get('client_pains', []))
        email = summary.get('email', "")
        if email and isinstance(email, str):
            all_emails.append(email)
        elif isinstance(email, list):
            all_emails.extend(email)
    
    final_summary['client_level'] = sorted(list(set(all_levels)))
    final_summary['learning_goals'] = sorted(list(set(all_goals)))
    final_summary['purchased_products'] = sorted(list(set(all_products)))
    final_summary['client_pains'] = sorted(list(set(all_pains)))
    final_summary['email'] = sorted(list(set(e for e in all_emails if e)))

    if summaries:
        last_summary = summaries[-1]
        final_summary['lead_qualification'] = last_summary.get('lead_qualification', 'не определено')
        final_summary['funnel_stage'] = last_summary.get('funnel_stage', 'не применимо')

    summary_texts_to_merge = ""
    for i, summary in enumerate(summaries):
        summary_texts_to_merge += f"### Часть {i+1}:\n{summary.get('summary', '')}\n\n"

    final_prompt = MERGE_PROMPT + summary_texts_to_merge
    final_summary_text = call_gemini_for_text_with_retry(final_prompt, credentials)
    final_summary['summary'] = final_summary_text

    return final_summary

def insert_user_profile(conn, conv_id: int, vk_info: Dict, summary_data: Dict):
    logger = logging.getLogger(__name__)
    logger.info(f"Создание профиля для conv_id={conv_id}...")
    with conn.cursor() as cur:
        query = """
            INSERT INTO user_profiles (
                conv_id, first_name, last_name, city, sex,
                dialogue_summary, client_level, learning_goals, 
                client_pains, email, lead_qualification, funnel_stage,
                can_write, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (conv_id) DO NOTHING;
        """
        
        email_val = summary_data.get('email')
        # Обрабатываем email: если строка непустая, разделяем по запятым, иначе NULL
        emails_list = [e.strip() for e in email_val.split(',')] if email_val else None

        params = (
            conv_id,
            vk_info.get('first_name', 'Неизвестно'),
            vk_info.get('last_name', 'Неизвестно'),
            vk_info.get('city', {}).get('title') if vk_info.get('city') else None,
            vk_info.get('sex'),
            summary_data.get('summary'),
            summary_data.get('client_level'),
            summary_data.get('learning_goals'),
            summary_data.get('client_pains'),
            emails_list,
            # Оборачиваем в список, чтобы соответствовать типу TEXT[] в БД
            [summary_data.get('lead_qualification')] if summary_data.get('lead_qualification') else None,
            [summary_data.get('funnel_stage')] if summary_data.get('funnel_stage') else None,
            True
        )
        cur.execute(query, params)
    conn.commit()
    logger.info(f"Профиль для conv_id={conv_id} успешно создан.")


def process_orphan_user(conv_id, vk_token, credentials, dry_run):
    logger = logging.getLogger(__name__)
    try:
        # DB connection создается для каждого потока
        conn = get_db_connection()

        vk_info = get_vk_user_info(conv_id, vk_token)
        if not vk_info:
            return 'error', conv_id, "Не удалось получить данные из VK"

        dialogue_history = get_dialogue_history(conn, conv_id)
        if not dialogue_history:
            return 'skipped', conv_id, "Нет истории диалога"
        
        dialogue_text = preprocess_dialogue(dialogue_history)
        
        # Обновляем промпт, чтобы он содержал информацию о пользователе из ВК
        prompt_with_vk_info = SYSTEM_PROMPT.format(vk_user_info=json.dumps(vk_info, ensure_ascii=False, indent=2))
        full_prompt = f"{prompt_with_vk_info}\n\n{dialogue_text}"
        
        if dry_run:
            logger.info(f"[DRY-RUN] Для conv_id={conv_id} будет сгенерирован и загружен профиль.")
            return 'dry_run_success', conv_id, None

        # Реальный вызов
        summary_data = call_gemini_with_retry(full_prompt, credentials)
        insert_user_profile(conn, conv_id, vk_info, summary_data)
        
        conn.close()
        return 'success', conv_id, None
    except Exception as e:
        error_message = f"Критическая ошибка в потоке для conv_id={conv_id}: {type(e).__name__}: {e}"
        logging.error(error_message, exc_info=True)
        return 'error', conv_id, error_message
    finally:
        if 'conn' in locals() and conn and not conn.closed:
            conn.close()


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    load_dotenv()

    parser = argparse.ArgumentParser(description="Создание недостающих профилей пользователей.")
    parser.add_argument('--execute', action='store_true', help="Реально создать профили в БД.")
    args = parser.parse_args()
    dry_run = not args.execute

    if dry_run:
        logger.info("="*50 + "\nЗАПУСК В БЕЗОПАСНОМ РЕЖИМЕ (DRY-RUN).\n" + "="*50)
    else:
        logger.warning("="*50 + "\n!!! ЗАПУСК В БОЕВОМ РЕЖИМЕ !!!\n" + "="*50)
        time.sleep(3)

    # --- Загрузка учетных данных ---
    vk_token = os.getenv("VK_TOKEN1")
    if not vk_token:
        logger.critical("Критическая ошибка: Переменная окружения 'VK_TOKEN1' не найдена.")
        return
        
    try:
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip(' "')
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        logger.info("Учетные данные Google Cloud успешно загружены.")
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить учетные данные Google Cloud. Ошибка: {e}")
        return

    conn = None
    try:
        conn = get_db_connection()
        orphan_ids = get_orphan_conv_ids(conn)
        conn.close() # Закрываем основное соединение, потоки откроют свои

        if not orphan_ids:
            return

        logger.info(f"Начинаем обработку {len(orphan_ids)} пользователей в {MAX_WORKERS} потоков...")
        
        success_count, error_count, skipped_count = 0, 0, 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(process_orphan_user, conv_id, vk_token, credentials, dry_run): conv_id for conv_id in orphan_ids}
            
            for future in tqdm(as_completed(future_to_id), total=len(orphan_ids), desc="Создание профилей"):
                try:
                    status, _, _ = future.result()
                    if status == 'success' or status == 'dry_run_success':
                        success_count += 1
                    elif status == 'skipped':
                        skipped_count += 1
                    else:
                        error_count += 1
                except Exception as exc:
                    logging.error(f"ID {future_to_id[future]} вызвал исключение: {exc}", exc_info=True)
                    error_count += 1

        print("\n" + "="*30)
        print("Анализ завершен.")
        print(f"  Успешно обработано/запланировано: {success_count}")
        print(f"  Пропущено (нет диалогов): {skipped_count}")
        print(f"  Ошибок: {error_count}")
        if error_count > 0:
            print(f"  Подробности об ошибках смотрите в файле: {LOG_FILE_NAME}")
        print("="*30)

    except Exception as e:
        logger.critical(f"Произошла глобальная ошибка в main: {e}", exc_info=True)
    finally:
        if conn and not conn.closed:
            conn.close()

if __name__ == "__main__":
    main()