# ====
#    СЕРВИС ИНКРЕМЕНТАЛЬНОГО ОБНОВЛЕНИЯ САММАРИ (ВЕРСИЯ 3.1 - VERTEX AI)
# ====
#
# ЧТО ДЕЛАЕТ ЭТОТ СКРИПТ:
#
# Этот скрипт — "умный архивариус", работающий в фоновом режиме. Он запускается
# после того, как ИИ-сотрудник ответил клиенту.
#
# ЕГО ЗАДАЧИ:
#
# 1.  ДОПОЛНИТЬ САММАРИ: Он берет существующее саммари диалога, последние несколько
#    сообщений и с помощью нейросети Gemini "дописывает" в саммари новую информацию,
#    не теряя старую.
#
# 2.  ИЗВЛЕЧЬ НОВЫЕ ФАКТЫ: Анализирует только самые свежие сообщения на предмет
#    новых "болей" клиента, его целей, упомянутых покупок и т.д.
#
# 3.  ОБОГАТИТЬ КАРТОЧКУ КЛИЕНТА: Аккуратно добавляет найденные факты в профиль
#    клиента в базе данных, не перезаписывая, а дополняя существующие списки.
#
# 4.  ПОЧИСТИТЬ ИСТОРИЮ: Чтобы база диалогов не разрасталась до бесконечности,
#    он удаляет все сообщения, кроме последних 30, сохраняя историю компактной.
#
# КАК ЗАПУСКАЕТСЯ:
# Его вызывает основной скрипт (main.py) асинхронно, передавая ему ID диалога
# (conv_id) через стандартный ввод.
#
# ====

import os
import sys
import json
import logging
import re
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

try:
    import vertexai
    from vertexai.generative_models import GenerativeModel
    from google.oauth2 import service_account
    VERTEXAI_AVAILABLE = True
except ImportError:
    VERTEXAI_AVAILABLE = False

if not VERTEXAI_AVAILABLE:
    print("Критическая ошибка: Vertex AI SDK не доступен. Установите библиотеку 'google-cloud-aiplatform'.", file=sys.stderr)
    sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "zeta-tracer-462306-r7")
LOCATION = os.environ.get("GEMINI_LOCATION", "us-central1")

MODEL_NAME = "gemini-2.5-flash"
API_TIMEOUT = 45

NUM_MESSAGES_TO_FETCH = 15
NUM_MESSAGES_TO_KEEP = 30
LOG_FILE_NAME = "summary_updater.log"

FUNNEL_STAGE_HIERARCHY = {
    'предложение по продуктам ещё не сделано': 1,
    'сделано предложение по продуктам': 2,
    'сделано новое предложение': 3,
    'клиент думает': 4,
    'отказ от покупки': 5,
    'решение принято (ожидаем оплату)': 6,
    'покупка совершена': 7,
    'не применимо': 0
}

PROMPT_INCREMENTAL_SUMMARY = """
Ты — ИИ-редактор, специализирующийся на обновлении CRM-записей.
Тебе предоставлено СУЩЕСТВУЮЩЕЕ САММАРИ по клиенту и НОВЫЕ СООБЩЕНИЯ из его недавнего диалога.

Твоя задача — инкрементально обновить саммари. Прочитай новые сообщения и ДОПОЛНИ существующее саммари новыми фактами, деталями, вопросами клиента, его возражениями, а также ответами и предложениями со стороны школы.

КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО терять информацию из старого саммари. Ты должен вернуть ЕДИНОЕ, ПОЛНОЕ, ОБНОВЛЕННОЕ саммари, которое включает в себя как старую, так и новую информацию, изложенную логично и последовательно. Не добавляй никаких вступлений или заключений, верни только текст самого саммари.

--- СУЩЕСТВУЮЩЕЕ САММАРИ ---
{existing_summary}

--- НОВЫЕ СООБЩЕНИЯ ДЛЯ АНАЛИЗА ---
{new_messages_text}

--- ИТОГОВОЕ ОБНОВЛЕННОЕ САММАРИ ---
"""

PROMPT_EXTRACT_NEW_FACTS = """
Ты — продвинутый CRM-аналитик. Проанализируй ТОЛЬКО ЭТОТ ФРАГМЕНТ ДИАЛОГА и извлеки из него ключевую информацию.
Верни ответ СТРОГО в формате JSON-объекта без каких-либо дополнительных слов или markdown-разметки.
Если в этих сообщениях нет информации для какого-то поля, верни для него пустой список `[]` или пустую строку `""`.

### СТРУКТУРА JSON-ОТВЕТА:
```json
{{
  "client_level": ["уровни", "упомянутые", "в новых сообщениях"],
  "learning_goals": ["цели", "из новых сообщений"],
  "purchased_products": ["купленные продукты", "из новых сообщений"],
  "client_pains": ["боли и проблемы", "из новых сообщений"],
  "email": ["список", "всех", "email", "из новых сообщений"],
  "lead_qualification": "оцени 'теплоту' на основе этих сообщений",
  "funnel_stage": "определи этап воронки по этим сообщениям",
  "client_activity": "определи активность клиента по этим сообщениям"
}}
```

ИНСТРУКЦИИ ПО ЗАПОЛНЕНИЮ ПОЛЕЙ (ПРИМЕНЯЙ ТОЛЬКО К НОВЫМ СООБЩЕНИЯМ):
client_level: Уровни, упомянутые клиентом ('начинающий', 'продвинутый' и т.д.).
learning_goals: Цели обучения ('импровизация', 'подбор на слух' и т.д.).
purchased_products: Курсы, которые клиент упомянул как уже купленные.
client_pains: Трудности и "боли" ("не хватает времени", "сложно играть" и т.д.).
email: Извлеки ВСЕ email-адреса, которые клиент написал. Верни их в виде списка строк.
lead_qualification: Оцени "теплоту" клиента ('холодный', 'тёплый', 'горячий', 'клиент', 'не определено').
funnel_stage: Определи этап воронки. Если клиенту, который уже купил или отказался, делают НОВОЕ предложение, используй статус 'сделано новое предложение'. Если не можешь определить этап, используй 'не применимо'. Возможные значения: 'предложение по продуктам ещё не сделано', 'сделано предложение по продуктам', 'сделано новое предложение', 'клиент думает', 'отказ от покупки', 'решение принято (ожидаем оплату)', 'покупка совершена', 'не применимо'.
client_activity: Определи активность клиента в этом фрагменте диалога. 'активен': если во фрагменте есть ДВА или БОЛЕЕ сообщений от клиента. 'пассивен': если во фрагменте только ОДНО или НЕТ сообщений от клиента.

--- НОВЫЕ СООБЩЕНИЯ ДЛЯ АНАЛИЗА ---
{new_messages_text}
"""

def setup_logging():
    # Делаем conv_id доступным глобально для логгера
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.conv_id = globals().get('conv_id', 'N/A')
        return record
    logging.setLogRecordFactory(record_factory)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - CONV_ID: %(conv_id)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_NAME, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stderr)
        ]
    )

def get_db_connection():
    if not DATABASE_URL:
        raise ConnectionError("Переменная окружения DATABASE_URL не установлена!")
    return psycopg2.connect(DATABASE_URL)

def format_messages_for_prompt(messages):
    if not messages:
        return "Нет новых сообщений."

    formatted_lines = []
    for msg in messages:
        role_map = {'bot': 'ассистент', 'user': 'клиент', 'operator': 'оператор'}
        role = role_map.get(msg.get('role'), msg.get('role', 'unknown'))
        message_text = re.sub(r'^\[.*?\]\s*', '', msg.get('message', ''))
        formatted_lines.append(f"{role.capitalize()}: {message_text}")

    return "\n".join(formatted_lines)

def call_gemini_api(model, prompt, expect_json=False):
    """
    Вызывает модель Gemini через Vertex AI SDK с обработкой ошибок.
    """
    try:
        logging.info(f"Отправляем запрос в Gemini. Промпт (первые 200 символов): {prompt[:200]}...")
        
        response = model.generate_content(prompt)
        
        raw_response = response.text
        logging.info(f"Получен ответ от Gemini (длина: {len(raw_response)} символов): {raw_response[:300]}...")
        
        if expect_json:
            logging.info("Ожидаем JSON-ответ, начинаем парсинг.")
            # Попытка найти JSON внутри markdown-блока
            match = re.search(r"```(json)?\s*([\s\S]*?)\s*```", raw_response)
            if match:
                json_str = match.group(2)
                logging.info(f"Найден JSON в markdown-блоке. Попытка парсинга: {json_str[:200]}")
            else:
                json_str = raw_response
                logging.info(f"Markdown-блок не найден. Попытка парсинга всего ответа.")
            
            parsed_response = json.loads(json_str)
            logging.info(f"JSON успешно распарсен: {parsed_response}")
            return parsed_response
        else:
            return raw_response.strip()

    except json.JSONDecodeError as je:
        logging.error(f"Ошибка парсинга JSON от Gemini: {je}. Сырой ответ: {raw_response}")
        raise
    except Exception as e:
        logging.error(f"Ошибка вызова Vertex AI API или обработки ответа: {e}", exc_info=True)
        raise

def merge_profiles(old_profile, new_facts, new_summary):
    logging.info(f"Начинаем слияние профилей. Старый профиль: {old_profile}")
    logging.info(f"Новые факты для слияния: {new_facts}")

    updated_profile = old_profile.copy()
    updated_profile['dialogue_summary'] = new_summary
    updated_profile['last_updated'] = datetime.now(timezone.utc)

    new_activity = new_facts.get('client_activity', 'пассивен')
    updated_profile['client_activity'] = new_activity

    old_qualifications = old_profile.get('lead_qualification') or []
    new_qual_assessment = new_facts.get('lead_qualification')

    final_qualifications = []
    is_client = 'клиент' in old_qualifications or new_qual_assessment == 'клиент'
    if is_client:
        final_qualifications.append('клиент')

    if new_qual_assessment and new_qual_assessment not in ['клиент', 'не определено']:
        final_qualifications.append(new_qual_assessment)
    elif not new_qual_assessment and any(q in old_qualifications for q in ['холодный', 'тёплый', 'горячий']):
         old_temp = next((q for q in old_qualifications if q in ['холодный', 'тёплый', 'горячий']), None)
         if old_temp:
             final_qualifications.append(old_temp)

    updated_profile['lead_qualification'] = list(dict.fromkeys(final_qualifications))

    old_stage = old_profile.get('funnel_stage', 'не применимо')
    new_stage_assessment = new_facts.get('funnel_stage')

    if not new_stage_assessment or new_stage_assessment == 'не применимо':
        updated_profile['funnel_stage'] = old_stage
    else:
        old_stage_val = FUNNEL_STAGE_HIERARCHY.get(old_stage, 0)
        new_stage_val = FUNNEL_STAGE_HIERARCHY.get(new_stage_assessment, 0)

        if new_stage_assessment == 'сделано новое предложение':
            updated_profile['funnel_stage'] = new_stage_assessment
        elif old_stage in ['покупка совершена', 'отказ от покупки']:
            updated_profile['funnel_stage'] = new_stage_assessment
        elif new_stage_val > old_stage_val:
            updated_profile['funnel_stage'] = new_stage_assessment
        else:
            updated_profile['funnel_stage'] = old_stage

    old_emails = set(old_profile.get('email') or [])
    new_emails = set(new_facts.get('email') or [])
    updated_profile['email'] = sorted(list(old_emails.union(new_emails)))

    for key in ['client_level', 'learning_goals', 'client_pains']:
        combined_set = set(old_profile.get(key) or [])
        combined_set.update(new_facts.get(key, []))
        updated_profile[key] = sorted(list(combined_set))

    logging.info(f"Результат слияния профилей: {updated_profile}")
    return updated_profile

def update_and_cleanup_database(conv_id, updated_profile, new_facts, cur):
    logging.info(f"Подготовка к обновлению профиля. Данные для записи: {updated_profile}")

    update_query = """
    UPDATE user_profiles SET
    dialogue_summary = %(dialogue_summary)s, lead_qualification = %(lead_qualification)s,
    funnel_stage = %(funnel_stage)s, client_level = %(client_level)s,
    learning_goals = %(learning_goals)s,
    client_pains = %(client_pains)s, email = %(email)s,
    client_activity = %(client_activity)s, last_updated = %(last_updated)s
    WHERE conv_id = %(conv_id)s;
    """
    logging.info(f"Выполняем UPDATE запрос для conv_id: {conv_id}")
    cur.execute(update_query, updated_profile)
    affected_rows = cur.rowcount
    logging.info(f"UPDATE выполнен. Затронуто строк: {affected_rows}")

    if affected_rows == 0:
        logging.warning(f"ВНИМАНИЕ: UPDATE не затронул ни одной строки! Возможно, профиль не существует.")
    else:
        logging.info(f"Профиль пользователя успешно обновлен в транзакции.")

    # Обновление купленных продуктов в отдельной таблице
    new_purchased_products = new_facts.get('purchased_products', [])
    if new_purchased_products:
        logging.info(f"Обновляем информацию о купленных продуктах: {new_purchased_products}")
        cur.execute("SELECT product_name FROM purchased_products WHERE conv_id = %s", (conv_id,))
        existing_products = {row[0] for row in cur.fetchall()}
        
        products_to_insert = [p for p in new_purchased_products if p not in existing_products]
        
        if products_to_insert:
            insert_query = "INSERT INTO purchased_products (conv_id, product_name) VALUES (%s, %s)"
            data_to_insert = [(conv_id, product) for product in products_to_insert]
            cur.executemany(insert_query, data_to_insert)
            logging.info(f"Добавлено {len(data_to_insert)} новых записей в purchased_products.")
        else:
            logging.info("Новых купленных продуктов для добавления не найдено.")

    cutoff_timestamp_query = """
        SELECT created_at FROM dialogues
        WHERE conv_id = %s
        ORDER BY created_at DESC
        LIMIT 1 OFFSET %s;
    """
    cur.execute(cutoff_timestamp_query, (conv_id, NUM_MESSAGES_TO_KEEP - 1))
    cutoff_result = cur.fetchone()

    if cutoff_result:
        cutoff_timestamp = cutoff_result[0]
        cleanup_query = """
            DELETE FROM dialogues
            WHERE conv_id = %s AND created_at < %s;
        """
        cur.execute(cleanup_query, (conv_id, cutoff_timestamp))
        logging.info(f"{conv_id} - Очистка старых диалогов завершена. Удалено {cur.rowcount} сообщений.")
    else:
        logging.info(f"{conv_id} - Сообщений меньше {NUM_MESSAGES_TO_KEEP}, очистка не требуется.")

def main():
    setup_logging()

    conv_id = 0
    try:
        conv_id_str = sys.stdin.read().strip()
        if not conv_id_str.isdigit():
            raise ValueError(f"Получен некорректный conv_id: '{conv_id_str}'")
        conv_id = int(conv_id_str)
        # Делаем conv_id глобальным для использования в логах
        globals()['conv_id'] = conv_id
    except Exception as e:
        logging.error(f"Критическая ошибка при чтении conv_id из stdin: {e}", extra={'conv_id': 'N/A'})
        sys.exit(1)

    logging.info("Запущен процесс обновления саммари.")

    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        logging.critical("КРИТИЧЕСКАЯ ОШИБКА: Переменная окружения 'GOOGLE_APPLICATION_CREDENTIALS' не установлена.")
        sys.exit(1)

    credentials_path = credentials_path.strip(' "')
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=credentials)
        model = GenerativeModel(MODEL_NAME)
        logging.info("Учетные данные Vertex AI успешно загружены. Модель инициализирована.")
    except Exception as e:
        logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось инициализировать Vertex AI. Ошибка: {e}")
        sys.exit(1)

    # === ШАГ 1: Извлечение данных БЕЗ блокировки ===
    initial_profile_for_prompt = None
    messages_for_prompt = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM user_profiles WHERE conv_id = %s", (conv_id,))
            profile_data = cur.fetchone()
            if not profile_data:
                raise ValueError(f"Профиль для conv_id {conv_id} не найден в user_profiles.")
            initial_profile_for_prompt = dict(profile_data)

            optimized_messages_query = """
            WITH latest_messages AS (
                SELECT * FROM dialogues WHERE conv_id = %s ORDER BY created_at DESC LIMIT %s
            )
            SELECT * FROM latest_messages ORDER BY created_at ASC;
            """
            cur.execute(optimized_messages_query, (conv_id, NUM_MESSAGES_TO_FETCH))
            messages_for_prompt = cur.fetchall()

    except Exception as e:
        logging.error(f"{conv_id} - Ошибка на этапе чтения данных из БД: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

    if not messages_for_prompt:
        logging.info(f"{conv_id} - Нет сообщений для анализа. Процесс завершен.")
        return

    # === ШАГ 2: Все долгие сетевые операции ===
    new_summary = ""
    new_facts = {}
    try:
        new_messages_text = format_messages_for_prompt(messages_for_prompt)

        summary_prompt = PROMPT_INCREMENTAL_SUMMARY.format(
            existing_summary=initial_profile_for_prompt.get('dialogue_summary', 'Саммари еще не создано.'),
            new_messages_text=new_messages_text
        )
        new_summary = call_gemini_api(model, summary_prompt, expect_json=False)
        logging.info("Новое инкрементальное саммари успешно сгенерировано.")
        logging.info(f"Новое саммари (первые 200 символов): {new_summary[:200]}...")

        facts_prompt = PROMPT_EXTRACT_NEW_FACTS.format(new_messages_text=new_messages_text)
        new_facts = call_gemini_api(model, facts_prompt, expect_json=True)
        logging.info("Новые факты успешно извлечены.")
        logging.info(f"Извлеченные факты: {json.dumps(new_facts, ensure_ascii=False, indent=2)}")

    except Exception as e:
        logging.error(f"Критическая ошибка во время вызова Gemini API: {e}", exc_info=True)
        sys.exit(1)

    # === ШАГ 3: Короткая атомарная транзакция для записи данных ===
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM user_profiles WHERE conv_id = %s FOR UPDATE", (conv_id,))
            current_profile_in_db = dict(cur.fetchone())

            updated_profile = merge_profiles(current_profile_in_db, new_facts, new_summary)
            updated_profile['conv_id'] = conv_id
            
            update_and_cleanup_database(conv_id, updated_profile, new_facts, cur)
            
            conn.commit()
            logging.info(f"{conv_id} - Транзакция обновления и очистки успешно завершена.")

    except Exception as e:
        logging.error(f"{conv_id} - Ошибка на этапе записи в БД. Транзакция будет отменена: {e}", exc_info=True)
        if conn:
            conn.rollback()
            logging.info(f"{conv_id} - Транзакция отменена из-за ошибки.")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()