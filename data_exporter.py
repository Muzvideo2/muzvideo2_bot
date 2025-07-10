import os
import psycopg2
import psycopg2.extras
from collections import defaultdict

# --- Настройки ---
DIALOGUES_FILE = 'filtered_dialogues.md'
PROFILES_FILE = 'found_user_profiles.md'
DAYS_INTERVAL = 14

def get_db_connection():
    """Устанавливает соединение с базой данных, используя DATABASE_URL."""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("Ошибка: Переменная окружения DATABASE_URL не установлена.")
        return None
    try:
        conn = psycopg2.connect(database_url)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def export_filtered_dialogues(conn):
    """
    Выгружает диалоги за последние N дней, в которых есть ответы от пользователя,
    и сохраняет их в файл.
    Возвращает список уникальных conv_id.
    """
    print(f"Шаг 1: Выгрузка диалогов за последние {DAYS_INTERVAL} дней...")
    
    unique_conv_ids = set()
    dialogues_by_conv_id = defaultdict(list)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # 1. Находим conv_id, где были сообщения от пользователя
            cur.execute(
                f"""
                SELECT DISTINCT conv_id FROM dialogues
                WHERE role = 'user' AND created_at >= NOW() - INTERVAL '{DAYS_INTERVAL} days';
                """
            )
            active_conv_ids_rows = cur.fetchall()
            if not active_conv_ids_rows:
                print("Не найдено активных диалогов с ответами пользователей за указанный период.")
                return []
            
            active_conv_ids = tuple([row['conv_id'] for row in active_conv_ids_rows])

            # 2. Выгружаем все сообщения для этих conv_id
            cur.execute(
                """
                SELECT conv_id, role, message, created_at FROM dialogues
                WHERE conv_id = ANY(%s)
                ORDER BY conv_id, created_at;
                """,
                (list(active_conv_ids),)
            )
            all_messages = cur.fetchall()

            for msg in all_messages:
                dialogues_by_conv_id[msg['conv_id']].append(msg)
                unique_conv_ids.add(msg['conv_id'])

        # 3. Сохраняем в файл
        with open(DIALOGUES_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Диалоги с активными пользователями за последние {DAYS_INTERVAL} дней\n\n")
            for conv_id, messages in sorted(dialogues_by_conv_id.items()):
                f.write(f"---\n\n")
                f.write(f"## Диалог: {conv_id}\n\n")
                for msg in messages:
                    f.write(f"**{msg['role']}** ({msg['created_at']}):\n")
                    f.write(f"{msg['message']}\n\n")
        
        print(f"Успешно сохранено {len(unique_conv_ids)} диалогов в файл '{DIALOGUES_FILE}'.")
        return list(unique_conv_ids)

    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при работе с PostgreSQL: {error}")
        return []

def export_user_profiles(conn, conv_ids):
    """
    Выгружает профили пользователей по списку conv_id и сохраняет их в файл.
    """
    if not conv_ids:
        print("\nШаг 2: Список conv_id пуст, пропуск выгрузки профилей.")
        with open(PROFILES_FILE, 'w', encoding='utf-8') as f:
            f.write("# Карточки пользователей\n\n")
            f.write("Не найдено пользователей для выгрузки, так как не было найдено активных диалогов.\n")
        return

    print(f"\nШаг 2: Выгрузка {len(conv_ids)} профилей пользователей...")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT * FROM user_profiles WHERE conv_id = ANY(%s);",
                (conv_ids,)
            )
            profiles = cur.fetchall()

            with open(PROFILES_FILE, 'w', encoding='utf-8') as f:
                f.write("# Карточки пользователей\n\n")
                if not profiles:
                    f.write("Не найдено профилей для пользователей с активными диалогами.\n")
                    print("Предупреждение: Не найдено ни одного профиля для найденных conv_id.")
                    return

                for profile in profiles:
                    f.write(f"---\n\n")
                    f.write(f"## Карточка клиента: {profile['conv_id']}\n\n")
                    for key, value in profile.items():
                        f.write(f"- **{key}**: {value}\n")
                    f.write("\n")

        print(f"Успешно сохранено {len(profiles)} профилей в файл '{PROFILES_FILE}'.")

    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при работе с PostgreSQL: {error}")


def main():
    """Основная функция для запуска экспорта."""
    conn = get_db_connection()
    if conn:
        try:
            active_conv_ids = export_filtered_dialogues(conn)
            if active_conv_ids:
                export_user_profiles(conn, active_conv_ids)
        finally:
            conn.close()
            print("\nСоединение с базой данных закрыто.")
    
    print("\nРабота скрипта завершена.")

if __name__ == '__main__':
    main() 