import psycopg2
import os

try:
    # Подключаемся к базе данных
    conn = psycopg2.connect(
        dbname=os.environ.get("PG_DBNAME"),
        user=os.environ.get("PG_USER"),
        password=os.environ.get("PG_PASSWORD"),
        host=os.environ.get("PG_HOST"),
        port=os.environ.get("PG_PORT"),
    )
    print("Подключение успешно!")  # Уведомление в случае успешного подключения
    conn.close()  # Закрываем соединение
except Exception as e:
    print("Ошибка подключения:", e)  # Сообщение об ошибке