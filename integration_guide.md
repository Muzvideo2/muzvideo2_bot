# Руководство по интеграции модуля напоминаний

## Обзор

Модуль напоминаний (`reminder_service.py`) разработан как изолированный сервис, который требует минимальной интеграции с основным приложением (`main.py`).

## Шаги интеграции

### 1. Создание таблицы в БД

Выполните SQL-скрипт `create_reminders_table.sql` в вашей базе данных:

```bash
psql $DATABASE_URL < create_reminders_table.sql
```

### 2. Добавление переменных окружения

Убедитесь, что следующие переменные окружения установлены:
- `DATABASE_URL` - URL подключения к PostgreSQL
- `GOOGLE_APPLICATION_CREDENTIALS` - путь к файлу учетных данных Google Cloud
- `VK_COMMUNITY_TOKEN` - токен сообщества VK (уже должен быть установлен)

### 3. Интеграция с main.py

Добавьте следующие строки в `main.py`:

#### В начало файла (импорты):
```python
# Добавить после существующих импортов
from reminder_service import initialize_reminder_service, process_new_message as process_reminder
```

#### В функцию `handle_new_message` (после сохранения сообщения в БД):
```python
# Добавить после строки store_dialog_in_db() для сообщений пользователя
# Это должно быть ПОСЛЕ сохранения сообщения в БД, но ДО генерации ответа

# Асинхронно проверяем сообщение на наличие договоренностей о напоминании
try:
    threading.Thread(
        target=process_reminder,
        args=(actual_conv_id,),
        daemon=True
    ).start()
except Exception as e:
    logging.error(f"Ошибка при запуске анализа напоминаний: {e}")
```

#### В блок `if __name__ == "__main__":` (инициализация при запуске):
```python
# Добавить перед app.run()
# Инициализация сервиса напоминаний
if not initialize_reminder_service():
    logging.error("Не удалось инициализировать сервис напоминаний. Продолжаем без него.")
```

### 4. Интеграция активации напоминаний

Для полной интеграции активации напоминаний потребуется модификация функции `generate_and_send_response`. Добавьте новый параметр:

```python
def generate_and_send_response(conv_id_to_respond, vk_api_for_sending, vk_callback_data, model, reminder_context=None):
    # ... существующий код ...
    
    # Если это вызов от напоминания, добавляем контекст в начало промпта
    if reminder_context:
        context_from_builder = f"[СИСТЕМНОЕ УВЕДОМЛЕНИЕ] Сработало напоминание. Причина: '{reminder_context}'. Проанализируй весь диалог и реши, уместно ли сейчас возобновлять общение. Если да — напиши релевантное сообщение клиенту. Если нет — верни ПУСТУЮ СТРОКУ.\n\n{context_from_builder}"
```

### 5. Создание эндпоинта для активации напоминаний

Добавьте новый эндпоинт в Flask приложение:

```python
@app.route("/activate_reminder", methods=["POST"])
def activate_reminder():
    """
    Эндпоинт для активации напоминания из reminder_service.
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
        
        # Генерируем и отправляем ответ с контекстом напоминания
        generate_and_send_response(
            conv_id_to_respond=conv_id,
            vk_api_for_sending=vk_api_local,
            vk_callback_data={},  # Минимальные данные
            model=app.model,
            reminder_context=reminder_context
        )
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        logging.error(f"Ошибка при активации напоминания: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
```

## Тестирование

1. Запустите скрипт верификации для проверки логики:
   ```bash
   export TEST_DATABASE_URL="postgresql://test_user:test_pass@localhost/test_db"
   python verification_script.py
   ```

2. Проверьте отчет в файле `test_report.md`

3. Протестируйте в боевом режиме:
   - Отправьте сообщение "Напомните мне завтра в 10:00 об оплате"
   - Проверьте, что напоминание создано в БД
   - Дождитесь срабатывания напоминания

## Мониторинг

Логи сервиса напоминаний сохраняются в файл `reminder_service.log`. 

Для мониторинга напоминаний используйте SQL запросы:

```sql
-- Активные напоминания
SELECT * FROM reminders WHERE status = 'active' ORDER BY reminder_datetime;

-- Статистика по напоминаниям
SELECT status, COUNT(*) FROM reminders GROUP BY status;

-- Напоминания конкретного пользователя
SELECT * FROM reminders WHERE conv_id = 12345678 ORDER BY created_at DESC;
```

## Безопасность

1. Все напоминания проходят двойную проверку:
   - При создании (семантический анализ)
   - При активации (проверка контекста)

2. Администратор (conv_id: 78671089) имеет расширенные права:
   - Может создавать напоминания для любого пользователя
   - Может отменять любые напоминания

3. Защита от спама:
   - Система не создает повторяющиеся напоминания
   - Игнорирует личные просьбы, не связанные со школой

## Расширение функциональности

Для добавления новых типов напоминаний или изменения логики:

1. Обновите промпт `PROMPT_ANALYZE_DIALOGUE` в `reminder_service.py`
2. Добавьте новые сценарии в `scenarios.json`
3. Запустите `verification_script.py` для проверки
4. Итеративно исправляйте ошибки на основе `test_report.md`