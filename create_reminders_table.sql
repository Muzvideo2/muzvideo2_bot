-- =======================================================================================
-- SQL-скрипт для создания таблицы напоминаний (reminders)
-- Версия: 1.0
-- =======================================================================================

-- Создание таблицы reminders
CREATE TABLE IF NOT EXISTS reminders (
    id SERIAL PRIMARY KEY,
    conv_id BIGINT NOT NULL,
    reminder_datetime TIMESTAMPTZ NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'active', 
    -- Возможные статусы:
    -- active - активное напоминание
    -- in_progress - напоминание обрабатывается
    -- done - напоминание выполнено
    -- cancelled_by_user - отменено пользователем
    -- cancelled_by_reminder - отменено AI-агентом напоминаний
    -- cancelled_by_communicator - отменено AI-коммуникатором
    -- cancelled_by_admin - отменено администратором
    reminder_context_summary TEXT NOT NULL,
    cancellation_reason TEXT, -- Причина отмены/переноса/изменения статуса
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by_conv_id BIGINT, -- ID того, кто создал напоминание
    client_timezone VARCHAR(50) DEFAULT 'Europe/Moscow' -- Часовой пояс клиента
);

-- Создание индексов для оптимизации запросов
CREATE INDEX idx_reminders_status_datetime ON reminders (status, reminder_datetime);
CREATE INDEX idx_reminders_conv_id ON reminders (conv_id);
CREATE INDEX idx_reminders_created_at ON reminders (created_at);

-- Комментарии к полям
COMMENT ON TABLE reminders IS 'Таблица для хранения напоминаний о будущих контактах с клиентами';
COMMENT ON COLUMN reminders.id IS 'Уникальный идентификатор напоминания';
COMMENT ON COLUMN reminders.conv_id IS 'ID диалога/клиента, для которого создано напоминание';
COMMENT ON COLUMN reminders.reminder_datetime IS 'Дата и время, когда должно сработать напоминание (с учетом часового пояса)';
COMMENT ON COLUMN reminders.status IS 'Текущий статус напоминания';
COMMENT ON COLUMN reminders.reminder_context_summary IS 'Краткое описание причины напоминания для AI-коммуникатора';
COMMENT ON COLUMN reminders.cancellation_reason IS 'Причина отмены, переноса или изменения статуса напоминания';
COMMENT ON COLUMN reminders.created_at IS 'Дата и время создания напоминания';
COMMENT ON COLUMN reminders.created_by_conv_id IS 'ID пользователя, создавшего напоминание (NULL - автоматически, ADMIN_CONV_ID - администратор)';
COMMENT ON COLUMN reminders.client_timezone IS 'Часовой пояс клиента для корректного отображения времени';

-- Пример добавления тестовых данных (закомментировано)
/*
INSERT INTO reminders (conv_id, reminder_datetime, reminder_context_summary, created_by_conv_id)
VALUES 
    (12345678, '2024-01-15 10:00:00+03', 'Клиент попросил напомнить об оплате курса', NULL),
    (87654321, '2024-01-16 14:00:00+03', 'Клиент взял время подумать до понедельника', NULL),
    (11111111, '2024-01-17 19:00:00+03', 'Администратор просил напомнить клиенту о консультации', 78671089);
*/