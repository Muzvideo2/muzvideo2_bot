# 🚀 СТРАТЕГИЧЕСКИЙ АГЕНТ MUZВIDEO2 - ОБНОВЛЁННАЯ ВЕРСИЯ

## 📋 Описание

Интегрированная система стратегического анализа клиентов, которая:

1. **Точно обновляет карточки** клиентов (использует логику из `summary_updater.py`)
2. **Разрабатывает стратегии** краткосрочные и долгосрочные (логика из `prompt.txt`)  
3. **Ставит обязательные напоминания** каждому клиенту (никто не остаётся без внимания)
4. **Анализирует на Gemini 2.5 Pro** для максимальной точности

## 🆕 Что нового в этой версии

### ✅ Исправлена проблема несоответствия
- **Раньше**: ИИ-агент из `summary_updater.py` делал ошибки в определении стадии воронки и теплоты
- **Теперь**: Стратегический агент использует **ТУ ЖЕ логику**, но на более мощной модели

### ✅ Добавлены столбцы стратегии в БД
```sql
-- Новые столбцы в user_profiles:
short_term_strategy      TEXT    -- Краткосрочная стратегия (1-2 взаимодействия)  
long_term_strategy       TEXT    -- Долгосрочная стратегия (LTV повышение)
last_strategy_analysis   TIMESTAMP -- Время последнего анализа
strategy_analysis_data   JSONB   -- Полные результаты анализа
```

### ✅ Обязательные напоминания
- **Каждый** клиент получает напоминание
- Время от 1 дня до 2 месяцев в зависимости от ситуации
- Всегда **весомая причина** для клиента (не техническая)

## 🛠 Быстрый старт

### 1. Миграция базы данных
```bash
cd strategy_agent
python strategic_agent_main.py --migrate
```

### 2. Тест одного клиента
```bash
python strategic_agent_main.py --test-client 181601225
```

### 3. Массовый анализ (10 клиентов)
```bash
python strategic_agent_main.py --batch 10
```

### 4. Поиск новых приоритетных пользователей
```bash
python strategic_agent_main.py --search-new
```

### 5. Полный цикл (50 клиентов)
```bash
python strategic_agent_main.py --migrate --search-new --batch 50
```

## 📁 Структура файлов

```
strategy_agent/
├── strategic_agent_main.py              # 🚀 Главный скрипт запуска
├── client_card_analyzer.py              # 🧠 Обновлённый ИИ-анализатор (Gemini 2.5 Pro)
├── client_processing_orchestrator.py    # 🎭 Оркестратор обработки
├── database_migration_strategy.sql      # 🗄️ Миграция БД для стратегии  
├── search_people.py                     # 🔍 Поиск приоритетных клиентов
├── data_exporter.py                     # 📤 Экспорт данных из БД
├── results_parser.py                    # 📝 Обработка результатов анализа
├── simple_results_processor.py          # 📝 Упрощённая обработка
├── founded_people_20250821_192925.py    # 📊 Готовый список 500 пользователей
└── exported_data/                       # 📁 Экспортированные данные
```

## 🔧 Детальные команды

### Оркестратор (все в одном)
```bash
# Один клиент
python client_processing_orchestrator.py single 181601225

# Несколько клиентов
python client_processing_orchestrator.py multiple 181601225 515099352 515099353

# По критериям (без активных напоминаний, лимит 20)
python client_processing_orchestrator.py criteria --no_active_reminders --limit 20
```

### Отдельные компоненты
```bash
# Поиск 500 приоритетных клиентов
python search_people.py

# Экспорт данных одного клиента  
python data_exporter.py 181601225

# Анализ из JSON файла
python client_card_analyzer.py exported_data/client_data_181601225_20250820.json

# Обработка результатов
python simple_results_processor.py analysis_results/strategic_analysis_181601225_20250820.json
```

## 📊 Статистика выполнения

После каждого запуска вы увидите отчёт:
```
=====================================
РЕЗУЛЬТАТ СТРАТЕГИЧЕСКОГО АНАЛИЗА КЛИЕНТА 181601225  
=====================================
Квалификация: ['тёплый']
Этап воронки: клиент думает
Активность: активен

Краткосрочная стратегия: Добиться понимания потребностей в обучении  
Долгосрочная стратегия: Стать постоянным клиентом набора "Всё включено"

Напоминание через: 3 дней
Причина: Уточнить результаты размышлений о курсах
=====================================
```

## ⚙️ Настройка окружения

### Обязательные переменные
```bash
# .env файл должен содержать:
DATABASE_URL=postgresql://user:password@host:port/database
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCP_PROJECT_ID=your-gcp-project-id
GEMINI_LOCATION=us-central1
```

### Установка зависимостей
```bash
pip install -r requirements.txt
```

Требуются:
- `google-cloud-aiplatform` (Vertex AI)
- `psycopg2-binary` (PostgreSQL)
- `python-dotenv` (переменные окружения)
- `tqdm` (прогресс-бары)

## 🔍 Логика работы системы

### 1. Точный анализ карточки (как в summary_updater.py)
- ✅ Строгие критерии для квалификации лида
- ✅ Защита критических этапов воронки от понижения  
- ✅ Иерархия этапов воронки
- ✅ Дополнение профиля, а не замена

### 2. Стратегический анализ (как internal_analysis в prompt.txt)
- 🎯 Психологический портрет клиента
- 📊 Оценка комфорта в диалоге (1-10)
- 📈 Краткосрочная стратегия (1-2 взаимодействия)
- 🚀 Долгосрочная стратегия (LTV повышение)

### 3. Обязательные напоминания
- ⏰ Каждый клиент получает напоминание
- 🎯 Цель: провоцировать на разговор, двигать по воронке
- 📅 Время: от 1 дня до 2 месяцев
- 💬 Причина: всегда весомая для клиента

## 🚨 Устранение неполадок

### Google Cloud ошибки
```bash
# Проверьте переменные
echo $GOOGLE_APPLICATION_CREDENTIALS
echo $GCP_PROJECT_ID

# Проверьте файл сервисного аккаунта
ls -la $GOOGLE_APPLICATION_CREDENTIALS
```

### Ошибки БД
```bash
# Проверьте соединение
python -c "import psycopg2; print(psycopg2.connect('$DATABASE_URL'))"

# Проверьте миграцию
python strategic_agent_main.py --migrate
```

### Логи
Все логи записываются в:
- `strategic_agent_main.log` - основной лог
- `orchestrator.log` - лог оркестратора  
- `search_people.log` - лог поиска людей
- `client_card_analyzer.log` - лог анализатора

## 📈 Мониторинг результатов

### Проверка обновлений в БД
```sql
-- Клиенты с обновлённой стратегией  
SELECT conv_id, short_term_strategy, long_term_strategy, last_strategy_analysis 
FROM user_profiles 
WHERE last_strategy_analysis IS NOT NULL
ORDER BY last_strategy_analysis DESC;

-- Созданные напоминания
SELECT r.conv_id, r.reminder_datetime, r.reminder_context_summary
FROM reminders r 
WHERE r.status = 'active' 
AND r.created_at > NOW() - INTERVAL '1 day'
ORDER BY r.reminder_datetime;
```

## 🎯 Рекомендуемый рабочий процесс

### Ежедневно:
```bash
# 1. Найти новых приоритетных пользователей
python strategic_agent_main.py --search-new

# 2. Проанализировать топ-50 без напоминаний
python client_processing_orchestrator.py criteria --no_active_reminders --limit 50
```

### Еженедельно:
```bash
# Полная миграция + анализ 100 клиентов
python strategic_agent_main.py --migrate --batch 100
```

## 💡 Полезные советы

1. **Начинайте с малого**: тестируйте на 1-5 клиентах перед массовым запуском
2. **Мониторьте логи**: все ошибки записываются в detail
3. **Проверяйте БД**: убедитесь, что стратегии записываются корректно
4. **Готовый список**: используйте `founded_people_20250821_192925.py` для быстрого старта

## 📞 Поддержка

При возникновении проблем:
1. Проверьте логи в файлах `.log`
2. Убедитесь в корректности переменных окружения
3. Протестируйте на одном клиенте перед массовым запуском

---

**Создано**: Август 2025  
**Версия**: 2.0 - Интегрированная стратегическая система  
**Статус**: ✅ Готова к production использованию