@echo off
chcp 65001 >nul

echo [АВТОЗАПУСК] Стратегический агент MuzVideo2
echo Время запуска: %date% %time%
echo ================================================

REM Устанавливаем переменные окружения
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\Asus\Documents\MuzVideo2\multi_agent_piano_school\sekrety\muzvideo2-c16c31353196.json
set GOOGLE_CLOUD_PROJECT=muzvideo2
set VERTEX_AI_LOCATION=us-central1

REM Переходим в директорию скрипта
cd /d "%~dp0"

echo [1/2] Поиск новых приоритетных клиентов (исключая тех, у кого есть активные напоминания)...
python search_people.py
if %ERRORLEVEL% neq 0 (
    echo [ОШИБКА] Поиск клиентов завершился с ошибкой
    pause
    exit /b %ERRORLEVEL%
)
echo [OK] Поиск клиентов завершён успешно

echo [2/2] Запуск AI обработки найденных клиентов...
python auto_strategy_agent.py
if %ERRORLEVEL% neq 0 (
    echo [ОШИБКА] AI обработка завершилась с ошибкой
    pause
    exit /b %ERRORLEVEL%
)

echo ================================================
echo [УСПЕХ] Все этапы выполнены успешно!
echo Время завершения: %date% %time%
echo ================================================

REM Для автоматического запуска по расписанию убрать pause
pause