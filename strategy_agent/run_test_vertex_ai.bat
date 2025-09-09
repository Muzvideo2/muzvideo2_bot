@echo off
chcp 65001 >nul

echo [ТЕСТ] Проверка учетных данных Vertex AI
echo Время запуска: %date% %time%
echo ================================================

REM Устанавливаем переменные окружения
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\Asus\Documents\MuzVideo2\multi_agent_piano_school\sekrety\muzvideo2-c16c31353196.json

REM Определяем PROJECT_ID из файла учетных данных
for /f "tokens=*" %%i in ('powershell -command "(Get-Content '%GOOGLE_APPLICATION_CREDENTIALS%' | ConvertFrom-Json).project_id"') do set GOOGLE_CLOUD_PROJECT=%%i
echo Определен PROJECT_ID: %GOOGLE_CLOUD_PROJECT%

set VERTEX_AI_LOCATION=us-central1

REM Переходим в директорию скрипта
cd /d "%~dp0"

echo [ЗАПУСК] Тестирование Vertex AI...
python test_vertex_ai.py
if %ERRORLEVEL% neq 0 (
    echo [ОШИБКА] Тест завершился с ошибкой
    pause
    exit /b %ERRORLEVEL%
)

echo ================================================
echo [УСПЕХ] Тест завершён успешно!
echo Время завершения: %date% %time%
echo ================================================

pause