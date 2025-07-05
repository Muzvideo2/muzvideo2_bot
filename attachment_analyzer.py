# --- Описание ---
# Скрипт анализирует скачанные вложения из папки downloaded_attachments.
# Использует Vertex AI (gemini-2.5-flash) для полного анализа всех типов вложений:
# - Фото/стикеры: полное OCR извлечение текста
# - Голосовые: полная транскрипция речи
# - ВИДЕО: скачивание первых 2 минут + полный анализ видеоряда через Gemini
# - Репосты/музыка: анализ метаданных
# Создает контекст для понимания бота и быстрых ответов. Работает локально.
# --- Конец описания ---

import os
import json
import logging
import base64
import mimetypes
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting, HarmCategory
from google.oauth2 import service_account
import requests

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('attachment_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AttachmentAnalyzer:
    def __init__(self):
        """Инициализация анализатора вложений"""
        self.project_id = None
        self.location = "us-central1"  # Попробуем другие регионы если не работает
        self.model_name = "gemini-2.5-flash"  # Актуальная модель Gemini 2.5
        self.model = None
        
        # Папки для работы
        self.download_dir = "downloaded_attachments"
        self.results_dir = "analysis_results"
        self.ensure_results_directory()
        
        # Настройки безопасности для модели
        self.safety_settings = [
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
            ),
        ]
        
        # Промпты для разных типов контента
        self.prompts = {
            'photo': """Проанализируй это изображение максимально подробно. 
                       ОБЯЗАТЕЛЬНО извлеки ВЕСЬ видимый текст (OCR). 
                       Если это чек - укажи все товары, цены, общую сумму. 
                       Если это скриншот ошибки - опиши ошибку и код.
                       Если это документ - извлеки весь текст.
                       Отвечай на русском языке.""",
                       
            'audio_message': """Транскрибируй это голосовое сообщение ПОЛНОСТЬЮ.
                               Переведи всю речь в текст с максимальной точностью.
                               Отвечай на русском языке.""",
                               
            'video': """Проанализируй это видео максимально подробно:
                       1. СОДЕРЖАНИЕ: Что происходит? Кто участвует? О чем говорят?
                       2. ВИЗУАЛЬНОЕ: Опиши обстановку, объекты, действия
                       3. АУДИО: Транскрибируй речь и звуки
                       4. ТЕКСТ НА ЭКРАНЕ: Извлеки ВЕСЬ видимый текст
                       5. КОНТЕКСТ: Тема, жанр, настроение видео
                       Отвечай на русском языке.""",
                       
            'sticker': """Опиши этот стикер.
                         Если на нем есть текст - извлеки его ПОЛНОСТЬЮ.
                         Опиши изображение и эмоциональный контекст.
                         Отвечай на русском языке.""",
                         
            'wall': """Проанализируй этот репост.
                      Опиши источник, тему, основное содержание.
                      Если есть вложенные медиа - опиши их.
                      Отвечай на русском языке.""",
                      
            'audio': """Проанализируй информацию об этой музыкальной композиции.
                       Опиши исполнителя, название, жанр если возможно.
                       Отвечай на русском языке."""
        }
        
    def ensure_results_directory(self):
        """Создание папки для результатов"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.info(f"Создана папка для результатов: {self.results_dir}")
            
    def load_vertex_ai_credentials(self):
        """Загрузка ключей Vertex AI"""
        try:
            # Ищем JSON файл с ключами в текущей папке
            json_files = [f for f in os.listdir('.') if f.endswith('.json') and 'key' in f.lower()]
            
            if not json_files:
                print("\n" + "="*60)
                print("НАСТРОЙКА VERTEX AI КЛЮЧЕЙ")
                print("="*60)
                print("Необходимо разместить JSON файл с ключами от Google Cloud.")
                print("Варианты размещения:")
                print("1. Поместите JSON файл в текущую папку (рядом с этим скриптом)")
                print("2. Назовите файл так, чтобы в имени было слово 'key'")
                print("   Например: 'my-project-key.json' или 'vertex-ai-key.json'")
                print("\nИли установите переменную окружения GOOGLE_APPLICATION_CREDENTIALS")
                print("="*60)
                raise ValueError("JSON файл с ключами Vertex AI не найден")
            
            # Берем первый найденный файл
            credentials_path = json_files[0]
            logger.info(f"Найден файл ключей: {credentials_path}")
            
            # Загружаем ключи
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            
            # Читаем project_id из файла
            with open(credentials_path, 'r') as f:
                key_data = json.load(f)
                self.project_id = key_data.get('project_id')
                
            if not self.project_id:
                raise ValueError("project_id не найден в JSON файле")
                
            logger.info(f"Vertex AI ключи загружены. Проект: {self.project_id}")
            return credentials
            
        except Exception as e:
            logger.error(f"Ошибка загрузки ключей Vertex AI: {e}")
            raise
            
    def initialize_vertex_ai(self):
        """Инициализация Vertex AI"""
        try:
            credentials = self.load_vertex_ai_credentials()
            
            print(f"\n🔧 ДИАГНОСТИКА VERTEX AI:")
            print(f"   Проект: {self.project_id}")
            print(f"   Регион: {self.location}")
            print(f"   Модель: {self.model_name}")
            
            # Инициализация Vertex AI
            vertexai.init(
                project=self.project_id,
                location=self.location,
                credentials=credentials
            )
            
            # Создаем модель
            self.model = GenerativeModel(
                model_name=self.model_name,
                safety_settings=self.safety_settings
            )
            
            logger.info(f"Vertex AI инициализирован. Модель: {self.model_name}")
            print(f"   ✅ Инициализация успешна")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации Vertex AI: {e}")
            print(f"   ❌ Ошибка: {e}")
            
            # Пробуем другие регионы
            alternative_locations = ["europe-west1", "asia-northeast1", "us-east1"]
            for alt_location in alternative_locations:
                try:
                    print(f"\n🔄 Пробуем регион: {alt_location}")
                    self.location = alt_location
                    vertexai.init(
                        project=self.project_id,
                        location=self.location,
                        credentials=credentials
                    )
                    self.model = GenerativeModel(
                        model_name=self.model_name,
                        safety_settings=self.safety_settings
                    )
                    print(f"   ✅ Регион {alt_location} работает!")
                    logger.info(f"Vertex AI работает в регионе: {alt_location}")
                    return
                except Exception as alt_e:
                    print(f"   ❌ Регион {alt_location} не работает: {alt_e}")
                    continue
                    
            raise Exception("Все регионы Vertex AI недоступны")
            
    def download_and_trim_video(self, video_url: str, output_path: str, duration_seconds: int = 120) -> bool:
        """Скачивание и обрезка видео до указанной продолжительности"""
        try:
            logger.info(f"Скачиваем и обрезаем видео: {video_url}")
            
            # Создаем временный файл для полного видео
            temp_video = output_path.replace('.mp4', '_temp.mp4')
            
            # Скачиваем видео
            response = requests.get(video_url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(temp_video, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Видео скачано: {temp_video}")
            
            # Обрезаем до указанной продолжительности с помощью ffmpeg
            ffmpeg_cmd = [
                'ffmpeg',
                '-i', temp_video,
                '-t', str(duration_seconds),  # Длительность в секундах
                '-c:v', 'libx264',  # Кодек видео
                '-c:a', 'aac',      # Кодек аудио
                '-y',               # Перезаписать выходной файл
                output_path
            ]
            
            logger.info(f"Обрезаем видео командой: {' '.join(ffmpeg_cmd)}")
            
            result = subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 минут максимум
            )
            
            # Удаляем временный файл
            if os.path.exists(temp_video):
                os.remove(temp_video)
            
            if result.returncode == 0:
                logger.info(f"Видео успешно обрезано: {output_path}")
                return True
            else:
                logger.error(f"Ошибка ffmpeg: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("Превышено время ожидания обработки видео")
            return False
        except Exception as e:
            logger.error(f"Ошибка скачивания/обрезки видео: {e}")
            return False

    def load_file_as_part(self, file_path: str, attachment_type: str) -> Optional[Part]:
        """Загрузка файла как Part для Vertex AI"""
        try:
            # Определяем MIME тип
            mime_type, _ = mimetypes.guess_type(file_path)
            
            if attachment_type == 'photo':
                if not mime_type or not mime_type.startswith('image/'):
                    mime_type = 'image/jpeg'
            elif attachment_type == 'audio_message':
                if not mime_type or not mime_type.startswith('audio/'):
                    mime_type = 'audio/ogg'
            elif attachment_type == 'sticker':
                if not mime_type or not mime_type.startswith('image/'):
                    mime_type = 'image/png'
            elif attachment_type == 'video':
                if not mime_type or not mime_type.startswith('video/'):
                    mime_type = 'video/mp4'
                    
            # Читаем файл
            with open(file_path, 'rb') as f:
                file_data = f.read()
                
            # Создаем Part
            part = Part.from_data(data=file_data, mime_type=mime_type)
            logger.info(f"Файл загружен как Part: {file_path} ({mime_type})")
            return part
            
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {file_path}: {e}")
            return None
            
    def analyze_attachment(self, file_path: str, attachment_type: str, metadata: Dict) -> Dict:
        """Анализ одного вложения"""
        try:
            logger.info(f"Анализируем {attachment_type}: {file_path}")
            
            result = {
                'file': file_path,
                'type': attachment_type,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata,
                'analysis': None,
                'error': None
            }
            
            # Получаем промпт для типа
            prompt = self.prompts.get(attachment_type, "Опиши содержимое максимально подробно.")
            
            if attachment_type in ['photo', 'audio_message', 'sticker']:
                # Файлы, которые нужно передать в модель
                if os.path.exists(file_path):
                    part = self.load_file_as_part(file_path, attachment_type)
                    if part:
                        # Генерируем анализ
                        response = self.model.generate_content([prompt, part])
                        result['analysis'] = response.text
                    else:
                        result['error'] = "Не удалось загрузить файл для анализа"
                else:
                    result['error'] = f"Файл не найден: {file_path}"
                    
            elif attachment_type == 'video':
                # Для видео: скачиваем, обрезаем и анализируем через Gemini
                result['analysis'] = self.analyze_video_content(file_path, metadata)
                    
            elif attachment_type in ['wall', 'audio']:
                # Для этих типов анализируем метаданные
                metadata_analysis = self.analyze_metadata(metadata, attachment_type)
                result['analysis'] = metadata_analysis
                
            else:
                result['error'] = f"Неизвестный тип вложения: {attachment_type}"
                
            return result
            
        except Exception as e:
            logger.error(f"Ошибка анализа {file_path}: {e}")
            return {
                'file': file_path,
                'type': attachment_type,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata,
                'analysis': None,
                'error': str(e)
            }
            
    def get_video_url_via_api(self, video_id: int, owner_id: int, access_key: str) -> Optional[str]:
        """Получение URL видео через VK API"""
        try:
            # Попробуем получить URL через прямой запрос к VK API
            # Используем публичные методы VK API
            api_url = "https://api.vk.com/method/video.get"
            params = {
                'videos': f"{owner_id}_{video_id}_{access_key}",
                'v': '5.131',
                'access_token': 'service'  # Попробуем без токена для публичных видео
            }
            
            response = requests.get(api_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'response' in data and 'items' in data['response'] and data['response']['items']:
                    video_data = data['response']['items'][0]
                    if 'files' in video_data:
                        files = video_data['files']
                        for quality in ['mp4_720', 'mp4_480', 'mp4_360', 'mp4_240']:
                            if quality in files:
                                return files[quality]
            
            logger.warning("Не удалось получить URL видео через API")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения URL через API: {e}")
            return None

    def analyze_video_content(self, json_file_path: str, metadata: Dict) -> str:
        """Полный анализ видео через скачивание и анализ Gemini"""
        try:
            # Получаем данные о видео из JSON файла
            original_data = metadata.get('original_data', {})
            video_url = None
            
            # Проверяем статус обработки видео
            processing = original_data.get('processing', 0)
            if processing == 1:
                logger.warning("Видео еще обрабатывается на VK, анализируем метаданные")
                return f"⚠️ ВИДЕО В ОБРАБОТКЕ\n\n{self.analyze_metadata(metadata, 'video')}"
            
            # Ищем URL видео в данных
            if 'files' in original_data:
                files = original_data['files']
                # Берем видео наилучшего качества
                for quality in ['mp4_720', 'mp4_480', 'mp4_360', 'mp4_240']:
                    if quality in files:
                        video_url = files[quality]
                        break
            
            # Альтернативные поля для URL видео
            if not video_url:
                for field in ['player', 'external', 'mp4_720', 'mp4_480', 'mp4_360', 'mp4_240']:
                    if field in original_data and original_data[field]:
                        video_url = original_data[field]
                        break
            
            # Попробуем получить URL через VK API
            if not video_url:
                video_id = original_data.get('id')
                owner_id = original_data.get('owner_id')
                access_key = original_data.get('access_key')
                
                if video_id and owner_id and access_key:
                    logger.info(f"Пытаемся получить URL видео через API: {owner_id}_{video_id}")
                    video_url = self.get_video_url_via_api(video_id, owner_id, access_key)
                        
            if not video_url:
                logger.error("URL видео не найден - ПОЛНЫЙ АНАЛИЗ ВИДЕО НЕВОЗМОЖЕН")
                return f"❌ АНАЛИЗ ВИДЕО НЕ ВЫПОЛНЕН\n\nПричина: URL видео не найден в метаданных и не получен через API\n\n{self.analyze_metadata(metadata, 'video')}"
            
            # Создаем путь для скачанного видео
            video_filename = os.path.basename(json_file_path).replace('.json', '.mp4')
            video_path = os.path.join(self.download_dir, video_filename)
            
            logger.info(f"Начинаем скачивание видео: {video_url}")
            
            # Скачиваем и обрезаем видео до 2 минут
            if self.download_and_trim_video(video_url, video_path, duration_seconds=120):
                logger.info(f"Видео готово для анализа: {video_path}")
                
                # Анализируем видео через Gemini
                part = self.load_file_as_part(video_path, 'video')
                if part:
                    # Расширенный промпт для видео
                    video_prompt = """Проанализируй это видео максимально подробно:
                    
                    1. СОДЕРЖАНИЕ: Что происходит в видео? Кто участвует? О чем говорят?
                    2. ВИЗУАЛЬНОЕ: Опиши обстановку, объекты, действия
                    3. АУДИО: Если есть речь - транскрибируй ключевые фразы
                    4. ТЕКСТ НА ЭКРАНЕ: Если есть любой текст - извлеки его ПОЛНОСТЬЮ
                    5. КОНТЕКСТ: Какая тема/жанр видео? Образовательное, развлекательное, музыкальное?
                    6. ЭМОЦИИ: Какое настроение передает видео?
                    
                    Отвечай на русском языке максимально подробно."""
                    
                    response = self.model.generate_content([video_prompt, part])
                    
                    # Добавляем метаданные к анализу
                    title = original_data.get('title', 'Без названия')
                    description = original_data.get('description', '')
                    duration = original_data.get('duration', 0)
                    views = original_data.get('views', 0)
                    
                    full_analysis = f"🎥 ВИДЕО АНАЛИЗ: {title}\n\n"
                    full_analysis += f"📊 МЕТАДАННЫЕ:\n"
                    if description:
                        full_analysis += f"   Описание: {description}\n"
                    if duration:
                        full_analysis += f"   Продолжительность: {duration} секунд\n"
                    if views:
                        full_analysis += f"   Просмотры: {views}\n"
                    
                    full_analysis += f"\n🤖 АНАЛИЗ СОДЕРЖИМОГО:\n{response.text}"
                    
                    logger.info(f"Видео успешно проанализировано: {video_path}")
                    return full_analysis
                else:
                    logger.error("Не удалось загрузить видео для анализа")
                    return self.analyze_metadata(metadata, 'video')
            else:
                logger.error("Не удалось скачать видео")
                return self.analyze_metadata(metadata, 'video')
                
        except Exception as e:
            logger.error(f"Ошибка анализа видео: {e}")
            return f"Ошибка анализа видео: {str(e)}\n\n{self.analyze_metadata(metadata, 'video')}"

    def analyze_metadata(self, metadata: Dict, attachment_type: str) -> str:
        """Анализ метаданных для типов без прямого файла"""
        try:
            original_data = metadata.get('original_data', {})
            
            if attachment_type == 'video':
                title = original_data.get('title', 'Без названия')
                description = original_data.get('description', '')
                duration = original_data.get('duration', 0)
                views = original_data.get('views', 0)
                
                analysis = f"Видео: '{title}'"
                if description:
                    analysis += f"\nОписание: {description}"
                if duration:
                    analysis += f"\nПродолжительность: {duration} секунд"
                if views:
                    analysis += f"\nПросмотры: {views}"
                    
                return analysis
                
            elif attachment_type == 'wall':
                text = original_data.get('text', '')
                from_id = original_data.get('from_id', '')
                post_type = original_data.get('post_type', '')
                
                analysis = f"Репост"
                if from_id:
                    analysis += f" от ID {from_id}"
                if text:
                    analysis += f"\nТекст: {text}"
                if post_type:
                    analysis += f"\nТип поста: {post_type}"
                    
                return analysis
                
            elif attachment_type == 'audio':
                artist = original_data.get('artist', 'Неизвестный исполнитель')
                title = original_data.get('title', 'Без названия')
                duration = original_data.get('duration', 0)
                
                analysis = f"Музыка: {artist} - {title}"
                if duration:
                    analysis += f"\nПродолжительность: {duration} секунд"
                    
                return analysis
                
            return "Анализ метаданных недоступен"
            
        except Exception as e:
            logger.error(f"Ошибка анализа метаданных: {e}")
            return f"Ошибка анализа метаданных: {str(e)}"
            
    def find_attachments(self) -> Dict[str, List[Dict]]:
        """Поиск всех скачанных вложений"""
        attachments = {
            'photo': [],
            'video': [],
            'sticker': [],
            'wall': [],
            'audio': [],
            'audio_message': []
        }
        
        if not os.path.exists(self.download_dir):
            logger.warning(f"Папка {self.download_dir} не найдена")
            return attachments
            
        # Загружаем отчет о скачивании для получения метаданных
        report_path = os.path.join(self.download_dir, 'download_report.json')
        download_report = {}
        if os.path.exists(report_path):
            try:
                with open(report_path, 'r', encoding='utf-8') as f:
                    download_report = json.load(f)
            except Exception as e:
                logger.error(f"Ошибка чтения отчета скачивания: {e}")
        
        # Ищем файлы в корне папки downloaded_attachments
        for filename in os.listdir(self.download_dir):
            file_path = os.path.join(self.download_dir, filename)
            
            # Пропускаем папки и служебные файлы
            if os.path.isdir(file_path) or filename == 'download_report.json':
                continue
                
            # Определяем тип вложения по имени файла
            attachment_type = None
            metadata = {}
            
            if filename.startswith('photo_') and filename.endswith('.jpg'):
                attachment_type = 'photo'
            elif filename.startswith('voice_') and filename.endswith('.mp3'):
                attachment_type = 'audio_message'
            elif filename.startswith('sticker_') and filename.endswith('.png'):
                attachment_type = 'sticker'
            elif filename.startswith('video_') and filename.endswith('.json'):
                attachment_type = 'video'
            elif filename.startswith('wall_') and filename.endswith('.json'):
                attachment_type = 'wall'
            elif filename.startswith('audio_') and filename.endswith('.json'):
                attachment_type = 'audio'
                
            if attachment_type:
                # Создаем базовые метаданные
                metadata = {
                    'type': attachment_type,
                    'local_file': file_path,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Для JSON файлов читаем содержимое как метаданные
                if filename.endswith('.json'):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_content = json.load(f)
                            metadata['original_data'] = file_content
                    except Exception as e:
                        logger.error(f"Ошибка чтения JSON файла {filename}: {e}")
                
                attachments[attachment_type].append({
                    'file': file_path,
                    'metadata': metadata
                })
                
        # Логируем что нашли
        for att_type, files in attachments.items():
            if files:
                logger.info(f"Найдено {att_type}: {len(files)} файлов")
                
        return attachments
        
    def analyze_all_attachments(self):
        """Анализ всех найденных вложений"""
        logger.info("Начинаем анализ всех вложений...")
        
        attachments = self.find_attachments()
        all_results = {}
        
        total_files = sum(len(files) for files in attachments.values())
        processed = 0
        
        for attachment_type, files in attachments.items():
            logger.info(f"Анализируем {attachment_type}: {len(files)} файлов")
            all_results[attachment_type] = []
            
            for file_info in files:
                processed += 1
                logger.info(f"Прогресс: {processed}/{total_files}")
                
                result = self.analyze_attachment(
                    file_info['file'],
                    attachment_type,
                    file_info['metadata']
                )
                
                all_results[attachment_type].append(result)
                
                # Сохраняем промежуточный результат
                self.save_individual_result(result)
                
        # Сохраняем общий отчет
        self.save_final_report(all_results)
        logger.info("Анализ завершен")
        
    def save_individual_result(self, result: Dict):
        """Сохранение результата анализа отдельного файла"""
        try:
            filename = os.path.basename(result['file'])
            result_filename = f"analysis_{filename}.json"
            result_path = os.path.join(self.results_dir, result_filename)
            
            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Ошибка сохранения результата: {e}")
            
    def save_final_report(self, all_results: Dict):
        """Сохранение финального отчета"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'summary': {},
                'results': all_results
            }
            
            # Создаем сводку
            for attachment_type, results in all_results.items():
                total = len(results)
                successful = 0
                errors = 0
                partial = 0  # Частичный успех (метаданные без полного анализа)
                
                for r in results:
                    if r.get('error'):
                        errors += 1
                    elif r.get('analysis'):
                        analysis = r.get('analysis', '')
                        if '❌ АНАЛИЗ ВИДЕО НЕ ВЫПОЛНЕН' in analysis:
                            partial += 1
                        elif '⚠️ ВИДЕО В ОБРАБОТКЕ' in analysis:
                            partial += 1
                        else:
                            successful += 1
                    else:
                        errors += 1
                
                # Для видео считаем успешными только полные анализы
                actual_success_rate = (successful/total*100) if total > 0 else 0
                
                report['summary'][attachment_type] = {
                    'total': total,
                    'successful': successful,
                    'partial': partial,
                    'errors': errors,
                    'success_rate': f"{actual_success_rate:.1f}%",
                    'notes': f"Полный анализ: {successful}, Только метаданные: {partial}, Ошибки: {errors}" if attachment_type == 'video' else None
                }
                
            # Сохраняем отчет
            report_path = os.path.join(self.results_dir, 'final_analysis_report.json')
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Финальный отчет сохранен: {report_path}")
            
            # Выводим краткую сводку
            self.print_summary(report['summary'])
            
            # Создаем контекст для бота
            self.create_bot_context(all_results)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения финального отчета: {e}")
            
    def create_bot_context(self, all_results: Dict):
        """Создание краткого контекста для бота"""
        try:
            bot_context = {
                'timestamp': datetime.now().isoformat(),
                'attachment_insights': {},
                'quick_responses': {}
            }
            
            for attachment_type, results in all_results.items():
                successful_results = [r for r in results if r.get('analysis')]
                
                if successful_results:
                    # Создаем краткие инсайты для каждого типа
                    insights = []
                    quick_responses = []
                    
                    for result in successful_results:
                        analysis = result.get('analysis', '')
                        
                        if attachment_type == 'photo':
                            # Извлекаем ключевые данные из OCR
                            if 'чек' in analysis.lower():
                                insights.append("Обнаружен чек - бот может помочь с учетом расходов")
                                quick_responses.append("Вижу чек! Могу помочь с анализом трат или категоризацией покупок.")
                            elif 'ошибка' in analysis.lower():
                                insights.append("Обнаружен скриншот ошибки - бот может помочь с диагностикой")
                                quick_responses.append("Вижу ошибку на скриншоте. Могу помочь разобраться с проблемой!")
                            elif any(word in analysis.lower() for word in ['цена', 'стоимость', 'рублей']):
                                insights.append("Обнаружена ценовая информация")
                                quick_responses.append("Вижу информацию о ценах. Нужна помощь с выбором или сравнением?")
                                
                        elif attachment_type == 'audio_message':
                            # Анализируем тематику голосового
                            if any(word in analysis.lower() for word in ['музыка', 'играть', 'фортепиано']):
                                insights.append("Голосовое о музыке/обучении")
                                quick_responses.append("Слышу, что речь о музыке! Готов помочь с обучением или вопросами по фортепиано.")
                            elif any(word in analysis.lower() for word in ['проблема', 'помощь', 'вопрос']):
                                insights.append("Голосовое с запросом помощи")
                                quick_responses.append("Понял ваш вопрос из голосового. Готов помочь!")
                            else:
                                quick_responses.append("Прослушал ваше сообщение. Чем могу помочь?")
                                
                        elif attachment_type == 'video':
                            # Анализируем контент видео
                            if any(word in analysis.lower() for word in ['урок', 'обучение', 'играет']):
                                insights.append("Видео с обучающим контентом")
                                quick_responses.append("Отличное видео! Вижу, что связано с обучением. Есть вопросы по технике или материалу?")
                            elif 'музыка' in analysis.lower():
                                insights.append("Музыкальное видео")
                                quick_responses.append("Прекрасная музыка! Хотите обсудить произведение или технику исполнения?")
                                
                        elif attachment_type == 'sticker':
                            # Определяем эмоциональный контекст стикера
                            if any(word in analysis.lower() for word in ['привет', 'здравствуй']):
                                quick_responses.append("Привет! Как дела?")
                            elif any(word in analysis.lower() for word in ['спасибо', 'благодар']):
                                quick_responses.append("Пожалуйста! Всегда рад помочь!")
                            elif any(word in analysis.lower() for word in ['грустн', 'печал']):
                                quick_responses.append("Понимаю ваше настроение. Чем могу поддержать?")
                            elif any(word in analysis.lower() for word in ['радост', 'счастлив']):
                                quick_responses.append("Рад, что у вас хорошее настроение! 😊")
                    
                    bot_context['attachment_insights'][attachment_type] = insights
                    bot_context['quick_responses'][attachment_type] = quick_responses
            
            # Сохраняем контекст для бота
            context_path = os.path.join(self.results_dir, 'bot_context.json')
            with open(context_path, 'w', encoding='utf-8') as f:
                json.dump(bot_context, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Контекст для бота сохранен: {context_path}")
            
        except Exception as e:
            logger.error(f"Ошибка создания контекста для бота: {e}")
            
    def print_summary(self, summary: Dict):
        """Вывод краткой сводки результатов"""
        print("\n" + "="*60)
        print("РЕЗУЛЬТАТЫ АНАЛИЗА ВЛОЖЕНИЙ")
        print("="*60)
        
        for attachment_type, stats in summary.items():
            if attachment_type == 'video':
                # Специальный вывод для видео
                if stats['successful'] > 0:
                    status = "✅"
                elif stats['partial'] > 0:
                    status = "⚠️"
                else:
                    status = "❌"
                print(f"{status} {attachment_type}: {stats['successful']}/{stats['total']} полный анализ ({stats['success_rate']})")
                if stats['partial'] > 0:
                    print(f"   📊 {stats['partial']} видео - только метаданные (URL не найден)")
            else:
                status = "✅" if stats['errors'] == 0 else "⚠️" if stats['successful'] > 0 else "❌"
                print(f"{status} {attachment_type}: {stats['successful']}/{stats['total']} успешно ({stats['success_rate']})")
            
        print("="*60)
        print(f"📁 Результаты сохранены в папке: {self.results_dir}")
        print("="*60)
        
    def run(self):
        """Запуск анализа вложений"""
        try:
            logger.info("Запуск анализатора вложений")
            
            # Инициализация
            self.initialize_vertex_ai()
            
            # Анализ всех вложений
            self.analyze_all_attachments()
            
            logger.info("Анализ завершен успешно")
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise

if __name__ == "__main__":
    analyzer = AttachmentAnalyzer()
    analyzer.run() 