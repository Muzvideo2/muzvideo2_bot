# --- Описание ---
# Этот скрипт является классом-анализатором для различных типов вложений.
# Он НЕ работает самостоятельно, а вызывается из main.py.
# main.py передает в него путь к ВРЕМЕННОМУ файлу для анализа.
# Использует Vertex AI (gemini-2.5-flash) для анализа:
# - Фото/стикеры: полное OCR извлечение текста
# - Голосовые: полная транскрипция речи
# - ВИДЕО: анализ превью-кадров (обложка + 3-4 кадра) через Gemini
# - Репосты/музыка: анализ метаданных
# --- Конец описания ---

import os
import json
import logging
import base64
import mimetypes
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
    def __init__(self, model: Optional[GenerativeModel] = None):
        """
        Инициализация анализатора вложений.
        Принимает уже инициализированную модель Vertex AI.
        """
        self.model = model
        
        # Папки для работы (используются только для локальных тестов, не для main.py)
        self.results_dir = "analysis_results"
        self.ensure_results_directory()
        
        # Настройки безопасности для модели (остаются здесь, т.к. могут быть специфичны для анализатора)
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
                               
            'video': """Проанализируй этот превью-кадр видео максимально подробно:
                       1. СОДЕРЖАНИЕ: Что происходит? Кто участвует? О чем говорят?
                       2. ВИЗУАЛЬНОЕ: Опиши обстановку, объекты, действия
                       3. ТЕКСТ НА ЭКРАНЕ: Извлеки ВЕСЬ видимый текст
                       4. КОНТЕКСТ: Тема, жанр, настроение видео
                       5. ДЕТАЛИ: Все важные элементы на кадре
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
            
    def download_frame(self, url: str, filename: str) -> Optional[str]:
        """Скачивание кадра видео по URL"""
        try:
            logger.info(f"Скачиваем кадр: {url}")
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                # Создаем папку для кадров если нет
                frames_dir = os.path.join(self.results_dir, 'frames')
                if not os.path.exists(frames_dir):
                    os.makedirs(frames_dir)
                    
                filepath = os.path.join(frames_dir, filename)
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Кадр скачан: {filename}")
                return filepath
            else:
                logger.error(f"Ошибка скачивания кадра: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Ошибка скачивания кадра {filename}: {e}")
            return None
            
    def analyze_frame_with_gemini(self, image_path: str, frame_type: str, video_info: Dict) -> str:
        """Анализ кадра видео с помощью Gemini"""
        try:
            # Загружаем изображение
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            image_part = Part.from_data(
                data=image_data,
                mime_type="image/jpeg"
            )
            
            # Формируем промпт в зависимости от типа кадра
            if frame_type == "first_frame":
                prompt = f"""
Анализируй этот кадр-обложку видео "{video_info.get('title', 'Без названия')}".

ЗАДАЧА: Детально опиши что происходит на этом кадре. Это начальный кадр видео.

Описание видео: {video_info.get('description', 'Нет описания')}
Длительность: {video_info.get('duration', 0)} секунд

АНАЛИЗИРУЙ:
1. Что изображено на кадре
2. Какие объекты, люди, инструменты видны
3. Обстановка и окружение
4. Текст, если есть
5. Предположение о содержании видео на основе этого кадра

Отвечай подробно на русском языке.
"""
            else:
                prompt = f"""
Анализируй этот превью-кадр из видео "{video_info.get('title', 'Без названия')}".

ЗАДАЧА: Детально опиши что происходит на этом кадре из середины/конца видео.

Описание видео: {video_info.get('description', 'Нет описания')}
Длительность: {video_info.get('duration', 0)} секунд

АНАЛИЗИРУЙ:
1. Что изображено на кадре
2. Какие действия происходят
3. Изменения по сравнению с возможным началом
4. Детали и объекты
5. Текст или интерфейс, если видны

Отвечай подробно на русском языке.
"""
            
            response = self.model.generate_content([prompt, image_part])
            return response.text
            
        except Exception as e:
            logger.error(f"Ошибка анализа кадра {image_path}: {e}")
            return f"Ошибка анализа: {str(e)}"
            
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
                # Для видео: анализируем превью-кадры через Gemini
                result['analysis'] = self.analyze_video_frames(file_path, metadata)
                    
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
            
    def analyze_video_frames(self, json_file_path: str, metadata: Dict) -> str:
        """Анализ видео через превью-кадры"""
        try:
            # Получаем данные о видео из JSON файла
            original_data = metadata.get('original_data', {})
            
            # Проверяем статус обработки видео
            processing = original_data.get('processing', 0)
            if processing == 1:
                logger.warning("Видео еще обрабатывается на VK, анализируем только метаданные")
                return f"⚠️ ВИДЕО В ОБРАБОТКЕ\n\n{self.analyze_metadata(metadata, 'video')}"
            
            # Создаем результат анализа
            video_id = f"{original_data.get('owner_id', '')}_{original_data.get('id', '')}"
            result = {
                "video_id": video_id,
                "title": original_data.get('title', 'Без названия'),
                "description": original_data.get('description', ''),
                "duration": original_data.get('duration', 0),
                "views": original_data.get('views', 0),
                "frames_analyzed": [],
                "analysis_summary": "",
                "timestamp": datetime.now().isoformat()
            }
            
            frames_analyzed = 0
            
            # Анализируем обложку (first_frame)
            first_frame_url = original_data.get('first_frame_800') or original_data.get('first_frame_320')
            if first_frame_url:
                frame_file = f"first_frame_{video_id.replace('_', '-')}.jpg"
                frame_path = self.download_frame(first_frame_url, frame_file)
                if frame_path:
                    analysis = self.analyze_frame_with_gemini(frame_path, "first_frame", original_data)
                    result["frames_analyzed"].append({
                        "type": "first_frame",
                        "file": frame_file,
                        "analysis": analysis
                    })
                    frames_analyzed += 1
                    
            # Анализируем превью-кадры
            for i, quality in enumerate(['photo_800', 'photo_320', 'photo_130']):
                if quality in original_data and original_data[quality]:
                    frame_file = f"preview_{i+1}_{video_id.replace('_', '-')}.jpg"
                    frame_path = self.download_frame(original_data[quality], frame_file)
                    if frame_path:
                        analysis = self.analyze_frame_with_gemini(frame_path, "preview", original_data)
                        result["frames_analyzed"].append({
                            "type": f"preview_{i+1}",
                            "file": frame_file,
                            "analysis": analysis
                        })
                        frames_analyzed += 1
                        
            # Создаем общую сводку
            if frames_analyzed > 0:
                summary_parts = []
                summary_parts.append(f"📹 ВИДЕО: {result['title']}")
                summary_parts.append(f"⏱️ Длительность: {result['duration']} сек")
                summary_parts.append(f"👀 Просмотры: {result['views']}")
                summary_parts.append(f"🎬 Проанализировано кадров: {frames_analyzed}")
                
                for frame in result["frames_analyzed"]:
                    summary_parts.append(f"\n--- {frame['type'].upper()} ---")
                    # Берем первые 500 символов анализа для сводки
                    analysis_preview = frame['analysis'][:500] + "..." if len(frame['analysis']) > 500 else frame['analysis']
                    summary_parts.append(analysis_preview)
                
                result["analysis_summary"] = "\n".join(summary_parts)
                
                logger.info(f"Видео успешно проанализировано: {frames_analyzed} кадров")
                return result["analysis_summary"]
            else:
                logger.error("Не удалось скачать ни одного кадра для анализа")
                return f"❌ АНАЛИЗ ПРЕВЬЮ-КАДРОВ НЕ ВЫПОЛНЕН\n\nПричина: Не найдены URL кадров в метаданных\n\n{self.analyze_metadata(metadata, 'video')}"
                
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
            # self.initialize_vertex_ai() # Удалено, модель передается в __init__
            
            # Анализ всех вложений
            self.analyze_all_attachments()
            
            logger.info("Анализ завершен успешно")
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise

if __name__ == "__main__":
    analyzer = AttachmentAnalyzer()
    analyzer.run() 