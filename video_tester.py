# --- Описание ---
# Скрипт для изолированного тестирования анализа видео по ID
# Анализирует превью-кадры (обложки и промежуточные кадры) из VK API
# Позволяет протестировать конкретные видео без повторного анализа уже обработанных
# Результат сохраняется в отдельную папку video_test_results/
# --- Конец описания ---

import os
import json
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('video_tester.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class VideoTester:
    def __init__(self):
        self.results_dir = "video_test_results"
        self.setup_directories()
        self.load_vertex_ai_keys()
        self.init_vertex_ai()
        
    def setup_directories(self):
        """Создание необходимых директорий"""
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(f"{self.results_dir}/frames", exist_ok=True)
        
    def load_vertex_ai_keys(self):
        """Загрузка ключей Vertex AI"""
        # Ищем JSON файл с ключами
        key_files = [f for f in os.listdir('.') if f.endswith('.json') and 'key' in f.lower()]
        
        if not key_files:
            raise FileNotFoundError("Не найден JSON файл с ключами Vertex AI")
            
        key_file = key_files[0]
        logger.info(f"Найден файл ключей: {key_file}")
        
        with open(key_file, 'r', encoding='utf-8') as f:
            self.vertex_keys = json.load(f)
            
        # Устанавливаем переменную окружения для аутентификации
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file
        
        self.project_id = self.vertex_keys.get('project_id')
        logger.info(f"Vertex AI ключи загружены. Проект: {self.project_id}")
        
    def init_vertex_ai(self):
        """Инициализация Vertex AI"""
        try:
            vertexai.init(project=self.project_id, location="us-central1")
            self.model = GenerativeModel("gemini-2.5-flash")
            logger.info("Vertex AI инициализирован. Модель: gemini-2.5-flash")
            print("   ✅ Vertex AI готов к работе")
        except Exception as e:
            logger.error(f"Ошибка инициализации Vertex AI: {e}")
            raise
            
    def get_video_info_from_vk(self, video_id: str) -> Optional[Dict]:
        """Получение информации о видео через VK API или локальные файлы"""
        try:
            # Сначала пробуем найти локальный JSON файл
            local_files = {
                "78671089_456239944": "downloaded_attachments/video_3.json",
                "78671089_456239983": "downloaded_attachments/video_1.json"
            }
            
            if video_id in local_files and os.path.exists(local_files[video_id]):
                logger.info(f"Используем локальный файл для {video_id}: {local_files[video_id]}")
                with open(local_files[video_id], 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Если локального файла нет, пробуем API
            vk_token = os.getenv('VK_GROUP_TOKEN')
            
            api_url = "https://api.vk.com/method/video.get"
            params = {
                'videos': video_id,
                'v': '5.131'
            }
            
            # Если есть токен, используем его
            if vk_token:
                params['access_token'] = vk_token
                logger.info(f"Используем VK токен для получения видео {video_id}")
            else:
                logger.warning("VK токен не найден, пробуем без токена")
            
            response = requests.get(api_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if 'response' in data and 'items' in data['response'] and data['response']['items']:
                    return data['response']['items'][0]
                elif 'error' in data:
                    logger.error(f"VK API ошибка: {data['error']}")
                    
            logger.error(f"Не удалось получить информацию о видео {video_id}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о видео: {e}")
            return None
            
    def download_frame(self, url: str, filename: str) -> Optional[str]:
        """Скачивание кадра по URL"""
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                filepath = f"{self.results_dir}/frames/{filename}"
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Кадр скачан: {filename}")
                return filepath
            return None
        except Exception as e:
            logger.error(f"Ошибка скачивания кадра {filename}: {e}")
            return None
            
    def analyze_frame_with_gemini(self, image_path: str, frame_type: str, video_info: Dict) -> str:
        """Анализ кадра с помощью Gemini"""
        try:
            # Загружаем изображение как base64
            import base64
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
            
    def test_video(self, video_id: str) -> Dict:
        """Тестирование конкретного видео"""
        logger.info(f"Начинаем тест видео: {video_id}")
        
        # Получаем информацию о видео
        video_info = self.get_video_info_from_vk(video_id)
        if not video_info:
            return {"error": "Не удалось получить информацию о видео"}
            
        result = {
            "video_id": video_id,
            "title": video_info.get('title', 'Без названия'),
            "description": video_info.get('description', ''),
            "duration": video_info.get('duration', 0),
            "views": video_info.get('views', 0),
            "frames_analyzed": [],
            "analysis_summary": "",
            "timestamp": datetime.now().isoformat()
        }
        
        # Анализируем обложку (first_frame)
        first_frame_url = video_info.get('first_frame_800') or video_info.get('first_frame_320')
        if first_frame_url:
            frame_file = f"first_frame_{video_id.replace('_', '-')}.jpg"
            frame_path = self.download_frame(first_frame_url, frame_file)
            if frame_path:
                analysis = self.analyze_frame_with_gemini(frame_path, "first_frame", video_info)
                result["frames_analyzed"].append({
                    "type": "first_frame",
                    "file": frame_file,
                    "analysis": analysis
                })
                
        # Анализируем превью-кадры
        for i, quality in enumerate(['photo_800', 'photo_320', 'photo_130']):
            if quality in video_info and video_info[quality]:
                frame_file = f"preview_{i+1}_{video_id.replace('_', '-')}.jpg"
                frame_path = self.download_frame(video_info[quality], frame_file)
                if frame_path:
                    analysis = self.analyze_frame_with_gemini(frame_path, "preview", video_info)
                    result["frames_analyzed"].append({
                        "type": f"preview_{i+1}",
                        "file": frame_file,
                        "analysis": analysis
                    })
                    
        # Создаем общую сводку
        if result["frames_analyzed"]:
            summary_parts = []
            summary_parts.append(f"📹 ВИДЕО: {result['title']}")
            summary_parts.append(f"⏱️ Длительность: {result['duration']} сек")
            summary_parts.append(f"👀 Просмотры: {result['views']}")
            summary_parts.append(f"🎬 Проанализировано кадров: {len(result['frames_analyzed'])}")
            
            for frame in result["frames_analyzed"]:
                summary_parts.append(f"\n--- {frame['type'].upper()} ---")
                summary_parts.append(frame['analysis'][:300] + "..." if len(frame['analysis']) > 300 else frame['analysis'])
                
            result["analysis_summary"] = "\n".join(summary_parts)
        else:
            result["analysis_summary"] = "❌ Не удалось проанализировать ни одного кадра"
            
        return result
        
    def save_result(self, result: Dict, video_id: str):
        """Сохранение результата"""
        filename = f"{self.results_dir}/test_{video_id.replace('_', '-')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            
        # Также сохраняем человеко-читаемый отчет, если есть анализ
        if "analysis_summary" in result:
            readable_filename = f"{self.results_dir}/readable_{video_id.replace('_', '-')}.md"
            with open(readable_filename, 'w', encoding='utf-8') as f:
                f.write(result["analysis_summary"])
            
        logger.info(f"Результат сохранен: {filename}")
        
def main():
    # Тестируемые видео
    test_videos = [
        "78671089_456239944",  # "Новогодний вечер" - игра на инструменте
        "78671089_456239983"   # "Видео от Сергея Филимонова" - возможно скринкаст
    ]
    
    tester = VideoTester()
    
    print("🎬 ИЗОЛИРОВАННЫЙ ТЕСТЕР ВИДЕО")
    print("="*50)
    
    for video_id in test_videos:
        print(f"\n📹 Тестируем видео: {video_id}")
        
        result = tester.test_video(video_id)
        
        if "error" in result:
            print(f"❌ Ошибка: {result['error']}")
        else:
            print(f"✅ Анализ завершен: {result['title']}")
            print(f"   Кадров проанализировано: {len(result['frames_analyzed'])}")
            
        tester.save_result(result, video_id)
        
    print(f"\n📁 Все результаты сохранены в папке: {tester.results_dir}")
    print("="*50)

if __name__ == "__main__":
    main() 