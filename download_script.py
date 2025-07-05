# --- Описание ---
# Скрипт подключается к VK API и ищет в диалогах группы различные типы вложений:
# фото, голосовые сообщения, видео, стикеры, репосты, музыку.
# Скачивает по 3 экземпляра каждого типа в исходном формате для дальнейшего анализа.
# Сохраняет метаданные и создает отчет со ссылками на найденные вложения.
# --- Конец описания ---

import os
import json
import requests
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
import time
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_attachments.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AttachmentDownloader:
    def __init__(self):
        """Инициализация загрузчика вложений"""
        self.vk_session = None
        self.vk = None
        self.group_id = None
        
        # Целевые типы вложений и их счетчики
        self.target_types = {
            'photo': {'count': 0, 'target': 3, 'items': []},
            'audio_message': {'count': 0, 'target': 3, 'items': []},
            'video': {'count': 0, 'target': 3, 'items': []},
            'sticker': {'count': 0, 'target': 3, 'items': []},
            'wall': {'count': 0, 'target': 3, 'items': []},  # репосты
            'audio': {'count': 0, 'target': 3, 'items': []}   # музыка
        }
        
        # Папка для сохранения
        self.download_dir = "downloaded_attachments"
        self.ensure_download_directory()
        
    def ensure_download_directory(self):
        """Создание папки для загрузок"""
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            logger.info(f"Создана папка для загрузок: {self.download_dir}")
            
        # Создаем подпапки для каждого типа
        for attachment_type in self.target_types.keys():
            type_dir = os.path.join(self.download_dir, attachment_type)
            if not os.path.exists(type_dir):
                os.makedirs(type_dir)
                
    def load_environment_variables(self):
        """Загрузка переменных окружения"""
        try:
            self.vk_token = os.getenv('VK_TOKEN')
            self.group_id = os.getenv('VK_GROUP_ID')
            
            logger.info(f"VK_TOKEN загружен: {'Да' if self.vk_token else 'Нет'}")
            logger.info(f"VK_GROUP_ID загружен: {'Да' if self.group_id else 'Нет'}")
            
            if not self.vk_token:
                raise ValueError("VK_TOKEN не найден в переменных окружения")
            if not self.group_id:
                raise ValueError("VK_GROUP_ID не найден в переменных окружения")
                
            # Проверяем, что group_id - это число
            try:
                self.group_id = int(self.group_id)
                logger.info(f"Group ID: {self.group_id}")
            except ValueError:
                raise ValueError(f"VK_GROUP_ID должен быть числом, получено: {self.group_id}")
                
            logger.info("Переменные окружения успешно загружены")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки переменных окружения: {e}")
            raise
            
    def initialize_vk_api(self):
        """Инициализация VK API"""
        try:
            self.vk_session = vk_api.VkApi(token=self.vk_token)
            self.vk = self.vk_session.get_api()
            
            # Проверяем соединение через информацию о группе
            try:
                group_info = self.vk.groups.getById(group_id=self.group_id)
                if group_info:
                    logger.info(f"VK API инициализирован. Группа: {group_info[0]['name']}")
                else:
                    logger.warning("Не удалось получить информацию о группе, но API инициализирован")
            except Exception as e:
                logger.warning(f"Не удалось получить информацию о группе: {e}")
                # Пробуем альтернативную проверку
                try:
                    # Простая проверка API через метод groups.getById без параметров
                    test_response = self.vk.groups.getById()
                    logger.info("VK API инициализирован (альтернативная проверка)")
                except Exception as e2:
                    logger.warning(f"Альтернативная проверка также не удалась: {e2}")
                    logger.info("VK API инициализирован без проверки")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации VK API: {e}")
            raise
            
    def get_conversations(self, offset: int = 0, count: int = 100) -> List[Dict]:
        """Получение списка диалогов группы"""
        try:
            response = self.vk.messages.getConversations(
                group_id=self.group_id,
                count=count,
                offset=offset,
                extended=1
            )
            
            conversations = response.get('items', [])
            logger.info(f"Получено {len(conversations)} диалогов (offset: {offset})")
            return conversations
            
        except Exception as e:
            logger.error(f"Ошибка получения диалогов: {e}")
            return []
            
    def get_conversation_messages(self, peer_id: int, offset: int = 0, count: int = 100) -> List[Dict]:
        """Получение сообщений из конкретного диалога"""
        try:
            response = self.vk.messages.getHistory(
                peer_id=peer_id,
                count=count,
                offset=offset,
                group_id=self.group_id
            )
            
            messages = response.get('items', [])
            logger.info(f"Получено {len(messages)} сообщений из диалога {peer_id}")
            return messages
            
        except Exception as e:
            logger.error(f"Ошибка получения сообщений из диалога {peer_id}: {e}")
            return []
            
    def process_attachments(self, message: Dict, peer_id: int) -> bool:
        """Обработка вложений в сообщении"""
        attachments = message.get('attachments', [])
        if not attachments:
            return False
            
        found_new = False
        
        for attachment in attachments:
            att_type = attachment.get('type')
            
            # Проверяем, нужен ли нам этот тип и не превышен ли лимит
            if att_type in self.target_types:
                if self.target_types[att_type]['count'] < self.target_types[att_type]['target']:
                    success = self.download_attachment(attachment, att_type, peer_id, message.get('id'))
                    if success:
                        self.target_types[att_type]['count'] += 1
                        found_new = True
                        logger.info(f"Найден {att_type} ({self.target_types[att_type]['count']}/{self.target_types[att_type]['target']})")
                        
        return found_new
        
    def download_attachment(self, attachment: Dict, att_type: str, peer_id: int, message_id: int) -> bool:
        """Скачивание конкретного вложения"""
        try:
            attachment_data = attachment.get(att_type, {})
            
            # Подготавливаем метаданные
            metadata = {
                'type': att_type,
                'peer_id': peer_id,
                'message_id': message_id,
                'timestamp': datetime.now().isoformat(),
                'original_data': attachment_data
            }
            
            file_url = None
            file_extension = ""
            
            # Обработка разных типов вложений
            if att_type == 'photo':
                # Берем фото максимального размера
                sizes = attachment_data.get('sizes', [])
                if sizes:
                    max_size = max(sizes, key=lambda x: x.get('width', 0) * x.get('height', 0))
                    file_url = max_size.get('url')
                    file_extension = '.jpg'
                    
            elif att_type == 'audio_message':
                file_url = attachment_data.get('link_ogg')
                file_extension = '.ogg'
                
            elif att_type == 'video':
                # Для видео сохраняем метаданные, так как прямая ссылка может быть недоступна
                file_extension = '.json'
                # Видео файлы VK требуют особой обработки
                
            elif att_type == 'sticker':
                # Берем стикер максимального размера
                images = attachment_data.get('images', [])
                if images:
                    max_image = max(images, key=lambda x: x.get('width', 0))
                    file_url = max_image.get('url')
                    file_extension = '.png'
                    
            elif att_type == 'wall':
                # Репост - сохраняем метаданные
                file_extension = '.json'
                
            elif att_type == 'audio':
                # Музыка - сохраняем метаданные (прямые ссылки недоступны)
                file_extension = '.json'
                
            # Генерируем имя файла
            filename = f"{att_type}_{self.target_types[att_type]['count'] + 1}_{peer_id}_{message_id}{file_extension}"
            filepath = os.path.join(self.download_dir, att_type, filename)
            
            # Скачиваем файл или сохраняем метаданные
            if file_url and att_type in ['photo', 'audio_message', 'sticker']:
                success = self.download_file(file_url, filepath)
                if success:
                    metadata['local_file'] = filepath
                    metadata['download_url'] = file_url
            else:
                # Сохраняем только метаданные для видео, репостов, музыки
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                success = True
                
            if success:
                # Сохраняем метаданные в отдельный файл
                metadata_path = filepath.replace(file_extension, '_metadata.json')
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                    
                # Добавляем в список найденных
                self.target_types[att_type]['items'].append({
                    'file': filepath,
                    'metadata': metadata,
                    'url': file_url
                })
                
                logger.info(f"Скачан {att_type}: {filename}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка скачивания вложения {att_type}: {e}")
            
        return False
        
    def download_file(self, url: str, filepath: str) -> bool:
        """Скачивание файла по URL"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Файл скачан: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка скачивания файла {url}: {e}")
            return False
            
    def is_collection_complete(self) -> bool:
        """Проверка, собраны ли все типы вложений"""
        for att_type, info in self.target_types.items():
            if info['count'] < info['target']:
                return False
        return True
        
    def search_attachments(self):
        """Основной метод поиска вложений"""
        logger.info("Начинаем поиск вложений...")
        
        conversations_offset = 0
        conversations_processed = 0
        max_conversations = 1000  # Ограничение для безопасности
        
        while not self.is_collection_complete() and conversations_processed < max_conversations:
            # Получаем диалоги пачками
            conversations = self.get_conversations(offset=conversations_offset, count=100)
            
            if not conversations:
                logger.warning("Больше нет диалогов для обработки")
                break
                
            for conversation in conversations:
                peer_id = conversation['conversation']['peer']['id']
                
                # Обрабатываем сообщения в диалоге
                messages_offset = 0
                messages_processed = 0
                max_messages_per_dialog = 200  # Ограничение сообщений на диалог
                
                while messages_processed < max_messages_per_dialog:
                    messages = self.get_conversation_messages(peer_id, offset=messages_offset, count=100)
                    
                    if not messages:
                        break
                        
                    for message in messages:
                        if self.process_attachments(message, peer_id):
                            # Если нашли новое вложение, проверяем завершенность
                            if self.is_collection_complete():
                                logger.info("Все типы вложений собраны!")
                                return
                                
                    messages_offset += len(messages)
                    messages_processed += len(messages)
                    
                    # Небольшая пауза для соблюдения rate limits
                    time.sleep(0.1)
                    
                conversations_processed += 1
                
                # Логируем прогресс
                if conversations_processed % 10 == 0:
                    logger.info(f"Обработано диалогов: {conversations_processed}")
                    self.log_progress()
                    
            conversations_offset += len(conversations)
            
        logger.info("Поиск завершен")
        self.log_progress()
        
    def log_progress(self):
        """Логирование текущего прогресса"""
        logger.info("=== ПРОГРЕСС СБОРА ВЛОЖЕНИЙ ===")
        for att_type, info in self.target_types.items():
            logger.info(f"{att_type}: {info['count']}/{info['target']}")
            
    def generate_report(self):
        """Генерация отчета о скачанных вложениях"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'files': {}
        }
        
        for att_type, info in self.target_types.items():
            report['summary'][att_type] = {
                'collected': info['count'],
                'target': info['target'],
                'completed': info['count'] >= info['target']
            }
            
            report['files'][att_type] = []
            for item in info['items']:
                report['files'][att_type].append({
                    'file': item['file'],
                    'url': item.get('url'),
                    'peer_id': item['metadata']['peer_id'],
                    'message_id': item['metadata']['message_id']
                })
                
        report_path = os.path.join(self.download_dir, 'download_report.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Отчет сохранен: {report_path}")
        
        # Выводим отчет в консоль
        print("\n" + "="*50)
        print("ОТЧЕТ О СКАЧАННЫХ ВЛОЖЕНИЯХ")
        print("="*50)
        
        for att_type, info in self.target_types.items():
            status = "✅" if info['count'] >= info['target'] else "❌"
            print(f"{status} {att_type}: {info['count']}/{info['target']}")
            
            for item in info['items']:
                print(f"   📁 {item['file']}")
                if item.get('url'):
                    print(f"   🔗 {item['url']}")
                    
        print("="*50)
        
    def run(self):
        """Запуск процесса сбора вложений"""
        try:
            logger.info("Запуск скрипта сбора вложений")
            
            # Инициализация
            self.load_environment_variables()
            self.initialize_vk_api()
            
            # Поиск и скачивание
            self.search_attachments()
            
            # Генерация отчета
            self.generate_report()
            
            logger.info("Скрипт завершен успешно")
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise

if __name__ == "__main__":
    downloader = AttachmentDownloader()
    downloader.run() 