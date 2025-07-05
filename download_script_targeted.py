# --- Описание ---
# Целевой скрипт для поиска недостающих вложений в конкретном диалоге
# Загружает уже собранные данные и ищет только то, что еще нужно найти
# Работает быстро и целенаправленно в указанном peer_id
# --- Конец описания ---

import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType

# Загружаем переменные окружения
load_dotenv()

def get_vk_session():
    """Получение сессии VK API"""
    try:
        vk_token = os.getenv('VK_TOKEN')
        if not vk_token:
            print("❌ Ошибка: VK_TOKEN не найден в .env файле")
            return None
            
        vk_session = vk_api.VkApi(token=vk_token)
        vk = vk_session.get_api()
        
        # Проверяем соединение
        group_info = vk.groups.getById()
        print(f"✅ Подключение к VK API успешно. Группа: {group_info[0]['name']}")
        
        return vk_session, vk
        
    except Exception as e:
        print(f"❌ Ошибка подключения к VK API: {e}")
        return None, None

def get_target_attachments():
    """Определяем какие вложения нужно найти"""
    target_types = {
        'photo': 3,
        'video': 3, 
        'sticker': 3,
        'wall': 3,  # репосты
        'audio': 3,
        'audio_message': 3  # голосовые
    }
    print(f"🎯 Ищем в вашем диалоге (СНАЧАЛА НОВЫЕ сообщения): {target_types}")
    return target_types

def search_in_dialog(vk, peer_id, target_types, max_messages=200):
    """Поиск всех типов вложений в конкретном диалоге"""
    print(f"\n🔍 Поиск в диалоге peer_id={peer_id}")
    print(f"Ищем: {target_types}")
    
    found_attachments = {att_type: [] for att_type in target_types.keys()}
    
    try:
        # Получаем историю сообщений С КОНЦА (самые свежие)
        response = vk.messages.getHistory(
            peer_id=peer_id,
            count=max_messages,
            extended=1,
            rev=0  # 0 = сначала новые сообщения, 1 = сначала старые
        )
        
        messages = response['items']
        print(f"📝 Загружено {len(messages)} сообщений для анализа")
        
        for message in messages:
            if not message.get('attachments'):
                continue
                
            for attachment in message['attachments']:
                att_type = attachment['type']
                
                # Проверяем, нужен ли нам этот тип
                if att_type not in target_types:
                    continue
                    
                # Проверяем, не достигли ли мы лимита
                if len(found_attachments[att_type]) >= target_types[att_type]:
                    continue
                    
                # Добавляем найденное вложение
                attachment_info = {
                    'message_id': message['id'],
                    'date': message['date'],
                    'attachment': attachment
                }
                
                found_attachments[att_type].append(attachment_info)
                print(f"✅ Найдено {att_type}: {len(found_attachments[att_type])}/{target_types[att_type]}")
                
    except Exception as e:
        print(f"❌ Ошибка при поиске в диалоге: {e}")
        
    return found_attachments

def download_attachment(attachment_info, attachment_type, index, output_dir):
    """Скачивание одного вложения"""
    try:
        attachment = attachment_info['attachment']
        
        if attachment_type == 'photo':
            # Берем фото максимального размера
            sizes = attachment['photo']['sizes']
            max_size = max(sizes, key=lambda x: x['width'] * x['height'])
            url = max_size['url']
            filename = f"photo_{index+1}.jpg"
            
        elif attachment_type == 'video':
            # Для видео нужно получить ссылку через API
            video_id = attachment['video']['id']
            owner_id = attachment['video']['owner_id']
            # Простое сохранение метаданных
            filename = f"video_{index+1}.json"
            with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
                json.dump(attachment['video'], f, ensure_ascii=False, indent=2)
            return True
            
        elif attachment_type == 'audio':
            # Аудио файлы - сохраняем метаданные
            filename = f"audio_{index+1}.json"
            with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
                json.dump(attachment['audio'], f, ensure_ascii=False, indent=2)
            return True
            
        elif attachment_type == 'audio_message':
            # Голосовые сообщения
            url = attachment['audio_message']['link_mp3']
            filename = f"voice_{index+1}.mp3"
            
        elif attachment_type == 'sticker':
            # Стикеры
            images = attachment['sticker']['images']
            max_image = max(images, key=lambda x: x['width'] * x['height'])
            url = max_image['url']
            filename = f"sticker_{index+1}.png"
            
        elif attachment_type == 'wall':
            # Репосты - сохраняем метаданные
            filename = f"wall_{index+1}.json"
            with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
                json.dump(attachment['wall'], f, ensure_ascii=False, indent=2)
            return True
            
        else:
            print(f"⚠️ Неизвестный тип вложения: {attachment_type}")
            return False
            
        # Скачиваем файл по URL
        if 'url' in locals():
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'wb') as f:
                f.write(response.content)
                
            print(f"✅ Скачано: {filename}")
            return True
            
    except Exception as e:
        print(f"❌ Ошибка скачивания {attachment_type}: {e}")
        return False



def main():
    print("🎯 ПОИСК ВСЕХ ВЛОЖЕНИЙ В ВАШЕМ ДИАЛОГЕ")
    print("=" * 60)
    
    # Определяем что нужно найти
    target_types = get_target_attachments()
    
    # Используем ваш ID для поиска в личном диалоге
    peer_id = 78671089  # Ваш ID оператора
    print(f"\n🎯 Поиск в вашем личном диалоге (peer_id={peer_id})")
        
    # Подключаемся к VK
    vk_session, vk = get_vk_session()
    if not vk:
        return
        
    # Ищем в диалоге
    found = search_in_dialog(vk, peer_id, target_types, max_messages=200)
    
    if not found:
        print("❌ Ничего не найдено в указанном диалоге")
        return
        
    # Создаем папки для скачивания
    os.makedirs('downloaded_attachments', exist_ok=True)
    
    # Скачиваем найденные вложения
    print("\n📥 Скачивание найденных вложений...")
    for att_type, items in found.items():
        if items:  # Только если что-то найдено
            print(f"\n📁 Скачиваем {att_type}:")
            
            for i, item in enumerate(items):
                download_attachment(item, att_type, i, 'downloaded_attachments')
                time.sleep(0.5)  # Небольшая пауза
            
    # Создаем новый отчет
    report_data = {
        'search_date': datetime.now().isoformat(),
        'search_type': 'targeted_dialog_search',
        'peer_id': peer_id,
        'found_attachments': {}
    }
    
    for att_type, items in found.items():
        report_data['found_attachments'][att_type] = []
        for item in items:
            report_data['found_attachments'][att_type].append({
                'message_id': item['message_id'],
                'date': item['date']
            })
    
    # Сохраняем отчет
    with open('downloaded_attachments/download_report.json', 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ ПОИСК ЗАВЕРШЕН!")
    print(f"Найдено и скачано:")
    for att_type, items in found.items():
        if items:
            print(f"• {att_type}: {len(items)}/{target_types[att_type]}")
        else:
            print(f"• {att_type}: 0/{target_types[att_type]} ❌")
            
    print(f"\n📁 Все файлы сохранены в папку: downloaded_attachments/")
    print(f"📋 Отчет сохранен в: downloaded_attachments/download_report.json")

if __name__ == "__main__":
    main() 