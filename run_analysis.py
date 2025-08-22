#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для автоматизации процесса анализа карточки клиента.

Этот скрипт принимает conv_id как аргумент командной строки,
вызывает data_exporter.py для экспорта данных клиента в JSON-файл,
затем вызывает client_card_analyzer.py для анализа этих данных.
"""

import argparse
import logging
import subprocess
import sys
import re
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_analysis.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_data_exporter(conv_id: int) -> str:
    """
    Запускает data_exporter.py с указанным conv_id и возвращает путь к JSON-файлу.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
        
    Returns:
        str: Путь к созданному JSON-файлу
        
    Raises:
        subprocess.CalledProcessError: Если data_exporter.py завершился с ошибкой
        ValueError: Если путь к файлу не найден в выводе data_exporter.py
    """
    logger.info(f"Запуск data_exporter.py с conv_id: {conv_id}")
    
    try:
        # Запуск data_exporter.py
        result = subprocess.run(
            [sys.executable, "data_exporter.py", str(conv_id)],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Поиск пути к файлу в выводе
        output = result.stdout
        match = re.search(r"Данные клиента успешно экспортированы в файл: (.+)", output)
        if match:
            json_file_path = match.group(1)
            logger.info(f"Файл данных клиента создан: {json_file_path}")
            return json_file_path
        else:
            logger.error("Не удалось найти путь к файлу в выводе data_exporter.py")
            raise ValueError("Не удалось найти путь к файлу в выводе data_exporter.py")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"data_exporter.py завершился с ошибкой: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запуске data_exporter.py: {e}")
        raise

def run_client_card_analyzer(json_file_path: str) -> str:
    """
    Запускает client_card_analyzer.py с указанным JSON-файлом и возвращает путь к файлу с результатами анализа.
    
    Args:
        json_file_path (str): Путь к JSON-файлу с данными клиента
        
    Returns:
        str: Путь к файлу с результатами анализа
        
    Raises:
        subprocess.CalledProcessError: Если client_card_analyzer.py завершился с ошибкой
        ValueError: Если путь к файлу результатов не найден в выводе client_card_analyzer.py
    """
    logger.info(f"Запуск client_card_analyzer.py с файлом: {json_file_path}")
    
    try:
        # Запуск client_card_analyzer.py
        result = subprocess.run(
            [sys.executable, "client_card_analyzer.py", json_file_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Поиск пути к файлу результатов в выводе
        output = result.stdout
        match = re.search(r"Анализ карточки клиента завершен. Результат сохранен в: (.+)", output)
        if match:
            result_file_path = match.group(1)
            logger.info(f"Файл с результатами анализа создан: {result_file_path}")
            return result_file_path
        else:
            logger.error("Не удалось найти путь к файлу результатов в выводе client_card_analyzer.py")
            raise ValueError("Не удалось найти путь к файлу результатов в выводе client_card_analyzer.py")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"client_card_analyzer.py завершился с ошибкой: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запуске client_card_analyzer.py: {e}")
        raise

def main(conv_id: int) -> None:
    """
    Основная функция скрипта.
    
    Args:
        conv_id (int): Идентификатор диалога клиента
    """
    logger.info(f"Запуск автоматизированного анализа для клиента с conv_id: {conv_id}")
    
    try:
        # 1. Запуск data_exporter.py
        json_file_path = run_data_exporter(conv_id)
        
        # 2. Запуск client_card_analyzer.py
        result_file_path = run_client_card_analyzer(json_file_path)
        
        # 3. Вывод пути к файлу с результатами анализа
        print(f"Анализ карточки клиента завершен. Результат сохранен в: {result_file_path}")
        logger.info(f"Автоматизированный анализ завершен. Результат: {result_file_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении автоматизированного анализа: {e}")
        print(f"Ошибка при выполнении автоматизированного анализа: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description="Автоматизация процесса анализа карточки клиента")
    parser.add_argument("conv_id", type=int, help="Идентификатор диалога клиента (conv_id)")
    
    args = parser.parse_args()
    
    # Запуск основной функции
    main(args.conv_id)