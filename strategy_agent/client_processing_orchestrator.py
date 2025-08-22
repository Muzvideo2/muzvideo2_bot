#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единый скрипт-оркестратор для каскадной обработки клиентов
Интегрирует: data_exporter -> client_card_analyzer -> simple_results_processor
"""

import os
import sys
import json
import argparse
import logging
import signal
import psycopg2
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path
from tqdm import tqdm
import glob

# Импорты рабочих компонентов
import data_exporter
from client_card_analyzer import ClientCardAnalyzer
import simple_results_processor
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("orchestrator.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Флаг для graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Обработчик сигнала для graceful shutdown"""
    global shutdown_requested
    logger.info("Получен сигнал завершения. Завершаем текущую операцию...")
    print("\n[CTRL+C] Завершаем обработку после текущего клиента...")
    shutdown_requested = True

# Устанавливаем обработчик сигналов
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class ClientProcessingOrchestrator:
    """Оркестратор для каскадной обработки клиентов"""
    
    def __init__(self):
        """Инициализация оркестратора"""
        self.analyzer = None
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
    def _init_analyzer(self):
        """Ленивая инициализация анализатора"""
        if self.analyzer is None:
            try:
                logger.info("Инициализируем AI анализатор...")
                self.analyzer = ClientCardAnalyzer()
                logger.info("AI анализатор успешно инициализирован")
            except Exception as e:
                logger.error(f"Ошибка инициализации анализатора: {e}")
                raise
    
    def _get_db_connection(self):
        """Получение соединения с БД"""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL не установлен")
        return psycopg2.connect(database_url)
    
    def _find_latest_analysis_file(self, conv_id: int) -> Optional[str]:
        """Поиск последнего файла результатов анализа для клиента"""
        pattern = f"analysis_results/*{conv_id}*.json"
        files = glob.glob(pattern)
        
        if not files:
            # Пробуем альтернативные паттерны
            pattern = f"analysis_results/strategic_analysis_{conv_id}_*.json"
            files = glob.glob(pattern)
        
        if files:
            # Берем самый новый файл
            latest_file = max(files, key=os.path.getctime)
            logger.info(f"Найден файл результатов анализа: {latest_file}")
            return latest_file
        
        logger.warning(f"Файл результатов анализа для клиента {conv_id} не найден")
        return None
    
    def process_single_client(self, conv_id: int) -> Dict[str, Any]:
        """
        Обработка одного клиента через всю цепочку
        
        Args:
            conv_id: ID клиента
            
        Returns:
            dict: Результат обработки
        """
        logger.info(f"=== Начинаем обработку клиента {conv_id} ===")
        
        result = {
            'conv_id': conv_id,
            'success': False,
            'steps_completed': [],
            'errors': [],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Шаг 1: Экспорт данных клиента
            logger.info(f"Шаг 1: Экспорт данных клиента {conv_id}")
            try:
                exported_file = data_exporter.main(conv_id)
                result['steps_completed'].append('export')
                result['exported_file'] = exported_file
                logger.info(f"[OK] Данные экспортированы в: {exported_file}")
            except Exception as e:
                error_msg = f"Ошибка экспорта данных: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            # Шаг 2: AI анализ карточки клиента
            logger.info(f"Шаг 2: AI анализ клиента {conv_id}")
            try:
                self._init_analyzer()
                # Передаем экспортированный файл напрямую в анализатор
                analysis_result = self.analyzer.analyze_client(conv_id, save_to_file=True, exported_data_file=exported_file)
                result['steps_completed'].append('analysis')
                result['analysis_result'] = analysis_result
                logger.info(f"[OK] AI анализ завершен успешно")
            except Exception as e:
                error_msg = f"Ошибка AI анализа: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            # Шаг 3: Поиск файла результатов анализа
            logger.info(f"Шаг 3: Поиск файла результатов анализа")
            analysis_file = self._find_latest_analysis_file(conv_id)
            if not analysis_file:
                error_msg = "Файл результатов анализа не найден"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            result['analysis_file'] = analysis_file
            
            # Шаг 4: Обработка результатов (обновление БД и создание напоминаний)
            logger.info(f"Шаг 4: Обработка результатов анализа")
            try:
                success = simple_results_processor.process_analysis_results(analysis_file, conv_id)
                if success:
                    result['steps_completed'].append('processing')
                    logger.info(f"[OK] Результаты успешно обработаны")
                else:
                    error_msg = "Ошибка обработки результатов"
                    logger.error(error_msg)
                    result['errors'].append(error_msg)
                    return result
            except Exception as e:
                error_msg = f"Ошибка обработки результатов: {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                return result
            
            # Успешное завершение
            result['success'] = True
            logger.info(f"[OK] Клиент {conv_id} успешно обработан")
            return result
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка при обработке клиента {conv_id}: {e}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
            return result
    
    def process_multiple_clients(self, client_list: List[int]) -> Dict[str, Any]:
        """
        Массовая обработка клиентов с прогресс-баром
        
        Args:
            client_list: Список ID клиентов
            
        Returns:
            dict: Результат массовой обработки
        """
        global shutdown_requested
        
        logger.info(f"=== Начинаем массовую обработку {len(client_list)} клиентов ===")
        
        results = {
            'total_clients': len(client_list),
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'client_results': [],
            'start_time': datetime.now(timezone.utc).isoformat(),
            'end_time': None
        }
        
        # Прогресс-бар
        with tqdm(total=len(client_list), desc="Обработка клиентов") as pbar:
            for i, conv_id in enumerate(client_list):
                if shutdown_requested:
                    logger.info("Получен запрос на завершение. Останавливаем массовую обработку.")
                    break
                
                pbar.set_description(f"Обрабатываем клиента {conv_id}")
                
                try:
                    client_result = self.process_single_client(conv_id)
                    results['client_results'].append(client_result)
                    results['processed'] += 1
                    
                    if client_result['success']:
                        results['successful'] += 1
                        self.stats['successful'] += 1
                    else:
                        results['failed'] += 1
                        self.stats['failed'] += 1
                        self.stats['errors'].extend(client_result['errors'])
                    
                    self.stats['processed'] += 1
                    
                except Exception as e:
                    error_msg = f"Критическая ошибка при обработке клиента {conv_id}: {e}"
                    logger.error(error_msg)
                    
                    error_result = {
                        'conv_id': conv_id,
                        'success': False,
                        'errors': [error_msg],
                        'steps_completed': []
                    }
                    
                    results['client_results'].append(error_result)
                    results['processed'] += 1
                    results['failed'] += 1
                    self.stats['failed'] += 1
                    self.stats['errors'].append(error_msg)
                
                # Обновляем прогресс-бар
                pbar.update(1)
                pbar.set_postfix({
                    'Успешно': results['successful'],
                    'Ошибки': results['failed']
                })
        
        results['end_time'] = datetime.now(timezone.utc).isoformat()
        
        # Выводим итоговую статистику
        self._print_processing_summary(results)
        
        return results
    
    def _print_processing_summary(self, results: Dict[str, Any]):
        """Вывод итоговой статистики обработки"""
        print(f"\n{'='*50}")
        print(f"ИТОГИ МАССОВОЙ ОБРАБОТКИ")
        print(f"{'='*50}")
        print(f"Всего клиентов:     {results['total_clients']}")
        print(f"Обработано:         {results['processed']}")
        print(f"Успешно:           {results['successful']}")
        print(f"С ошибками:        {results['failed']}")
        
        if results['failed'] > 0:
            print(f"\nОШИБКИ:")
            for result in results['client_results']:
                if not result['success'] and result['errors']:
                    print(f"  Клиент {result['conv_id']}: {'; '.join(result['errors'])}")
        
        print(f"{'='*50}")
    
    def get_clients_by_criteria(self, criteria: Dict[str, Any]) -> List[int]:
        """
        Конфигуратор SQL-запросов для выборки клиентов из БД
        
        Args:
            criteria: Словарь критериев для выборки
            
        Returns:
            list: Список conv_id клиентов
        """
        logger.info(f"Выборка клиентов по критериям: {criteria}")
        
        # Базовый запрос
        base_query = "SELECT DISTINCT conv_id FROM user_profiles WHERE 1=1"
        params = []
        conditions = []
        
        # Критерий: квалификация лида
        if 'lead_qualification' in criteria:
            qual = criteria['lead_qualification']
            if qual:
                conditions.append("lead_qualification @> %s")
                params.append([qual])
        
        # Критерий: этап воронки
        if 'funnel_stage' in criteria:
            stage = criteria['funnel_stage']
            if stage:
                conditions.append("funnel_stage = %s")
                params.append(stage)
        
        # Критерий: уровень клиента
        if 'client_level' in criteria:
            level = criteria['client_level']
            if level:
                conditions.append("client_level @> %s")
                params.append([level])
        
        # Критерий: есть покупки
        if criteria.get('has_purchases'):
            conditions.append("""
                conv_id IN (SELECT DISTINCT conv_id FROM purchased_products)
            """)
        
        # Критерий: нет покупок
        if criteria.get('no_purchases'):
            conditions.append("""
                conv_id NOT IN (SELECT DISTINCT conv_id FROM purchased_products)
            """)
        
        # Критерий: последняя активность
        if 'last_activity_days' in criteria:
            days = criteria['last_activity_days']
            if days:
                conditions.append("""
                    conv_id IN (
                        SELECT conv_id FROM dialogues 
                        WHERE created_at > NOW() - INTERVAL '%s days'
                    )
                """)
                params.append(days)
        
        # Критерий: нет анализа
        if criteria.get('no_recent_analysis'):
            conditions.append("""
                (last_analysis_at IS NULL OR last_analysis_at < NOW() - INTERVAL '7 days')
            """)
        
        # Критерий: есть активные напоминания
        if criteria.get('has_active_reminders'):
            conditions.append("""
                conv_id IN (SELECT DISTINCT conv_id FROM reminders WHERE status = 'active')
            """)
        
        # Критерий: нет активных напоминаний
        if criteria.get('no_active_reminders'):
            conditions.append("""
                conv_id NOT IN (SELECT DISTINCT conv_id FROM reminders WHERE status = 'active')
            """)
        
        # Критерий: лимит результатов
        limit_clause = ""
        if 'limit' in criteria and criteria['limit']:
            limit_clause = f" LIMIT {int(criteria['limit'])}"
        
        # Составляем финальный запрос
        if conditions:
            query = f"{base_query} AND {' AND '.join(conditions)} ORDER BY conv_id{limit_clause}"
        else:
            query = f"{base_query} ORDER BY conv_id{limit_clause}"
        
        logger.info(f"SQL запрос: {query}")
        logger.info(f"Параметры: {params}")
        
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                results = [row[0] for row in cur.fetchall()]
            
            conn.close()
            logger.info(f"Найдено {len(results)} клиентов по критериям")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка выборки клиентов: {e}")
            raise

def create_cli_parser():
    """Создание парсера командной строки"""
    parser = argparse.ArgumentParser(
        description='Оркестратор для каскадной обработки клиентов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Обработка одного клиента:
   python client_processing_orchestrator.py single 181601225

2. Обработка списка клиентов:
   python client_processing_orchestrator.py multiple 181601225 515099352 123456789

3. Обработка "горячих" клиентов:
   python client_processing_orchestrator.py criteria --lead_qualification горячий --limit 10

4. Обработка клиентов без недавнего анализа:
   python client_processing_orchestrator.py criteria --no_recent_analysis --limit 20

5. Обработка клиентов с покупками без активных напоминаний:
   python client_processing_orchestrator.py criteria --has_purchases --no_active_reminders
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Команды')
    
    # Команда: single
    single_parser = subparsers.add_parser('single', help='Обработка одного клиента')
    single_parser.add_argument('conv_id', type=int, help='ID клиента (conv_id)')
    
    # Команда: multiple
    multiple_parser = subparsers.add_parser('multiple', help='Обработка списка клиентов')
    multiple_parser.add_argument('conv_ids', nargs='+', type=int, help='Список ID клиентов')
    
    # Команда: criteria
    criteria_parser = subparsers.add_parser('criteria', help='Обработка клиентов по критериям')
    criteria_parser.add_argument('--lead_qualification', choices=['холодный', 'тёплый', 'горячий', 'клиент'],
                               help='Квалификация лида')
    criteria_parser.add_argument('--funnel_stage', help='Этап воронки')
    criteria_parser.add_argument('--client_level', help='Уровень клиента')
    criteria_parser.add_argument('--has_purchases', action='store_true', help='Клиенты с покупками')
    criteria_parser.add_argument('--no_purchases', action='store_true', help='Клиенты без покупок')
    criteria_parser.add_argument('--last_activity_days', type=int, help='Активность за последние N дней')
    criteria_parser.add_argument('--no_recent_analysis', action='store_true', help='Нет анализа за последние 7 дней')
    criteria_parser.add_argument('--has_active_reminders', action='store_true', help='Есть активные напоминания')
    criteria_parser.add_argument('--no_active_reminders', action='store_true', help='Нет активных напоминаний')
    criteria_parser.add_argument('--limit', type=int, help='Лимит количества клиентов')
    
    return parser

def main():
    """Основная функция CLI интерфейса"""
    parser = create_cli_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Создаем оркестратор
    orchestrator = ClientProcessingOrchestrator()
    
    try:
        if args.command == 'single':
            # Обработка одного клиента
            logger.info(f"Запуск обработки одного клиента: {args.conv_id}")
            result = orchestrator.process_single_client(args.conv_id)
            
            print(f"\n{'='*50}")
            print(f"РЕЗУЛЬТАТ ОБРАБОТКИ КЛИЕНТА {args.conv_id}")
            print(f"{'='*50}")
            print(f"Успех: {'ДА' if result['success'] else 'НЕТ'}")
            print(f"Выполненные шаги: {', '.join(result['steps_completed'])}")
            
            if result['errors']:
                print(f"Ошибки: {'; '.join(result['errors'])}")
            
            return 0 if result['success'] else 1
            
        elif args.command == 'multiple':
            # Обработка списка клиентов
            logger.info(f"Запуск массовой обработки клиентов: {args.conv_ids}")
            results = orchestrator.process_multiple_clients(args.conv_ids)
            
            return 0 if results['failed'] == 0 else 1
            
        elif args.command == 'criteria':
            # Обработка по критериям
            criteria = {}
            if args.lead_qualification:
                criteria['lead_qualification'] = args.lead_qualification
            if args.funnel_stage:
                criteria['funnel_stage'] = args.funnel_stage
            if args.client_level:
                criteria['client_level'] = args.client_level
            if args.has_purchases:
                criteria['has_purchases'] = True
            if args.no_purchases:
                criteria['no_purchases'] = True
            if args.last_activity_days:
                criteria['last_activity_days'] = args.last_activity_days
            if args.no_recent_analysis:
                criteria['no_recent_analysis'] = True
            if args.has_active_reminders:
                criteria['has_active_reminders'] = True
            if args.no_active_reminders:
                criteria['no_active_reminders'] = True
            if args.limit:
                criteria['limit'] = args.limit
            
            if not criteria:
                print("Ошибка: Необходимо указать хотя бы один критерий для выборки")
                return 1
            
            logger.info(f"Запуск обработки по критериям: {criteria}")
            
            # Выбираем клиентов
            client_list = orchestrator.get_clients_by_criteria(criteria)
            
            if not client_list:
                print("По указанным критериям клиенты не найдены")
                return 0
            
            print(f"Найдено {len(client_list)} клиентов для обработки")
            print(f"Клиенты: {client_list}")
            
            # Подтверждение
            if len(client_list) > 5:
                confirm = input(f"\nОбработать {len(client_list)} клиентов? (да/нет): ")
                if confirm.lower() not in ['да', 'yes', 'y']:
                    print("Обработка отменена пользователем")
                    return 0
            
            # Обрабатываем
            results = orchestrator.process_multiple_clients(client_list)
            
            return 0 if results['failed'] == 0 else 1
    
    except KeyboardInterrupt:
        print("\nОбработка прервана пользователем")
        return 130
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())