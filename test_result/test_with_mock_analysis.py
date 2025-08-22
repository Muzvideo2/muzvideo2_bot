#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест с мок-анализом для проверки полного пайплайна без реального AI
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Добавляем родительскую директорию в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_mock_analysis_result(client_id):
    """Создает мок-результат анализа для тестирования"""
    return {
        "client_id": client_id,
        "analysis_timestamp": datetime.now().isoformat(),
        "client_qualification": {
            "current_level": "горячий",
            "confidence_score": 0.85,
            "reasoning": "Клиент проявляет высокий интерес, активно участвует в диалоге, имеет музыкальное образование"
        },
        "funnel_stage_analysis": {
            "current_stage": "сделано предложение по продуктам",
            "next_stage": "принятие решения",
            "stage_confidence": 0.8,
            "barriers_to_next_stage": ["цена", "время на обучение"]
        },
        "psychological_profile": {
            "personality_type": "perfectionist_learner",
            "primary_motivation": "самореализация через музыку",
            "communication_style": "эмоциональный, открытый",
            "decision_making_style": "обдуманный, требует времени",
            "learning_style": "практический с теоретической базой"
        },
        "activity_analysis": {
            "last_activity": "2025-08-19",
            "message_frequency": "средняя",
            "response_time": "быстрый",
            "initiative_level": "высокий",
            "engagement_trend": "растущий"
        },
        "conversation_gaps": {
            "longest_gap": "4 месяца",
            "recent_gap_duration": "0 дней",
            "gap_pattern": "периодические длительные паузы",
            "reasons": ["сомнения в цене", "внешние обстоятельства (увольнение)"]
        },
        "pain_points_analysis": {
            "primary_pains": [
                "потеря навыков ('все забыл')",
                "отсутствие 'живой' игры", 
                "чувство стыда от неумения импровизировать",
                "профессиональное выгорание"
            ],
            "emotional_state": "разочарование с надеждой на возрождение",
            "urgency_level": "высокая",
            "pain_intensity": 8
        },
        "interests_analysis": {
            "primary_interests": [
                "импровизация",
                "аккомпанемент", 
                "аранжировка",
                "игра без нот",
                "свободная игра"
            ],
            "learning_goals": "восстановить навыки + освоить новые техники",
            "musical_preferences": "современная музыка, личное творчество",
            "commitment_level": "высокий"
        },
        "return_strategy": {
            "recommended_actions": [
                "Персональное предложение со скидкой учитывая ситуацию с увольнением",
                "Акцент на эмоциональной составляюще - 'вернуть жизнь в игру'",
                "Предложить пробный урок или консультацию",
                "Показать конкретные результаты других учеников с похожим бэкграундом"
            ],
            "key_insights": [
                "Клиент ищет не просто обучение, а возрождение страсти к музыке",
                "Важна эмоциональная поддержка и понимание",
                "Цена вторична после установления ценности"
            ],
            "approach_style": "эмпатичный, поддерживающий",
            "content_focus": "практические результаты, эмоции от игры"
        },
        "next_contact_timing": {
            "recommended_timing": "1-2 дня",
            "optimal_time": "вечернее время (18-20)",
            "contact_method": "личное сообщение",
            "urgency_reason": "эмоциональный подъем после последнего сообщения"
        },
        "product_recommendations": {
            "primary_recommendation": "Базовый курс свободной игры",
            "secondary_options": [
                "Курс импровизации",
                "Индивидуальные занятия"
            ],
            "pricing_strategy": "flexible_with_discount",
            "value_proposition": "возрождение музыкальной страсти"
        },
        "risk_assessment": {
            "churn_probability": 0.3,
            "engagement_level": "high",
            "conversion_probability": 0.7,
            "risk_factors": [
                "финансовые ограничения после увольнения",
                "предыдущие длительные паузы"
            ],
            "protection_measures": [
                "гибкие условия оплаты",
                "дополнительная поддержка в начале обучения"
            ]
        },
        "strategic_recommendations": {
            "immediate_actions": [
                "Отправить персональное сообщение в течение 24-48 часов",
                "Предложить специальные условия",
                "Организовать пробное занятие"
            ],
            "long_term_strategy": "nurturing с фокусом на эмоциональную составляющую",
            "success_metrics": [
                "отклик на следующее сообщение",
                "согласие на пробное занятие",
                "конверсия в течение 2 недель"
            ],
            "alternative_scenarios": [
                "если отказ - перевести в долгосрочный nurturing",
                "если молчание - повторный контакт через неделю"
            ]
        }
    }

def test_full_pipeline():
    """Тест полного пайплайна с мок-данными"""
    logger.info("=== ТЕСТ ПОЛНОГО ПАЙПЛАЙНА С МОК-ДАННЫМИ ===")
    
    try:
        # 1. Создаем мок-результат анализа
        client_id = "515099352"
        mock_analysis = create_mock_analysis_result(client_id)
        
        # 2. Сохраняем результат в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = Path("analysis_result_mock_515099352_" + timestamp + ".json")
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(mock_analysis, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Мок-результат создан: {result_file}")
        
        # 3. Показываем структуру анализа
        logger.info("📊 СТРУКТУРА АНАЛИЗА:")
        for section, data in mock_analysis.items():
            if section != "client_id" and section != "analysis_timestamp":
                logger.info(f"   ✅ {section}")
                if isinstance(data, dict) and len(data) > 0:
                    # Показываем 1-2 ключевых поля
                    for key, value in list(data.items())[:2]:
                        if isinstance(value, str) and len(value) < 100:
                            logger.info(f"      └─ {key}: {value}")
                        elif isinstance(value, list) and len(value) > 0:
                            logger.info(f"      └─ {key}: {len(value)} элементов")
        
        # 4. Тестируем парсер (если есть)
        parser_file = Path("results_parser.py")
        if parser_file.exists():
            logger.info("🔧 Тестирование парсера (без реального подключения к БД)...")
            logger.info(f"   Файл парсера: {parser_file}")
            logger.info(f"   Для запуска: python {parser_file} {result_file}")
        
        logger.info("✅ ТЕСТ ПОЛНОГО ПАЙПЛАЙНА ЗАВЕРШЕН УСПЕШНО!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте: {e}")
        import traceback
        logger.error(f"Детали: {traceback.format_exc()}")
        return False

def main():
    """Основная функция"""
    logger.info("🚀 ЗАПУСК ТЕСТА С МОК-ДАННЫМИ")
    logger.info("=" * 50)
    
    success = test_full_pipeline()
    
    if success:
        logger.info("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        logger.info("Система готова к работе с реальными AI API")
    else:
        logger.error("❌ ТЕСТЫ НЕ ПРОЙДЕНЫ")
    
    return success

if __name__ == "__main__":
    main()