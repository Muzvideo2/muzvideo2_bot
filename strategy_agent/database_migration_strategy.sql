-- ====================================
-- МИГРАЦИЯ БД: Добавление столбцов стратегии
-- ====================================
--
-- Этот скрипт добавляет столбцы краткосрочной и долгосрочной стратегии
-- в таблицу user_profiles для работы ИИ-агента-стратега
--

BEGIN;

-- Добавляем столбец краткосрочной стратегии
ALTER TABLE user_profiles 
ADD COLUMN IF NOT EXISTS short_term_strategy TEXT DEFAULT NULL;

-- Добавляем столбец долгосрочной стратегии  
ALTER TABLE user_profiles 
ADD COLUMN IF NOT EXISTS long_term_strategy TEXT DEFAULT NULL;

-- Добавляем столбец последнего стратегического анализа
ALTER TABLE user_profiles 
ADD COLUMN IF NOT EXISTS last_strategy_analysis TIMESTAMP WITH TIME ZONE DEFAULT NULL;

-- Добавляем столбец для хранения полного JSON результата стратегического анализа
ALTER TABLE user_profiles 
ADD COLUMN IF NOT EXISTS strategy_analysis_data JSONB DEFAULT NULL;

-- Добавляем индекс для быстрого поиска пользователей, нуждающихся в стратегическом анализе
CREATE INDEX IF NOT EXISTS idx_user_profiles_strategy_analysis 
ON user_profiles(last_strategy_analysis, conv_id);

-- Комментарии к новым столбцам
COMMENT ON COLUMN user_profiles.short_term_strategy IS 'Краткосрочная стратегия (1-2 взаимодействия) для работы с клиентом';
COMMENT ON COLUMN user_profiles.long_term_strategy IS 'Долгосрочная стратегия (LTV повышение) для работы с клиентом';  
COMMENT ON COLUMN user_profiles.last_strategy_analysis IS 'Время последнего стратегического анализа ИИ-агентом';
COMMENT ON COLUMN user_profiles.strategy_analysis_data IS 'Полные результаты стратегического анализа в JSON формате';

COMMIT;

-- Проверяем результат миграции
SELECT column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'user_profiles' 
AND column_name IN ('short_term_strategy', 'long_term_strategy', 'last_strategy_analysis', 'strategy_analysis_data')
ORDER BY column_name;