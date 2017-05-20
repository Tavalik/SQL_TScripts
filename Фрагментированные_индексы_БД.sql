-------------------------------------------
-- Показывает фрагментированные индексы для указанной базы данных
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 20.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПЕРЕМЕННЫЕ
-- База данных для анализа
USE WorkBase 

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Отбираем объекты, которые:
--	 являются индексами (index_id > 0)
--   фрагментация которых более 5% 
--   количество страниц в индексе более 128
SELECT
	OBJECT_NAME(object_id) AS TableName, 
	object_id,
    index_id,
    partition_number,
	page_count, 
	partition_number, 
	index_type_desc,
    avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')
WHERE index_id > 0 
	AND avg_fragmentation_in_percent > 5.0
	AND page_count > 128
ORDER BY avg_fragmentation_in_percent DESC

GO
