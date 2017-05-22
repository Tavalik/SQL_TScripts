-------------------------------------------
-- Выполняет реорганизацию либо дефрагменатцию индексов указанной базы данных
--	Алгоритм пработы:
--		1. Собираются информация обо всех фрагментированных индексах (степерь фрагментации более 5%)
--		2. Если фрагментация менее или равна 30% тогда выполняется дефрагментация, иначе реиндексация индекса
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 20.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- База данных для анализа
USE WorkBase

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ 
DECLARE @object_id int; -- ID объекта
DECLARE @index_id int; -- ID индекса
DECLARE @partition_number bigint; -- количество секций если индекс секционирован
DECLARE @schemaname nvarchar(130); -- имя схемы в которой находится таблица
DECLARE @objectname nvarchar(130); -- имя таблицы 
DECLARE @indexname nvarchar(130); -- имя индекса
DECLARE @partitionnum bigint; -- номер секции
DECLARE @fragmentation_in_percent float; -- процент фрагментации индекса
DECLARE @command nvarchar(4000); -- инструкция T-SQL для дефрагментации либо ренидексации

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Отключаем вывод количества возвращаемых строк, это несколько ускорит обработку
SET NOCOUNT ON;

-- Удалим временные таблицы, если вдруг они есть
IF OBJECT_ID('tempdb.dbo.#work_to_do') IS NOT NULL DROP TABLE #work_to_do

-- Отбор таблиц и индексов с помощью системного представления sys.dm_db_index_physical_stats
-- Отбор только тех объектов которые:
--	 являются индексами (index_id > 0)
--   фрагментация которых более 5% 
--   количество страниц в индексе более 128 
SELECT
    object_id,
    index_id,
    partition_number,
    avg_fragmentation_in_percent AS fragmentation_in_percent
INTO #work_to_do
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')
WHERE index_id > 0 
	AND avg_fragmentation_in_percent > 5.0
	AND page_count > 128;

-- Объявление Открытие курсора курсора для чтения секций
DECLARE partitions CURSOR FOR SELECT * FROM #work_to_do;
OPEN partitions;

-- Цикл по секциям
FETCH NEXT FROM partitions INTO @object_id, @index_id, @partition_number, @fragmentation_in_percent;
WHILE @@FETCH_STATUS = 0
    BEGIN		
		
		-- Собираем имена объектов по ID		
        SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)
        FROM sys.objects AS o
			JOIN sys.schemas as s ON s.schema_id = o.schema_id
        WHERE o.object_id = @object_id;
        
		SELECT @indexname = QUOTENAME(name)
        FROM sys.indexes
        WHERE  object_id = @object_id AND index_id = @index_id;
        
		SELECT @partition_number = count (*)
        FROM sys.partitions
        WHERE object_id = @object_id AND index_id = @index_id;

		-- Если фрагментация менее или равна 30% тогда дефрагментация, иначе реиндексация
        IF @fragmentation_in_percent < 30.0
            SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REORGANIZE';
        IF @fragmentation_in_percent >= 30.0
            SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REBUILD';
        IF @partition_number > 1
            SET @command = @command + N' PARTITION=' + CAST(@partition_number AS nvarchar(10));
			
		-- Выполняем команду				
        EXEC (@command);
		PRINT N'Index: object_id=' + STR(@object_id) + ', index_id=' + STR(@index_id) + ', fragmentation_in_percent=' + STR(@fragmentation_in_percent);
        PRINT N'Executed: ' + @command;
		
		-- Следующий элемент цикла
		FETCH NEXT FROM partitions INTO @object_id, @index_id, @partition_number, @fragmentation_in_percent;

	END;

-- Закрытие курсора
CLOSE partitions;
DEALLOCATE partitions;

-- Удаление временной таблицы
DROP TABLE #work_to_do;

GO
