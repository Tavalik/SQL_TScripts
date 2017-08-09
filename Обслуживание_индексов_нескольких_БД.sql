-------------------------------------------
-- Скрипт выполняет реорганизацию либо дефрагменатцию индексов баз данных
--	Алгоритм работы:
--		1. Отбираются базы данных по задаваемому условнию
--		2. Для каждой из баз данных:
--			2.1. Собираются информация обо всех фрагментированных индексах (степерь фрагментации более 5%)
--			2.2. Если фрагментация менее или равна 30% тогда выполняется дефрагментация, иначе реиндексация индекса
--		3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 09.08.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

---------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Условие для выборки, '%' - все базы данных 
DECLARE @namelike varchar(100) = 'WorkBase%'
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profilename as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ 
DECLARE @database_id varchar(100) -- ID баз данных
DECLARE @database_name varchar(100) -- Имена баз данных
DECLARE @command nvarchar(4000); -- инструкция T-SQL для дефрагментации либо ренидексации
DECLARE @subject as NVARCHAR(1000) = '' -- тема сообщения
DECLARE @finalmassage as NVARCHAR(4000) = '' -- текст сообщения

-------------------------------------------
-- ТЕЛО СКРИПТА
USE master

-- Отключаем вывод количества возвращаемых строк, это несколько ускорит обработку
SET NOCOUNT ON;

-- Выбираем базы данных
DECLARE DBcursor CURSOR FOR 
(
	SELECT 
		database_id as database_id,
		name as database_name
	FROM sys.databases d
	WHERE 
		d.name <> 'tempdb'
		AND d.name <> 'master'
		AND d.name <> 'model'
		AND d.name <> 'msdb'
		AND d.state_desc = 'ONLINE' -- база должна быть в сети 
		AND d.name like @namelike -- база должна содержать указанное слово   
)

-- Цикл по всем базам, попавшим в выборку
OPEN DBcursor
FETCH NEXT FROM DBcursor INTO @database_id, @database_name
WHILE @@FETCH_STATUS = 0
	BEGIN

	-- База данных из цикла
	PRINT N'----------------------------------------------------------'
	PRINT N'USE [' + @database_name + N']'
	SET @command = 
	N'USE [' + @database_name + N']

	DECLARE @object_id int; -- ID объекта
	DECLARE @index_id int; -- ID индекса
	DECLARE @partition_number bigint; -- количество секций если индекс секционирован
	DECLARE @schemaname nvarchar(130); -- имя схемы в которой находится таблица
	DECLARE @objectname nvarchar(130); -- имя таблицы 
	DECLARE @indexname nvarchar(130); -- имя индекса
	DECLARE @partitionnum bigint; -- номер секции
	DECLARE @fragmentation_in_percent float; -- процент фрагментации индекса
	DECLARE @command nvarchar(4000); -- инструкция T-SQL для дефрагментации либо ренидексации

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
	FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, ''LIMITED'')
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
			WHERE object_id = @object_id AND index_id = @index_id;
        
			SELECT @partition_number = count (*)
			FROM sys.partitions
			WHERE object_id = @object_id AND index_id = @index_id;

			-- Если фрагментация менее или равна 30% тогда дефрагментация, иначе реиндексация
			IF @fragmentation_in_percent < 30.0
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REORGANIZE'';
			IF @fragmentation_in_percent >= 30.0
				SET @command = N''ALTER INDEX '' + @indexname + N'' ON '' + @schemaname + N''.'' + @objectname + N'' REBUILD'';
			IF @partition_number > 1
				SET @command = @command + N'' PARTITION='' + CAST(@partition_number AS nvarchar(10));
			
			-- Выполняем команду				
			PRINT N''    Executed: '' + @command;
			EXEC sp_executesql @command			
		
			-- Следующий элемент цикла
			FETCH NEXT FROM partitions INTO @object_id, @index_id, @partition_number, @fragmentation_in_percent;

		END;

	-- Закрытие курсора
	CLOSE partitions;
	DEALLOCATE partitions;

	-- Удаление временной таблицы
	DROP TABLE #work_to_do;';
	BEGIN TRY
		EXEC sp_executesql @command	
		SET @finalmassage = @finalmassage + 'Успешное выполнение операций обслуживания индексов для базы данных ' + @database_name + CHAR(13) + CHAR(13)
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'БЫЛИ ОШИБКИ при выполнении операций обслуживания индексов '
		SET @finalmassage = @finalmassage + 'ОШИБКА обслуживания индекса для базы данных ' + @database_name + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL: ' + CHAR(13) + @command + CHAR(13) + CHAR(13)  
	END CATCH;
	
	-- Следующая база данных
	FETCH NEXT FROM DBcursor INTO @database_id, @database_name
	END;

CLOSE DBcursor;
DEALLOCATE DBcursor;

-- Формируем сообщение об успешном или не успешном выполнении операций
IF @subject = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешное выполнение операций обслуживания индексов '
END

-- Если задан профиль электронной почты, отправим сообщение
PRINT N'----------------------------------------------------------'
IF @profilename <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profilename,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- Выводим сообщение о результате
SELECT
	@subject as subject, 
	@finalmassage as finalmassage 

GO
