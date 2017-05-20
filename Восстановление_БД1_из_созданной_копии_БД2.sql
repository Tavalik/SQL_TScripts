-------------------------------------------
-- Скрипт восстанавливет одну базу данных из резервной копии другой базы на текущий момент времени ("перезаливает" базу). 
--	Алгоритм пработы:
--		1. Создается резервная копия базы-источнка с флагом "Только резервное копирование"
--		2. Восстановливается база-назначения из полученой резервной копии
--		3. База-назначения переводится в простую модель восстановления
--		4. База-назначения сжимается
--		5. Файл резервной копии удаляется
--		6. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 20.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПЕРЕМЕННЫЕ
-- База данных назначения
DECLARE @DBName_To as nvarchar(40) = 'TestBase'
-- База данных источник						
DECLARE @DBName_From as nvarchar(40) = 'WorkBase'
-- Каталог для резервной копии
DECLARE @Path as nvarchar(400) = 'E:\Backup_SQL'
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profile_name as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ	
DECLARE @SQLString NVARCHAR(4000)
DECLARE @backupfile NVARCHAR(500)
DECLARE @physicalName NVARCHAR(500), @logicalName NVARCHAR(500)
DECLARE @out as int = 0
DECLARE @subject as NVARCHAR(100) = ''
DECLARE @finalmassage as NVARCHAR(1000) = ''

-------------------------------------------
-- ТЕЛО СКРИПТА
use master

-- 1. Создаем резервную копию с флагом "Только резервное копирование"
-- Формируем строку для исполнения
SET @backupfile = @Path + '\\' + @DBName_From + '_' + Replace(CONVERT(nvarchar, GETDATE(), 126),':','-') + '.bak'
SET @SQLString = 
	N'BACKUP DATABASE [' + @DBName_From + ']
	TO DISK = N''' + @backupfile + '''  
	WITH NOFORMAT, NOINIT,
	SKIP, NOREWIND, NOUNLOAD, STATS = 10, COPY_ONLY'

-- Выводим и выполняем полученную инструкцию
PRINT @SQLString
BEGIN TRY 
	EXEC sp_executesql @SQLString
END TRY
BEGIN CATCH  
	-- Ошбика выполнения операции
	SET @subject = 'ОШИБКА Создания резервной копии базы ' + @DBName_From
	SET @finalmassage = 'Ошибка создания резервной копии базы ' + @DBName_From + ' в каталог ' + @Path + CHAR(13) + CHAR(13)
		+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
		+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
		+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
END CATCH;

-- 2. Загружаем полученный файл резервной копии
IF @subject = ''
BEGIN
	
	-- Формируем строку для исполнения	
	SET @SQLString = 
	N'RESTORE DATABASE [' + @DBName_To + ']
	FROM DISK = N''' + @backupfile + '''   
	WITH  
	FILE = 1,'

	-- Переименуем файлы базы данных на исходные
	-- Новый цикл по всем файлам базы данных
	DECLARE fnc CURSOR LOCAL FAST_FORWARD FOR 
	(
		SELECT
			t_From.name,
			t_To.physical_name
		FROM sys.master_files as t_To 
			join sys.master_files as t_From 
			on t_To.file_id = t_From.file_id
		WHERE t_To.database_id = DB_ID(@DBName_To) 
			and t_From.database_id = DB_ID(@DBName_From)
	)
	OPEN fnc;
	FETCH fnc INTO @logicalName, @physicalName;
	WHILE @@FETCH_STATUS=0
		BEGIN
			SET @SQLString = @SQLString + '
			MOVE N''' + @logicalName + ''' TO N''' + @physicalName + ''','
			FETCH fnc INTO @logicalName, @physicalName;
		END;
	CLOSE fnc;
	DEALLOCATE fnc;

	SET @SQLString = @SQLString + '
	RECOVERY,
	REPLACE,
	STATS = 5'

	-- Выводим и выполняем полученную инструкцию
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
			SET @finalmassage = 'Ошибка восстановления полной резервной копии для базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
	END CATCH;
END

-- 3. Переводим базу в простую модель восстановления
IF @subject = '2'
BEGIN	
	
	-- Формируем строку для исполнения
	SET @SQLString = 'ALTER DATABASE ' + @DBName_To + ' SET RECOVERY SIMPLE;'
	
	-- Выводим и выполняем полученную инструкцию
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
		SET @finalmassage = 'Ошибка перевода в простую модель восстановления базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
	END CATCH;
END

-- 4. Запускаем сжатие базы данных
IF @subject = '2'
BEGIN

	-- Формируем строку для исполнения
	SET @SQLString = 'DBCC SHRINKDATABASE(N''' + @DBName_To + ''');'
					
	-- Выводим и выполняем полученную инструкцию
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
		SET @finalmassage = 'Ошибка сжатия базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
	END CATCH;
END	

-- 5. Если файл был создан, удалим файл резервной копии
BEGIN TRY
	EXEC master.dbo.xp_fileexist @backupfile, @out out
	IF @out = 1 EXEC master.dbo.xp_delete_file 0, @backupfile
END TRY
BEGIN CATCH  
	-- Ошбика выполнения операции
	SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
	SET @finalmassage = 'Ошибка удаления файла резервной копии ' + @backupfile + CHAR(13) + CHAR(13)
		+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
		+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
		+ 'Текст T-SQL:' + CHAR(13) + 'master.dbo.xp_delete_file 0, ' + @backupfile  
END CATCH;
	
-- Если ошибок не было, сформируем текст сообщения
IF @subject = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешное восстановление базы данных ' + @DBName_To
	SET @finalmassage = 'Успешное восстановление базы данных ' + @DBName_To + ' из резервной копии базы данных ' + @DBName_From
END

-- 6. Если задан профиль электронной почты, отправим сообщение
IF @profile_name <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profile_name,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- Выводим сообщение о результате
SELECT
	@subject as subject,
	@finalmassage as finalmassage

GO
