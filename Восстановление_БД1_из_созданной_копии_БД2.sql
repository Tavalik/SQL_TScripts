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
-- Версия от 18.05.2017
-------------------------------------------

-- НАСТРАИВАЕМЫЕ ПЕРЕМЕННЫЕ
-- База данных назначения
DECLARE @DBName_To as nvarchar(16) = 'TestBase'
-- База данных источник						
DECLARE @DBName_From as nvarchar(16) = 'WorkBase'
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
DECLARE @error as int
DECLARE @subject as NVARCHAR(100)
DECLARE @finalmassage as NVARCHAR(1000)

-------------------------------------------
-- ТЕЛО СКРИПТА
use master

-- Формируем строку для исполнения
SET @backupfile = N'' + @Path + '\\' + @DBName_From + '_' + Replace(CONVERT(nvarchar, GETDATE(), 126),':','-') + '.bak'' '
SET @SQLString = 
	N'BACKUP DATABASE [' + @DBName_From + ']
	TO DISK = N''' + @backupfile + 
	'WITH NOFORMAT, NOINIT,
	SKIP, NOREWIND, NOUNLOAD, STATS = 10, COPY_ONLY'

-- Исполнение
PRINT @SQLString
EXEC sp_executesql @SQLString
SET @error = @@error
IF @error <> 0
	BEGIN
		-- Ошбика выполнения операции
		SET @subject = 'ОШИБКА Создания резервной копии базы ' + @DBName_From
		SET @finalmassage = 'Ошибка создания резервной копии базы ' + @DBName_From + ' в каталог ' + @Path + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(@error as nvarchar(5)) + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString 
	END
ELSE
	BEGIN

		-- 1. Загружаем полученный файл резервной копии
		SET @SQLString = 
		N'RESTORE DATABASE [' + @DBName_To + ']
		FROM DISK = N''' + @backupfile + ' 
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
		EXEC sp_executesql @SQLString
		SET @error = @@error
		IF @error <> 0
			BEGIN
				-- Если были ошибки, то восстановить полную копию не удалось
				SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
				SET @finalmassage = 'Ошибка восстановления полной резервной копии для базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
					+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
					+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
			END
		ELSE 
			BEGIN

			-- 2. Переводим базу в простую модель восстановления
			SET @SQLString = 
				'ALTER DATABASE ' + @DBName_To + ' SET RECOVERY SIMPLE;'
				
			-- Выводим и выполняем полученную инструкцию
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- Ошибка перевода базы в простую модель восстановлеия
					SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
					SET @finalmassage = 'Ошибка перевода в простую модель восстановления базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
						+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
						+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
				END
			ELSE
				BEGIN

				-- 3. Запускаем сжатие базы данных
				SET @SQLString = 
					'DBCC SHRINKDATABASE(N''' + @DBName_To + ''');'
					
				-- Выводим и выполняем полученную инструкцию
				PRINT @SQLString
				EXEC sp_executesql @SQLString
				SET @error = @@error
				IF @error <> 0
					BEGIN
						-- Ошбика сжатия базы данных
						SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
						SET @finalmassage = 'Ошибка сжатия базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
							+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
							+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
					END
				ELSE
					BEGIN
						-- Успешное выполнение всех операций
						SET @subject = 'Успешное восстановление базы данных ' + @DBName_To
						SET @finalmassage = 'Успешное восстановление базы данных ' + @DBName_To + ' из резервной копии базы данных ' + @DBName_From
					END
				END
			END
		END

-- Разрешим SQL серверу команду xp_cmdshell
EXEC sp_configure 'show advanced options', 1
RECONFIGURE
EXEC sp_configure 'xp_cmdshell', 1
RECONFIGURE

-- Удалим файл резервной копии
SET @SQLString = 'master..xp_cmdshell ''del ' + @backupfile + ''
PRINT @SQLString
EXEC sp_executesql @SQLString
SET @error = @@error
IF @error <> 0
	BEGIN
		SET @subject = @subject + ', но не удалось удалить файл резервной копии'
		SET @finalmassage = @finalmassage + ', но не удалось удалить файл: ' + @backupfile + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
	END

-- Запрещаем команду
EXEC sp_configure 'xp_cmdshell', 0
RECONFIGURE

-- Если задан профиль электронной почты, отправим сообщение
IF @profile_name <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profile_name,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- Выводим сообщение о результате
SELECT
	@subject as massage

GO
