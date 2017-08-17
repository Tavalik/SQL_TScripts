-------------------------------------------
-- Скрипт восстанавливет одну базу данных из резервной копии другой базы на указанную дату ("перезаливает" базу). 
--	Алгоритм работы:
--		1. Подбирается последовательная цепочка файлов резервной копии базы-источника на указанную дату
--		2. Восстановливается база-назначения из полученой последовательности файлов
--		3. База-назначения переводится в оперативный режим
--		4. База-назначения переводится в простую модель восстановления
--		5. База-назначения сжимается
--		6. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 17.08.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПЕРЕМЕННЫЕ
-- База данных назначения
DECLARE @DBName_To as nvarchar(40) = 'TestBase'
-- База данных источник						
DECLARE @DBName_From as nvarchar(40) = 'WorkBase'
-- Дата, на котороую собирается цепочка файлов резервных копий, в формате '20160315 12:00:00'							
DECLARE @BackupTime as datetime = GETDATE()
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

-- Удалим временные таблицы, если вдруг они есть
IF OBJECT_ID('tempdb.dbo.#BackupFiles') IS NOT NULL DROP TABLE #BackupFiles
IF OBJECT_ID('tempdb.dbo.#FullBackup') IS NOT NULL DROP TABLE #FullBackup
IF OBJECT_ID('tempdb.dbo.#DiffBackup') IS NOT NULL DROP TABLE #DiffBackup
IF OBJECT_ID('tempdb.dbo.#LogBackup') IS NOT NULL DROP TABLE #LogBackup
IF OBJECT_ID('tempdb.dbo.#BackupFilesFinal') IS NOT NULL DROP TABLE #BackupFilesFinal

-- Соберем данные о всех сдаланных раннее бэкапах
SELECT
	backupset.backup_start_date,
	backupset.backup_set_uuid,
	backupset.differential_base_guid,
	backupset.[type] as btype,
	backupmediafamily.physical_device_name
INTO #BackupFiles	
FROM msdb.dbo.backupset AS backupset
    INNER JOIN msdb.dbo.backupmediafamily AS backupmediafamily 
	ON backupset.media_set_id = backupmediafamily.media_set_id
WHERE backupset.database_name = @DBName_From 
	and backupset.backup_start_date < @BackupTime
	and backupset.is_copy_only = 0 -- флаг "Только резервное копирование"
	and backupset.is_snapshot = 0 -- флаг "Не snapshot"
	and (backupset.description is null or backupset.description not like 'Image-level backup') -- Защита от Veeam Backup & Replication
	and device_type <> 7
ORDER BY 
	backupset.backup_start_date DESC

-- Найдем последний полный бэкап
SELECT TOP 1
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name,
	BackupFiles.backup_set_uuid	
INTO #FullBackup	 
FROM #BackupFiles AS BackupFiles
WHERE btype = 'D'
ORDER BY backup_start_date DESC

-- Найдем последний разностный бэкап
SELECT TOP 1
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name
INTO #DiffBackup	 
FROM #BackupFiles AS BackupFiles
	INNER JOIN #FullBackup AS FullBackup
	ON BackupFiles.differential_base_guid = FullBackup.backup_set_uuid
WHERE BackupFiles.btype = 'I'
ORDER BY BackupFiles.backup_start_date DESC

-- Соберем бэкапы журналов транзакций
SELECT
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name
INTO #LogBackup	
FROM #BackupFiles AS BackupFiles
	INNER JOIN
	(
		SELECT MAX(table_backup_start_date.backup_start_date) AS backup_start_date
		FROM 
		(
			SELECT backup_start_date
			FROM #FullBackup
			UNION ALL
			SELECT backup_start_date
			FROM #DiffBackup
		) AS table_backup_start_date
	) AS table_lsn
	ON BackupFiles.backup_start_date > table_lsn.backup_start_date
WHERE BackupFiles.btype = 'L'

-- Инициируем цикл по объединению всех трех таблиц
SELECT physical_device_name
INTO #BackupFilesFinal
FROM 
(
	SELECT
		backup_start_date,
		physical_device_name
	FROM #FullBackup
	UNION ALL
	SELECT
		backup_start_date,
		physical_device_name
	FROM #DiffBackup
	UNION ALL
	SELECT
		backup_start_date,
		physical_device_name
	FROM #LogBackup
) AS T 
ORDER BY backup_start_date

-- Соберем файлы в цикл
DECLARE bkf CURSOR LOCAL FAST_FORWARD FOR 
(
	SELECT physical_device_name
	FROM #BackupFilesFinal
);

-- Начало цикла
OPEN bkf;

-- Прочитаем первый элемент цикла, им может быть только полная резервная копия
FETCH bkf INTO @backupfile;
IF @@FETCH_STATUS<>0
	-- Если получить элемент не удалось, то полная резерная копия не найдена
	BEGIN
		SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
		SET @finalmassage = 'Не найдена полная резервная копия для базы данных ' + @DBName_From
	END
ELSE
	BEGIN

	--Далее загружаем все файлы резервных копий в 3 этапа:

	-- 1. Загружаем полный бэкап
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
	NORECOVERY,
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

		-- 2. Загружаем разностный бэкап и журналы транзакций
		FETCH bkf INTO @backupfile;
		WHILE @@FETCH_STATUS=0
			BEGIN
			set @SQLString = 
			N'RESTORE DATABASE ' + @DBName_To + '
			FROM DISK = ''' + @backupfile + '''
			WITH
			FILE = 1,
			NORECOVERY,
			STATS = 5'
			
			-- Выводим и выполняем полученную инструкцию
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- Если ошибка, прерываем цикл
				BREAK
				END
			ELSE
				BEGIN
					-- Ошибок нет, обрабатываем следующий элемент цикла
					FETCH bkf INTO @backupfile;
				END
			END;
		
		IF @error <> 0
			BEGIN
				-- Во время цикла, была ошибка
				SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
				SET @finalmassage = 'Ошибка восстановления резервной копии для базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
					+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
					+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
			END
		ELSE
			BEGIN

			-- 3. Переводим базу в оперативный режим
			SET @SQLString = 
			N'RESTORE DATABASE ' + @DBName_To + '
			WITH RECOVERY'
			
			-- Выводим и выполняем полученную инструкцию
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- Ошибка перевода базы в оперативный режим
					SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName_To
					SET @finalmassage = 'Ошибка перевода в оперативный режим базы данных ' + @DBName_To + CHAR(13) + CHAR(13)
						+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
						+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
				END
			ELSE	
				BEGIN

				-- Переводим базу в простую модель восстановления
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

					-- Запускаем сжатие базы данных
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
							SET @finalmassage = 'Успешное восстановление базы данных ' + @DBName_To + ' из резервной копии базы данных ' + @DBName_From + ' на момент времени ' + Replace(CONVERT(nvarchar, @BackupTime, 126),':','-')
						END
					END
				END
			END
		END			
	END

-- Завершаем цикл
CLOSE bkf;
DEALLOCATE bkf;

-- Удаляем временные таблицы
drop table #BackupFiles
drop table #FullBackup
drop table #DiffBackup
drop table #LogBackup
drop table #BackupFilesFinal

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
