-------------------------------------------
-- Скрипт переводит базы данных в простую модель восстановления и запускает сжатие базы данных
--  (для экономии места на тестовых базах)
--	Алгоритм работы:
--		1. Отбираются базы данных по задаваемому условнию
--		2. Каждая из базы данных:
--			2.1 Переводится в простую модель восстановления
--			2.2 Сжимается база данных
--		3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 09.08.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

---------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Условие для выборки, '%' - все базы данных 
DECLARE @namelike varchar(100) = '%fto%'
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profilename as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ
DECLARE @SQLString NVARCHAR(4000)
DECLARE @DBName varchar(100)
DECLARE @subject as NVARCHAR(1000) = ''
DECLARE @finalmassage as NVARCHAR(4000) = ''

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Отбоерем базы для выполнения операций
DECLARE DBcursor CURSOR FOR 
(
	SELECT d.name as DatabaseName 
	FROM sys.databases d
	WHERE d.name <> 'tempdb'
		AND d.name <> 'master'
		AND d.name <> 'model'
		AND d.name <> 'msdb'
		AND d.state_desc = 'ONLINE' -- база должна быть в сети
		AND d.name like @namelike -- база должна содержать указанное слово  
)

-- Цикл по всем базам, попавшим в выборку
OPEN DBcursor
FETCH NEXT FROM DBcursor INTO @DBName
WHILE @@FETCH_STATUS = 0
BEGIN

	-- Переводим базы в простую модель восстановления
	SET @SQLString = 'ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE WITH NO_WAIT;'
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @finalmassage = @finalmassage + 'ОШИБКА перевода базы данных ' + @DBName + ' в простую модель восстановоления ' + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13)  
	END CATCH;

	-- Запускаем сжатие базы данных
	SET @SQLString = 'DBCC SHRINKDATABASE(N''' + @DBName + ''');'	
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @finalmassage = @finalmassage + 'ОШИБКА сжатия базы данных ' + @DBName + ' в простую модель восстановоления ' + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13)  
	END CATCH;	 

	-- Следующий элемент цикла
    FETCH NEXT FROM DBcursor 
    INTO @DBName
	 
END
CLOSE DBcursor;
DEALLOCATE DBcursor;

-- Формируем сообщение об успешном или не успешном выполнении операций
IF @finalmassage = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешное выполнение операций с базами данных '
	SET @finalmassage = 'Успешный перевод в простую модель восстановления и сжатие баз данных '
END
ELSE
	-- Были ошибки
	SET @subject = 'БЫЛИ ОШИБКИ при выполненит операций с базами данных '

-- Если задан профиль электронной почты, отправим сообщение
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
