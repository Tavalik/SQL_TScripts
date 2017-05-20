-------------------------------------------
-- Скрипт переводит указанную базу данных в простую модель восстановления и запускает сжатие базы данных
--  (для экономии места на тестовых базах)
--	Алгоритм работы:
--		1. Переоводит базу данных в простую модель восстановления
--		2. Сжимает базу данных
--		3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 20.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Имя базы данных 
DECLARE @DBName varchar(100) = 'TestBase'
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profilename as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ
DECLARE @SQLString NVARCHAR(4000)
DECLARE @subject as NVARCHAR(1000) = ''
DECLARE @finalmassage as NVARCHAR(4000) = ''

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Переводим базы в простую модель восстановления
SET @SQLString = 'ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE WITH NO_WAIT;'	
PRINT @SQLString
BEGIN TRY 
	EXEC sp_executesql @SQLString
END TRY
BEGIN CATCH  
	-- Ошбика выполнения операции
	SET @subject = 'ОШИБКА перевода базы данных ' + @DBName + ' в простую модель восстановоления '
	SET @finalmassage = 'ОШИБКА перевода базы данных ' + @DBName + ' в простую модель восстановоления ' + CHAR(13) + CHAR(13)
		+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
		+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
		+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
END CATCH;

-- Запускаем сжатие базы данных
IF @subject = ''
BEGIN	
	SET @SQLString = 'DBCC SHRINKDATABASE(N''' + @DBName + ''');'	
	PRINT @SQLString
	BEGIN TRY 
		EXEC sp_executesql @SQLString
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'ОШИБКА сжатия базы данных ' + @DBName
		SET @finalmassage = 'ОШИБКА сжатия базы данных ' + @DBName + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString  
	END CATCH;
END

-- Формируем сообщение об успешном выполнении операций
IF @subject = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешный перевод в простую модель восстановления и сжатие базы данных ' + @DBName
	SET @finalmassage = 'Успешный перевод в простую модель восстановления и сжатие базы данных ' + @DBName
END

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
	

