-------------------------------------------
-- Скрипт очищает указанные таблицы в указанной базе данных
--	Алгоритм работы:
--		1. Для каждой из указанных имен таблиц:
--			1.1. Проверяется наличие текущей таблицы в базе данных
--			1.2. Если таблица найдена, то она очищается
--		3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 09.08.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

---------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Условие для выборки, '%' - все базы данных 
DECLARE @namelike varchar(100) = 'WorkBase%'
-- Имена таблиц для очистки (необходимо добавить имена таблиц в таблицу @Table)
DECLARE @Table table(TablName varchar(100));
INSERT INTO @Table VALUES ('_Table1')
INSERT INTO @Table VALUES ('_Table2')
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profilename as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ 
DECLARE @SQLString NVARCHAR(4000)
DECLARE @DBName varchar(100)
DECLARE @TableName varchar(100)
DECLARE @subject as NVARCHAR(1000) = '' -- тема сообщения
DECLARE @finalmassage as NVARCHAR(4000) = '' -- текст сообщения

-------------------------------------------
-- ТЕЛО СКРИПТА
USE master

-- Цикл по всем базам данных
DECLARE DBcursor CURSOR FOR 
(
	SELECT d.name as DatabaseName 
	FROM sys.databases d
	WHERE 
		d.name <> 'tempdb'
		AND d.name <> 'master'
		AND d.name <> 'model'
		AND d.name <> 'msdb'
		AND d.state_desc = 'ONLINE' -- база должна быть в сети 
		AND d.name like @namelike -- база должна содержать указанное слово 
)

-- Цикл по всем указанным таблицам
DECLARE TableCursor CURSOR FOR 
(
	SELECT * FROM @Table
)

-- Цикл по всем базам, попавшим в выборку
OPEN DBcursor
FETCH NEXT FROM DBcursor INTO @DBName
WHILE @@FETCH_STATUS = 0
BEGIN

	PRINT N'----------------------------------------------------------'
	PRINT N'USE [' + @DBName + N']'

	-- Цикл по всем указанным таблицам
	OPEN TableCursor
	FETCH NEXT FROM TableCursor INTO @TableName
	WHILE @@FETCH_STATUS = 0
	BEGIN

		-- Удаляем таблицу, если такая есть
		SET @SQLString = 'USE [' + @DBName + '] IF NOT OBJECT_ID(N''[' + @TableName + ']'',''U'') IS NULL TRUNCATE TABLE [dbo].[' + @TableName + '];'
	
		-- Выполняем инструкцию		
		PRINT @SQLString
		BEGIN TRY
			EXEC sp_executesql @SQLString	
			SET @finalmassage = @finalmassage + 'Успешная очистка таблицы ' + @TableName + ' в базе данных ' + @DBName + CHAR(13) + CHAR(13)
		END TRY
		BEGIN CATCH  
			-- Ошбика выполнения операции
			SET @subject = 'БЫЛИ ОШИБКИ при очистке таблиц '
			SET @finalmassage = @finalmassage + 'ОШИБКА очистки таблицы ' + @TableName + ' в базе данных ' + @DBName + CHAR(13) + CHAR(13)
				+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
				+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
				+ 'Текст T-SQL: ' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13)  
		END CATCH;
	
		-- Следующий элемент цикла по таблицам
		FETCH NEXT FROM TableCursor INTO @TableName
	 
	END
	CLOSE TableCursor;

	-- Следующий элемент цикла по базам данных
	FETCH NEXT FROM DBcursor INTO @DBName
	 
END
DEALLOCATE TableCursor;
CLOSE DBcursor;
DEALLOCATE DBcursor;

-- Формируем сообщение об успешном или не успешном выполнении операций
IF @subject = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешная очистка таблиц '
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
