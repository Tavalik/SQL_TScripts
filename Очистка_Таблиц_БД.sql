-------------------------------------------
-- Скрипт очищает указанные таблицы в указанной базе данных
--	Алгоритм работы:
--		1. Для каждой из указанных имен таблиц:
--			1.1. Проверяется наличие текущей таблицы в базе данных
--			1.2. Если таблица найдена, то она очищается
--		3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 22.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

---------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Текущая база данных
USE WorkBase
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
DECLARE @TableName varchar(100)
DECLARE @subject as NVARCHAR(1000) = '' -- тема сообщения
DECLARE @finalmassage as NVARCHAR(4000) = '' -- текст сообщения

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Цикл по всем указанным таблицам
DECLARE TableCursor CURSOR FOR 
(
	SELECT * FROM @Table
)
OPEN TableCursor
FETCH NEXT FROM TableCursor INTO @TableName
WHILE @@FETCH_STATUS = 0
BEGIN

	-- Удаляем таблицу, если такая есть
	SET @SQLString = 'IF NOT OBJECT_ID(N''[' + @TableName + ']'',''U'') IS NULL TRUNCATE TABLE [dbo].[' + @TableName + '];'
			
	PRINT @SQLString
	BEGIN TRY
		EXEC sp_executesql @SQLString	
		SET @finalmassage = @finalmassage + 'Успешная очистка таблицы ' + @TableName + ' в базе данных ' + DB_NAME() + CHAR(13) + CHAR(13)
	END TRY
	BEGIN CATCH  
		-- Ошбика выполнения операции
		SET @subject = 'БЫЛИ ОШИБКИ при очистке таблиц в базе данных ' + DB_NAME()
		SET @finalmassage = @finalmassage + 'ОШИБКА очистки таблицы ' + @TableName + ' в базе данных ' + DB_NAME() + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL: ' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13)  
	END CATCH;
	
	-- Следующий элемент цикла
    FETCH NEXT FROM TableCursor INTO @TableName
	 
END
CLOSE TableCursor;
DEALLOCATE TableCursor;

-- Формируем сообщение об успешном или не успешном выполнении операций
IF @subject = ''
BEGIN
	-- Успешное выполнение всех операций
	SET @subject = 'Успешная очистка таблиц в базе данных ' + DB_NAME()
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
