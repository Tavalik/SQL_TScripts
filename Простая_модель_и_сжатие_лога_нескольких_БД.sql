-------------------------------------------
-- Скрипт переводит базы данных в простую модель восстановления и запускает сжатие файла журнала транзакций 
--  (для экономии места на тестовых базах)
--     Алгоритм работы:
--           1. Отбираются базы данных по задаваемому условнию
--           2. Каждая из базы данных:
--                  2.1 Переводится в простую модель восстановления
--                  2.2 Сжимается файл журнала транзакций
--           3. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
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
DECLARE @DBLogName varchar(100)
DECLARE @subject as NVARCHAR(1000) = ''
DECLARE @finalmassage as NVARCHAR(4000) = ''

-------------------------------------------
-- ТЕЛО СКРИПТА

-- Отберем базы для выполнения операций
DECLARE DBcursor CURSOR FOR 
(
       SELECT
             t_Name.name as DatabaseName,
             t_LogName.name as DatabaseLogName
       FROM sys.databases as t_Name
             Inner join sys.master_files as t_LogName
                    on t_Name.database_id = t_LogName.database_id         
       WHERE t_Name.database_id > 4
             AND t_Name.state_desc = 'ONLINE' -- база должна быть в сети
             AND t_Name like @namelike -- база должна содержать указанное слово  
             AND t_LogName.type = 1
)

-- Цикл по всем базам, попавшим в выборку
OPEN DBcursor
FETCH NEXT FROM DBcursor INTO @DBName, @DBLogName 
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
       SET @SQLString = 'USE [' + @DBName + '];  DBCC SHRINKFILE(' + @DBLogName + ', 0, truncateonly);'     
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
    INTO @DBName, @DBLogName
       
END
CLOSE DBcursor;
DEALLOCATE DBcursor;

-- Формируем сообщение об успешном или не успешном выполнении операций
IF @finalmassage = ''
BEGIN
       -- Успешное выполнение всех операций
       SET @subject = 'Успешное выполнение операций с базами данных '
       SET @finalmassage = 'Успешный перевод в простую модель восстановления и сжатие файла журнала транзакций'
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
