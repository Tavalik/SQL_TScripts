-------------------------------------------
-- Скрипт перемещает все файлы базы данных tempdb в указанный каталог
-- Версия от 27.10.2021
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПЕРЕМЕННЫЕ
-- Новый каталог для базы tempdb
DECLARE @Path as NVARCHAR(400) = 'G:\SQL_DATA'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ
DECLARE @physicalName NVARCHAR(500), @logicalName NVARCHAR(500)
DECLARE @SQLString NVARCHAR(400)

-------------------------------------------
-- ТЕЛО СКРИПТА
USE master;

-- Цикл по всем файлам базы данных tempdb
DECLARE fnc CURSOR LOCAL FAST_FORWARD FOR 
	(
		SELECT
			name,
			physical_name
		FROM sys.master_files 
		WHERE database_id = DB_ID('tempdb')
	)
OPEN fnc;
FETCH fnc INTO @logicalName, @physicalName;
WHILE @@FETCH_STATUS=0
	
	BEGIN
		SET @SQLString = '
		ALTER DATABASE tempdb
		MODIFY FILE (NAME = ' + @logicalName + ', FILENAME = ''' + @Path + '\' + REVERSE(SUBSTRING(REVERSE(@physicalName), 1, CHARINDEX('\', REVERSE(@physicalName))-1)) + ''');'

		PRINT @SQLString
		EXEC sp_executesql @SQLString

		FETCH fnc INTO @logicalName, @physicalName;
	END;

CLOSE fnc;
DEALLOCATE fnc;
