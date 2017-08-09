-------------------------------------------
-- Скрипт создает резвервные копии баз данных
--	Алгоритм работы:
--		1. Отбираются базы данных по задаваемому условнию
--			1.1 В указанном каталоге создается подкаталог с именем базы данных
--			1.2 Создаются резервные копии определенного типа каждой из баз данных в созданном подкаталоге
--		2. Отправляется электронное сообщение о результате работы с использованием настроенного почтового профиля
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 09.08.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
-- Условие для выборки, '%' - все базы данных 
DECLARE @namelike varchar(100) = 'Work%'
-- Каталог для резервной копии
DECLARE @Path as nvarchar(400) = 'E:\Backup_SQL'
-- Тип резервного копирования:
--		0 - Полная резервная копия с флагом "Только резервное копирование"
--		1 - Полная резервная копия
--		2 - Разностная резервная копия
--		3 - Копия журнала транзакций
DECLARE @Type as int = 0
-- Сжимать резервные копии:
--		0 - Не сжимать или по умолчанию
--		1 - Сжимать
DECLARE @Compression as int = 0
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profilename as nvarchar(100) = 'ОсновнойПрофиль'
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ
DECLARE @SQLString NVARCHAR(4000)
DECLARE @DBName varchar(100)
DECLARE @subdir NVARCHAR(400) = ''
DECLARE @subject as NVARCHAR(100) = ''
DECLARE @finalmassage as NVARCHAR(1000) = ''

-------------------------------------------
-- ТЕЛО СКРИПТА
use master

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

	-- Создаем вложенный каталог с именем базы
	SET @subdir = @Path + '\\' + @DBName
	BEGIN TRY 
		EXEC master.dbo.xp_create_subdir @subdir 
	END TRY
	BEGIN CATCH
		-- Ошбика выполнения операции
		SET @finalmassage = @finalmassage + 'Ошибка создания каталога: ' + @subdir + CHAR(13) + CHAR(13)
			+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
			+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
			+ 'Текст T-SQL:' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13) 
		SET @subdir = '' 
	END CATCH;
	
	IF @subdir <> ''
	BEGIN
		
		-- Формируем строку для исполнения
		IF @Type = 3 SET @SQLString = 
			N'BACKUP LOG [' + @DBName + ']
			TO DISK = N''' + @subdir + '\\' + @DBName + '_' + Replace(CONVERT(nvarchar, GETDATE(), 126),':','-') + '.trn'' '
		ELSE SET @SQLString = 
			N'BACKUP DATABASE [' + @DBName + ']
			TO DISK = N''' + @subdir + '\\' + @DBName + '_' + Replace(CONVERT(nvarchar, GETDATE(), 126),':','-') + '.bak'' '
		set @SQLString = @SQLString +		  
			'WITH NOFORMAT, NOINIT,
			SKIP, NOREWIND, NOUNLOAD, STATS = 10'
		IF @Compression = 1 SET @SQLString = @SQLString + ', COMPRESSION'
		IF @Type = 0 SET @SQLString = @SQLString + ', COPY_ONLY'
		IF @Type = 2 SET @SQLString = @SQLString + ', DIFFERENTIAL'

		-- Выводим и выполняем полученную инструкцию
		PRINT @SQLString
		BEGIN TRY
			EXEC sp_executesql @SQLString
		END TRY
		BEGIN CATCH  
			-- Ошбика выполнения операции
			SET @finalmassage = @finalmassage + 'Ошибка создания резервной копии базы ' + @DBName + ' в каталог ' + @subdir + CHAR(13) + CHAR(13)
				+ 'Код ошибки: ' + CAST(ERROR_NUMBER() as nvarchar(10)) + CHAR(13) + CHAR(13)
				+ 'Текст ошибки: ' + ERROR_MESSAGE()  + CHAR(13) + CHAR(13)
				+ 'Текст T-SQL:' + CHAR(13) + @SQLString + CHAR(13) + CHAR(13)  
		END CATCH;
	END
	
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
	SET @subject = 'Успешное создание резервных копий баз данных '
	SET @finalmassage = 'Успешное создание резервных копий всех баз данных '
END
ELSE
	-- Были ошибки
	SET @subject = 'БЫЛИ ОШИБКИ при создании резервных копий баз данных '

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
