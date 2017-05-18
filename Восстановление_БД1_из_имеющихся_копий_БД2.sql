-------------------------------------------
-- ������ �������������� ���� ���� ������ �� ��������� ����� ������ ���� �� ��������� ���� ("������������" ����). 
--	�������� ������:
--		1. ����������� ���������������� ������� ������ ��������� ����� ����-��������� �� ��������� ����
--		2. ����������������� ����-���������� �� ��������� ������������������ ������
--		3. ����-���������� ����������� � ����������� �����
--		4. ����-���������� ����������� � ������� ������ ��������������
--		5. ����-���������� ���������
--		6. ������������ ����������� ��������� � ���������� ������ � �������������� ������������ ��������� �������
-- �����: ������ ������� (Tavalik.ru)
-- ������ �� 18.05.2017
-------------------------------------------

-- ������������� ����������
-- ���� ������ ����������
DECLARE @DBName_To as nvarchar(40) = 'TestBase'
-- ���� ������ ��������						
DECLARE @DBName_From as nvarchar(40) = 'WorkBase'
-- ����, �� �������� ���������� ������� ������ ��������� �����, � ������� '20160315 12:00:00'							
DECLARE @BackupTime as datetime = GETDATE()
-- ��� ��������� �������, ��� �������� ���������� �����									
DECLARE @profile_name as nvarchar(100) = '���_��������'
-- ���������� ��������� ����������� �����, ����������� ������ ";"				
DECLARE @recipients as nvarchar(500) = 'admin@mydomen.com'

-------------------------------------------
-- ��������� ����������	
DECLARE @SQLString NVARCHAR(4000)
DECLARE @backupfile NVARCHAR(500)
DECLARE @physicalName NVARCHAR(500), @logicalName NVARCHAR(500)
DECLARE @error as int
DECLARE @subject as NVARCHAR(100)
DECLARE @finalmassage as NVARCHAR(1000)

-------------------------------------------
-- ���� �������
use master

-- ������ ��������� �������, ���� ����� ��� ����
IF OBJECT_ID('tempdb.dbo.#BackupFiles') IS NOT NULL DROP TABLE #BackupFiles
IF OBJECT_ID('tempdb.dbo.#FullBackup') IS NOT NULL DROP TABLE #FullBackup
IF OBJECT_ID('tempdb.dbo.#DiffBackup') IS NOT NULL DROP TABLE DiffBackup
IF OBJECT_ID('tempdb.dbo.#LogBackup') IS NOT NULL DROP TABLE LogBackup

-- ������� ������ � ���� ��������� ������ �������
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
	--and backupset.is_copy_only = 0 -- ���� "������ ��������� �����������"
ORDER BY 
	backupset.backup_start_date DESC

-- ������ ��������� ������ �����
SELECT TOP 1
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name,
	BackupFiles.backup_set_uuid	
INTO #FullBackup	 
FROM #BackupFiles AS BackupFiles
WHERE btype = 'D'
ORDER BY backup_start_date DESC

-- ������ ��������� ���������� �����
SELECT TOP 1
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name
INTO #DiffBackup	 
FROM #BackupFiles AS BackupFiles
	INNER JOIN #FullBackup AS FullBackup
	ON BackupFiles.differential_base_guid = FullBackup.backup_set_uuid
WHERE BackupFiles.btype = 'I'
ORDER BY BackupFiles.backup_start_date DESC

-- ������� ������ �������� ����������
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
ORDER BY BackupFiles.backup_start_date DESC

-- ���������� ���� �� ����������� ���� ���� ������
DECLARE bkf CURSOR LOCAL FAST_FORWARD FOR 
(
	SELECT
		physical_device_name
	FROM #FullBackup
	UNION ALL
	SELECT
		physical_device_name
	FROM #DiffBackup
	UNION ALL
	SELECT
		physical_device_name
	FROM #LogBackup
);

-- ������ �����
OPEN bkf;

-- ��������� ������ ������� �����, �� ����� ���� ������ ������ ��������� �����
FETCH bkf INTO @backupfile;
IF @@FETCH_STATUS<>0
	-- ���� �������� ������� �� �������, �� ������ �������� ����� �� �������
	BEGIN
		SET @subject = '������ �������������� ���� ������ ' + @DBName_To
		SET @finalmassage = '�� ������� ������ ��������� ����� ��� ���� ������ ' + @DBName_From
	END
ELSE
	BEGIN

	--����� ��������� ��� ����� ��������� ����� � 3 �����:

	-- 1. ��������� ������ �����
	SET @SQLString = 
	N'RESTORE DATABASE [' + @DBName_To + ']
	FROM DISK = N''' + @backupfile + ''' 
	WITH  
	FILE = 1,'

	-- ����������� ����� ���� ������ �� ��������
	-- ����� ���� �� ���� ������ ���� ������
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

	-- ������� � ��������� ���������� ����������
	PRINT @SQLString
	EXEC sp_executesql @SQLString
	SET @error = @@error
	IF @error <> 0
		BEGIN
			-- ���� ���� ������, �� ������������ ������ ����� �� �������
			SET @subject = '������ �������������� ���� ������ ' + @DBName_To
			SET @finalmassage = '������ �������������� ������ ��������� ����� ��� ���� ������ ' + @DBName_To + CHAR(13) + CHAR(13)
				+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
				+ '����� T-SQL: ' + CHAR(13) + @SQLString
		END
	ELSE 
		BEGIN

		-- 2. ��������� ���������� ����� � ������� ����������
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
			
			-- ������� � ��������� ���������� ����������
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- ���� ������, ��������� ����
				BREAK
				END
			ELSE
				BEGIN
					-- ������ ���, ������������ ��������� ������� �����
					FETCH bkf INTO @backupfile;
				END
			END;
		
		IF @error <> 0
			BEGIN
				-- �� ����� �����, ���� ������
				SET @subject = '������ �������������� ���� ������ ' + @DBName_To
				SET @finalmassage = '������ �������������� ��������� ����� ��� ���� ������ ' + @DBName_To + CHAR(13) + CHAR(13)
					+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
					+ '����� T-SQL: ' + CHAR(13) + @SQLString
			END
		ELSE
			BEGIN

			-- 3. ��������� ���� � ����������� �����
			SET @SQLString = 
			N'RESTORE DATABASE ' + @DBName_To + '
			WITH RECOVERY'
			
			-- ������� � ��������� ���������� ����������
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- ������ �������� ���� � ����������� �����
					SET @subject = '������ �������������� ���� ������ ' + @DBName_To
					SET @finalmassage = '������ �������� � ����������� ����� ���� ������ ' + @DBName_To + CHAR(13) + CHAR(13)
						+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
						+ '����� T-SQL: ' + CHAR(13) + @SQLString
				END
			ELSE	
				BEGIN

				-- ��������� ���� � ������� ������ ��������������
				SET @SQLString = 
					'ALTER DATABASE ' + @DBName_To + ' SET RECOVERY SIMPLE;'
				
				-- ������� � ��������� ���������� ����������
				PRINT @SQLString	
				EXEC sp_executesql @SQLString
				SET @error = @@error
				IF @error <> 0
					BEGIN
						-- ������ �������� ���� � ������� ������ �������������
						SET @subject = '������ �������������� ���� ������ ' + @DBName_To
						SET @finalmassage = '������ �������� � ������� ������ �������������� ���� ������ ' + @DBName_To + CHAR(13) + CHAR(13)
							+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
							+ '����� T-SQL: ' + CHAR(13) + @SQLString
					END
				ELSE
					BEGIN

					-- ��������� ������ ���� ������
					SET @SQLString = 
						'DBCC SHRINKDATABASE(N''' + @DBName_To + ''');'
					
					-- ������� � ��������� ���������� ����������
					PRINT @SQLString
					EXEC sp_executesql @SQLString
					SET @error = @@error
					IF @error <> 0
						BEGIN
							-- ������ ������ ���� ������
							SET @subject = '������ �������������� ���� ������ ' + @DBName_To
							SET @finalmassage = '������ ������ ���� ������ ' + @DBName_To + CHAR(13) + CHAR(13)
								+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
								+ '����� T-SQL: ' + CHAR(13) + @SQLString
						END
					ELSE
						BEGIN
							-- �������� ���������� ���� ��������
							SET @subject = '�������� �������������� ���� ������ ' + @DBName_To
							SET @finalmassage = '�������� �������������� ���� ������ ' + @DBName_To + ' �� ��������� ����� ���� ������ ' + @DBName_From + ' �� ������ ������� ' + Replace(CONVERT(nvarchar, @BackupTime, 126),':','-')
						END
					END
				END
			END
		END			
	END

-- ��������� ����
CLOSE bkf;
DEALLOCATE bkf;

-- ������� ��������� �������
drop table #BackupFiles
drop table #FullBackup
drop table #DiffBackup
drop table #LogBackup

-- ���� ����� ������� ����������� �����, �������� ���������
IF @profile_name <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profile_name,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- ������� ��������� � ����������
SELECT
	@subject as massage
