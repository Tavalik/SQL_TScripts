-------------------------------------------
-- Скрипт показывает текущие соединения с MS SQL-сервером
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 22.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- ТЕЛО СКРИПТА
SELECT 
	program_name, 
	net_transport 
FROM sys.dm_exec_sessions AS sessions 
	left join sys.dm_exec_connections AS connections 
	ON sessions.session_id = connections.session_id 
WHERE
	not sessions.program_name is null
ORDER BY
	program_name
	
GO 
