-------------------------------------------
-- Скрипт показывает версию MS SQL-сервера
-- Автор: Онянов Виталий (Tavalik.ru)
-- Версия от 22.05.2017
-- Свежие версии скриптов: https://github.com/Tavalik/SQL_TScripts

-------------------------------------------
-- ТЕЛО СКРИПТА
SELECT
	SERVERPROPERTY('MachineName') AS ComputerName,
	SERVERPROPERTY('ServerName') AS InstanceName,  
	SERVERPROPERTY('Edition') AS Edition,
	SERVERPROPERTY('ProductVersion') AS ProductVersion,  
	SERVERPROPERTY('ProductLevel') AS ProductLevel;
	
GO 