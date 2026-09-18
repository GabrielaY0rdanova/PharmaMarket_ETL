-- =================================================
-- 00_CreateDatabase.sql
-- Creates the SQLCMD-configured target database.
-- Creates the database if it does not exist and sets the context
-- =================================================

-- ==========================
-- CREATE DATABASE IF NOT EXISTS
-- ==========================
DECLARE @DatabaseName SYSNAME = N'$(DatabaseName)';
DECLARE @QuotedDatabaseName NVARCHAR(258);
DECLARE @CreateDatabaseSql NVARCHAR(MAX);

IF @DatabaseName = N'' OR @DatabaseName LIKE N'$%'
BEGIN
    THROW 51000, 'DatabaseName SQLCMD variable is not configured.', 1;
END;

SET @QuotedDatabaseName = QUOTENAME(@DatabaseName);

IF DB_ID(@DatabaseName) IS NULL
BEGIN
    SET @CreateDatabaseSql = N'CREATE DATABASE ' + @QuotedDatabaseName + N';';
    EXEC sys.sp_executesql @CreateDatabaseSql;
    PRINT N'Database ' + @QuotedDatabaseName + N' created successfully.';
END
ELSE
BEGIN
    PRINT N'Database ' + @QuotedDatabaseName + N' already exists.';
END
GO

-- ==========================
-- SET CONTEXT TO DATABASE
-- ==========================
USE [$(DatabaseName)];
GO

-- ==========================
-- NOTE:
-- All subsequent ETL scripts (01_… to 07_…) assume this database context.
-- Run this script first to ensure a clean and consistent environment.
-- =================================================
