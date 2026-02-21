-- =================================================
-- 00_CreateDatabase.sql
-- Script to create the PharmaMarketAnalytics database
-- Creates the database if it does not exist and sets the context
-- =================================================

-- ==========================
-- CREATE DATABASE IF NOT EXISTS
-- ==========================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PharmaMarketAnalytics')
BEGIN
    CREATE DATABASE PharmaMarketAnalytics;
    PRINT 'Database PharmaMarketAnalytics created successfully.';
END
ELSE
BEGIN
    PRINT 'Database PharmaMarketAnalytics already exists.';
END
GO

-- ==========================
-- SET CONTEXT TO DATABASE
-- ==========================
USE PharmaMarketAnalytics;
GO

-- ==========================
-- NOTE:
-- All subsequent ETL scripts (01_… to 07_…) assume this database context.
-- Run this script first to ensure a clean and consistent environment.
-- =================================================
