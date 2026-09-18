-- =================================================
-- 00_ResetSchema.sql
-- Removes the PharmaMarketAnalytics ETL schema in
-- dependency-safe order before a complete rebuild.
--
-- WARNING:
-- This script deletes all ETL tables and their data.
-- Run it only when intentionally rebuilding the database.
-- =================================================

USE [$(DatabaseName)];
GO

SET XACT_ABORT ON;

IF N'$(AllowDestructiveReset)' <> N'YES'
BEGIN
    THROW 51000, 'Schema reset refused. Set AllowDestructiveReset=YES explicitly.', 1;
END;

BEGIN TRY
    BEGIN TRANSACTION;

    -- Child tables must be removed before their parent tables.
    DROP TABLE IF EXISTS Generic_Indication;
    DROP TABLE IF EXISTS Medicine_PackageContainer;
    DROP TABLE IF EXISTS Medicine_PackageSize;
    DROP TABLE IF EXISTS Medicine;
    DROP TABLE IF EXISTS Generic;
    DROP TABLE IF EXISTS Indication;
    DROP TABLE IF EXISTS Manufacturer;
    DROP TABLE IF EXISTS Dosage_Form;
    DROP TABLE IF EXISTS Drug_Class;

    -- Remove staging tables left by an interrupted run.
    DROP TABLE IF EXISTS Staging_Medicine;
    DROP TABLE IF EXISTS Staging_Generic;
    DROP TABLE IF EXISTS Staging_Indication;
    DROP TABLE IF EXISTS Staging_Manufacturer;
    DROP TABLE IF EXISTS Staging_Dosage_Form;
    DROP TABLE IF EXISTS Staging_Drug_Class;

    COMMIT TRANSACTION;
    PRINT 'PharmaMarketAnalytics ETL schema reset successfully.';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
GO
