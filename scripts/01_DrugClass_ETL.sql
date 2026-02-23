-- =================================================
-- 01_DrugClass_ETL.sql
-- ETL script for loading Drug Class data from CSV
-- Creates staging table, cleans data, and inserts into final table
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Drug_Class;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Drug_Class (
    Drug_Class_ID INT IDENTITY(1,1) PRIMARY KEY,
    Drug_Class_Name NVARCHAR(255) NOT NULL UNIQUE,
    Slug NVARCHAR(255)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Drug_Class;

-- ==========================
-- CREATE staging table (match CSV exactly)
-- ==========================
CREATE TABLE Staging_Drug_Class (
    Drug_Class_ID INT,
    Drug_Class_Name NVARCHAR(255),
    Slug NVARCHAR(255)
);

-- ==========================
-- BULK INSERT INTO STAGING TABLE
-- ==========================
-- IMPORTANT:
-- Update the file path below to match the location
-- of the cloned repository on your local machine.
--
-- Example root folder:
-- E:\Data Analysis\My Projects\PharmaMarket_ETL\
--
-- SQL Server must have access to this location.
-- ==========================
BULK INSERT Staging_Drug_Class
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Drug_Class.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',
    TABLOCK
);

-- ==========================
-- INSERT into final table with deduplication
-- ==========================
WITH Base AS (
    SELECT
        CleanName = LTRIM(RTRIM(REPLACE(Drug_Class_Name, CHAR(160), ''))),
        CleanSlug = LTRIM(RTRIM(REPLACE(Slug, CHAR(160), '')))
    FROM Staging_Drug_Class
),
Cleaned AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CleanName
            ORDER BY CleanSlug
        ) AS rn
    FROM Base
)
INSERT INTO Drug_Class (Drug_Class_Name, Slug)
SELECT CleanName, CleanSlug
FROM Cleaned
WHERE rn = 1;

-- ==========================
-- CLEANUP
-- ==========================
DROP TABLE Staging_Drug_Class;

-- ==========================
-- VERIFY
-- ==========================
SELECT COUNT(*) AS Drug_Class_Count FROM Drug_Class;
