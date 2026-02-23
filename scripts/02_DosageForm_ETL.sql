-- =================================================
-- 02_DosageForm_ETL.sql
-- ETL script for loading Dosage Form data from CSV
-- Creates staging table, cleans and deduplicates data,
-- and inserts into the final Dosage_Form table
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Dosage_Form;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Dosage_Form (
    Dosage_Form_ID INT IDENTITY(1,1) PRIMARY KEY,
    Dosage_Form_Name NVARCHAR(255) NOT NULL UNIQUE,
    Slug NVARCHAR(255)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Dosage_Form;

-- ==========================
-- CREATE staging table (match CSV exactly)
-- ==========================
CREATE TABLE Staging_Dosage_Form (
    Dosage_Form_ID INT,
    Dosage_Form_Name NVARCHAR(255),
    Slug NVARCHAR(255),
    Brand_Names_Count INT
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
BULK INSERT Staging_Dosage_Form
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Dosage_Form.csv'
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
        CleanName = LTRIM(RTRIM(REPLACE(Dosage_Form_Name, CHAR(160), ''))),
        CleanSlug = LTRIM(RTRIM(REPLACE(Slug, CHAR(160), '')))
    FROM Staging_Dosage_Form
),
Cleaned AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CleanName
            ORDER BY CleanSlug
        ) AS rn
    FROM Base
)
INSERT INTO Dosage_Form (Dosage_Form_Name, Slug)
SELECT CleanName, CleanSlug
FROM Cleaned
WHERE rn = 1
  AND CleanName IS NOT NULL
  AND CleanName <> '';

-- ==========================
-- CLEANUP
-- ==========================
DROP TABLE Staging_Dosage_Form;

-- ==========================
-- VERIFY
-- ==========================
SELECT COUNT(*) AS Dosage_Form_Count FROM Dosage_Form;