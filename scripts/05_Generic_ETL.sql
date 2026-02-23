-- =================================================
-- 05_Generic_ETL.sql
-- ETL script for loading Generic data from CSV
-- Creates staging table, cleans data, inserts missing Drug Classes,
-- and inserts into the final Generic table with proper FK mapping
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Generic;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Generic (
    Generic_ID INT IDENTITY(1,1) PRIMARY KEY,
    Generic_Name NVARCHAR(255) NOT NULL UNIQUE,
    Slug NVARCHAR(255),
    Drug_Class_ID INT NOT NULL,

    CONSTRAINT FK_Generic_DrugClass
        FOREIGN KEY (Drug_Class_ID)
        REFERENCES Drug_Class(Drug_Class_ID)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Generic;

-- ==========================
-- CREATE staging table (match CSV exactly)
-- ==========================
CREATE TABLE Staging_Generic (
    Generic_ID NVARCHAR(255),
    Generic_Name NVARCHAR(255),
    Slug NVARCHAR(255),
    Drug_Class NVARCHAR(255)
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
BULK INSERT Staging_Generic
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Generic.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',
    FORMAT = 'CSV',
    FIELDQUOTE = '"',
    TABLOCK
);

-- ==========================
-- INSERT MISSING DRUG CLASSES
-- ==========================
WITH Cleaned AS (
    SELECT
        Clean_Generic_Name = LTRIM(RTRIM(REPLACE(Generic_Name, CHAR(160), ''))),
        Clean_Slug         = LTRIM(RTRIM(REPLACE(Slug, CHAR(160), ''))),
        Clean_Drug_Class   = LTRIM(RTRIM(REPLACE(Drug_Class, CHAR(160), '')))
    FROM Staging_Generic
)
INSERT INTO Drug_Class (Drug_Class_Name)
SELECT DISTINCT C.Clean_Drug_Class
FROM Cleaned C
LEFT JOIN Drug_Class DC
    ON C.Clean_Drug_Class = DC.Drug_Class_Name
WHERE DC.Drug_Class_ID IS NULL
  AND C.Clean_Drug_Class IS NOT NULL
  AND C.Clean_Drug_Class <> '';

-- ==========================
-- INSERT INTO GENERIC TABLE
-- ==========================
WITH Cleaned AS (
    SELECT
        Clean_Generic_Name = LTRIM(RTRIM(REPLACE(Generic_Name, CHAR(160), ''))),
        Clean_Slug         = LTRIM(RTRIM(REPLACE(Slug, CHAR(160), ''))),
        Clean_Drug_Class   = LTRIM(RTRIM(REPLACE(Drug_Class, CHAR(160), '')))
    FROM Staging_Generic
)
INSERT INTO Generic (Generic_Name, Slug, Drug_Class_ID)
SELECT DISTINCT
    C.Clean_Generic_Name,
    C.Clean_Slug,
    DC.Drug_Class_ID
FROM Cleaned C
INNER JOIN Drug_Class DC
    ON C.Clean_Drug_Class = DC.Drug_Class_Name
WHERE C.Clean_Generic_Name IS NOT NULL
  AND C.Clean_Generic_Name <> '';

-- ==========================
-- CLEANUP
-- ==========================
DROP TABLE Staging_Generic;

-- ==========================
-- VERIFY
-- ==========================
SELECT COUNT(*) AS Drug_Class_Count FROM Drug_Class;
SELECT COUNT(*) AS Generic_Count FROM Generic;
