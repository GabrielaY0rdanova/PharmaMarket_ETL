-- =================================================
-- 04_Indication_ETL.sql
-- ETL script for loading Indication data from CSV
-- Creates staging table, cleans and deduplicates data,
-- and inserts into the final Indication table
-- =================================================


-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Indication;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Indication (
    Indication_ID INT IDENTITY(1,1) PRIMARY KEY,
    Indication_Name NVARCHAR(255) NOT NULL UNIQUE,
    Slug NVARCHAR(255)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Indication;

-- ==========================
-- CREATE staging table (match CSV exactly)
-- ==========================
CREATE TABLE Staging_Indication (
    Indication_ID INT,
    Indication_Name NVARCHAR(255),
    Slug NVARCHAR(255),
    Generics_Count NVARCHAR(255)
);

-- ==========================
-- TRUNCATE staging just in case
-- ==========================
TRUNCATE TABLE Staging_Indication;

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
BULK INSERT Staging_Indication
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Indication.csv'
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
        CleanName = LTRIM(RTRIM(REPLACE(Indication_Name, CHAR(160), ''))),
        CleanSlug = LTRIM(RTRIM(REPLACE(Slug, CHAR(160), '')))
    FROM Staging_Indication
),
Cleaned AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CleanName
            ORDER BY CleanSlug
        ) AS rn
    FROM Base
)
INSERT INTO Indication (Indication_Name, Slug)
SELECT CleanName, CleanSlug
FROM Cleaned
WHERE rn = 1
  AND CleanName IS NOT NULL
  AND CleanName <> '';

-- ==========================
-- CLEANUP
-- ==========================
DROP TABLE Staging_Indication;
