-- =================================================
-- 06_Medicine_ETL.sql
-- ETL script for loading Medicine (Brand) data from CSV
-- Creates staging table, cleans data, and inserts into the final Medicine table
-- with proper FK mapping to Dosage_Form, Generic, and Manufacturer
-- =================================================


-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Medicine;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Medicine (
    Brand_ID INT IDENTITY(1,1) PRIMARY KEY,
    Brand_Name NVARCHAR(255),
    Type NVARCHAR(255),
    Slug NVARCHAR(255),
    Dosage_Form_ID INT,
    Generic_ID INT,
    Strength NVARCHAR(255),
    Manufacturer_ID INT,
    Package_Container NVARCHAR(255),
    Package_Size NVARCHAR(255),

    CONSTRAINT FK_Medicine_DosageForm
        FOREIGN KEY (Dosage_Form_ID)
        REFERENCES Dosage_Form(Dosage_Form_ID),

    CONSTRAINT FK_Medicine_Generic
        FOREIGN KEY (Generic_ID)
        REFERENCES Generic(Generic_ID),

    CONSTRAINT FK_Medicine_Manufacturer
        FOREIGN KEY (Manufacturer_ID)
        REFERENCES Manufacturer(Manufacturer_ID)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Medicine;

-- ==========================
-- CREATE staging table (match CSV exactly)
-- ==========================
CREATE TABLE Staging_Medicine (
    Brand_ID NVARCHAR(255),
    Brand_Name NVARCHAR(255),
    Type NVARCHAR(255),
    Slug NVARCHAR(255),
    Dosage_Form NVARCHAR(255),
    Generic NVARCHAR(255),
    Strength NVARCHAR(255),
    Manufacturer NVARCHAR(255),
    Package_Container NVARCHAR(255),
    Package_Size NVARCHAR(255)
);

-- ==========================
-- TRUNCATE staging just in case
-- ==========================
TRUNCATE TABLE Staging_Medicine;

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
BULK INSERT Staging_Medicine
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Medicine.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001'
);

-- ==========================
-- CLEAN DATA AND INSERT INTO MEDICINE
-- ==========================
WITH Cleaned AS (
    SELECT
        Clean_Brand_Name       = LTRIM(RTRIM(REPLACE(Brand_Name, ';', ','))),
        Clean_Type             = LTRIM(RTRIM(REPLACE(Type, ';', ','))),
        Clean_Slug             = LTRIM(RTRIM(REPLACE(Slug, ';', ','))),
        Clean_Dosage_Form      = LTRIM(RTRIM(REPLACE(Dosage_Form, ';', ','))),
        Clean_Generic          = LTRIM(RTRIM(REPLACE(Generic, ';', ','))),
        Clean_Strength         = LTRIM(RTRIM(REPLACE(Strength, ';', ','))),
        Clean_Manufacturer     = LTRIM(RTRIM(REPLACE(Manufacturer, ';', ','))),
        Clean_Package_Container = LTRIM(RTRIM(REPLACE(Package_Container, ';', ','))),
        Clean_Package_Size     = LTRIM(RTRIM(REPLACE(Package_Size, ';', ',')))
    FROM Staging_Medicine
)
INSERT INTO Medicine (
    Brand_Name, Type, Slug, Dosage_Form_ID, Generic_ID, Strength,
    Manufacturer_ID, Package_Container, Package_Size
)
SELECT DISTINCT
    C.Clean_Brand_Name,
    C.Clean_Type,
    C.Clean_Slug,
    DF.Dosage_Form_ID,
    G.Generic_ID,
    C.Clean_Strength,
    M.Manufacturer_ID,
    C.Clean_Package_Container,
    C.Clean_Package_Size
FROM Cleaned C
LEFT JOIN Dosage_Form DF
    ON C.Clean_Dosage_Form = DF.Dosage_Form_Name
LEFT JOIN Generic G
    ON C.Clean_Generic = G.Generic_Name
LEFT JOIN Manufacturer M
    ON C.Clean_Manufacturer = M.Manufacturer_Name;

-- ==========================
-- CLEANUP staging table
-- ==========================
DROP TABLE Staging_Medicine;
