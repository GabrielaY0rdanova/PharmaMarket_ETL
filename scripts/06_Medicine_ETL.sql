-- =================================================
-- 06_Medicine_ETL.sql
-- ETL script for loading Medicine (Brand) data from CSV
-- Creates staging table, cleans data, and inserts into the final Medicine table
-- with proper FK mapping to Dosage_Form, Generic, and Manufacturer
--
-- Package_Container and Unit_Price are temporary columns used to pass
-- raw pricing and container data to 07b_Medicine_PackageContainer_ETL.sql.
-- Both columns are dropped by 07b on completion.
--
-- Package_Size is left as raw NVARCHAR for consumption by
-- 07_Medicine_PackageSize_ETL.sql, which will populate the child table
-- and drop this column on completion.
--
-- Currency character: NCHAR(2547) = ৳ (Bengali taka sign)
--
-- Package_Container formats in source data:
--   Format A single : '0.2 ml pre-filled syringe: ৳ 1,450.00'
--   Format A multiple: '37.5 ml bottle: ৳ 130.00,50 ml bottle: ৳ 160.00'
--   Format B        : 'Unit Price: ৳ 8.00,(60's pack: ৳ 480.00),'
--
-- This script extracts Unit_Price from Package_Container into a
-- temporary column, then cleans Package_Container of all pricing
-- artifacts so 07b receives clean input:
--   - Pack fragments stripped: '2 ml ampoule,(5's pack' → '2 ml ampoule'
--   - Orphaned prices stripped: '1 ml ampoule,405.13,(5's pack' → '1 ml ampoule'
--   - Triple mess fixed: '1 ml ampoule,(10's pack,,10 ml vial' → '1 ml ampoule,10 ml vial'
--   - Orphaned price between containers: '10 mg vial,420.00,50 mg vial' → '10 mg vial,50 mg vial'
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP final table if exists
-- ==========================
DROP TABLE IF EXISTS Medicine;

-- ==========================
-- CREATE final table
-- ==========================
CREATE TABLE Medicine (
    Brand_ID            INT IDENTITY(1,1) PRIMARY KEY,
    Brand_Name          NVARCHAR(255),
    Type                NVARCHAR(255),
    Slug                NVARCHAR(255),
    Dosage_Form_ID      INT,
    Generic_ID          INT,
    Strength            NVARCHAR(255),
    Manufacturer_ID     INT,
    Package_Container   NVARCHAR(255),   -- temporary: processed by 07b_Medicine_PackageContainer_ETL.sql
    Unit_Price          DECIMAL(10,2),   -- temporary: dropped by 07b_Medicine_PackageContainer_ETL.sql
    Package_Size        NVARCHAR(255),   -- temporary: dropped by 07_Medicine_PackageSize_ETL.sql

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
    Brand_ID            NVARCHAR(255),
    Brand_Name          NVARCHAR(255),
    Type                NVARCHAR(255),
    Slug                NVARCHAR(255),
    Dosage_Form         NVARCHAR(255),
    Generic             NVARCHAR(255),
    Strength            NVARCHAR(255),
    Manufacturer        NVARCHAR(255),
    Package_Container   NVARCHAR(255),
    Package_Size        NVARCHAR(255)
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
BULK INSERT Staging_Medicine
FROM 'E:\Data Analysis\My Projects\PharmaMarket_ETL\source_data\Medicine.csv'
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
-- CLEAN DATA AND INSERT INTO MEDICINE
-- NOTE: Medicine.csv contains 59 true duplicate rows
-- (same Brand_Name + Strength + Dosage_Form + Manufacturer).
-- These survive the DISTINCT clause due to CSV parsing artifacts.
-- Candidate for deduplication in the Data Cleaning project.
-- ==========================
WITH Cleaned AS (
    SELECT
        Clean_Brand_Name        = LTRIM(RTRIM(REPLACE(Brand_Name, ';', ','))),
        Clean_Type              = LTRIM(RTRIM(REPLACE(Type, ';', ','))),
        Clean_Slug              = LTRIM(RTRIM(REPLACE(Slug, ';', ','))),
        Clean_Dosage_Form       = LTRIM(RTRIM(REPLACE(Dosage_Form, ';', ','))),
        Clean_Generic           = LTRIM(RTRIM(REPLACE(Generic, ';', ','))),
        Clean_Strength          = LTRIM(RTRIM(REPLACE(Strength, ';', ','))),
        Clean_Manufacturer      = LTRIM(RTRIM(REPLACE(Manufacturer, ';', ','))),
        Clean_Package_Container = LTRIM(RTRIM(REPLACE(Package_Container, ';', ','))),
        Clean_Package_Size      = LTRIM(RTRIM(REPLACE(Package_Size, ';', ',')))
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
-- CLEANUP STAGING
-- ==========================
DROP TABLE Staging_Medicine;

-- ==========================
-- STEP: EXTRACT UNIT_PRICE FROM PACKAGE_CONTAINER
--
-- Format B with pack info: 'Unit Price: ৳ 8.00,(60's pack: ৳ 480.00),'
--   → extract number between 'Unit Price: ৳ ' and first '('
--
-- Format B with no pack info: 'Unit Price: ৳ 12.00'
--   → extract number after '৳ ' to end of string
--
-- Format A single (1 ৳): '0.2 ml pre-filled syringe: ৳ 1,450.00'
--   → extract number after ': ৳ ' to end of string
--
-- Format A multiple (2+ ৳): '37.5 ml bottle: ৳ 130.00,50 ml bottle: ৳ 160.00'
--   → no single unit price — leave Unit_Price as NULL
--   → 07b will extract per-container prices directly from Package_Container
-- ==========================

-- Format B with pack info
UPDATE Medicine
SET Unit_Price = TRY_CAST(
    REPLACE(
        LTRIM(RTRIM(
            SUBSTRING(
                Package_Container,
                CHARINDEX('Unit Price: ' + NCHAR(2547) + ' ', Package_Container)
                    + LEN('Unit Price: ' + NCHAR(2547) + ' '),
                CHARINDEX('(', Package_Container)
                    - CHARINDEX('Unit Price: ' + NCHAR(2547) + ' ', Package_Container)
                    - LEN('Unit Price: ' + NCHAR(2547) + ' ')
            )
        )),
    ',', '')
AS DECIMAL(10,2))
WHERE Package_Container LIKE 'Unit Price: ' + NCHAR(2547) + '%'
  AND CHARINDEX('(', Package_Container) > 0;

-- Format B with no pack info
UPDATE Medicine
SET Unit_Price = TRY_CAST(
    REPLACE(
        LTRIM(RTRIM(
            SUBSTRING(
                Package_Container,
                CHARINDEX(NCHAR(2547) + ' ', Package_Container) + 2,
                LEN(Package_Container)
            )
        )),
    ',', '')
AS DECIMAL(10,2))
WHERE Package_Container LIKE 'Unit Price: ' + NCHAR(2547) + '%'
  AND CHARINDEX('(', Package_Container) = 0;

-- Format A single (exactly 1 ৳)
UPDATE Medicine
SET Unit_Price = TRY_CAST(
    REPLACE(
        LTRIM(RTRIM(
            SUBSTRING(
                Package_Container,
                CHARINDEX(': ' + NCHAR(2547) + ' ', Package_Container)
                    + LEN(': ' + NCHAR(2547) + ' '),
                LEN(Package_Container)
            )
        )),
    ',', '')
AS DECIMAL(10,2))
WHERE Package_Container NOT LIKE 'Unit Price: ' + NCHAR(2547) + '%'
  AND Package_Container LIKE '%' + NCHAR(2547) + '%'
  AND LEN(Package_Container)
      - LEN(REPLACE(Package_Container, NCHAR(2547), '')) = 1;

-- Format A multiple: Unit_Price stays NULL
-- 07b extracts per-container prices directly

-- ==========================
-- VERIFY UNIT_PRICE EXTRACTION
-- ==========================
SELECT
    SUM(CASE WHEN Package_Container LIKE 'Unit Price: ' + NCHAR(2547) + '%'
             AND Unit_Price IS NULL THEN 1 ELSE 0 END)      AS FormatB_Null_Price,
    SUM(CASE WHEN Package_Container NOT LIKE 'Unit Price: ' + NCHAR(2547) + '%'
             AND Package_Container LIKE '%' + NCHAR(2547) + '%'
             AND LEN(Package_Container)
                 - LEN(REPLACE(Package_Container, NCHAR(2547), '')) = 1
             AND Unit_Price IS NULL THEN 1 ELSE 0 END)      AS FormatA_Single_Null_Price,
    SUM(CASE WHEN Unit_Price IS NOT NULL THEN 1 ELSE 0 END) AS Populated_Unit_Price,
    SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END)     AS Total_Null_Unit_Price
FROM Medicine;

-- ==========================
-- STEP: CLEAN PACKAGE_CONTAINER FOR 07b
--
-- Goal: 07b needs clean input — container descriptions only,
-- no ৳ signs, no pack fragments, no orphaned prices.
-- All pricing data has already been extracted into Unit_Price above.
--
-- Cleaning order matters — more specific patterns first.
-- ==========================

-- CLEAN 1: Format B rows — strip 'Unit Price: ৳ X.XX,' prefix and pack fragment
-- Leaves NULL — Format B rows have no physical container description
-- e.g. 'Unit Price: ৳ 8.00,(60's pack: ৳ 480.00),' → NULL
UPDATE Medicine
SET Package_Container = NULL
WHERE Package_Container LIKE 'Unit Price: ' + NCHAR(2547) + '%';

-- CLEAN 2: Triple mess — container + pack fragment + another container
-- e.g. '1 ml ampoule,(10's pack,,10 ml vial' → '1 ml ampoule,10 ml vial'
-- Pattern: strip from first ',(' to next ',,' then rejoin
UPDATE Medicine
SET Package_Container = LTRIM(RTRIM(
    -- First container: everything before the pack fragment
    LEFT(Package_Container, CHARINDEX(',(', Package_Container) - 1)
    + ','
    -- Second container: everything after the ',,' separator
    + LTRIM(SUBSTRING(
        Package_Container,
        CHARINDEX(',,', Package_Container) + 2,
        LEN(Package_Container)
    ))
))
WHERE Package_Container LIKE '%,(%'
  AND Package_Container LIKE '%,,%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%';

-- CLEAN 3: Container + orphaned price + pack fragment
-- e.g. '1 ml ampoule,405.13,(5's pack' → '1 ml ampoule'
-- e.g. '0.75 mg pre-filled syringe,395.00,(4's pack' → '0.75 mg pre-filled syringe'
-- Pattern: strip from first comma onward (orphaned price + pack fragment follow)
UPDATE Medicine
SET Package_Container = LTRIM(RTRIM(
    LEFT(Package_Container, CHARINDEX(',', Package_Container) - 1)
))
WHERE Package_Container LIKE '%,[0-9]%.[0-9][0-9]%,(%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
  AND Package_Container NOT LIKE '%,,%';

-- CLEAN 4: Container + pack fragment only (no orphaned price, no ৳)
-- e.g. '2 ml ampoule,(5's pack' → '2 ml ampoule'
-- Pattern: strip from ',(' onward
UPDATE Medicine
SET Package_Container = LTRIM(RTRIM(
    LEFT(Package_Container, CHARINDEX(',(', Package_Container) - 1)
))
WHERE Package_Container LIKE '%,(%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
  AND Package_Container NOT LIKE '%,,%';

-- CLEAN 4b: Container + pack fragment where ৳ is also present
-- e.g. '3 ml cartridge: ৳ 723.40,(5's pack: ৳ 200.00)' → '3 ml cartridge: ৳ 723.40'
-- e.g. '250 mg vial: ৳ 20.00,(5's pack' → '250 mg vial: ৳ 20.00'
-- Unit_Price already extracted above — safe to strip pack fragment regardless of ৳
-- Pattern: strip from ',(' onward
UPDATE Medicine
SET Package_Container = LTRIM(RTRIM(
    LEFT(Package_Container, CHARINDEX(',(', Package_Container) - 1)
))
WHERE Package_Container LIKE '%,(%'
  AND Package_Container LIKE '%' + NCHAR(2547) + '%'
  AND Package_Container NOT LIKE '%,,%';

-- CLEAN 5: Container + orphaned price + container
-- e.g. '10 mg vial,420.00,50 mg vial' → '10 mg vial,50 mg vial'
-- e.g. '10 mg vial,50 mg vial,534.10,100 mg vial' → '10 mg vial,50 mg vial,100 mg vial'
-- Pattern: remove any comma-separated segment that is purely numeric (price fragment)
UPDATE Medicine
SET Package_Container = LTRIM(RTRIM(
    LEFT(Package_Container,
        CHARINDEX(',', Package_Container) - 1)
    + ','
    + LTRIM(SUBSTRING(
        Package_Container,
        CHARINDEX(',',
            Package_Container,
            CHARINDEX(',', Package_Container) + 1
        ) + 1,
        LEN(Package_Container)
    ))
))
WHERE Package_Container LIKE '%,[0-9]%.[0-9][0-9],%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
  AND Package_Container NOT LIKE '%,(%'
  AND Package_Container NOT LIKE '%,,%';

-- ==========================
-- VERIFY CLEAN
-- ==========================

-- All ৳ signs should be gone from Package_Container
SELECT
    SUM(CASE WHEN Package_Container LIKE '%' + NCHAR(2547) + '%'
             THEN 1 ELSE 0 END)                              AS Currency_Remaining,
    SUM(CASE WHEN Package_Container LIKE '%,(%'
             THEN 1 ELSE 0 END)                              AS Pack_Fragments_Remaining,
    SUM(CASE WHEN Package_Container LIKE '%,[0-9]%.[0-9][0-9]%'
             THEN 1 ELSE 0 END)                              AS Price_Fragments_Remaining,
    SUM(CASE WHEN Package_Container IS NULL
             THEN 1 ELSE 0 END)                              AS Null_Container,
    COUNT(*)                                                 AS Total_Rows
FROM Medicine;

-- Pattern distribution after cleaning
SELECT
    CASE
        WHEN Package_Container IS NULL              THEN 'NULL (Format B)'
        WHEN Package_Container LIKE '%,%'           THEN 'Multi-container'
        ELSE                                             'Single container'
    END AS Pattern,
    COUNT(*) AS Row_Count
FROM Medicine
GROUP BY
    CASE
        WHEN Package_Container IS NULL              THEN 'NULL (Format B)'
        WHEN Package_Container LIKE '%,%'           THEN 'Multi-container'
        ELSE                                             'Single container'
    END
ORDER BY Row_Count DESC;

-- CLEAN 6: Format C rows — double container+pack block structure
-- e.g. '2 ml ampoule: ৳ 40.00,(5's pack: ৳ 200.00),,2 ml ampoule: ৳ 40.00,(10's pack: ৳ 400.00),'
-- These rows have container+pack blocks concatenated with ',,' separator.
-- Unit_Price already extracted. Set Package_Container to NULL —
-- pack size data for these rows belongs in Medicine_PackageSize.
UPDATE Medicine
SET Package_Container = NULL
WHERE Package_Container LIKE '%),,%';

-- ==========================
-- VERIFY FINAL
-- ==========================
SELECT COUNT(*) AS Medicine_Count FROM Medicine;

SELECT
    SUM(CASE WHEN Package_Size IS NOT NULL THEN 1 ELSE 0 END) AS Rows_With_Package_Size,
    SUM(CASE WHEN Package_Container IS NOT NULL
             AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
             THEN 1 ELSE 0 END)                               AS Clean_Container_Rows,
    SUM(CASE WHEN Package_Container LIKE '%' + NCHAR(2547) + '%'
             THEN 1 ELSE 0 END)                               AS Dirty_Container_Rows
FROM Medicine;