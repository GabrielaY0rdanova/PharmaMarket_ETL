-- =================================================
-- 07_Medicine_PackageSize_ETL.sql
-- Creates and populates the Medicine_PackageSize child table
-- Parses raw Package_Size strings from Medicine into
-- normalized rows with separate Pack_Size and Pack_Price columns
--
-- Input format examples:
--   Single:  "(100's pack: ৳ 100.00)"
--   Double:  "(100's pack: ৳ 100.00),(150's pack: ৳ 150.00)"
--   Triple:  "(100's pack: ৳ 100.00),(150's pack: ৳ 150.00),(200's pack: ৳ 200.00)"
--
-- Currency character: NCHAR(2547) = ৳ (Bengali taka sign)
-- Pack_Price stored as DECIMAL(10,2) in BDT
-- Pack_Size stored as INT (unit count only, e.g. 100, 150, 200)
--
-- On completion, Package_Size column is dropped from Medicine.
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP child table if exists
-- ==========================
DROP TABLE IF EXISTS Medicine_PackageSize;

-- ==========================
-- CREATE child table
-- ==========================
CREATE TABLE Medicine_PackageSize (
    PackageSize_ID  INT IDENTITY(1,1) PRIMARY KEY,
    Brand_ID        INT NOT NULL,
    Pack_Size       INT,
    Pack_Price      DECIMAL(10,2),

    CONSTRAINT FK_PackageSize_Medicine
        FOREIGN KEY (Brand_ID)
        REFERENCES Medicine(Brand_ID)
);

-- ==========================
-- HELPER: extract a single block from Package_Size string
-- Block N is the Nth occurrence of (...) in the string
-- We use CHARINDEX('),(', ...) to find block boundaries
-- ==========================

-- Preview all three block extractions before inserting
SELECT
    Brand_ID,
    Package_Size,

    -- Block 1: everything up to first '),' or end of string
    CASE
        WHEN CHARINDEX('),(', Package_Size) > 0
        THEN LEFT(Package_Size, CHARINDEX('),(', Package_Size))
        ELSE Package_Size
    END AS Block_1,

    -- Block 2: between first and second '),'
    CASE
        WHEN CHARINDEX('),(', Package_Size) > 0
        THEN SUBSTRING(
                Package_Size,
                CHARINDEX('),(', Package_Size) + 2,
                CASE
                    WHEN CHARINDEX('),(', Package_Size, CHARINDEX('),(', Package_Size) + 1) > 0
                    THEN CHARINDEX('),(', Package_Size, CHARINDEX('),(', Package_Size) + 1)
                         - CHARINDEX('),(', Package_Size) - 2
                    ELSE LEN(Package_Size)
                END
             )
        ELSE NULL
    END AS Block_2,

    -- Block 3: after second '),'
    CASE
        WHEN CHARINDEX('),(', Package_Size, CHARINDEX('),(', Package_Size) + 1) > 0
        THEN SUBSTRING(
                Package_Size,
                CHARINDEX('),(', Package_Size, CHARINDEX('),(', Package_Size) + 1) + 2,
                LEN(Package_Size)
             )
        ELSE NULL
    END AS Block_3

FROM Medicine
WHERE Package_Size LIKE '%' + NCHAR(2547) + '%'
ORDER BY Brand_ID;

-- ==========================
-- HELPER: extract Pack_Size integer from a block
-- e.g. "(100's pack: ৳ 100.00)" → 100
-- Strip leading '(', take everything before the first "'"
-- ==========================

-- Preview Pack_Size extraction from Block 1
SELECT TOP 20
    Brand_ID,
    Package_Size,
    CAST(
        LEFT(
            LTRIM(REPLACE(Package_Size, '(', '')),
            CHARINDEX('''', LTRIM(REPLACE(Package_Size, '(', ''))) - 1
        )
    AS INT) AS Pack_Size_Preview
FROM Medicine
WHERE Package_Size LIKE '%' + NCHAR(2547) + '%'
  AND CHARINDEX('),(', Package_Size) = 0
ORDER BY Brand_ID;

-- ==========================
-- HELPER: extract Pack_Price decimal from a block
-- e.g. "(100's pack: ৳ 100.00)" → 100.00
-- Find ৳, take everything after '৳ ', strip closing ')', strip commas
-- ==========================

-- Preview Pack_Price extraction from Block 1
SELECT TOP 20
    Brand_ID,
    Package_Size,
    CAST(
        REPLACE(
            REPLACE(
                LTRIM(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX(NCHAR(2547) + ' ', Package_Size) + 2,
                        LEN(Package_Size)
                    )
                ),
            ')', ''),
        ',', '')
    AS DECIMAL(10,2)) AS Pack_Price_Preview
FROM Medicine
WHERE Package_Size LIKE '%' + NCHAR(2547) + '%'
  AND CHARINDEX('),(', Package_Size) = 0
ORDER BY Brand_ID;

-- ==========================
-- INSERT: Block 1 (all rows that have Package_Size)
-- ==========================
INSERT INTO Medicine_PackageSize (Brand_ID, Pack_Size, Pack_Price)
SELECT
    Brand_ID,

    -- Pack_Size: integer before the first apostrophe
    TRY_CAST(
        LEFT(
            LTRIM(REPLACE(Package_Size, '(', '')),
            CHARINDEX('''', LTRIM(REPLACE(Package_Size, '(', ''))) - 1
        )
    AS INT),

    -- Pack_Price: decimal after first ৳ sign, strip ) and commas
    TRY_CAST(
        REPLACE(
            REPLACE(
                LTRIM(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX(NCHAR(2547) + ' ', Package_Size) + 2,
                        CHARINDEX(')', Package_Size,
                            CHARINDEX(NCHAR(2547) + ' ', Package_Size))
                            - CHARINDEX(NCHAR(2547) + ' ', Package_Size) - 2
                    )
                ),
            ')', ''),
        ',', '')
    AS DECIMAL(10,2))

FROM Medicine
WHERE Package_Size LIKE '%' + NCHAR(2547) + '%';

-- ==========================
-- VERIFY Block 1 insert
-- ==========================
SELECT COUNT(*) AS Block1_Rows FROM Medicine_PackageSize;

-- ==========================
-- INSERT: Block 2 (rows with at least 2 pack sizes)
-- ==========================
INSERT INTO Medicine_PackageSize (Brand_ID, Pack_Size, Pack_Price)
SELECT
    Brand_ID,

    -- Pack_Size from Block 2
    TRY_CAST(
        LEFT(
            LTRIM(REPLACE(
                SUBSTRING(
                    Package_Size,
                    CHARINDEX('),(', Package_Size) + 2,
                    LEN(Package_Size)
                ),
            '(', '')),
            CHARINDEX('''',
                LTRIM(REPLACE(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX('),(', Package_Size) + 2,
                        LEN(Package_Size)
                    ),
                '(', ''))) - 1
        )
    AS INT),

    -- Pack_Price from Block 2
    TRY_CAST(
        REPLACE(
            REPLACE(
                LTRIM(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX(NCHAR(2547) + ' ',
                            Package_Size,
                            CHARINDEX('),(', Package_Size)) + 2,
                        CHARINDEX(')',
                            Package_Size,
                            CHARINDEX(NCHAR(2547) + ' ',
                                Package_Size,
                                CHARINDEX('),(', Package_Size)))
                            - CHARINDEX(NCHAR(2547) + ' ',
                                Package_Size,
                                CHARINDEX('),(', Package_Size)) - 2
                    )
                ),
            ')', ''),
        ',', '')
    AS DECIMAL(10,2))

FROM Medicine
WHERE CHARINDEX('),(', Package_Size) > 0;

-- ==========================
-- VERIFY Block 2 insert
-- ==========================
SELECT COUNT(*) AS Total_Rows_After_Block2 FROM Medicine_PackageSize;

-- ==========================
-- INSERT: Block 3 (rows with 3 pack sizes)
-- ==========================
INSERT INTO Medicine_PackageSize (Brand_ID, Pack_Size, Pack_Price)
SELECT
    Brand_ID,

    -- Pack_Size from Block 3
    TRY_CAST(
        LEFT(
            LTRIM(REPLACE(
                SUBSTRING(
                    Package_Size,
                    CHARINDEX('),(', Package_Size,
                        CHARINDEX('),(', Package_Size) + 1) + 2,
                    LEN(Package_Size)
                ),
            '(', '')),
            CHARINDEX('''',
                LTRIM(REPLACE(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX('),(', Package_Size,
                            CHARINDEX('),(', Package_Size) + 1) + 2,
                        LEN(Package_Size)
                    ),
                '(', ''))) - 1
        )
    AS INT),

    -- Pack_Price from Block 3
    TRY_CAST(
        REPLACE(
            REPLACE(
                LTRIM(
                    SUBSTRING(
                        Package_Size,
                        CHARINDEX(NCHAR(2547) + ' ',
                            Package_Size,
                            CHARINDEX('),(', Package_Size,
                                CHARINDEX('),(', Package_Size) + 1)) + 2,
                        CHARINDEX(')',
                            Package_Size,
                            CHARINDEX(NCHAR(2547) + ' ',
                                Package_Size,
                                CHARINDEX('),(', Package_Size,
                                    CHARINDEX('),(', Package_Size) + 1)))
                            - CHARINDEX(NCHAR(2547) + ' ',
                                Package_Size,
                                CHARINDEX('),(', Package_Size,
                                    CHARINDEX('),(', Package_Size) + 1)) - 2
                    )
                ),
            ')', ''),
        ',', '')
    AS DECIMAL(10,2))

FROM Medicine
WHERE CHARINDEX('),(', Package_Size,
          CHARINDEX('),(', Package_Size) + 1) > 0;

-- ==========================
-- VERIFY Block 3 insert
-- ==========================
SELECT COUNT(*) AS Total_Rows_After_Block3 FROM Medicine_PackageSize;

-- ==========================
-- VERIFY: spot check joined output
-- ==========================
SELECT TOP 20
    m.Brand_ID,
    m.Brand_Name,
    m.Package_Size       AS Raw_Package_Size,
    ps.Pack_Size,
    ps.Pack_Price
FROM Medicine m
INNER JOIN Medicine_PackageSize ps ON m.Brand_ID = ps.Brand_ID
ORDER BY m.Brand_ID, ps.Pack_Size;

-- ==========================
-- VERIFY: NULL check on child table
-- ==========================
SELECT
    COUNT(*)                                                    AS Total_Rows,
    SUM(CASE WHEN Pack_Size IS NULL THEN 1 ELSE 0 END)         AS Null_Pack_Size,
    SUM(CASE WHEN Pack_Price IS NULL THEN 1 ELSE 0 END)        AS Null_Pack_Price
FROM Medicine_PackageSize;

-- ==========================
-- DROP Package_Size from Medicine
-- Now that child table is populated, raw column is no longer needed
-- ==========================
ALTER TABLE Medicine
DROP COLUMN Package_Size;

-- ==========================
-- FINAL VERIFY
-- ==========================
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Medicine'
ORDER BY ORDINAL_POSITION;

SELECT
    COUNT(*)                                                        AS Total_Medicine_Rows,
    SUM(CASE WHEN Package_Container IS NULL THEN 1 ELSE 0 END)     AS Null_Container,
    SUM(CASE WHEN Package_Container COLLATE Latin1_General_BIN
             LIKE '%[^ -~]%' THEN 1 ELSE 0 END)                    AS Encoding_In_Container
FROM Medicine;

SELECT
    COUNT(*)                                                    AS Total_PackageSize_Rows,
    SUM(CASE WHEN Pack_Size IS NULL THEN 1 ELSE 0 END)         AS Null_Pack_Size,
    SUM(CASE WHEN Pack_Price IS NULL THEN 1 ELSE 0 END)        AS Null_Pack_Price,
    COUNT(DISTINCT Brand_ID)                                    AS Distinct_Medicines
FROM Medicine_PackageSize;
