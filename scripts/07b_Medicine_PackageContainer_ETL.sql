-- =================================================
-- 07b_Medicine_PackageContainer_ETL.sql
-- Creates and populates the Medicine_PackageContainer child table
-- Parses cleaned Package_Container strings from Medicine into
-- normalized rows with separate Container_Size and Unit_Price columns
--
-- Runs AFTER 07_Medicine_PackageSize_ETL.sql
-- Reads Package_Container from Medicine (cleaned by 06_Medicine_ETL.sql)
-- On completion, drops Package_Container and Unit_Price from Medicine
--
-- Input patterns after 06 cleaning:
--   NULL        : Format B rows — no physical container (unit price already in Medicine.Unit_Price)
--   Single      : '100 ml bottle: ৳ 130.00'
--   Multiple    : '37.5 ml bottle: ৳ 130.00,50 ml bottle: ৳ 160.00'
--   No price    : '100 ml bottle' (rare — µg rows and similar)
--
-- Currency character: NCHAR(2547) = ৳ (Bengali taka sign)
-- Unit_Price stored as DECIMAL(10,2) in BDT
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP child table if exists
-- ==========================
DROP TABLE IF EXISTS Medicine_PackageContainer;

-- ==========================
-- CREATE child table
-- ==========================
CREATE TABLE Medicine_PackageContainer (
    PackageContainer_ID  INT IDENTITY(1,1) PRIMARY KEY,
    Brand_ID             INT NOT NULL,
    Container_Size       NVARCHAR(255),
    Unit_Price           DECIMAL(10,2),

    CONSTRAINT FK_PackageContainer_Medicine
        FOREIGN KEY (Brand_ID)
        REFERENCES Medicine(Brand_ID)
);

-- ==========================
-- VERIFY input before inserting
-- Confirm 06 left clean data
-- ==========================
SELECT
    CASE
        WHEN Package_Container IS NULL
            THEN 'NULL (Format B — no container)'
        WHEN Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
         AND Package_Container NOT LIKE '%,%'
            THEN 'Single — no price'
        WHEN Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
         AND Package_Container LIKE '%,%'
            THEN 'Multiple — no price'
        WHEN LEN(Package_Container) - LEN(REPLACE(Package_Container, NCHAR(2547), '')) = 1
            THEN 'Single — with price'
        WHEN LEN(Package_Container) - LEN(REPLACE(Package_Container, NCHAR(2547), '')) > 1
            THEN 'Multiple — with price'
        ELSE 'Unknown'
    END AS Pattern,
    COUNT(*) AS Row_Count
FROM Medicine
GROUP BY
    CASE
        WHEN Package_Container IS NULL
            THEN 'NULL (Format B — no container)'
        WHEN Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
         AND Package_Container NOT LIKE '%,%'
            THEN 'Single — no price'
        WHEN Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
         AND Package_Container LIKE '%,%'
            THEN 'Multiple — no price'
        WHEN LEN(Package_Container) - LEN(REPLACE(Package_Container, NCHAR(2547), '')) = 1
            THEN 'Single — with price'
        WHEN LEN(Package_Container) - LEN(REPLACE(Package_Container, NCHAR(2547), '')) > 1
            THEN 'Multiple — with price'
        ELSE 'Unknown'
    END
ORDER BY Row_Count DESC;

-- ==========================
-- INSERT: Single container with price
-- e.g. '100 ml bottle: ৳ 130.00'
-- Container_Size = everything before ': ৳'
-- Unit_Price     = everything after '৳ '
-- ==========================
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    -- Container_Size: everything before ': ৳'
    LTRIM(RTRIM(
        LEFT(Package_Container,
            CHARINDEX(': ' + NCHAR(2547), Package_Container) - 1)
    )),
    -- Unit_Price: everything after '৳ ', strip commas
    TRY_CAST(
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
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container NOT LIKE '%,%'
  AND Package_Container LIKE '%' + NCHAR(2547) + '%';

-- ==========================
-- VERIFY single with price
-- ==========================
SELECT COUNT(*) AS Single_With_Price_Rows FROM Medicine_PackageContainer;

-- ==========================
-- INSERT: Single container with no price
-- e.g. '75 µg pre-filled syringe'
-- Container_Size = full string, Unit_Price = NULL
-- ==========================
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(Package_Container)),
    NULL
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container NOT LIKE '%,%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%';

-- ==========================
-- VERIFY single no price
-- ==========================
SELECT COUNT(*) AS Total_After_Single_No_Price FROM Medicine_PackageContainer;

-- ==========================
-- INSERT: Multiple containers with price — Segment 1
-- e.g. '37.5 ml bottle: ৳ 130.00,50 ml bottle: ৳ 160.00'
-- Segment 1 = from start to first '),' boundary or first ৳ occurrence
--
-- Strategy: find each ৳ sign position, extract description
-- by looking backward to previous comma (or start of string)
-- and price by looking forward to next comma (or end of string)
-- ==========================

-- Segment 1 for all multi-container rows
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    -- Container_Size: from start to first ': ৳'
    LTRIM(RTRIM(
        LEFT(Package_Container,
            CHARINDEX(': ' + NCHAR(2547), Package_Container) - 1)
    )),
    -- Unit_Price: from after first '৳ ' to next comma or end
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(
                        Package_Container,
                        CHARINDEX(NCHAR(2547) + ' ', Package_Container) + 2,
                        LEN(Package_Container)
                    ),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(
                                Package_Container,
                                CHARINDEX(NCHAR(2547) + ' ', Package_Container) + 2,
                                LEN(Package_Container)
                            )) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(
                                Package_Container,
                                CHARINDEX(NCHAR(2547) + ' ', Package_Container) + 2,
                                LEN(Package_Container)
                            )) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container LIKE '%,%'
  AND Package_Container LIKE '%' + NCHAR(2547) + '%';

-- ==========================
-- VERIFY segment 1
-- ==========================
SELECT COUNT(*) AS Total_After_Seg1 FROM Medicine_PackageContainer;

-- ==========================
-- INSERT: Multiple containers — Segments 2 through 7
-- For each segment N, find the Nth ৳ sign position,
-- extract description by looking backward to previous comma
-- and price by looking forward to next comma or end of string
-- ==========================

-- Segment 2
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 2
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- Segment 3
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container,
                CHARINDEX(NCHAR(2547), Package_Container) + 1) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 3
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- Segment 4
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container,
                CHARINDEX(NCHAR(2547), Package_Container,
                    CHARINDEX(NCHAR(2547), Package_Container) + 1) + 1) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 4
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- Segment 5
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container,
                CHARINDEX(NCHAR(2547), Package_Container,
                    CHARINDEX(NCHAR(2547), Package_Container,
                        CHARINDEX(NCHAR(2547), Package_Container) + 1) + 1) + 1) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 5
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- Segment 6
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container,
                CHARINDEX(NCHAR(2547), Package_Container,
                    CHARINDEX(NCHAR(2547), Package_Container,
                        CHARINDEX(NCHAR(2547), Package_Container,
                            CHARINDEX(NCHAR(2547), Package_Container) + 1) + 1) + 1) + 1) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 6
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- Segment 7
;WITH Positions AS (
    SELECT
        Brand_ID,
        Package_Container,
        CHARINDEX(NCHAR(2547), Package_Container,
            CHARINDEX(NCHAR(2547), Package_Container,
                CHARINDEX(NCHAR(2547), Package_Container,
                    CHARINDEX(NCHAR(2547), Package_Container,
                        CHARINDEX(NCHAR(2547), Package_Container,
                            CHARINDEX(NCHAR(2547), Package_Container,
                                CHARINDEX(NCHAR(2547), Package_Container) + 1) + 1) + 1) + 1) + 1) + 1
        ) AS TakaPos
    FROM Medicine
    WHERE Package_Container IS NOT NULL
      AND Package_Container LIKE '%,%'
      AND LEN(Package_Container)
          - LEN(REPLACE(Package_Container, NCHAR(2547), '')) >= 7
)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            LEN(LEFT(Package_Container, TakaPos - 3))
                - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2,
            TakaPos - 2
                - (LEN(LEFT(Package_Container, TakaPos - 3))
                    - CHARINDEX(',', REVERSE(LEFT(Package_Container, TakaPos - 3))) + 2)
        )
    )),
    TRY_CAST(
        REPLACE(
            LTRIM(RTRIM(
                LEFT(
                    SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container)),
                    CASE
                        WHEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) > 0
                        THEN CHARINDEX(',',
                            SUBSTRING(Package_Container, TakaPos + 2, LEN(Package_Container))) - 1
                        ELSE LEN(Package_Container)
                    END
                )
            )),
        ',', '')
    AS DECIMAL(10,2))
FROM Positions;

-- ==========================
-- INSERT: Multiple containers with no price
-- e.g. '120 metered doses,120 metered doses (refill)'
-- e.g. '10 gm tube,15 gm tube'
-- Split on comma, insert one row per segment, Unit_Price = NULL
-- Handle up to 3 segments (covers all cases in data)
-- ==========================

-- Segment 1 — no price multi
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        LEFT(Package_Container, CHARINDEX(',', Package_Container) - 1)
    )),
    NULL
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container LIKE '%,%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%';

-- Segment 2 — no price multi
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        CASE
            -- Two segments only: take everything after first comma
            WHEN CHARINDEX(',', Package_Container,
                    CHARINDEX(',', Package_Container) + 1) = 0
            THEN SUBSTRING(
                    Package_Container,
                    CHARINDEX(',', Package_Container) + 1,
                    LEN(Package_Container))
            -- Three segments: take between first and second comma
            ELSE SUBSTRING(
                    Package_Container,
                    CHARINDEX(',', Package_Container) + 1,
                    CHARINDEX(',', Package_Container,
                        CHARINDEX(',', Package_Container) + 1)
                    - CHARINDEX(',', Package_Container) - 1)
        END
    )),
    NULL
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container LIKE '%,%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%';

-- Segment 3 — no price multi (only rows with 3 segments)
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    LTRIM(RTRIM(
        SUBSTRING(
            Package_Container,
            CHARINDEX(',', Package_Container,
                CHARINDEX(',', Package_Container) + 1) + 1,
            LEN(Package_Container)
        )
    )),
    NULL
FROM Medicine
WHERE Package_Container IS NOT NULL
  AND Package_Container LIKE '%,%'
  AND Package_Container NOT LIKE '%' + NCHAR(2547) + '%'
  -- Only rows with at least 2 commas (3 segments)
  AND LEN(Package_Container)
      - LEN(REPLACE(Package_Container, ',', '')) >= 2;

-- ==========================
-- INSERT: Format B rows — unit price only, no container description
-- Package_Container was set to NULL by 06 (CLEAN 1)
-- Unit_Price was extracted by 06 into Medicine.Unit_Price
-- These rows have no physical container — Container_Size = NULL
-- Medicines with no price at all (Unit_Price IS NULL) are excluded —
-- they will correctly show up in the neither-child-tables validation check.
-- ==========================
INSERT INTO Medicine_PackageContainer (Brand_ID, Container_Size, Unit_Price)
SELECT
    Brand_ID,
    NULL,
    Unit_Price
FROM Medicine
WHERE Package_Container IS NULL
  AND Unit_Price IS NOT NULL;

-- ==========================
-- VERIFY Format B insert
-- ==========================
SELECT COUNT(*) AS Total_After_FormatB FROM Medicine_PackageContainer;

-- ==========================
-- VERIFY all inserts complete
-- ==========================
SELECT
    COUNT(*)                                                        AS Total_Rows,
    SUM(CASE WHEN Container_Size IS NULL THEN 1 ELSE 0 END)        AS Null_Container_Size,
    SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END)            AS Null_Unit_Price,
    COUNT(DISTINCT Brand_ID)                                        AS Distinct_Medicines
FROM Medicine_PackageContainer;

-- Spot check
SELECT TOP 20
    m.Brand_ID,
    m.Brand_Name,
    m.Package_Container  AS Raw_Input,
    pc.Container_Size,
    pc.Unit_Price
FROM Medicine m
INNER JOIN Medicine_PackageContainer pc ON m.Brand_ID = pc.Brand_ID
ORDER BY m.Brand_ID, pc.PackageContainer_ID;

-- Container count distribution
SELECT
    Container_Count,
    COUNT(*) AS Medicine_Count
FROM (
    SELECT Brand_ID, COUNT(*) AS Container_Count
    FROM Medicine_PackageContainer
    GROUP BY Brand_ID
) counts
GROUP BY Container_Count
ORDER BY Container_Count;

-- ==========================
-- DROP Package_Container and Unit_Price from Medicine
-- All data now lives in Medicine_PackageContainer
-- ==========================
ALTER TABLE Medicine
DROP COLUMN Package_Container;

ALTER TABLE Medicine
DROP COLUMN Unit_Price;

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
    COUNT(*)                                                        AS Total_Medicine_Rows
FROM Medicine;

SELECT
    COUNT(*)                                                        AS Total_PackageContainer_Rows,
    SUM(CASE WHEN Container_Size IS NULL THEN 1 ELSE 0 END)        AS Null_Container_Size,
    SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END)            AS Null_Unit_Price,
    COUNT(DISTINCT Brand_ID)                                        AS Distinct_Medicines
FROM Medicine_PackageContainer;
