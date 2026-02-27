-- =================================================
-- 09_Validation.sql
-- Sanity checks and sample previews for PharmaMarketAnalytics
-- Run this script AFTER all ETL scripts (00 through 08)
--
-- Expected results summary:
--   Section 1   — all RowCounts > 0
--   Section 2   — all DuplicateCounts = 0 (except Medicine: 59 known true duplicates)
--   Section 3   — most IssueCounts = 0 (see documented exceptions)
--   Section 4   — all IssueCounts = 0
--   Section 5   — all IssueCounts = 0
--   Section 6   — review as informational
--   Section 7   — all IssueCounts = 0
--   Section 8   — all IssueCounts = 0
--   Sections 9-10 — review manually for sense-checking
-- =================================================

USE PharmaMarketAnalytics;
GO

-- =================================================
-- SECTION 1: ROW COUNTS
-- Quick confirmation that all tables were populated.
-- =================================================

SELECT 'Drug_Class'                  AS TableName, COUNT(*) AS [RowCount] FROM Drug_Class
UNION ALL
SELECT 'Dosage_Form',                               COUNT(*) FROM Dosage_Form
UNION ALL
SELECT 'Manufacturer',                              COUNT(*) FROM Manufacturer
UNION ALL
SELECT 'Indication',                                COUNT(*) FROM Indication
UNION ALL
SELECT 'Generic',                                   COUNT(*) FROM Generic
UNION ALL
SELECT 'Medicine',                                  COUNT(*) FROM Medicine
UNION ALL
SELECT 'Medicine_PackageSize',                      COUNT(*) FROM Medicine_PackageSize
UNION ALL
SELECT 'Medicine_PackageContainer',                 COUNT(*) FROM Medicine_PackageContainer
UNION ALL
SELECT 'Generic_Indication',                        COUNT(*) FROM Generic_Indication
ORDER BY TableName;
GO

-- =================================================
-- SECTION 2: DUPLICATE CHECKS
-- Deduplication was applied during ETL — no duplicates should remain.
-- =================================================

SELECT 'Drug_Class — duplicate Drug_Class_Name' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Drug_Class_Name, COUNT(*) AS cnt
    FROM Drug_Class
    GROUP BY Drug_Class_Name
    HAVING COUNT(*) > 1
) d;

SELECT 'Dosage_Form — duplicate Dosage_Form_Name' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Dosage_Form_Name, COUNT(*) AS cnt
    FROM Dosage_Form
    GROUP BY Dosage_Form_Name
    HAVING COUNT(*) > 1
) d;

SELECT 'Manufacturer — duplicate Manufacturer_Name' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Manufacturer_Name, COUNT(*) AS cnt
    FROM Manufacturer
    GROUP BY Manufacturer_Name
    HAVING COUNT(*) > 1
) d;

SELECT 'Indication — duplicate Indication_Name' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Indication_Name, COUNT(*) AS cnt
    FROM Indication
    GROUP BY Indication_Name
    HAVING COUNT(*) > 1
) d;

SELECT 'Generic — duplicate Generic_Name' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Generic_Name, COUNT(*) AS cnt
    FROM Generic
    GROUP BY Generic_Name
    HAVING COUNT(*) > 1
) d;

-- Medicine: Brand_Name alone is not a unique key — the same brand exists
-- in multiple strengths and dosage forms, which is expected and legitimate.
-- This check targets true duplicates: identical Brand_Name + Strength +
-- Dosage_Form_ID + Manufacturer_ID.
-- Known result: 59 — duplicate rows in raw CSV caused by CSV parsing.
-- Candidate for deduplication in the Data Cleaning project.
SELECT 'Medicine — true duplicates (Brand + Strength + DosageForm + Manufacturer)' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Brand_Name, Strength, Dosage_Form_ID, Manufacturer_ID, COUNT(*) AS cnt
    FROM Medicine
    GROUP BY Brand_Name, Strength, Dosage_Form_ID, Manufacturer_ID
    HAVING COUNT(*) > 1
) d;

-- Medicine_PackageSize: natural key is Brand_ID + Pack_Size + Pack_Price.
-- The same pack size can legitimately appear twice for a Brand_ID if the
-- containers differ in price (e.g. Colmint 30's pack at ৳180.00 and ৳210.60,
-- Neos-R 5's pack at ৳40.00 and ৳175.00 for different ampoule sizes).
-- Known result: 1 — Unisaline Fruity triplicate caused by upstream Medicine
-- duplicate in raw CSV. Candidate for deduplication in the Data Cleaning project.
SELECT 'Medicine_PackageSize — duplicate Brand_ID + Pack_Size + Pack_Price' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Brand_ID, Pack_Size, Pack_Price, COUNT(*) AS cnt
    FROM Medicine_PackageSize
    GROUP BY Brand_ID, Pack_Size, Pack_Price
    HAVING COUNT(*) > 1
) d;

-- Medicine_PackageContainer: natural key is Brand_ID + Container_Size + Unit_Price.
-- Known result: 3 — Cholera Fluid, Glucose Saline, Normal Saline duplicates
-- caused by upstream Medicine duplicates in the raw CSV.
-- Candidate for deduplication in the Data Cleaning project.
SELECT 'Medicine_PackageContainer — duplicate Brand_ID + Container_Size + Unit_Price' AS Check_Name,
       COUNT(*) AS DuplicateCount
FROM (
    SELECT Brand_ID, Container_Size, Unit_Price, COUNT(*) AS cnt
    FROM Medicine_PackageContainer
    GROUP BY Brand_ID, Container_Size, Unit_Price
    HAVING COUNT(*) > 1
) d;
GO

-- =================================================
-- SECTION 3: NULL / BLANK CHECKS ON CRITICAL FIELDS
-- =================================================

SELECT 'Drug_Class — NULL or blank Drug_Class_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Drug_Class
WHERE Drug_Class_Name IS NULL OR LTRIM(RTRIM(Drug_Class_Name)) = '';

SELECT 'Dosage_Form — NULL or blank Dosage_Form_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Dosage_Form
WHERE Dosage_Form_Name IS NULL OR LTRIM(RTRIM(Dosage_Form_Name)) = '';

SELECT 'Manufacturer — NULL or blank Manufacturer_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Manufacturer
WHERE Manufacturer_Name IS NULL OR LTRIM(RTRIM(Manufacturer_Name)) = '';

SELECT 'Indication — NULL or blank Indication_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Indication
WHERE Indication_Name IS NULL OR LTRIM(RTRIM(Indication_Name)) = '';

SELECT 'Generic — NULL or blank Generic_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Generic
WHERE Generic_Name IS NULL OR LTRIM(RTRIM(Generic_Name)) = '';

-- Drug_Class_ID is NOT NULL in Generic — should always be 0
SELECT 'Generic — NULL Drug_Class_ID' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Generic
WHERE Drug_Class_ID IS NULL;

-- Known result: 0
SELECT 'Medicine — NULL Dosage_Form_ID' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Dosage_Form_ID IS NULL;

-- Known result: 214
SELECT 'Medicine — NULL Generic_ID (no match in Generic)' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Generic_ID IS NULL;

-- Known result: 147
SELECT 'Medicine — NULL Manufacturer_ID (no match in Manufacturer)' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Manufacturer_ID IS NULL;

-- Known result: 0
SELECT 'Medicine — NULL or blank Brand_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Brand_Name IS NULL OR LTRIM(RTRIM(Brand_Name)) = '';

-- Known result: 0
SELECT 'Medicine_PackageSize — NULL Pack_Size' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageSize
WHERE Pack_Size IS NULL;

-- Known result: 0
SELECT 'Medicine_PackageSize — NULL Pack_Price' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageSize
WHERE Pack_Price IS NULL;

-- Checks for NULL Container_Size on rows that also have a NULL Unit_Price —
-- these would be truly empty rows with no useful data.
-- Format B rows (Container_Size = NULL, Unit_Price populated) are expected and valid.
-- Known result: 0
SELECT 'Medicine_PackageContainer — NULL Container_Size and NULL Unit_Price' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Container_Size IS NULL
  AND Unit_Price IS NULL;

-- Known result: 39
-- Breakdown:
--   ~8  rows : Container_Size = 'Not for sale'    — no pricing data in source
--   ~28 rows : Container_Size = 'Price Unavailable' — price pending publication
--   ~3  rows : µg pre-filled syringe rows          — valid container, no listed price
-- All are legitimate source data values, not ETL parsing failures.
SELECT 'Medicine_PackageContainer — NULL Unit_Price' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Unit_Price IS NULL;
GO

-- =================================================
-- SECTION 4: REFERENTIAL INTEGRITY CHECKS
-- Verifies FK relationships are intact.
-- All counts should be 0.
-- =================================================

SELECT 'Generic -> Drug_Class broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Generic g
LEFT JOIN Drug_Class dc ON g.Drug_Class_ID = dc.Drug_Class_ID
WHERE dc.Drug_Class_ID IS NULL;

SELECT 'Medicine -> Dosage_Form broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine m
INNER JOIN Dosage_Form df ON m.Dosage_Form_ID = df.Dosage_Form_ID
WHERE df.Dosage_Form_ID IS NULL;

SELECT 'Medicine -> Generic broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine m
INNER JOIN Generic g ON m.Generic_ID = g.Generic_ID
WHERE g.Generic_ID IS NULL;

SELECT 'Medicine -> Manufacturer broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine m
INNER JOIN Manufacturer mf ON m.Manufacturer_ID = mf.Manufacturer_ID
WHERE mf.Manufacturer_ID IS NULL;

SELECT 'Medicine_PackageSize -> Medicine broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageSize ps
LEFT JOIN Medicine m ON ps.Brand_ID = m.Brand_ID
WHERE m.Brand_ID IS NULL;

SELECT 'Medicine_PackageContainer -> Medicine broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer pc
LEFT JOIN Medicine m ON pc.Brand_ID = m.Brand_ID
WHERE m.Brand_ID IS NULL;

SELECT 'Generic_Indication -> Generic broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Generic_Indication gi
LEFT JOIN Generic g ON gi.Generic_ID = g.Generic_ID
WHERE g.Generic_ID IS NULL;

SELECT 'Generic_Indication -> Indication broken FK' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Generic_Indication gi
LEFT JOIN Indication i ON gi.Indication_ID = i.Indication_ID
WHERE i.Indication_ID IS NULL;
GO

-- =================================================
-- SECTION 5: DATA QUALITY — LEFTOVER SEMICOLONS
-- Package_Container and Unit_Price are dropped by 07b —
-- checks target only columns that survive into final schema.
-- All counts should be 0.
-- =================================================

SELECT 'Medicine — leftover semicolons in Brand_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Brand_Name LIKE '%;%';

SELECT 'Medicine — leftover semicolons in Strength' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Strength LIKE '%;%';

SELECT 'Medicine_PackageContainer — leftover semicolons in Container_Size' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Container_Size LIKE '%;%';
GO

-- =================================================
-- SECTION 6: ENCODING CHECKS
-- Medicine_PackageContainer.Container_Size replaces the dropped
-- Medicine.Package_Container column for encoding verification.
-- =================================================

-- Known result: 0 — ৳ signs were cleaned by 06 and do not appear in Container_Size
SELECT 'Medicine_PackageContainer — currency character (৳) in Container_Size' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Container_Size LIKE '%' + NCHAR(2547) + '%';

SELECT 'Medicine_PackageContainer — non-ASCII characters in Container_Size' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Container_Size COLLATE Latin1_General_BIN LIKE '%[^ -~]%'
  AND Container_Size NOT LIKE '%µ%';

-- Known result: 3 — µg rows legitimately contain the µ character
SELECT 'Medicine_PackageContainer — rows with µ character (expected 3)' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine_PackageContainer
WHERE Container_Size LIKE '%µ%';
GO

-- =================================================
-- SECTION 7: DRUG CLASS CROSS-CHECK
-- Informational — documents the coverage gap between
-- DrugClass.csv and Generic.csv.
-- =================================================

SELECT 'Drug_Class — entries with no linked generics' AS Check_Name,
       COUNT(*) AS Count
FROM Drug_Class dc
LEFT JOIN Generic g ON dc.Drug_Class_ID = g.Drug_Class_ID
WHERE g.Generic_ID IS NULL;

SELECT
    'Has generics' AS Category, COUNT(DISTINCT dc.Drug_Class_ID) AS Count
FROM Drug_Class dc
INNER JOIN Generic g ON dc.Drug_Class_ID = g.Drug_Class_ID
UNION ALL
SELECT
    'No generics'  AS Category, COUNT(DISTINCT dc.Drug_Class_ID) AS Count
FROM Drug_Class dc
LEFT JOIN Generic g ON dc.Drug_Class_ID = g.Drug_Class_ID
WHERE g.Generic_ID IS NULL;

SELECT TOP 20
    dc.Drug_Class_Name,
    COUNT(g.Generic_ID) AS Generic_Count
FROM Drug_Class dc
LEFT JOIN Generic g ON dc.Drug_Class_ID = g.Drug_Class_ID
GROUP BY dc.Drug_Class_Name
ORDER BY Generic_Count DESC;
GO

-- =================================================
-- SECTION 8: MEDICINE_PACKAGESIZE DATA QUALITY
-- Note: Medicine.Unit_Price was dropped by 07b.
-- Price range check covers Medicine_PackageSize.Pack_Price only.
-- =================================================

-- Pack size distribution — review for outliers
SELECT
    Pack_Size,
    COUNT(*) AS Frequency
FROM Medicine_PackageSize
GROUP BY Pack_Size
ORDER BY Frequency DESC;

-- Price range — review for implausible values
SELECT
    MIN(Pack_Price)  AS Min_Pack_Price,
    MAX(Pack_Price)  AS Max_Pack_Price,
    AVG(Pack_Price)  AS Avg_Pack_Price
FROM Medicine_PackageSize;

-- Known result: 0 — ETL only handles up to 3 pack size blocks
SELECT 'Medicine_PackageSize — Brand_IDs with more than 3 pack sizes' AS Check_Name,
       COUNT(*) AS IssueCount
FROM (
    SELECT Brand_ID, COUNT(*) AS cnt
    FROM Medicine_PackageSize
    GROUP BY Brand_ID
    HAVING COUNT(*) > 3
) d;

-- Pack size counts per medicine — distribution
SELECT
    Pack_Size_Count,
    COUNT(*) AS Medicine_Count
FROM (
    SELECT Brand_ID, COUNT(*) AS Pack_Size_Count
    FROM Medicine_PackageSize
    GROUP BY Brand_ID
) counts
GROUP BY Pack_Size_Count
ORDER BY Pack_Size_Count;
GO

-- =================================================
-- SECTION 9: MEDICINE_PACKAGECONTAINER DATA QUALITY
-- =================================================

-- Container count per medicine — distribution
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

-- Price range — review for implausible values
SELECT
    MIN(Unit_Price) AS Min_Unit_Price,
    MAX(Unit_Price) AS Max_Unit_Price,
    AVG(Unit_Price) AS Avg_Unit_Price
FROM Medicine_PackageContainer
WHERE Unit_Price IS NOT NULL;

-- Medicines with no entry in either child table.
-- These are medicines where Package_Container = NULL AND Unit_Price = NULL
-- in the source — no pricing or container data was present at all.
-- Known result: 42 — confirmed as legitimate source data gaps, not ETL failures.
-- Candidate for review in the Data Cleaning project.SELECT 'Medicine — Brand_ID in neither PackageSize nor PackageContainer' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine m
WHERE NOT EXISTS (SELECT 1 FROM Medicine_PackageSize ps WHERE ps.Brand_ID = m.Brand_ID)
  AND NOT EXISTS (SELECT 1 FROM Medicine_PackageContainer pc WHERE pc.Brand_ID = m.Brand_ID);
GO

-- =================================================
-- SECTION 10: SAMPLE DATA PREVIEWS
-- Visual spot-check — review these rows manually.
-- =================================================

SELECT TOP 5 * FROM Drug_Class              ORDER BY Drug_Class_ID;
SELECT TOP 5 * FROM Dosage_Form             ORDER BY Dosage_Form_ID;
SELECT TOP 5 * FROM Manufacturer            ORDER BY Manufacturer_ID;
SELECT TOP 5 * FROM Indication              ORDER BY Indication_ID;
SELECT TOP 5 * FROM Generic                 ORDER BY Generic_ID;
SELECT TOP 5 * FROM Medicine                ORDER BY Brand_ID;
SELECT TOP 5 * FROM Medicine_PackageSize    ORDER BY PackageSize_ID;
SELECT TOP 5 * FROM Medicine_PackageContainer ORDER BY PackageContainer_ID;
SELECT TOP 5 * FROM Generic_Indication      ORDER BY Generic_Indication_ID;

-- Sample joined view of Medicine with all pack sizes
SELECT TOP 20
    m.Brand_ID,
    m.Brand_Name,
    m.Strength,
    ps.Pack_Size,
    ps.Pack_Price
FROM Medicine m
INNER JOIN Medicine_PackageSize ps ON m.Brand_ID = ps.Brand_ID
ORDER BY m.Brand_ID, ps.Pack_Size;

-- Sample joined view of Medicine with all container sizes
SELECT TOP 20
    m.Brand_ID,
    m.Brand_Name,
    m.Strength,
    pc.Container_Size,
    pc.Unit_Price
FROM Medicine m
INNER JOIN Medicine_PackageContainer pc ON m.Brand_ID = pc.Brand_ID
ORDER BY m.Brand_ID, pc.PackageContainer_ID;
GO

-- =================================================
-- SECTION 11: SUMMARY STATISTICS
-- =================================================

-- Top 10 manufacturers by number of medicines
SELECT TOP 10
    mf.Manufacturer_Name,
    COUNT(m.Brand_ID) AS Medicine_Count
FROM Manufacturer mf
LEFT JOIN Medicine m ON mf.Manufacturer_ID = m.Manufacturer_ID
GROUP BY mf.Manufacturer_Name
ORDER BY Medicine_Count DESC;

-- Medicines per dosage form
SELECT
    df.Dosage_Form_Name,
    COUNT(m.Brand_ID) AS Medicine_Count
FROM Dosage_Form df
LEFT JOIN Medicine m ON df.Dosage_Form_ID = m.Dosage_Form_ID
GROUP BY df.Dosage_Form_Name
ORDER BY Medicine_Count DESC;

-- Top 10 most-produced generics by number of brand medicines
SELECT TOP 10
    g.Generic_Name,
    COUNT(m.Brand_ID) AS Brand_Count
FROM Generic g
LEFT JOIN Medicine m ON g.Generic_ID = m.Generic_ID
GROUP BY g.Generic_Name
ORDER BY Brand_Count DESC;

-- Medicine type breakdown
SELECT
    Type,
    COUNT(*) AS Count
FROM Medicine
GROUP BY Type
ORDER BY Count DESC;

-- Generic_Indication pairs loaded
SELECT
    'Generic_Indication pairs loaded' AS Metric,
    COUNT(*)                          AS Count
FROM Generic_Indication;

-- Top 10 generics by number of indications
SELECT TOP 10
    g.Generic_Name,
    COUNT(gi.Indication_ID) AS Indication_Count
FROM Generic g
INNER JOIN Generic_Indication gi ON g.Generic_ID = gi.Generic_ID
GROUP BY g.Generic_Name
ORDER BY Indication_Count DESC;

-- Top 10 indications by number of generics treating them
SELECT TOP 10
    i.Indication_Name,
    COUNT(gi.Generic_ID) AS Generic_Count
FROM Indication i
INNER JOIN Generic_Indication gi ON i.Indication_ID = gi.Indication_ID
GROUP BY i.Indication_Name
ORDER BY Generic_Count DESC;

-- Medicine_PackageSize summary
SELECT
    COUNT(DISTINCT Brand_ID)    AS Medicines_With_Pack_Sizes,
    COUNT(*)                    AS Total_Pack_Size_Rows,
    MIN(Pack_Size)              AS Smallest_Pack,
    MAX(Pack_Size)              AS Largest_Pack,
    MIN(Pack_Price)             AS Cheapest_Pack,
    MAX(Pack_Price)             AS Most_Expensive_Pack
FROM Medicine_PackageSize;

-- Medicine_PackageContainer summary
SELECT
    COUNT(DISTINCT Brand_ID)    AS Medicines_With_Containers,
    COUNT(*)                    AS Total_Container_Rows,
    MIN(Unit_Price)             AS Cheapest_Container,
    MAX(Unit_Price)             AS Most_Expensive_Container
FROM Medicine_PackageContainer;
GO

-- =================================================
-- END OF VALIDATION SCRIPT
-- =================================================
