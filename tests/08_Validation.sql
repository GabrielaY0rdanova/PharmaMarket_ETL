-- =================================================
-- 08_Validation.sql
-- Sanity checks and sample previews for PharmaMarketAnalytics
-- Run this script AFTER all ETL scripts (00 through 07)
--
-- Expected results summary:
--   Section 1   — all RowCounts > 0
--   Section 2   — all DuplicateCounts = 0 (except Medicine: 59 known true duplicates)
--   Section 3   — most IssueCounts = 0 (see documented exceptions)
--   Section 4   — all IssueCounts = 0
--   Section 5   — all IssueCounts = 0
--   Section 6   — review as informational
--   Sections 7-8 — review manually for sense-checking
-- =================================================

USE PharmaMarketAnalytics;
GO

-- =================================================
-- SECTION 1: ROW COUNTS
-- Quick confirmation that all tables were populated.
-- =================================================

SELECT 'Drug_Class'          AS TableName, COUNT(*) AS [RowCount] FROM Drug_Class
UNION ALL
SELECT 'Dosage_Form',                       COUNT(*) FROM Dosage_Form
UNION ALL
SELECT 'Manufacturer',                      COUNT(*) FROM Manufacturer
UNION ALL
SELECT 'Indication',                        COUNT(*) FROM Indication
UNION ALL
SELECT 'Generic',                           COUNT(*) FROM Generic
UNION ALL
SELECT 'Medicine',                          COUNT(*) FROM Medicine
UNION ALL
SELECT 'Generic_Indication',                COUNT(*) FROM Generic_Indication
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
-- Cause: combination drugs (multiple active ingredients e.g. "325 mg+37.5 mg")
-- and herbal medicines do not map to a single generic.
-- This is a source data limitation, not an ETL error.
-- Candidate for further classification in the Data Cleaning project.
SELECT 'Medicine — NULL Generic_ID (no match in Generic)' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Generic_ID IS NULL;

-- Known result: 147
-- Cause: manufacturer name mismatches between Medicine.csv and Manufacturer.csv
-- (spelling variations, foreign manufacturers absent from Manufacturer.csv).
-- All 147 are allopathic medicines — not a categorization issue like Generic NULLs.
-- Candidate for manual mapping or fuzzy matching in the Data Cleaning project.
SELECT 'Medicine — NULL Manufacturer_ID (no match in Manufacturer)' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Manufacturer_ID IS NULL;

-- Known result: 0
SELECT 'Medicine — NULL or blank Brand_Name' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Brand_Name IS NULL OR LTRIM(RTRIM(Brand_Name)) = '';
GO

-- =================================================
-- SECTION 4: REFERENTIAL INTEGRITY CHECKS
-- Verifies FK relationships are intact.
-- All counts should be 0.
-- =================================================

-- Generic -> Drug_Class
-- Script 05 inserts missing Drug Classes before linking — should always be 0
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
-- Script 06 replaced semicolons with commas in Medicine.
-- Confirms no semicolons survived into the final table.
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

SELECT 'Medicine — leftover semicolons in Package_Container' AS Check_Name,
       COUNT(*) AS IssueCount
FROM Medicine
WHERE Package_Container LIKE '%;%';
GO

-- =================================================
-- SECTION 6: DRUG CLASS CROSS-CHECK
-- Informational — documents the coverage gap between
-- DrugClass.csv and Generic.csv.
--
-- Known findings:
-- 1. 421 out of 1599 drug classes (26%) have no linked generics.
--    Cause: DrugClass.csv and Generic.csv were compiled independently.
--    Not an ETL error — source data gap.
--
-- 2. Drug_Class_Name contains embedded indication data separated by commas
--    e.g. "Drugs for miotics and glaucoma,Open angle glaucoma"
--    Cause: Drug_Class.csv had combined fields not split during ETL.
--
-- 3. A row with Drug_Class_Name = ',' exists with 51 linked generics.
--    Cause: parsing artifact from the comma concatenation issue above.
--
-- 4. Encoding artifacts present e.g. "Parkinsonâ€™s disease" (garbled apostrophe).
--    Cause: UTF-8 encoding mismatch in source CSV.
--
-- All four are candidates for the Data Cleaning project.
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
-- SECTION 7: SAMPLE DATA PREVIEWS
-- Visual spot-check — review these rows manually.
-- =================================================

SELECT TOP 5 * FROM Drug_Class        ORDER BY Drug_Class_ID;
SELECT TOP 5 * FROM Dosage_Form       ORDER BY Dosage_Form_ID;
SELECT TOP 5 * FROM Manufacturer      ORDER BY Manufacturer_ID;
SELECT TOP 5 * FROM Indication        ORDER BY Indication_ID;
SELECT TOP 5 * FROM Generic           ORDER BY Generic_ID;
SELECT TOP 5 * FROM Medicine          ORDER BY Brand_ID;
SELECT TOP 5 * FROM Generic_Indication ORDER BY Generic_Indication_ID;
GO

-- =================================================
-- SECTION 8: SUMMARY STATISTICS
-- Quick data profile — useful for README documentation.
-- =================================================

-- Top 10 manufacturers by number of medicines
-- Known result: Incepta Pharmaceuticals Ltd. leads with 1182 medicines
SELECT TOP 10
    mf.Manufacturer_Name,
    COUNT(m.Brand_ID) AS Medicine_Count
FROM Manufacturer mf
LEFT JOIN Medicine m ON mf.Manufacturer_ID = m.Manufacturer_ID
GROUP BY mf.Manufacturer_Name
ORDER BY Medicine_Count DESC;

-- Medicines per dosage form
-- Known result: Tablet dominates at 9324 (~44% of all medicines)
-- Note: Bolus Tablet = 0 (exists in Dosage_Form but no medicines link to it)
SELECT
    df.Dosage_Form_Name,
    COUNT(m.Brand_ID) AS Medicine_Count
FROM Dosage_Form df
LEFT JOIN Medicine m ON df.Dosage_Form_ID = m.Dosage_Form_ID
GROUP BY df.Dosage_Form_Name
ORDER BY Medicine_Count DESC;

-- Top 10 most-produced generics by number of brand medicines
-- Known result: dominated by antibiotics (Cephalosporins, Azithromycin, Ciprofloxacin)
-- reflecting the Bangladesh pharmaceutical market
SELECT TOP 10
    g.Generic_Name,
    COUNT(m.Brand_ID) AS Brand_Count
FROM Generic g
LEFT JOIN Medicine m ON g.Generic_ID = m.Generic_ID
GROUP BY g.Generic_Name
ORDER BY Brand_Count DESC;

-- Medicine type breakdown
-- Known result: allopathic 21357 (98%), herbal 351 (2%)
SELECT
    Type,
    COUNT(*) AS Count
FROM Medicine
GROUP BY Type
ORDER BY Count DESC;

-- Generic_Indication pairs loaded
-- Known result: 1608
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
GO

-- =================================================
-- END OF VALIDATION SCRIPT
-- =================================================
