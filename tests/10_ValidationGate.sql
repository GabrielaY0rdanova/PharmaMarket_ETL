-- =================================================
-- 10_ValidationGate.sql
-- Blocking validation checks for the current static
-- PharmaMarket source snapshot.
--
-- Run after tests/09_Validation.sql. The script throws
-- an error when a critical expectation is not met.
-- =================================================

USE [$(DatabaseName)];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @Failures TABLE (
    Check_Name NVARCHAR(255) NOT NULL,
    Expected_Value BIGINT NOT NULL,
    Actual_Value BIGINT NOT NULL
);

-- Expected row counts for the preserved source snapshot.
INSERT INTO @Failures (Check_Name, Expected_Value, Actual_Value)
SELECT 'Drug_Class row count', 1599, COUNT(*) FROM Drug_Class HAVING COUNT(*) <> 1599
UNION ALL
SELECT 'Dosage_Form row count', 113, COUNT(*) FROM Dosage_Form HAVING COUNT(*) <> 113
UNION ALL
SELECT 'Manufacturer row count', 240, COUNT(*) FROM Manufacturer HAVING COUNT(*) <> 240
UNION ALL
SELECT 'Indication row count', 2043, COUNT(*) FROM Indication HAVING COUNT(*) <> 2043
UNION ALL
SELECT 'Generic row count', 1711, COUNT(*) FROM Generic HAVING COUNT(*) <> 1711
UNION ALL
SELECT 'Medicine row count', 21708, COUNT(*) FROM Medicine HAVING COUNT(*) <> 21708
UNION ALL
SELECT 'Medicine_PackageSize row count', 14349, COUNT(*) FROM Medicine_PackageSize HAVING COUNT(*) <> 14349
UNION ALL
SELECT 'Medicine_PackageContainer row count', 22707, COUNT(*) FROM Medicine_PackageContainer HAVING COUNT(*) <> 22707
UNION ALL
SELECT 'Generic_Indication row count', 1608, COUNT(*) FROM Generic_Indication HAVING COUNT(*) <> 1608;

-- Required unique business keys.
INSERT INTO @Failures (Check_Name, Expected_Value, Actual_Value)
SELECT 'Duplicate Drug_Class names', 0, COUNT(*)
FROM (
    SELECT Drug_Class_Name FROM Drug_Class GROUP BY Drug_Class_Name HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Duplicate Dosage_Form names', 0, COUNT(*)
FROM (
    SELECT Dosage_Form_Name FROM Dosage_Form GROUP BY Dosage_Form_Name HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Duplicate Manufacturer names', 0, COUNT(*)
FROM (
    SELECT Manufacturer_Name FROM Manufacturer GROUP BY Manufacturer_Name HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Duplicate Indication names', 0, COUNT(*)
FROM (
    SELECT Indication_Name FROM Indication GROUP BY Indication_Name HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Duplicate Generic names', 0, COUNT(*)
FROM (
    SELECT Generic_Name FROM Generic GROUP BY Generic_Name HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Duplicate Generic_Indication pairs', 0, COUNT(*)
FROM (
    SELECT Generic_ID, Indication_ID
    FROM Generic_Indication
    GROUP BY Generic_ID, Indication_ID
    HAVING COUNT(*) > 1
) duplicates
HAVING COUNT(*) <> 0;

-- Referential integrity checks. Nullable foreign keys are checked only
-- when the source row actually contains a foreign-key value.
INSERT INTO @Failures (Check_Name, Expected_Value, Actual_Value)
SELECT 'Orphan Generic.Drug_Class_ID', 0, COUNT(*)
FROM Generic g
LEFT JOIN Drug_Class dc ON g.Drug_Class_ID = dc.Drug_Class_ID
WHERE dc.Drug_Class_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Medicine.Dosage_Form_ID', 0, COUNT(*)
FROM Medicine m
LEFT JOIN Dosage_Form df ON m.Dosage_Form_ID = df.Dosage_Form_ID
WHERE m.Dosage_Form_ID IS NOT NULL AND df.Dosage_Form_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Medicine.Generic_ID', 0, COUNT(*)
FROM Medicine m
LEFT JOIN Generic g ON m.Generic_ID = g.Generic_ID
WHERE m.Generic_ID IS NOT NULL AND g.Generic_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Medicine.Manufacturer_ID', 0, COUNT(*)
FROM Medicine m
LEFT JOIN Manufacturer mf ON m.Manufacturer_ID = mf.Manufacturer_ID
WHERE m.Manufacturer_ID IS NOT NULL AND mf.Manufacturer_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Medicine_PackageSize.Brand_ID', 0, COUNT(*)
FROM Medicine_PackageSize ps
LEFT JOIN Medicine m ON ps.Brand_ID = m.Brand_ID
WHERE m.Brand_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Medicine_PackageContainer.Brand_ID', 0, COUNT(*)
FROM Medicine_PackageContainer pc
LEFT JOIN Medicine m ON pc.Brand_ID = m.Brand_ID
WHERE m.Brand_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Generic_Indication.Generic_ID', 0, COUNT(*)
FROM Generic_Indication gi
LEFT JOIN Generic g ON gi.Generic_ID = g.Generic_ID
WHERE g.Generic_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'Orphan Generic_Indication.Indication_ID', 0, COUNT(*)
FROM Generic_Indication gi
LEFT JOIN Indication i ON gi.Indication_ID = i.Indication_ID
WHERE i.Indication_ID IS NULL
HAVING COUNT(*) <> 0;

-- Critical completeness checks.
INSERT INTO @Failures (Check_Name, Expected_Value, Actual_Value)
SELECT 'NULL Medicine.Dosage_Form_ID', 0, COUNT(*)
FROM Medicine
WHERE Dosage_Form_ID IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'NULL Medicine_PackageSize.Pack_Size', 0, COUNT(*)
FROM Medicine_PackageSize
WHERE Pack_Size IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'NULL Medicine_PackageSize.Pack_Price', 0, COUNT(*)
FROM Medicine_PackageSize
WHERE Pack_Price IS NULL
HAVING COUNT(*) <> 0
UNION ALL
SELECT 'NULL Medicine_PackageContainer.Container_Type', 0, COUNT(*)
FROM Medicine_PackageContainer
WHERE Container_Type IS NULL
HAVING COUNT(*) <> 0;

IF EXISTS (SELECT 1 FROM @Failures)
BEGIN
    SELECT Check_Name, Expected_Value, Actual_Value
    FROM @Failures
    ORDER BY Check_Name;

    THROW 51000, 'PharmaMarket ETL validation failed.', 1;
END;

SELECT 'PASS' AS Validation_Status,
       'All blocking ETL checks passed.' AS Message;
GO
