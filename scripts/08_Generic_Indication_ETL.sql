-- =================================================
-- 08_Generic_Indication_ETL.sql
-- Populates the Generic_Indication junction table
-- from the indication column in Generic.csv
-- =================================================

USE PharmaMarketAnalytics;
GO

-- ==========================
-- DROP and recreate table
-- ==========================
DROP TABLE IF EXISTS Generic_Indication;

CREATE TABLE Generic_Indication (
    Generic_Indication_ID INT IDENTITY(1,1) PRIMARY KEY,
    Generic_ID            INT NOT NULL,
    Indication_ID         INT NOT NULL,

    CONSTRAINT FK_GenericIndication_Generic
        FOREIGN KEY (Generic_ID) REFERENCES Generic(Generic_ID),

    CONSTRAINT FK_GenericIndication_Indication
        FOREIGN KEY (Indication_ID) REFERENCES Indication(Indication_ID),

    CONSTRAINT UQ_Generic_Indication
        UNIQUE (Generic_ID, Indication_ID)
);

-- ==========================
-- DROP staging table if exists
-- ==========================
DROP TABLE IF EXISTS Staging_Generic;

-- ==========================
-- CREATE staging table
-- ==========================
CREATE TABLE Staging_Generic (
    Generic_ID   NVARCHAR(255),
    Generic_Name NVARCHAR(255),
    Slug         NVARCHAR(255),
    Drug_Class   NVARCHAR(255),
    Indication   NVARCHAR(500)
);

-- ==========================
-- BULK INSERT
-- ==========================
-- IMPORTANT: Update file path to match your local machine.
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
-- INSERT INTO Generic_Indication
-- ==========================
-- Matches cleaned indication names from staging to Indication table,
-- then maps generic names to Generic table.
-- Handles the one UTF-8 encoding artifact (CutaneousÂ) via REPLACE.
-- Skips rows with no indication or no match in either lookup table.
-- ==========================
WITH Cleaned AS (
    SELECT
        Clean_Generic_Name = LTRIM(RTRIM(REPLACE(Generic_Name, CHAR(160), ''))),
        Clean_Indication   = LTRIM(RTRIM(
                                REPLACE(
                                    REPLACE(Indication, CHAR(194), ''),  -- strip Â artifact
                                CHAR(160), '')
                             ))
    FROM Staging_Generic
    WHERE Indication IS NOT NULL
      AND LTRIM(RTRIM(Indication)) <> ''
)
INSERT INTO Generic_Indication (Generic_ID, Indication_ID)
SELECT DISTINCT
    g.Generic_ID,
    i.Indication_ID
FROM Cleaned c
INNER JOIN Generic g
    ON c.Clean_Generic_Name = g.Generic_Name
INNER JOIN Indication i
    ON c.Clean_Indication = i.Indication_Name;

-- ==========================
-- CLEANUP
-- ==========================
DROP TABLE Staging_Generic;

-- ==========================
-- VERIFY
-- ==========================
SELECT COUNT(*) AS Generic_Indication_Count FROM Generic_Indication;