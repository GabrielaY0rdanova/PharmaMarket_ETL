-- =================================================
-- 07_Generic_Indication_ETL.sql
-- ETL script for creating the Generic_Indication junction table
-- Represents a many-to-many relationship between Generics and Indications
-- No data is inserted in this script. Data should be populated via:
--   1. Future mapping CSV
--   2. Manual inserts
--   3. Application logic
-- =================================================

-- ==========================
-- DROP table if exists
-- ==========================
DROP TABLE IF EXISTS Generic_Indication;

-- ==========================
-- CREATE Generic_Indication table
-- ==========================
CREATE TABLE Generic_Indication (
    Generic_Indication_ID INT IDENTITY(1,1) PRIMARY KEY,
    Generic_ID INT NOT NULL,
    Indication_ID INT NOT NULL,

    CONSTRAINT FK_GenericIndication_Generic
        FOREIGN KEY (Generic_ID)
        REFERENCES Generic(Generic_ID),

    CONSTRAINT FK_GenericIndication_Indication
        FOREIGN KEY (Indication_ID)
        REFERENCES Indication(Indication_ID),

    CONSTRAINT UQ_Generic_Indication
        UNIQUE (Generic_ID, Indication_ID)
);
