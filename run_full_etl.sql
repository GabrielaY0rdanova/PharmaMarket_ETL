-- =================================================
-- run_full_etl.sql
-- Complete PharmaMarket ETL rebuild for SSMS SQLCMD Mode.
--
-- The default target is a disposable test database. Change the variables
-- only when you intentionally want to use another database or project path.
-- =================================================

:setvar DatabaseName "PharmaMarketAnalytics_ETL_Test"
:setvar AllowDestructiveReset "YES"
:on error exit

PRINT 'Target database: $(DatabaseName)';
GO

:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\00_CreateDatabase.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\00_ResetSchema.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\01_DrugClass_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\02_DosageForm_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\03_Manufacturer_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\04_Indication_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\05_Generic_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\06_Medicine_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\07_Medicine_PackageSize_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\07b_Medicine_PackageContainer_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\scripts\08_Generic_Indication_ETL.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\tests\09_Validation.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\tests\10_ValidationGate.sql"

PRINT 'Complete ETL rebuild and validation finished for $(DatabaseName).';
GO
