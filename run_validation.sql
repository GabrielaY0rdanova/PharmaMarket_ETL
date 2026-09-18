-- =================================================
-- run_validation.sql
-- Re-runs ETL validation without rebuilding the database.
-- Open in SSMS with SQLCMD Mode enabled.
-- =================================================

:setvar DatabaseName "PharmaMarketAnalytics_ETL_Test"
:on error exit

PRINT 'Validating database: $(DatabaseName)';
GO

:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\tests\09_Validation.sql"
:r "E:\Data Analysis\My Projects\PharmaMarket Data Platform\PharmaMarket_ETL\tests\10_ValidationGate.sql"

PRINT 'Validation finished for $(DatabaseName).';
GO
