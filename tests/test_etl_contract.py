import csv
import hashlib
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = PROJECT_ROOT / "scripts"
SOURCE_DIR = PROJECT_ROOT / "source_data"

DATA_SCRIPTS = (
    "01_DrugClass_ETL.sql",
    "02_DosageForm_ETL.sql",
    "03_Manufacturer_ETL.sql",
    "04_Indication_ETL.sql",
    "05_Generic_ETL.sql",
    "06_Medicine_ETL.sql",
    "07_Medicine_PackageSize_ETL.sql",
    "07b_Medicine_PackageContainer_ETL.sql",
    "08_Generic_Indication_ETL.sql",
)

DATABASE_SCRIPTS = (
    "00_CreateDatabase.sql",
    "00_ResetSchema.sql",
    *DATA_SCRIPTS,
)

EXPECTED_SOURCE_FILES = {
    "Dosage_Form.csv": (
        113,
        "b4a833b22db4d7ef635a52871889e1f605af5d3324341f9d0049b40ec7243c67",
    ),
    "Drug_Class.csv": (
        453,
        "cb030971ca5ad3205e9d8373d0e0489d103c57fb467eb177ec28f7c83e0283dd",
    ),
    "Generic.csv": (
        1711,
        "77d9406ecfdd50532338d2b992a019ed6aa3371e0a19a86a170f5ddf3727467b",
    ),
    "Indication.csv": (
        2043,
        "00caf01217f0fd11932da0fa5e221d037412595e27af78b49f2013f7cebc78d2",
    ),
    "Manufacturer.csv": (
        240,
        "e6774c5bd3c4f7cc6429816dcdd849b0544f63799c41ef2b22d29b92f0f2bf09",
    ),
    "Medicine.csv": (
        21714,
        "0b080d2e7081895f13e15e7342a1edca37585eca629a6d628b9679ec61a877af",
    ),
}


class EtlContractTests(unittest.TestCase):
    def test_source_snapshot_is_unchanged(self):
        actual_names = {path.name for path in SOURCE_DIR.glob("*.csv")}
        self.assertEqual(actual_names, set(EXPECTED_SOURCE_FILES))

        for name, (expected_rows, expected_hash) in EXPECTED_SOURCE_FILES.items():
            path = SOURCE_DIR / name
            with path.open("r", encoding="utf-8-sig", newline="") as source_file:
                row_count = sum(1 for _ in csv.DictReader(source_file))

            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(row_count, expected_rows, name)
            self.assertEqual(digest, expected_hash, name)

    def test_medicine_package_formats_fit_the_supported_parser_limits(self):
        path = SOURCE_DIR / "Medicine.csv"
        with path.open("r", encoding="utf-8-sig", newline="") as source_file:
            medicines = list(csv.DictReader(source_file))

        package_sizes = [(row.get("Package Size") or "").strip() for row in medicines]
        containers = [
            (row.get("package container") or "").strip() for row in medicines
        ]

        self.assertLessEqual(max(value.count("৳") for value in package_sizes), 3)
        self.assertLessEqual(max(value.count("৳") for value in containers), 7)

        unpriced_multi_containers = [
            value
            for value in containers
            if value and "৳" not in value and value.count(";") > 2
        ]
        self.assertEqual(unpriced_multi_containers, [])

    def test_data_scripts_have_transaction_guards(self):
        for name in DATA_SCRIPTS:
            sql = (SCRIPT_DIR / name).read_text(encoding="utf-8-sig")
            with self.subTest(script=name):
                self.assertEqual(sql.upper().count("BEGIN TRY"), 1)
                self.assertEqual(sql.upper().count("BEGIN TRANSACTION"), 1)
                self.assertEqual(sql.upper().count("COMMIT TRANSACTION"), 1)
                self.assertEqual(sql.upper().count("BEGIN CATCH"), 1)
                self.assertIn("ROLLBACK TRANSACTION", sql.upper())
                self.assertIn("SET XACT_ABORT ON", sql.upper())
                self.assertLess(
                    sql.upper().index("BEGIN TRANSACTION"),
                    sql.upper().index("COMMIT TRANSACTION"),
                )

    def test_all_database_scripts_use_the_sqlcmd_database_variable(self):
        for name in DATABASE_SCRIPTS:
            sql = (SCRIPT_DIR / name).read_text(encoding="utf-8-sig")
            with self.subTest(script=name):
                self.assertIn("USE [$(DatabaseName)]", sql)

        for name in ("09_Validation.sql", "10_ValidationGate.sql"):
            sql = (PROJECT_ROOT / "tests" / name).read_text(encoding="utf-8-sig")
            with self.subTest(script=name):
                self.assertIn("USE [$(DatabaseName)]", sql)

    def test_reset_requires_explicit_confirmation(self):
        sql = (SCRIPT_DIR / "00_ResetSchema.sql").read_text(encoding="utf-8-sig")
        self.assertIn("$(AllowDestructiveReset)", sql)
        self.assertIn("<> N'YES'", sql)

    def test_database_creation_uses_parameterized_dynamic_sql(self):
        sql = (SCRIPT_DIR / "00_CreateDatabase.sql").read_text(encoding="utf-8-sig")
        self.assertIn("QUOTENAME(@DatabaseName)", sql)
        self.assertIn("EXEC sys.sp_executesql @CreateDatabaseSql", sql)
        self.assertNotIn("EXEC(N'CREATE DATABASE '", sql)

    def test_full_runner_targets_the_etl_database_and_includes_all_steps(self):
        runner = (PROJECT_ROOT / "run_full_etl.sql").read_text(encoding="utf-8-sig")
        self.assertIn(
            ':setvar DatabaseName "PharmaMarketAnalytics_ETL"', runner
        )

        expected_includes = (
            *DATABASE_SCRIPTS,
            "09_Validation.sql",
            "10_ValidationGate.sql",
        )
        positions = []
        for name in expected_includes:
            self.assertEqual(runner.count(name), 1, name)
            positions.append(runner.index(name))
        self.assertEqual(positions, sorted(positions))

        include_lines = [
            line for line in runner.splitlines() if line.lstrip().startswith(":r ")
        ]
        self.assertEqual(len(include_lines), len(expected_includes))
        for line in include_lines:
            self.assertNotIn("$(ProjectRoot)", line)
            self.assertTrue(line.startswith(':r "E:\\'), line)

    def test_validation_runner_targets_the_etl_database(self):
        runner = (PROJECT_ROOT / "run_validation.sql").read_text(
            encoding="utf-8-sig"
        )
        self.assertIn(
            ':setvar DatabaseName "PharmaMarketAnalytics_ETL"', runner
        )
        self.assertEqual(runner.count("09_Validation.sql"), 1)
        self.assertEqual(runner.count("10_ValidationGate.sql"), 1)

    def test_scripts_do_not_reference_the_old_project_location(self):
        old_root = r"E:\Data Analysis\My Projects\PharmaMarket_ETL"
        for path in SCRIPT_DIR.glob("*.sql"):
            with self.subTest(script=path.name):
                self.assertNotIn(old_root, path.read_text(encoding="utf-8-sig"))


if __name__ == "__main__":
    unittest.main()
