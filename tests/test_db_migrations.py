from app.db import _migration_checksum, _migration_files


def test_migration_files_returns_sorted_sql_only(tmp_path):
    (tmp_path / "0002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a migration", encoding="utf-8")

    files = _migration_files(tmp_path)

    assert [path.name for path in files] == ["0001_first.sql", "0002_second.sql"]


def test_migration_checksum_is_stable_for_same_payload():
    ddl = "CREATE TABLE t(id INT);"

    checksum_a = _migration_checksum(ddl)
    checksum_b = _migration_checksum(ddl)

    assert checksum_a == checksum_b
