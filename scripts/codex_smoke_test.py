import sqlite3
from contextlib import closing


def main():
    with closing(sqlite3.connect(":memory:")) as database:
        database.execute("CREATE TABLE smoke_test (value INTEGER NOT NULL)")
        database.executemany("INSERT INTO smoke_test VALUES (?)", [(2,), (3,)])
        result = database.execute("SELECT COUNT(*), SUM(value) FROM smoke_test").fetchone()
        if result != (2, 5):
            raise SystemExit(f"FAIL: expected (2, 5), got {result!r}")
    print("PASS: SQLite write, read, and result check")


if __name__ == "__main__":
    main()
