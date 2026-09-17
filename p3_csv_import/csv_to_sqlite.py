#!/usr/bin/env python3
"""
Project 3 - Robust CSV -> SQLite user importer

Features
  * Streaming read (constant memory) with BOM handling and delimiter auto-detection
  * Flexible header mapping: "Full Name", "E-mail", "email_address" ... all resolve to canonical fields
  * Per-row validation & normalisation (trimmed names, lower-cased emails, multi-format dates -> ISO 8601)
  * Duplicate handling inside the file AND against the database (--on-conflict skip|update)
  * Batched executemany inside a single transaction (all-or-nothing), --dry-run to validate only
  * Rejected rows written to a rejects CSV with line number + reason
  * Every run is recorded in an import_runs audit table

Usage
  python csv_to_sqlite.py data/users.csv
  python csv_to_sqlite.py data/users.csv --db users.db --on-conflict update --batch-size 500
  python csv_to_sqlite.py data/users.csv --dry-run
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass, field, astuple
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Iterator

log = logging.getLogger("importer")

# Canonical field -> accepted header spellings (compared after normalising case/spaces/punctuation)
HEADER_ALIASES = {
    "name": {"name", "fullname", "username", "customername"},
    "email": {"email", "emailaddress", "mail"},
    "age": {"age", "years"},
    "city": {"city", "town", "location"},
    "signup_date": {"signupdate", "joined", "joindate", "createdat", "registrationdate"},
}
REQUIRED = ("name", "email")
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y/%m/%d")

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL CHECK (length(name) > 0),
    email       TEXT NOT NULL UNIQUE,
    age         INTEGER CHECK (age IS NULL OR age BETWEEN 0 AND 120),
    city        TEXT,
    signup_date TEXT,
    imported_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_city ON users(city);
CREATE TABLE IF NOT EXISTS import_runs (
    id          INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    rows_read   INTEGER, inserted INTEGER, updated INTEGER, skipped INTEGER, rejected INTEGER
);
"""


@dataclass(slots=True)
class User:
    name: str
    email: str
    age: int | None
    city: str | None
    signup_date: str | None


@dataclass
class Stats:
    rows_read: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    rejects: list[tuple[int, str, dict]] = field(default_factory=list)


# --------------------------------------------------------------------------- reading
def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z]", "", h.lower())


def build_header_map(headers: list[str]) -> dict[str, str]:
    """Map canonical field -> actual CSV header. Raises if required columns are missing."""
    mapping = {}
    for raw in headers:
        key = _norm_header(raw)
        for canonical, aliases in HEADER_ALIASES.items():
            if key in aliases and canonical not in mapping:
                mapping[canonical] = raw
    missing = [f for f in REQUIRED if f not in mapping]
    if missing:
        raise SystemExit(f"CSV is missing required column(s): {', '.join(missing)} (found: {headers})")
    unmapped = [h for h in headers if h not in mapping.values()]
    if unmapped:
        log.info("ignoring unmapped columns: %s", unmapped)
    return mapping


def read_rows(path: Path) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield (line_number, canonical_row). utf-8-sig strips an Excel BOM if present."""
    with path.open(newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(fh, dialect=dialect)
        header_map = build_header_map(reader.fieldnames or [])
        for row in reader:
            if not any((v or "").strip() for v in row.values()):
                continue  # skip blank lines
            yield reader.line_num, {f: (row.get(col) or "").strip() for f, col in header_map.items()}


# --------------------------------------------------------------------------- validation
def parse_date(value: str) -> str | None:
    if not value:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"unrecognised date {value!r}")


def validate(row: dict[str, str]) -> User:
    name = re.sub(r"\s+", " ", row.get("name", ""))
    if not name:
        raise ValueError("name is empty")
    email = row.get("email", "").lower()
    if not EMAIL_RE.fullmatch(email):
        raise ValueError(f"invalid email {email!r}")
    age = None
    if row.get("age"):
        if not row["age"].isdigit():
            raise ValueError(f"age not an integer: {row['age']!r}")
        age = int(row["age"])
        if age > 120:
            raise ValueError(f"age {age} out of range")
    return User(name, email, age, row.get("city") or None, parse_date(row.get("signup_date", "")))


def validated_users(path: Path, stats: Stats) -> Iterator[User]:
    seen: dict[str, int] = {}
    for line, row in read_rows(path):
        stats.rows_read += 1
        try:
            user = validate(row)
        except ValueError as exc:
            stats.rejects.append((line, str(exc), row))
            continue
        if user.email in seen:
            stats.rejects.append((line, f"duplicate email in file (first at line {seen[user.email]})", row))
            continue
        seen[user.email] = line
        yield user


# --------------------------------------------------------------------------- writing
def batched(it, size):
    it = iter(it)
    while chunk := list(islice(it, size)):
        yield chunk


def import_users(conn: sqlite3.Connection, users: Iterator[User], stats: Stats,
                 on_conflict: str, batch_size: int) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conflict_sql = {
        "skip": "DO NOTHING",
        "update": "DO UPDATE SET name=excluded.name, age=excluded.age, city=excluded.city, "
                  "signup_date=excluded.signup_date, imported_at=excluded.imported_at",
    }[on_conflict]
    sql = (f"INSERT INTO users (name, email, age, city, signup_date, imported_at) "
           f"VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(email) {conflict_sql}")

    count = lambda: conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    for batch in batched(users, batch_size):
        before, changes_before = count(), conn.total_changes
        conn.executemany(sql, [(*astuple(u), now) for u in batch])
        new_rows = count() - before
        touched = conn.total_changes - changes_before
        stats.inserted += new_rows
        stats.updated += touched - new_rows
        stats.skipped += len(batch) - touched
        log.debug("batch of %d: +%d new, %d touched", len(batch), new_rows, touched)


def write_rejects(path: Path, rejects) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["line", "reason", "name", "email", "age", "city", "signup_date"])
        for line, reason, row in rejects:
            w.writerow([line, reason, *(row.get(k, "") for k in HEADER_ALIASES)])


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv_file", type=Path)
    ap.add_argument("--db", type=Path, default=Path(__file__).resolve().parent / "users.db")
    ap.add_argument("--on-conflict", choices=("skip", "update"), default="skip",
                    help="what to do when the email already exists in the database")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--rejects", type=Path, help="rejects CSV path (default: <csv>_rejects.csv)")
    ap.add_argument("--dry-run", action="store_true", help="validate and report, but roll back")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)-7s %(message)s")

    if not args.csv_file.is_file():
        log.error("file not found: %s", args.csv_file)
        return 2

    stats = Stats()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with closing(sqlite3.connect(args.db)) as conn:
        conn.executescript(SCHEMA)
        try:
            conn.execute("BEGIN")
            import_users(conn, validated_users(args.csv_file, stats), stats, args.on_conflict, args.batch_size)
            if args.dry_run:
                conn.rollback()
                log.info("dry run - no changes committed")
            else:
                conn.execute(
                    "INSERT INTO import_runs (source_file, started_at, rows_read, inserted, updated, skipped, rejected)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (str(args.csv_file), started, stats.rows_read, stats.inserted,
                     stats.updated, stats.skipped, len(stats.rejects)))
                conn.commit()
        except (sqlite3.Error, csv.Error) as exc:
            conn.rollback()
            log.error("import aborted, transaction rolled back: %s", exc)
            return 1

        if stats.rejects:
            rej_path = args.rejects or args.csv_file.with_name(args.csv_file.stem + "_rejects.csv")
            write_rejects(rej_path, stats.rejects)
            for line, reason, _ in stats.rejects:
                log.warning("line %d rejected: %s", line, reason)
            log.info("rejected rows written to %s", rej_path)

        print(f"\nrows read {stats.rows_read} | inserted {stats.inserted} | updated {stats.updated} | "
              f"skipped (already in DB) {stats.skipped} | rejected {len(stats.rejects)}")
        if not args.dry_run:
            print(f"\nusers table now holds {conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]} rows:")
            for r in conn.execute("SELECT id, name, email, age, city, signup_date FROM users ORDER BY id"):
                print("  ", r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
