#!/usr/bin/env python3
"""
Project 1 - Book catalogue pipeline
External REST API (Open Library)  ->  validation  ->  SQLite (idempotent upsert)  ->  console report

Data sources
  * openlibrary (default) - the public Open Library Search API, https://openlibrary.org/search.json
                            (free, no API key; needs an internet connection)
  * --api-url URL         - any other API returning [{title, author, publication_year, genre}]
                            or {"data": [...], "total_pages": N}
  * --source mock         - bundled offline mock server serving data/books.json (demo / no internet)

Features
  * Paginated API client with retry + exponential backoff (handles 5xx / 429 / network errors)
  * Adapter that maps Open Library's response (author_name[], first_publish_year, subject[])
    onto our Book model, so storage/display code is source-independent
  * Strict validation; bad records are logged and skipped, not fatal
  * Idempotent UPSERT keyed on (title, author) - re-running never creates duplicates
  * Automatic schema migration for databases created by older versions of this script

Usage
  python books_pipeline.py                                   # 25 classic-literature books from Open Library
  python books_pipeline.py --query "george orwell" --limit 40
  python books_pipeline.py --query tolkien --author tolkien --since 1950
  python books_pipeline.py --all                             # show everything stored so far
  python books_pipeline.py --source mock --flaky             # offline demo, shows retry
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import threading
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable, Iterator

HERE = Path(__file__).resolve().parent
log = logging.getLogger("books")


# --------------------------------------------------------------------------- model
@dataclass(frozen=True, slots=True)
class Book:
    title: str
    author: str
    publication_year: int
    genre: str | None = None

    @classmethod
    def parse(cls, raw: dict) -> "Book":
        title = str(raw.get("title") or "").strip()
        author = str(raw.get("author") or "").strip()
        if not title:
            raise ValueError("missing title")
        if not author:
            raise ValueError("missing author")
        try:
            year = int(raw.get("publication_year"))
        except (TypeError, ValueError):
            raise ValueError(f"non-numeric publication_year {raw.get('publication_year')!r}") from None
        if not 1000 <= year <= date.today().year:
            raise ValueError(f"publication_year {year} out of range")
        genre = (str(raw["genre"]).strip() or None) if raw.get("genre") else None
        return cls(title, author, year, genre)


# --------------------------------------------------------------------------- http client
class ApiError(RuntimeError):
    pass


def get_json(url: str, *, attempts: int = 4, backoff: float = 0.4, timeout: float = 20.0) -> dict:
    """GET a JSON document, retrying transient failures with exponential backoff."""
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "books-pipeline/1.0 (student data project)"}
    )
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            retryable = exc.code >= 500 or exc.code == 429
            if not retryable:
                raise ApiError(f"HTTP {exc.code} for {url}") from exc
            problem = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            problem = type(exc).__name__
        except json.JSONDecodeError as exc:
            raise ApiError(f"invalid JSON from {url}") from exc

        if attempt == attempts:
            raise ApiError(f"giving up on {url} after {attempts} attempts ({problem})")
        wait = backoff * 2 ** (attempt - 1)
        log.warning("attempt %d/%d failed (%s); retrying in %.1fs", attempt, attempts, problem, wait)
        time.sleep(wait)
    raise AssertionError("unreachable")


def iter_api_records(base_url: str, per_page: int) -> Iterator[dict]:
    """Walk every page of the API. Also accepts a plain JSON list (non-paginated APIs)."""
    page = 1
    while True:
        sep = "&" if "?" in base_url else "?"
        payload = get_json(f"{base_url}{sep}{urllib.parse.urlencode({'page': page, 'per_page': per_page})}")
        if isinstance(payload, list):          # non-paginated API
            yield from payload
            return
        yield from payload.get("data", [])
        if page >= int(payload.get("total_pages", 1)):
            return
        page += 1


OPEN_LIBRARY_URL = "https://openlibrary.org/search.json"


GENRE_KEYWORDS = ("science fiction", "fantasy", "fiction", "poetry", "drama", "essays", "biography",
                  "history", "philosophy", "romance", "mystery", "horror", "adventure", "criticism")


def pick_genre(subjects: list[str]) -> str | None:
    """Open Library 'subjects' mix genres with topics ("Aunts", "Gold discoveries").
    Prefer the first subject that names a recognisable genre; fall back to the first subject."""
    for subject in subjects:                      # keep Open Library's own ordering
        if any(keyword in subject.lower() for keyword in GENRE_KEYWORDS):
            return subject.strip()[:40]
    return subjects[0].strip()[:40] if subjects else None


def normalise_open_library(doc: dict) -> dict:
    """Map one Open Library search 'doc' onto the fields Book.parse expects."""
    authors = doc.get("author_name") or []
    subjects = doc.get("subject") or []
    return {
        "title": doc.get("title"),
        "author": authors[0] if authors else "",
        "publication_year": doc.get("first_publish_year"),
        "genre": pick_genre(subjects),
    }


def iter_open_library(query: str, limit: int, per_page: int, base_url: str = OPEN_LIBRARY_URL) -> Iterator[dict]:
    """Page through Open Library search results until `limit` records or results run out."""
    fetched, page = 0, 1
    per_page = min(per_page, limit)
    while fetched < limit:
        params = {"q": query, "page": page, "limit": per_page,
                  "fields": "title,author_name,first_publish_year,subject"}
        payload = get_json(f"{base_url}?{urllib.parse.urlencode(params)}")
        docs = payload.get("docs", [])
        if page == 1:
            log.info("Open Library reports %s matches for %r", payload.get("numFound", "?"), query)
        if not docs:
            return
        for doc in docs[: limit - fetched]:
            fetched += 1
            yield normalise_open_library(doc)
        if page * per_page >= int(payload.get("numFound", 0)):
            return
        page += 1


# --------------------------------------------------------------------------- storage
SCHEMA = """
CREATE TABLE IF NOT EXISTS books (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    title            TEXT    NOT NULL COLLATE NOCASE,
    author           TEXT    NOT NULL COLLATE NOCASE,
    publication_year INTEGER NOT NULL CHECK (publication_year BETWEEN 1000 AND 9999),
    genre            TEXT,
    source           TEXT,
    first_seen_at    TEXT    NOT NULL,
    last_synced_at   TEXT    NOT NULL,
    UNIQUE (title, author)
);
CREATE INDEX IF NOT EXISTS idx_books_year   ON books(publication_year);
CREATE INDEX IF NOT EXISTS idx_books_author ON books(author);
"""

UPSERT = """
INSERT INTO books (title, author, publication_year, genre, source, first_seen_at, last_synced_at)
VALUES (:title, :author, :publication_year, :genre, :source, :now, :now)
ON CONFLICT (title, author) DO UPDATE SET
    publication_year = excluded.publication_year,
    genre            = excluded.genre,
    source           = excluded.source,
    last_synced_at   = excluded.last_synced_at
"""


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # migrate databases created before the 'source' column existed
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(books)")}
    if "source" not in columns:
        conn.execute("ALTER TABLE books ADD COLUMN source TEXT")
        conn.execute("UPDATE books SET source = 'mock' WHERE source IS NULL")  # older runs only had mock data
        conn.commit()
        log.info("migrated existing database: added 'source' column")
    return conn


def store(conn: sqlite3.Connection, books: Iterable[Book], source: str) -> tuple[int, int, str]:
    """Upsert all books in one transaction. Returns (inserted, updated, sync_timestamp)."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = [
        {"title": b.title, "author": b.author, "publication_year": b.publication_year,
         "genre": b.genre, "source": source, "now": now}
        for b in books
    ]
    with conn:  # commit on success, rollback on error
        before = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        conn.executemany(UPSERT, rows)
        after = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    inserted = after - before
    return inserted, len(rows) - inserted, now


def query_books(conn, author: str | None, since: int | None, synced_at: str | None = None) -> list[sqlite3.Row]:
    clauses, params = [], []
    if synced_at:
        clauses.append("last_synced_at = ?")
        params.append(synced_at)
    if author:
        clauses.append("author LIKE ?")
        params.append(f"%{author}%")
    if since is not None:
        clauses.append("publication_year >= ?")
        params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"SELECT id, title, author, publication_year, genre, source FROM books {where} "
        "ORDER BY publication_year, title", params
    ).fetchall()


# --------------------------------------------------------------------------- display
def display_width(text: str) -> int:
    """Terminal columns used by text (wide CJK characters take 2, combining marks take 0)."""
    return sum(0 if unicodedata.combining(ch) else 2 if unicodedata.east_asian_width(ch) in "WF" else 1
               for ch in text)


def clip(text: str, max_width: int) -> str:
    if display_width(text) <= max_width:
        return text
    out = ""
    for ch in text:
        if display_width(out + ch) > max_width - 1:
            break
        out += ch
    return out + "…"


def render_table(rows, headers: list[str], max_widths: dict[int, int] | None = None) -> str:
    """ASCII table; columns listed in max_widths (index -> width) are truncated with an ellipsis."""
    if not rows:
        return "(no rows)"
    max_widths = max_widths or {}
    cells = [[clip("" if v is None else unicodedata.normalize("NFC", str(v)), max_widths.get(i, 10**6))
              for i, v in enumerate(row)] for row in rows]
    widths = [max(display_width(h), *(display_width(r[i]) for r in cells)) for i, h in enumerate(headers)]
    pad = lambda v, w: v + " " * (w - display_width(v))
    line = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    fmt = lambda vals: "| " + " | ".join(pad(v, w) for v, w in zip(vals, widths)) + " |"
    return "\n".join([line, fmt(headers), line, *map(fmt, cells), line])


def decade_summary(rows) -> list[tuple]:
    """Books per decade with the 3 most common genres, computed from the displayed rows."""
    counts: dict[int, int] = Counter()
    genres: dict[int, Counter] = defaultdict(Counter)
    for row in rows:
        decade = row["publication_year"] // 10 * 10
        counts[decade] += 1
        if row["genre"]:
            genres[decade][row["genre"]] += 1
    return [(f"{d}s", counts[d], " | ".join(g for g, _ in genres[d].most_common(3)))
            for d in sorted(counts)]


# --------------------------------------------------------------------------- mock API
def _mock_handler(records: list[dict], flaky: bool):
    state = {"should_fail": flaky}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            url = urllib.parse.urlparse(self.path)
            if url.path != "/api/books":
                return self._reply(404, {"error": "not found"})
            if state["should_fail"]:
                state["should_fail"] = False
                return self._reply(503, {"error": "service warming up"})
            qs = urllib.parse.parse_qs(url.query)
            try:
                page = max(1, int(qs.get("page", ["1"])[0]))
                per_page = min(100, max(1, int(qs.get("per_page", ["10"])[0])))
            except ValueError:
                return self._reply(400, {"error": "page and per_page must be integers"})
            total_pages = max(1, -(-len(records) // per_page))
            start = (page - 1) * per_page
            self._reply(200, {"data": records[start:start + per_page], "page": page,
                              "per_page": per_page, "total": len(records), "total_pages": total_pages})

        def _reply(self, status: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):  # keep console clean
            pass

    return Handler


@contextmanager
def mock_api(data_file: Path, flaky: bool) -> Iterator[str]:
    records = json.loads(data_file.read_text(encoding="utf-8"))
    server = ThreadingHTTPServer(("127.0.0.1", 0), _mock_handler(records, flaky))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/api/books"
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- orchestration
def run(records: Iterable[dict], source: str, args) -> int:
    valid, rejected = [], 0
    for i, raw in enumerate(records, start=1):
        try:
            valid.append(Book.parse(raw))
        except ValueError as exc:
            rejected += 1
            log.warning("record #%d rejected: %s -> %s", i, exc, raw)

    with closing(open_db(args.db)) as conn:
        inserted, updated, synced_at = store(conn, valid, source)
        log.info("valid=%d rejected=%d inserted=%d updated=%d", len(valid), rejected, inserted, updated)

        rows = query_books(conn, args.author, args.since, None if args.all else synced_at)
        total = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        scope = "all stored books" if args.all else "books retrieved in this run"
        print(f"\n{scope.capitalize()} ({len(rows)} shown, {total} total in {args.db.name})")
        print(render_table(rows, ["ID", "Title", "Author", "Year", "Genre", "Source"],
                           max_widths={1: 45, 2: 25, 4: 28}))
        print("\nBooks per decade")
        print(render_table(decade_summary(rows), ["Decade", "Books", "Top genres"], max_widths={2: 60}))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=("openlibrary", "mock"), default="openlibrary")
    p.add_argument("--query", default="classic literature", help="Open Library search terms")
    p.add_argument("--limit", type=int, default=25, help="max books to fetch from Open Library")
    p.add_argument("--open-library-url", default=OPEN_LIBRARY_URL, help=argparse.SUPPRESS)
    p.add_argument("--api-url", help="any other API with the generic JSON shape (overrides --source)")
    p.add_argument("--data", type=Path, default=HERE / "data" / "books.json", help="mock API data file")
    p.add_argument("--db", type=Path, default=HERE / "books.db")
    p.add_argument("--per-page", type=int, default=10)
    p.add_argument("--flaky", action="store_true", help="mock API returns 503 once to demo retries")
    p.add_argument("--author", help="filter display by author substring")
    p.add_argument("--since", type=int, help="filter display to books published in/after this year")
    p.add_argument("--all", action="store_true", help="display every book in the database, not just this run")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    if hasattr(sys.stdout, "reconfigure"):  # never crash on consoles that can't print some characters
        sys.stdout.reconfigure(errors="replace")
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    try:
        if args.api_url:
            log.info("fetching from %s", args.api_url)
            return run(iter_api_records(args.api_url, args.per_page), "custom-api", args)
        if args.source == "openlibrary":
            log.info("fetching from Open Library (%s)", args.open_library_url)
            return run(iter_open_library(args.query, args.limit, args.per_page, args.open_library_url),
                       "openlibrary", args)
        with mock_api(args.data, args.flaky) as url:
            log.info("fetching from offline mock API %s", url)
            return run(iter_api_records(url, args.per_page), "mock", args)
    except ApiError as exc:
        log.error("%s", exc)
        log.error("no internet? try the offline demo:  python books_pipeline.py --source mock")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())