# Project 1 - API Data Retrieval and Storage (Books)

Fetches books from the **Open Library Search API** (a real, external, free REST API - no key needed),
stores them in a local SQLite database, and displays them.

## Run
```
python books_pipeline.py                                    # 25 "classic literature" books (needs internet)
python books_pipeline.py --query "george orwell" --limit 40 # different search, more books
python books_pipeline.py --author orwell --since 1940        # filter the displayed rows
python books_pipeline.py --all                              # show every book stored so far
python books_pipeline.py --db my_books.db                   # use a different database file
python books_pipeline.py --source mock --flaky              # offline demo; mock API fails once -> retry
```

| Option | Default | Meaning |
|---|---|---|
| `--query` | `classic literature` | Open Library search terms |
| `--limit` | `25` | Maximum books to fetch |
| `--per-page` | `10` | Books requested per API call |
| `--author`, `--since` | none | Filter what is displayed |
| `--all` | off | Display the whole database, not just this run |
| `--db` | `books.db` | SQLite database file |
| `--source` | `openlibrary` | `mock` = bundled offline data (`data/books.json`) |
| `--flaky` | off | Mock API returns HTTP 503 once to demonstrate retries |
| `--api-url` | none | Any other API returning `[{title, author, publication_year, genre}]` |

## External API used
`GET https://openlibrary.org/search.json?q=<query>&page=<n>&limit=<k>&fields=title,author_name,first_publish_year,subject`

| Open Library field | Stored as |
|---|---|
| `title` | `title` |
| `author_name[0]` (first author) | `author` |
| `first_publish_year` | `publication_year` |
| first genre-like entry in `subject` | `genre` |

## How it works
1. **Fetch** - `iter_open_library` requests page after page until `--limit` books are collected or
   results run out. `get_json` retries server errors (5xx), rate limiting (429) and network errors
   up to 4 attempts with exponential backoff (0.4s, 0.8s, 1.6s), and fails fast on other 4xx errors.
2. **Normalise** - `normalise_open_library` maps Open Library's field names onto the project's
   Book model, so the storage and display code work with any source.
3. **Validate** - `Book.parse` requires a non-empty title and author and an integer year between 1000
   and the current year. Invalid records are logged as WARNING and skipped; the run continues.
4. **Store** - `books.db` is created automatically by `sqlite3.connect()` on the first run, and the
   table by `CREATE TABLE IF NOT EXISTS`. All books are saved in one transaction with
   `INSERT ... ON CONFLICT(title, author) DO UPDATE`, so re-runs update instead of duplicating.
   Title and author use `COLLATE NOCASE` ("dune" = "Dune"). Each row records its `source`,
   `first_seen_at` and `last_synced_at`. Databases from older versions are migrated automatically.
5. **Display** - the books retrieved in this run (or all with `--all`) as an ASCII table, with long
   values truncated, plus books per decade with the top 3 genres.

## Viewing the database
`books.db` is a binary SQLite file, so a text editor can't display it. Use the VS Code
**SQLite Viewer** extension, **DB Browser for SQLite**, or:
```
python -c "import sqlite3; [print(r) for r in sqlite3.connect('books.db').execute('SELECT * FROM books')]"
```
`books.db-wal` / `books.db-shm` are normal SQLite WAL-mode files; keep them with `books.db`.

## Assumptions
- The task names no specific API; Open Library is used because it is free, needs no key, and returns
  title, author and publication year.
- A book's identity is (title, author), case-insensitive; re-fetching updates year, genre and source.
- Only the first listed author is stored.
- Genre is a best-effort label: the first Open Library subject that names a recognisable genre
  (fiction, poetry, essays, biography, ...), otherwise the first subject.
- Data is stored as Open Library provides it. Some `first_publish_year` values and subjects are
  wrong in the source (e.g. a modern biography dated 1657); this is a source data issue.
- `--source mock` uses `data/books.json` (18 records, 2 deliberately invalid) for offline demos only.
- Requires Python 3.10+ and an internet connection (except in mock mode). Standard library only.