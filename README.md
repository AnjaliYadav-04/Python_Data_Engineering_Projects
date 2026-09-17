# Python Data Engineering Projects

Three independent mini-projects covering the core data-engineering loop:
**fetch data from an API or file → validate and clean it → store or analyse it → present the results.**

| # | Folder | Task | Data source | Output |
|---|--------|------|-------------|--------|
| 1 | [`p1_books_api/`](p1_books_api/) | API data retrieval and storage | [Open Library Search API](https://openlibrary.org/search.json) (external REST API) | SQLite database + console tables |
| 2 | [`p2_scores_viz/`](p2_scores_viz/) | Data processing and visualization | JSON endpoint hosted in this repository (fetched over HTTPS) | Average score + bar-chart dashboard (PNG) + JSON report |
| 3 | [`p3_csv_import/`](p3_csv_import/) | CSV import to a database | `data/users.csv` | SQLite database + rejects CSV |

## Quick start
```
git clone https://github.com/AnjaliYadav-04/Python_Data_Engineering_Projects.git
cd Python_Data_Engineering_Projects

python -m venv venv
venv\Scripts\activate            # Windows  (macOS/Linux: source venv/bin/activate)
python -m pip install -r requirements.txt

cd p1_books_api  && python books_pipeline.py                  && cd ..
cd p2_scores_viz && python scores_analysis.py                 && cd ..
cd p3_csv_import && python csv_to_sqlite.py data/users.csv    && cd ..
```

| Project | Common options |
|---|---|
| 1 | `--query "george orwell" --limit 40`, `--all`, `--author orwell --since 1940`, `--source mock --flaky` (offline) |
| 2 | `--pass-mark 60`, `--api-url <url>`, `--offline` |
| 3 | `--on-conflict update`, `--dry-run`, `--batch-size 500` |

Every script supports `--help`.

## Requirements
- Python 3.10+
- Projects 1 and 3 use only the Python standard library
- Project 2 needs `matplotlib` (installed from `requirements.txt`)
- Internet connection for projects 1 and 2 (both have an offline mode)

## Highlights
- **Resilient HTTP clients**: retries with exponential backoff, pagination, clear error messages
- **Data validation**: invalid records are logged and skipped or reported, never silently stored
- **Idempotent storage**: SQLite upserts, so re-running never creates duplicates
- **Transactions**: imports are all-or-nothing
- **Audit trail**: source and sync timestamps per book; an `import_runs` table for CSV imports
- **Automatic schema migration** for databases created by older versions

## Global assumptions
1. **Project 1** uses a real external API, Open Library, because the task names no specific API and
   Open Library is free, needs no key, and returns title, author and publication year.
   `--source mock` runs it offline with bundled sample data.
2. **Project 2**: no public API provides student test scores, so the sample dataset is published as
   a JSON endpoint in this repository and consumed like a read-only REST API. `--offline` uses the local copy.
3. Student and user data are synthetic/fictional; emails use `example.*` domains.
4. Sample datasets intentionally include **invalid records** so the validation logic is demonstrated.
5. SQLite database files are created automatically next to each script on first run and are not
   committed to the repository.

Each project folder has its own README with detailed design notes and assumptions.