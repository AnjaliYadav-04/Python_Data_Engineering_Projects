# Project 3 - CSV Data Import to a Database (Users)

```
python csv_to_sqlite.py data/users.csv                        # import (skip existing emails)
python csv_to_sqlite.py data/users.csv --on-conflict update   # overwrite existing users
python csv_to_sqlite.py data/users.csv --dry-run              # validate only, nothing committed
```

## How it works
1. **Read** - streams the file with `csv.DictReader`; strips an Excel BOM; sniffs the delimiter
   (`,` `;` tab `|`); maps headers flexibly ("Full Name" -> name, "E-mail" -> email, ...).
2. **Validate & normalise** each row: collapse whitespace in names, lower-case emails, regex email
   check, age 0-120, dates in several formats converted to ISO `YYYY-MM-DD`.
3. **Deduplicate** - duplicates inside the file are rejected (first occurrence wins); emails already in
   the DB are skipped or updated per `--on-conflict`.
4. **Insert** in batches (`executemany`) inside **one transaction** - any DB error rolls everything back.
5. **Report** - counts of inserted / updated / skipped / rejected; rejected rows saved to
   `data/users_rejects.csv` with CSV line number and reason; each run logged in `import_runs`.

## Assumptions
- Email is the unique key for a user. Name and email are required; age, city and signup date are optional.
- Ambiguous numeric dates are read **day-first** (DD/MM/YYYY, DD-MM-YYYY), the convention in India/UK.
- File encoding is UTF-8 (with or without BOM).
- `data/users.csv` has 14 rows: 8 valid (including messy-but-fixable ones: extra spaces, upper-case
  email, quoted name containing a comma, blank age/date) and 6 invalid (bad email, empty name,
  non-numeric age, in-file duplicate, age 150, impossible date).
