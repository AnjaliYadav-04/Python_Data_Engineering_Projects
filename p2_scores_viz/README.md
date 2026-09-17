# Project 2 - Data Processing and Visualization (Student Scores)

Fetches student test scores from an API endpoint (a JSON dataset hosted in this project's public
GitHub repository), calculates the average score and other statistics, and draws a bar-chart dashboard.

## Run
```
pip install matplotlib
python scores_analysis.py                    # fetch from GitHub -> output/score_dashboard.png + output/report.json
python scores_analysis.py --offline          # no internet: use the bundled local copy
python scores_analysis.py --pass-mark 60     # change the pass mark
python scores_analysis.py --api-url https://example.com/api/scores   # any other endpoint
```

Default endpoint:
`https://raw.githubusercontent.com/AnjaliYadav-04/Python_Data_Engineering_Projects/main/p2_scores_viz/data/scores.json`

| Option | Default | Meaning |
|---|---|---|
| `--api-url` | GitHub endpoint above | URL returning a JSON list of score records |
| `--offline` | off | Use `data/scores.json` through a built-in local mock server |
| `--pass-mark` | `50` | Minimum passing score |
| `--out` | `output/score_dashboard.png` | Chart file |
| `--report` | `output/report.json` | JSON report file |

## How it works
1. **Fetch** - the score list is downloaded over HTTPS with retry and exponential backoff.
   Clear errors are shown for 404 (wrong URL or file not uploaded), an HTML page instead of JSON
   (non-Raw GitHub link) and no internet (suggests `--offline`).
2. **Clean** - missing, non-numeric or out-of-range (not 0-100) scores are excluded and listed
   under `rejected_records` in the report.
3. **Analyse** (Python `statistics` module) - overall **average score**, median, standard deviation,
   min/max, pass rate; per-student and per-subject averages; letter-grade distribution; top/bottom 3.
4. **Visualise** - a 3-panel dashboard:
   - ranked bar chart of each student's average, green = above class mean, amber = below mean
     but passing, red = failing, with dashed mean and dotted pass-mark reference lines;
   - subject averages with ±1 standard-deviation error bars;
   - grade distribution.
5. **Report** - `output/report.json` with all figures plus the `data_source` that was used.

## Assumptions
- No public API provides student test scores, so the sample dataset is published as a static JSON
  endpoint on GitHub and consumed exactly like a read-only REST API.
- One record per student per subject: `{"student_id", "name", "subject", "score"}`, scores out of 100.
- "Average score" = arithmetic mean of all valid individual scores (also reported per student/subject).
- Student averages weight every subject equally.
- Pass mark defaults to 50. Grade bands: A >= 85, B >= 70, C >= 60, D >= 50, F < 50.
- `data/scores.json`: 12 fictional students x 4 subjects, generated with a fixed random seed,
  plus 2 invalid rows (null score, score of 112).
- Chart rendered headlessly (Agg backend) to PNG so it works without a display.
- Requires Python 3.10+, `matplotlib`, and an internet connection (except with `--offline`).