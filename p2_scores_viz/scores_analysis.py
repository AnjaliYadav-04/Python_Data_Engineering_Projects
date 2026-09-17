#!/usr/bin/env python3
"""
Project 2 - Student test-score analytics
External API (JSON hosted on GitHub)  ->  cleaning  ->  statistics  ->  bar-chart dashboard (PNG) + JSON report

Data source
  * default   - scores.json served over HTTPS from the project's public GitHub repository
  * --api-url - any other endpoint returning a JSON list (or {"data": [...]})
  * --offline - bundled local copy served by a built-in mock server (no internet needed)

Features
  * HTTP client with retry/backoff and clear error messages (404, HTML instead of JSON, no internet)
  * Data-quality gate: null / non-numeric / out-of-range scores are excluded and reported
  * Statistics: class mean, median, std-dev, min/max, per-student & per-subject averages,
    letter-grade distribution, top & bottom performers
  * 3-panel dashboard: per-student average bars (colour-coded vs class mean, labelled),
    per-subject average bars with std-dev error bars, grade distribution bars
  * Machine-readable summary written to report.json

Usage
  python scores_analysis.py
  python scores_analysis.py --pass-mark 60 --out charts/dashboard.png
  python scores_analysis.py --offline
  python scores_analysis.py --api-url https://example.com/api/scores
Requires: matplotlib
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics as st
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

import matplotlib

matplotlib.use("Agg")  # headless rendering - works on servers/CI
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
DEFAULT_API_URL = ("https://raw.githubusercontent.com/AnjaliYadav-04/"
                   "Python_Data_Engineering_Projects/main/p2_scores_viz/data/scores.json")
log = logging.getLogger("scores")

# Bands align with the default pass mark of 50 (D is the lowest passing grade)
GRADE_BANDS = [(85, "A"), (70, "B"), (60, "C"), (50, "D"), (0, "F")]


@dataclass(frozen=True, slots=True)
class ScoreRecord:
    student_id: str
    name: str
    subject: str
    score: float


# --------------------------------------------------------------------------- acquisition
class FetchError(RuntimeError):
    pass


def fetch_json(url: str, retries: int = 3, base_delay: float = 0.5) -> list[dict]:
    """GET a JSON list of score records, retrying transient failures with exponential backoff."""
    request = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "User-Agent": "scores-analysis/1.0"})
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=15) as resp:
                text = resp.read().decode("utf-8-sig")
            break
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise FetchError(f"404 Not Found: {url}\n  Check the file is uploaded, the repository is "
                                 "public, and the branch/path in the URL are correct.") from exc
            if exc.code < 500 and exc.code != 429:
                raise FetchError(f"HTTP {exc.code} for {url}") from exc
            last_error = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
        if attempt == retries:
            raise FetchError(f"could not reach {url} after {retries} attempts ({last_error})\n"
                             "  No internet? Run with --offline to use the local copy.") from last_error
        delay = base_delay * 2 ** (attempt - 1)
        log.warning("fetch failed (%s), retry %d/%d in %.1fs", last_error, attempt, retries, delay)
        time.sleep(delay)

    try:
        body = json.loads(text)
    except json.JSONDecodeError as exc:
        hint = ("  The response looks like an HTML page - for GitHub use the 'Raw' link "
                "(raw.githubusercontent.com), not github.com/.../blob/...") if text.lstrip().startswith("<") else ""
        raise FetchError(f"response from {url} is not valid JSON\n{hint}") from exc
    records = body["data"] if isinstance(body, dict) and "data" in body else body
    if not isinstance(records, list):
        raise FetchError("expected a JSON list of score records")
    return records


@contextmanager
def serve_mock_api(data_dir: Path) -> Iterator[str]:
    """Serve data_dir over HTTP: GET /scores.json behaves like a read-only REST endpoint."""
    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def end_headers(self):
            self.send_header("Content-Type", "application/json")
            super().end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(QuietHandler, directory=str(data_dir)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/scores.json"
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------------- cleaning
def clean(raw_rows: list[dict]) -> tuple[list[ScoreRecord], list[dict]]:
    good, bad = [], []
    for row in raw_rows:
        try:
            if row.get("score") is None:
                raise ValueError("missing score")
            score = float(row["score"])
            if not 0 <= score <= 100:
                raise ValueError(f"score {score} outside 0-100")
            good.append(ScoreRecord(str(row["student_id"]), str(row["name"]).strip(),
                                    str(row["subject"]).strip().title(), score))
        except (KeyError, TypeError, ValueError) as exc:
            bad.append({"record": row, "reason": str(exc) or type(exc).__name__})
    for b in bad:
        log.warning("excluded %s: %s", b["record"].get("student_id"), b["reason"])
    return good, bad


# --------------------------------------------------------------------------- analysis
def letter(score: float) -> str:
    return next(g for cutoff, g in GRADE_BANDS if score >= cutoff)


def analyse(records: list[ScoreRecord], pass_mark: float) -> dict:
    if not records:
        raise ValueError("no valid records to analyse")
    scores = [r.score for r in records]

    by_student: dict[str, list[float]] = defaultdict(list)
    names: dict[str, str] = {}
    by_subject: dict[str, list[float]] = defaultdict(list)
    for r in records:
        by_student[r.student_id].append(r.score)
        names[r.student_id] = r.name
        by_subject[r.subject].append(r.score)

    student_avg = {sid: round(st.fmean(v), 2) for sid, v in by_student.items()}
    ranked = sorted(student_avg.items(), key=lambda kv: kv[1], reverse=True)

    return {
        "records_analysed": len(records),
        "class": {
            "mean": round(st.fmean(scores), 2),
            "median": round(st.median(scores), 2),
            "stdev": round(st.stdev(scores), 2) if len(scores) > 1 else 0.0,
            "min": min(scores),
            "max": max(scores),
            "pass_rate_pct": round(100 * sum(s >= pass_mark for s in scores) / len(scores), 1),
        },
        "students": [
            {"student_id": sid, "name": names[sid], "average": avg, "grade": letter(avg),
             "passed": avg >= pass_mark}
            for sid, avg in ranked
        ],
        "subjects": {
            subj: {"mean": round(st.fmean(v), 2),
                   "stdev": round(st.stdev(v), 2) if len(v) > 1 else 0.0,
                   "n": len(v)}
            for subj, v in sorted(by_subject.items())
        },
        "grade_distribution": {g: Counter(letter(a) for a in student_avg.values()).get(g, 0)
                               for _, g in reversed(GRADE_BANDS)},
        "top_3": [names[sid] for sid, _ in ranked[:3]],
        "bottom_3": [names[sid] for sid, _ in ranked[-3:]][::-1],
    }


# --------------------------------------------------------------------------- visualisation
def plot_dashboard(report: dict, pass_mark: float, out: Path) -> None:
    mean = report["class"]["mean"]
    students = report["students"]
    subjects = report["subjects"]

    fig = plt.figure(figsize=(14, 9), constrained_layout=True)
    grid = fig.add_gridspec(2, 2, height_ratios=[1.25, 1])
    ax_students = fig.add_subplot(grid[0, :])
    ax_subjects = fig.add_subplot(grid[1, 0])
    ax_grades = fig.add_subplot(grid[1, 1])

    # Panel 1: per-student averages
    labels = [s["name"] for s in students]
    values = [s["average"] for s in students]
    colours = ["#2e7d32" if v >= mean else ("#f9a825" if v >= pass_mark else "#c62828") for v in values]
    bars = ax_students.bar(labels, values, color=colours, edgecolor="black", linewidth=0.4)
    ax_students.bar_label(bars, fmt="%.1f", padding=2, fontsize=8)
    ax_students.axhline(mean, ls="--", color="#1565c0", lw=1.5, label=f"Class mean ({mean:.1f})")
    ax_students.axhline(pass_mark, ls=":", color="#c62828", lw=1.2, label=f"Pass mark ({pass_mark:g})")
    ax_students.set(title="Average score per student (ranked)", ylabel="Average score", ylim=(0, 105))
    ax_students.tick_params(axis="x", rotation=35)
    for tick in ax_students.get_xticklabels():
        tick.set_ha("right")
    ax_students.legend(loc="upper right")
    ax_students.grid(axis="y", alpha=0.3)

    # Panel 2: per-subject mean with std-dev error bars
    subj_names = list(subjects)
    subj_means = [subjects[s]["mean"] for s in subj_names]
    subj_sd = [subjects[s]["stdev"] for s in subj_names]
    bars = ax_subjects.bar(subj_names, subj_means, yerr=subj_sd, capsize=6,
                           color=plt.cm.viridis([0.2, 0.45, 0.65, 0.85][:len(subj_names)]))
    ax_subjects.bar_label(bars, fmt="%.1f", label_type="center", color="white", fontweight="bold")
    ax_subjects.set(title="Subject averages (±1 std dev)", ylabel="Score", ylim=(0, 105))
    ax_subjects.grid(axis="y", alpha=0.3)

    # Panel 3: grade distribution
    dist = report["grade_distribution"]
    bars = ax_grades.bar(list(dist), list(dist.values()),
                         color=["#c62828", "#ef6c00", "#f9a825", "#7cb342", "#2e7d32"])
    ax_grades.bar_label(bars, padding=2)
    ax_grades.set(title="Grade distribution (student averages)", xlabel="Grade", ylabel="Students")
    ax_grades.yaxis.get_major_locator().set_params(integer=True)
    ax_grades.set_ylim(0, max(dist.values()) + 1)

    c = report["class"]
    fig.suptitle(f"Test Score Dashboard  |  n={report['records_analysed']} scores  |  "
                 f"mean {c['mean']}  median {c['median']}  σ {c['stdev']}  pass rate {c['pass_rate_pct']}%",
                 fontsize=13, fontweight="bold")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api-url", default=DEFAULT_API_URL,
                    help="endpoint returning a JSON list (default: dataset hosted on GitHub)")
    ap.add_argument("--offline", action="store_true",
                    help="use the bundled local data via the built-in mock server")
    ap.add_argument("--data-dir", type=Path, default=HERE / "data")
    ap.add_argument("--pass-mark", type=float, default=50.0)
    ap.add_argument("--out", type=Path, default=HERE / "output" / "score_dashboard.png")
    ap.add_argument("--report", type=Path, default=HERE / "output" / "report.json")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    try:
        if args.offline:
            with serve_mock_api(args.data_dir) as url:
                log.info("offline mode: local mock API at %s", url)
                raw = fetch_json(url)
            source = "local mock API (offline)"
        else:
            log.info("fetching scores from %s", args.api_url)
            raw = fetch_json(args.api_url)
            source = args.api_url
    except FetchError as exc:
        log.error("%s", exc)
        return 1
    log.info("received %d records", len(raw))

    records, rejected = clean(raw)
    report = analyse(records, args.pass_mark)
    report["data_source"] = source
    report["rejected_records"] = rejected

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    plot_dashboard(report, args.pass_mark, args.out)

    c = report["class"]
    print(f"\nData source: {source}")
    print(f"Average score (all subjects, all students): {c['mean']}")
    print(f"Median {c['median']} | Std dev {c['stdev']} | Range {c['min']:g}-{c['max']:g} | "
          f"Pass rate {c['pass_rate_pct']}%")
    print("Subject averages: " + ", ".join(f"{k} {v['mean']}" for k, v in report["subjects"].items()))
    print(f"Top 3: {', '.join(report['top_3'])}")
    print(f"Excluded records: {len(rejected)}")
    print(f"Chart  -> {args.out}\nReport -> {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())