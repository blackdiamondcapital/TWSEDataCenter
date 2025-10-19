# Research Package for Journal of Financial Data Science (JFDS)

This folder contains materials to reproduce experiments and to prepare the manuscript.

## Scope
- Evaluate a high-concurrency batch update pipeline for Taiwan equities data.
- Quantify trade-offs among throughput, latency, error rate, and data integrity.

## Parameter Grid
- Concurrency: 2, 5, 10, 20
- Batch size: 5, 10, 20
- Backend: Flask threaded vs Waitress (Windows)
- Fixed dataset: same stock list and date range across runs

## Metrics
- Total elapsed time (from UI logs)
- Per-batch elapsed time (from UI logs)
- Throughput (stocks/minute)
- Failure/duplicate rates
- Data integrity (before/after counts via /api/health or stats panel)

## Procedure
1. Configure front-end parameters in `script.js` (concurrency and batchSize in `startUpdateProcess()`).
2. Run each parameter combination ≥3 times.
3. After each run, use the UI "Export Log" to save a CSV under `research/data/`.
4. Optionally capture server logs for backend comparisons.
5. Analyze with `research/analysis/analyze_logs.py` and generate figures.

## Analysis
- See `analysis/analyze_logs.py` for parsing, metrics, and plots.
- Outputs to `research/out/` by default.

## Reproducibility
- Include: exported CSV logs, parameter settings, environment details (CPU, RAM, OS, network), and DB snapshot notes.
- Requirements for analysis: `research/requirements_research.txt`.

## Manuscript
- See `paper/abstract.md` and `paper/outline.md`.
