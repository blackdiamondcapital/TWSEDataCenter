# Outline (JFDS)

1. Abstract
2. Introduction
   - Motivation: timely and reliable financial data updates
   - Contributions
3. Related Work
   - Financial data pipelines and systems
   - Concurrency in data engineering
4. System Design
   - Front end: `index.html`, `script.js` (concurrency, `runWithConcurrency()`, `startUpdateProcess()`)
   - Backend: `server.py`, Flask threaded vs Waitress on Windows
   - Storage: PostgreSQL schema and data integrity handling
   - Observability: UI logs, CSV export, total/batch elapsed time
5. Methodology
   - Dataset and tasks (TWSE/TPEX stocks, date ranges)
   - Parameter grid (concurrency, batch size, backend)
   - Metrics (latency, throughput, errors, integrity)
   - Environment (hardware, OS, network) and statistical tests
6. Results
   - Performance and stability
   - Best parameter combinations and trade-offs
7. Discussion
   - Limitations and threats to validity
   - Practical deployment guidance
8. Conclusion and Future Work
   - Automation, retries, scheduling, and distributed extensions
9. Reproducibility Package
   - Code, logs, scripts, and instructions
