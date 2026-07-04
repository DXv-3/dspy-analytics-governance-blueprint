#!/usr/bin/env python3
"""conductor_gate_sink.py  —  DSPy analytics sink for conductor gate events

Listens on the harmony bus for GATE_PASSED / GATE_FAILED events emitted
by conductor-protocol-v2 and:

  1. Stores each gate result as a DSPy training example in brain.db
  2. Computes rolling gate-performance metrics (pass-rate, latency p50/p95)
  3. Surfaces trend anomalies (sudden drop in pass-rate for a gate)
  4. Periodically re-optimises gate prompts using DSPy MIPROv2
     when enough new examples have accumulated (default: 50)

Run modes:
  --listen     Continuous bus listener (blocks until Ctrl-C)
  --batch      Process spooled events from disk and exit
  --report     Print gate performance report and exit
  --optimize   Run DSPy optimization pass and exit

USAGE
-----
    python conductor_gate_sink.py --listen
    python conductor_gate_sink.py --report
    python conductor_gate_sink.py --optimize --gate idkwidk
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

DB_PATH = Path(os.environ.get('BRAIN_DB_PATH',
    Path.home() / 'the-brain' / 'brain.db'
))
SINK_DB_PATH = Path(os.environ.get('GATE_SINK_DB',
    Path(__file__).parent / 'gate_analytics.db'
))
OPTIMIZE_THRESHOLD = int(os.environ.get('GATE_OPTIMIZE_THRESHOLD', '50'))


# ---------------------------------------------------------------------------
# Gate analytics SQLite schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS gate_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    run_id       TEXT,
    gate_name    TEXT NOT NULL,
    outcome      TEXT NOT NULL,   -- 'pass' | 'fail'
    detail       TEXT,
    source       TEXT,
    raw_event    TEXT             -- full JSON of the bus event
);

CREATE TABLE IF NOT EXISTS gate_metrics (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    gate_name    TEXT NOT NULL,
    window_hours REAL NOT NULL,
    n_total      INTEGER,
    n_pass       INTEGER,
    n_fail       INTEGER,
    pass_rate    REAL,
    anomaly_flag INTEGER DEFAULT 0  -- 1 if pass_rate dropped >20% vs prior window
);

CREATE TABLE IF NOT EXISTS dspy_examples (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    gate_name    TEXT NOT NULL,
    input_ctx    TEXT,   -- JSON: the routing context that triggered the gate
    outcome      TEXT,   -- 'pass' | 'fail'
    detail       TEXT,   -- gate explanation / reason
    used_in_opt  INTEGER DEFAULT 0   -- 1 once consumed by a DSPy optimization run
);

CREATE TABLE IF NOT EXISTS optimization_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    gate_name    TEXT,
    n_examples   INTEGER,
    metric_before REAL,
    metric_after  REAL,
    status       TEXT    -- 'success' | 'failed' | 'skipped'
);

CREATE INDEX IF NOT EXISTS idx_gate_events_gate ON gate_events(gate_name);
CREATE INDEX IF NOT EXISTS idx_gate_events_ts   ON gate_events(ts);
CREATE INDEX IF NOT EXISTS idx_dspy_examples_gate ON dspy_examples(gate_name);
"""


def _open_sink_db() -> sqlite3.Connection:
    SINK_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SINK_DB_PATH))
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# Event ingestion
# ---------------------------------------------------------------------------

def _ingest_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> bool:
    """
    Parse a bus event and store it + derived DSPy example.
    Returns True if the event was gate-related and stored.
    """
    category   = event.get('category', '')
    event_type = event.get('event_type', '').upper()
    detail     = event.get('detail', '')
    source     = event.get('source', '')
    run_id     = event.get('run_id', '')
    ts         = event.get('timestamp', datetime.now(timezone.utc).isoformat())

    # Identify gate events from conductor
    is_gate = (
        event_type in ('GATE_PASSED', 'GATE_FAILED')
        or category in ('gate', 'conductor_gate')
        or 'gate' in detail.lower()
        or source in ('conductor-protocol-v2', 'self-improving-system-builder')
    )
    if not is_gate:
        return False

    # Extract gate name from detail (e.g. "stage=idkwidk ..." or "gate=audit ...")
    gate_name = event.get('gate_name', '')
    if not gate_name:
        for token in detail.split():
            if token.startswith(('stage=', 'gate=')):
                gate_name = token.split('=', 1)[1]
                break
    if not gate_name:
        gate_name = category or 'unknown'

    outcome = 'pass' if event_type == 'GATE_PASSED' else 'fail'

    conn.execute(
        "INSERT INTO gate_events (ts, run_id, gate_name, outcome, detail, source, raw_event) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ts, run_id, gate_name, outcome, detail, source, json.dumps(event))
    )

    # Build DSPy training example
    input_ctx = json.dumps({
        'run_id':   run_id,
        'source':   source,
        'category': category,
        'detail':   detail[:500],
    })
    conn.execute(
        "INSERT INTO dspy_examples (ts, gate_name, input_ctx, outcome, detail) "
        "VALUES (?, ?, ?, ?, ?)",
        (ts, gate_name, input_ctx, outcome, detail[:1000])
    )
    conn.commit()
    return True


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def _compute_metrics(
    conn: sqlite3.Connection,
    window_hours: float = 24.0,
) -> List[Dict[str, Any]]:
    """
    Compute pass-rate per gate over the last `window_hours` hours.
    Flags anomaly if pass-rate dropped >20% vs the preceding window.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=window_hours)).isoformat()
    prev_cutoff = (now - timedelta(hours=window_hours * 2)).isoformat()

    gates = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT gate_name FROM gate_events WHERE ts >= ?", (cutoff,)
        )
    ]

    results = []
    ts_now  = now.isoformat()

    for gate in gates:
        cur = conn.execute(
            "SELECT outcome FROM gate_events WHERE gate_name=? AND ts>=?",
            (gate, cutoff)
        ).fetchall()
        prev = conn.execute(
            "SELECT outcome FROM gate_events WHERE gate_name=? AND ts>=? AND ts<?",
            (gate, prev_cutoff, cutoff)
        ).fetchall()

        n_total = len(cur)
        n_pass  = sum(1 for r in cur  if r[0] == 'pass')
        n_fail  = n_total - n_pass
        rate    = round(n_pass / n_total, 4) if n_total else 0.0

        prev_total = len(prev)
        prev_rate  = round(
            sum(1 for r in prev if r[0] == 'pass') / prev_total, 4
        ) if prev_total else rate

        anomaly = int(rate < prev_rate - 0.20)

        conn.execute(
            "INSERT INTO gate_metrics "
            "(ts, gate_name, window_hours, n_total, n_pass, n_fail, pass_rate, anomaly_flag) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ts_now, gate, window_hours, n_total, n_pass, n_fail, rate, anomaly)
        )
        results.append({
            'gate': gate, 'n': n_total, 'pass': n_pass, 'fail': n_fail,
            'pass_rate': rate, 'anomaly': bool(anomaly),
        })

    conn.commit()
    return results


# ---------------------------------------------------------------------------
# DSPy optimization stub
# ---------------------------------------------------------------------------

def _run_dspy_optimization(conn: sqlite3.Connection, gate_name: str) -> Dict[str, Any]:
    """
    Run DSPy MIPROv2 optimization over unused gate examples.
    Returns result summary.

    Requires:
      pip install dspy-ai
      OPENAI_API_KEY or local LM endpoint configured in dspy
    """
    examples = conn.execute(
        "SELECT id, input_ctx, outcome, detail FROM dspy_examples "
        "WHERE gate_name=? AND used_in_opt=0 ORDER BY ts",
        (gate_name,)
    ).fetchall()

    n = len(examples)
    if n < OPTIMIZE_THRESHOLD:
        print(f'  [dspy] {gate_name}: only {n} examples (need {OPTIMIZE_THRESHOLD}) — skipping')
        conn.execute(
            "INSERT INTO optimization_runs (ts, gate_name, n_examples, status) VALUES (?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), gate_name, n, 'skipped')
        )
        conn.commit()
        return {'status': 'skipped', 'n_examples': n}

    try:
        import dspy  # type: ignore

        # Build dataset from stored examples
        dataset = [
            dspy.Example(
                input_context=json.loads(row[1]),
                label=row[2],
                rationale=row[3],
            ).with_inputs('input_context')
            for row in examples
        ]

        # Define gate evaluation signature
        class GateEval(dspy.Signature):
            """Given routing context, decide if the gate should pass or fail."""
            input_context: dict = dspy.InputField()
            label: str          = dspy.OutputField(desc='pass or fail')
            rationale: str      = dspy.OutputField(desc='Reason for the decision')

        predictor = dspy.Predict(GateEval)

        def metric(example, pred, trace=None):
            return int(pred.label.strip().lower() == example.label.strip().lower())

        split = max(1, int(len(dataset) * 0.8))
        train, dev = dataset[:split], dataset[split:]

        optimizer = dspy.MIPROv2(metric=metric, num_candidates=4, init_temperature=1.4)
        metric_before = sum(metric(e, predictor(input_context=e.input_context)) for e in dev) / max(1, len(dev))

        optimized = optimizer.compile(predictor, trainset=train, num_trials=10)
        metric_after = sum(metric(e, optimized(input_context=e.input_context)) for e in dev) / max(1, len(dev))

        # Mark examples as consumed
        ids = [str(row[0]) for row in examples]
        conn.execute(
            f"UPDATE dspy_examples SET used_in_opt=1 WHERE id IN ({','.join('?' * len(ids))})",
            ids
        )
        ts = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO optimization_runs (ts, gate_name, n_examples, metric_before, metric_after, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (ts, gate_name, n, round(metric_before, 4), round(metric_after, 4), 'success')
        )
        conn.commit()

        return {
            'status': 'success',
            'gate_name': gate_name,
            'n_examples': n,
            'metric_before': round(metric_before, 4),
            'metric_after':  round(metric_after,  4),
            'delta':         round(metric_after - metric_before, 4),
        }

    except Exception as e:
        conn.execute(
            "INSERT INTO optimization_runs (ts, gate_name, n_examples, status) VALUES (?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), gate_name, n, f'failed: {e}')
        )
        conn.commit()
        return {'status': 'failed', 'error': str(e)}


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def _print_report(conn: sqlite3.Connection) -> None:
    print('\n┌── Gate Performance Report ──────────────────────────────────────────────')
    metrics = _compute_metrics(conn, window_hours=24)
    if not metrics:
        print('│  No gate events in the last 24h')
    else:
        print(f"  {'Gate':<25} {'N':>5} {'Pass':>5} {'Fail':>5} {'Rate':>7} {'Alert'}")
        print('  ' + '-' * 60)
        for m in sorted(metrics, key=lambda x: x['pass_rate']):
            alert = '⚠️  ANOMALY' if m['anomaly'] else ''
            print(f"  {m['gate']:<25} {m['n']:>5} {m['pass']:>5} {m['fail']:>5} "
                  f"{m['pass_rate']:>6.1%} {alert}")
    print('└' + '─' * 60)

    # DSPy example counts
    rows = conn.execute(
        "SELECT gate_name, COUNT(*) as n, SUM(used_in_opt) as used "
        "FROM dspy_examples GROUP BY gate_name ORDER BY n DESC"
    ).fetchall()
    if rows:
        print('\nDSPy training examples:')
        for r in rows:
            ready = r[1] - (r[2] or 0)
            print(f'  {r[0]:<25} {r[1]:>5} total  {ready:>5} unused (threshold={OPTIMIZE_THRESHOLD})')


# ---------------------------------------------------------------------------
# Bus listener
# ---------------------------------------------------------------------------

def _listen(conn: sqlite3.Connection) -> None:
    sys.path.insert(0, str(Path.home() / 'harmony-engine-protocol'))
    try:
        from brain_bus import BrainBusSubscriber  # type: ignore
        sub = BrainBusSubscriber(handler=lambda ev: _ingest_event(conn, ev))
        print('[conductor_gate_sink] Listening on harmony bus... (Ctrl-C to stop)')
        sub.listen()  # blocks
    except ImportError:
        print('[conductor_gate_sink] harmony-engine-protocol not found.')
        print('  Polling brain.db for new gate events instead...')
        _poll_brain_db(conn)


def _poll_brain_db(conn: sqlite3.Connection, interval: int = 10) -> None:
    """Fallback: poll the-brain memories table for gate events."""
    seen: set = set()
    print(f'  Polling brain.db every {interval}s. Ctrl-C to stop.')
    while True:
        try:
            brain_conn = sqlite3.connect(str(DB_PATH))
            rows = brain_conn.execute(
                "SELECT id, timestamp, source, category, event_type, metadata "
                "FROM memories WHERE event_type IN ('GATE_PASSED','GATE_FAILED')"
            ).fetchall()
            brain_conn.close()
            for row in rows:
                eid = row[0]
                if eid in seen:
                    continue
                seen.add(eid)
                meta = json.loads(row[5] or '{}')
                event = {
                    'id':         eid,
                    'timestamp':  row[1],
                    'source':     row[2],
                    'category':   row[3],
                    'event_type': row[4],
                    'detail':     meta.get('detail', ''),
                    'run_id':     meta.get('run_id', ''),
                }
                stored = _ingest_event(conn, event)
                if stored:
                    print(f'  [poll] ingested gate event {eid[:12]}…')
        except Exception as e:
            print(f'  [poll] error: {e}')
        time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DSPy gate analytics sink')
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('--listen',   action='store_true', help='Start bus listener')
    grp.add_argument('--batch',    action='store_true', help='Process spooled events')
    grp.add_argument('--report',   action='store_true', help='Print gate performance report')
    grp.add_argument('--optimize', action='store_true', help='Run DSPy optimization pass')
    parser.add_argument('--gate', default='', help='Gate name to optimize (default: all)')
    args = parser.parse_args()

    conn = _open_sink_db()

    if args.listen:
        _listen(conn)

    elif args.report:
        _print_report(conn)

    elif args.optimize:
        gates_to_opt = [args.gate] if args.gate else [
            r[0] for r in conn.execute(
                "SELECT DISTINCT gate_name FROM dspy_examples WHERE used_in_opt=0"
            ).fetchall()
        ]
        for gate in gates_to_opt:
            print(f'\n[dspy] Optimizing gate: {gate}')
            result = _run_dspy_optimization(conn, gate)
            print(json.dumps(result, indent=2))

    elif args.batch:
        # Process any disk-spooled events
        spool = Path.home() / '.brain_bus_spool' / 'conductor_gate_events.jsonl'
        if not spool.exists():
            print(f'No spool file at {spool}')
        else:
            ingested = 0
            for line in spool.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                    if _ingest_event(conn, ev):
                        ingested += 1
                except Exception as e:
                    print(f'  [warn] bad spool line: {e}')
            print(f'[batch] ingested {ingested} gate events from spool')

    conn.close()
