"""conductor_sink.py  —  DSPy Analytics & Governance Blueprint

LIVE HARMONY BUS CONSUMER + DSPY GATE ANALYTICS

Purpose
-------
Reads gate result events from the harmony brain_bus.jsonl spool in
real time, aggregates pass/fail/latency statistics per model and gate
number, then runs a DSPy-powered GateAnalyzer that produces:

  1. A trend narrative (human-readable summary)
  2. Routing recommendations (e.g. "avoid grok-3 for gate-3 regex tasks")
  3. A quality score for the current run window

Recommendations are written back to brain.db via the bus so the
conductor's pre-route query picks them up on the NEXT run automatically.

Usage
-----
  # Run continuously (tails the spool, analyses every --interval seconds)
  python conductor_sink.py

  # Run once over existing spool, print report, exit
  python conductor_sink.py --once

  # Tail last N events only
  python conductor_sink.py --tail 200

  # Dry run: analyse but do not write back to brain
  python conductor_sink.py --once --dry-run

Environment
-----------
  BRAIN_BUS_SPOOL    Path to brain_bus.jsonl spool directory.
                     Defaults to ~/.brain_bus_spool/
  HARMONY_PATH       Path to harmony-engine-protocol repo root.
                     Defaults to ../harmony-engine-protocol
  VINNY_LLM_BASE     Local LLM base URL for DSPy LM config.
                     Defaults to http://localhost:1234/v1
  VINNY_LLM_MODEL    Model name for DSPy LM.
                     Defaults to llama-3.2-3b
  CONDUCTOR_SINK_INTERVAL   Seconds between analysis passes (default 60)

Dependencies
------------
  pip install dspy-ai requests
  (harmony-engine-protocol must be on the path — see HARMONY_PATH)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Harmony bus import ────────────────────────────────────────────────────
HARMONY_PATH = Path(os.environ.get(
    "HARMONY_PATH",
    Path(__file__).resolve().parent.parent / "harmony-engine-protocol",
))
sys.path.insert(0, str(HARMONY_PATH))

try:
    from brain_bus import BrainBusPublisher  # type: ignore
    BUS_AVAILABLE = True
except ImportError:
    BUS_AVAILABLE = False
    print("[conductor_sink] ⚠  harmony-engine-protocol not found on path — "
          "set HARMONY_PATH. Running in offline/dry-run mode.", flush=True)

# ── DSPy setup ───────────────────────────────────────────────────────
try:
    import dspy
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False
    print("[conductor_sink] ⚠  dspy-ai not installed (pip install dspy-ai). "
          "Analytics will run in heuristic mode.", flush=True)

LLM_BASE  = os.environ.get("VINNY_LLM_BASE",  "http://localhost:1234/v1")
LLM_MODEL = os.environ.get("VINNY_LLM_MODEL", "llama-3.2-3b")

SPOOL_DIR = Path(os.environ.get(
    "BRAIN_BUS_SPOOL",
    Path.home() / ".brain_bus_spool",
))
SPOOL_FILE = SPOOL_DIR / "brain_bus.jsonl"

INTERVAL = int(os.environ.get("CONDUCTOR_SINK_INTERVAL", "60"))


# ── DSPy Signatures ─────────────────────────────────────────────────────
if DSPY_AVAILABLE:
    class GateAnalyzer(dspy.Signature):  # type: ignore
        """Analyze gate pass/fail statistics across models and produce routing recommendations.

        You are an AI operations analyst for a multi-model agent system.
        Given a JSON summary of recent gate outcomes per model, produce:
        1. A concise trend narrative (2-3 sentences)
        2. Specific routing recommendations (which models to prefer/avoid for which gate types)
        3. A quality score 0-100 for the current window

        Be specific. Mention model names and gate numbers. Do not hedge.
        """
        gate_stats_json: str = dspy.InputField(desc="JSON: {model: {gate_n: {pass, fail, avg_latency_ms}}}")
        window_events:   int = dspy.InputField(desc="Total events in analysis window")
        trend_narrative:           str = dspy.OutputField(desc="2-3 sentence trend summary")
        routing_recommendations:   str = dspy.OutputField(desc="Bulleted routing recommendations")
        quality_score:             int = dspy.OutputField(desc="0-100 quality score for this window")


    class HeuristicGateAnalyzer:
        """Fallback when DSPy unavailable: pure heuristics."""
        def __call__(self, gate_stats_json: str, window_events: int):
            stats = json.loads(gate_stats_json)
            recs  = []
            worst_model = None
            worst_rate  = 1.0
            best_model  = None
            best_rate   = 0.0
            total_pass = total_fail = 0

            for model, gates in stats.items():
                for gn, s in gates.items():
                    total_pass += s["pass"]
                    total_fail += s["fail"]
                    rate = s["pass"] / max(1, s["pass"] + s["fail"])
                    if rate < worst_rate:
                        worst_rate  = rate
                        worst_model = f"{model} gate-{gn}"
                    if rate > best_rate:
                        best_rate  = rate
                        best_model = f"{model} gate-{gn}"
                    if rate < 0.7:
                        recs.append(f"- Avoid {model} for gate-{gn} (pass rate {rate:.0%})")

            total = max(1, total_pass + total_fail)
            quality = int(100 * total_pass / total)
            narrative = (
                f"Window of {window_events} events: {total_pass} passed, "
                f"{total_fail} failed (quality={quality}%). "
                f"Best: {best_model or 'n/a'} ({best_rate:.0%}). "
                f"Worst: {worst_model or 'n/a'} ({worst_rate:.0%})."
            )
            recs_text = "\n".join(recs) if recs else "- No critical issues detected."
            return type("R", (), {
                "trend_narrative":         narrative,
                "routing_recommendations": recs_text,
                "quality_score":           quality,
            })()
else:
    class HeuristicGateAnalyzer:
        pass  # defined above only when dspy is available


# ── Spool reader ────────────────────────────────────────────────────────────
def read_spool(tail: Optional[int] = None) -> List[Dict[str, Any]]:
    if not SPOOL_FILE.exists():
        return []
    lines = SPOOL_FILE.read_text(errors="replace").splitlines()
    if tail is not None:
        lines = lines[-tail:]
    events = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return events


def aggregate(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate gate pass/fail stats per model."""
    # stats[model][gate_n] = {pass, fail, latencies}
    stats: Dict[str, Dict[str, Dict]] = defaultdict(lambda: defaultdict(lambda: {"pass": 0, "fail": 0, "latencies": []}))
    pipeline_stages: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for ev in events:
        category   = ev.get("category", "")
        event_type = ev.get("event_type", "")
        detail     = ev.get("detail", "")
        outcome    = ev.get("outcome", "")
        source     = ev.get("source", "unknown")

        # Gate results from conductor / self-improving-system-builder
        if category == "gate" or event_type in ("GATE_PASSED", "GATE_FAILED"):
            model  = _extract(detail, "model") or source
            gate_n = _extract(detail, "gate") or "?"
            latency = _extract_float(detail, "latency_ms")
            bucket = stats[model][gate_n]
            if outcome == "pass" or event_type == "GATE_PASSED":
                bucket["pass"] += 1
            else:
                bucket["fail"] += 1
            if latency:
                bucket["latencies"].append(latency)

        # Pipeline stages from vinny-stack
        if category == "pipeline" or source == "vinny-stack":
            stage = _extract(detail, "stage") or event_type
            pipeline_stages[source][stage] += 1

    # Compute avg latency
    clean: Dict[str, Dict] = {}
    for model, gates in stats.items():
        clean[model] = {}
        for gn, s in gates.items():
            lats = s["latencies"]
            clean[model][gn] = {
                "pass": s["pass"],
                "fail": s["fail"],
                "avg_latency_ms": int(sum(lats) / len(lats)) if lats else 0,
            }

    return {"gate_stats": clean, "pipeline_stages": dict(pipeline_stages)}


def _extract(text: str, key: str) -> str:
    """Extract key=value from a detail string."""
    import re
    m = re.search(rf"(?:^|\s){re.escape(key)}=([^\s]+)", text)
    return m.group(1) if m else ""


def _extract_float(text: str, key: str) -> Optional[float]:
    v = _extract(text, key)
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── Analysis pass ────────────────────────────────────────────────────────────
def run_analysis(
    events: List[Dict[str, Any]],
    dry_run: bool = False,
    bus_pub: Optional[Any] = None,
) -> Dict[str, Any]:
    if not events:
        print("[conductor_sink] no events to analyse.", flush=True)
        return {}

    agg = aggregate(events)
    gate_stats_json = json.dumps(agg["gate_stats"], indent=2)

    # ── Pick analyser ──────────────────────────────────────────────
    if DSPY_AVAILABLE:
        try:
            lm = dspy.LM(
                model          = f"openai/{LLM_MODEL}",
                api_base       = LLM_BASE,
                api_key        = "local",
                max_tokens     = 512,
                temperature    = 0.2,
            )
            dspy.configure(lm=lm)
            analyser = dspy.ChainOfThought(GateAnalyzer)
        except Exception as e:
            print(f"[conductor_sink] DSPy LM config failed ({e}), using heuristics.", flush=True)
            analyser = HeuristicGateAnalyzer()
    else:
        analyser = HeuristicGateAnalyzer()

    result = analyser(
        gate_stats_json = gate_stats_json,
        window_events   = len(events),
    )

    # ── Print report ────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n{'='*60}", flush=True)
    print(f"[conductor_sink] Analysis @ {ts}  events={len(events)}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"Trend:\n  {result.trend_narrative}", flush=True)
    print(f"\nRecommendations:\n{result.routing_recommendations}", flush=True)
    print(f"\nQuality Score: {result.quality_score}/100", flush=True)
    if agg["pipeline_stages"]:
        print(f"\nPipeline Stages (vinny-stack): {json.dumps(agg['pipeline_stages'], indent=2)}", flush=True)
    print(f"{'='*60}\n", flush=True)

    report = {
        "ts":                      ts,
        "window_events":           len(events),
        "trend_narrative":         result.trend_narrative,
        "routing_recommendations": result.routing_recommendations,
        "quality_score":           result.quality_score,
        "gate_stats":              agg["gate_stats"],
        "pipeline_stages":         agg["pipeline_stages"],
    }

    # ── Write recommendations back to brain.db via bus ──────────────
    if not dry_run and BUS_AVAILABLE and bus_pub:
        try:
            bus_pub.publish_learn(
                run_id     = f"dspy-sink-{ts.replace(':','-')}",
                source     = "dspy-analytics-governance-blueprint",
                category   = "governance",
                event_type = "ANALYTICS_REPORT",
                detail     = (
                    f"quality={result.quality_score} "
                    f"events={len(events)} "
                    f"recommendations={result.routing_recommendations[:200]}"
                ),
                outcome    = "pass" if int(result.quality_score) >= 70 else "warn",
            )
            print("[conductor_sink] ✓ report written to brain.db via bus", flush=True)
        except Exception as e:
            print(f"[conductor_sink] ⚠  bus write failed: {e}", flush=True)

    return report


# ── Entrypoint ──────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="DSPy Analytics conductor sink for the harmony bus."
    )
    parser.add_argument("--once",    action="store_true",  help="Run once and exit")
    parser.add_argument("--tail",    type=int, default=None, help="Only analyse last N spool events")
    parser.add_argument("--dry-run", action="store_true",  help="Do not write back to brain.db")
    parser.add_argument("--interval", type=int, default=INTERVAL,
                        help=f"Seconds between passes in continuous mode (default {INTERVAL})")
    args = parser.parse_args()

    bus_pub = None
    if BUS_AVAILABLE and not args.dry_run:
        try:
            bus_pub = BrainBusPublisher(source_repo="dspy-analytics-governance-blueprint")
        except Exception as e:
            print(f"[conductor_sink] ⚠  could not create BrainBusPublisher: {e}", flush=True)

    if args.once:
        events = read_spool(tail=args.tail)
        run_analysis(events, dry_run=args.dry_run, bus_pub=bus_pub)
        return

    # Continuous tail mode
    print(f"[conductor_sink] ✓ watching {SPOOL_FILE} every {args.interval}s ...", flush=True)
    seen_lines = 0
    while True:
        try:
            events = read_spool(tail=args.tail)
            new_events = events[seen_lines:]
            if new_events:
                run_analysis(new_events, dry_run=args.dry_run, bus_pub=bus_pub)
                seen_lines = len(events)
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n[conductor_sink] ✓ stopped.", flush=True)
            break
        except Exception as e:
            print(f"[conductor_sink] ⚠  error in main loop: {e}", flush=True)
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
