#!/usr/bin/env python3
"""
evaluate_votes.py -- Kalibrierung des Multi-LLM-Votings (B3, MASSNAHMENPLAN Teil B).

Beantwortet datenbasiert, was bisher anekdotisch war: Welches Modell vetoiert wie oft,
trifft es mit seinen Vetos Verlusttage, und was wäre der kontrafaktische P&L der
vermiedenen Tage gewesen? Grundlage sind die strukturierten Einzelvoten im
Entscheidungsjournal (decisions.decision_json.votes) und der Folgehandelstags-Return
des Underlyings aus der Marktdaten-DB (bars_1d).

Metriken je Modell:
  * n_votes, TRADE-Quote, Zustimmquote zum Aggregat (Dauer-Zustimmer-Erkennung)
  * Schemafehlerrate (validator reject_json/reject_schema) und Unreachable-Rate
  * Latenz p50/p95 (ms)
  * Veto-Trefferquote: Anteil der Vetos an Tagen, deren Folgetags-Return in
    Signalrichtung NEGATIV war (das Veto hat einen Verlusttag vermieden)
  * Kontrafaktischer Veto-P&L: Summe über Veto-Tage von
    -(Richtungs-Return x size_suggested_eur) + Round-Trip-Gebühr (gespart) --
    positiv = die Vetos haben netto Geld gespart.

Zusätzlich: Kalibrierung der scikit-Prüfinstanz (mlforecast.py) aus der Tabelle
ml_predictions -- Trefferquote vs. Basisrate, Brier vs. Klimatologie und der
kontrafaktische Beitrag der ML_DISAGREE-Tage (--ml-threshold, --no-ml).

Bewusst deskriptiv (kleine Stichprobe, keine automatische Modell-Abschaltung):
Das Ergebnis geht als Text in den Signal-Report und als CSV in die Projektarbeit
(Kap. 3.4 Evaluation). Early-Stop-Voten (validator=early_stop) und Pre-Gate-Läufe
(model=pregate) werden ausgewiesen, aber nicht in die Trefferquoten gemischt.

CLI:
  python3 evaluate_votes.py [--journal-db decisions.sqlite] [--market-db marketdata.sqlite]
                            [--symbol ^GSPC] [--fee-roundtrip 7.80] [--csv votes_stats.csv]

Cron (agent-vm, sonntags nach dem Vollvoten-Lauf):
  30 16 * * 0  cd /home/me/fom-ki-project && python3 evaluate_votes.py >> logs/evaluate.log 2>&1

Abhängigkeiten: nur Standardbibliothek.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone


def _percentile(values: list[float], p: float) -> float | None:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = max(0, min(len(vals) - 1, round(p / 100 * (len(vals) - 1))))
    return vals[k]


def load_daily_closes(market_db: str, symbol: str) -> list[tuple[str, float]]:
    """(date, close) aufsteigend aus bars_1d -- Basis für den Folgetags-Return."""
    con = sqlite3.connect(f"file:{market_db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts_utc_ms, close FROM bars_1d WHERE symbol=? ORDER BY ts_utc_ms",
            (symbol,)).fetchall()
    finally:
        con.close()
    return [(datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
             float(c)) for ms, c in rows]


def next_day_return_pct(closes: list[tuple[str, float]], day: str) -> float | None:
    """Return des NÄCHSTEN Handelstags nach `day` in Prozent (None, wenn der Folgetag
    noch nicht vorliegt -- z. B. der jüngste Entscheid oder Lücken in bars_1d)."""
    for i, (d, c) in enumerate(closes):
        if d == day and i + 1 < len(closes) and c:
            return (closes[i + 1][1] / c - 1) * 100
    return None


def load_decisions(journal_db: str) -> list[dict]:
    con = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts_utc, date, strategy, model, decision_json, request_json "
            "FROM decisions ORDER BY ts_utc").fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()
    out = []
    for ts, day, strat, model, dj, rj in rows:
        try:
            out.append({"ts_utc": ts, "date": day, "strategy": strat, "model": model,
                        "decision": json.loads(dj), "request": json.loads(rj or "{}")})
        except json.JSONDecodeError:
            continue
    return out


def evaluate(journal_db: str, market_db: str, symbol: str,
             fee_roundtrip: float) -> dict:
    """Aggregiert die Voting-Statistik je Modell + Lauf-Zähler. Reine Berechnung
    (testbar); die Formatierung übernimmt format_report()."""
    closes = load_daily_closes(market_db, symbol) if market_db else []
    decs = load_decisions(journal_db)
    stats: dict[str, dict] = {}
    runs = {"total": len(decs), "pregate": 0, "with_votes": 0, "no_return": 0}

    for rec in decs:
        d, req = rec["decision"], rec["request"]
        if rec["model"] == "pregate" or d.get("validator") == "hard_flag_pregate":
            runs["pregate"] += 1
            continue
        votes = d.get("votes") or []
        if not votes:
            continue
        runs["with_votes"] += 1
        direction = (req.get("signal") or {}).get("direction")
        size = float(req.get("size_suggested_eur") or 0)
        ret = next_day_return_pct(closes, rec["date"]) if closes else None
        # Richtungs-Return: positiv = der Handelstag WÄRE in Signalrichtung gut gewesen.
        dir_ret = (None if ret is None or direction not in ("LONG", "SHORT")
                   else (ret if direction == "LONG" else -ret))
        if dir_ret is None:
            runs["no_return"] += 1
        agg_action = d.get("action")
        for v in votes:
            m = str(v.get("model") or "?")
            s = stats.setdefault(m, {
                "n": 0, "trade": 0, "agree_aggregate": 0, "early_stop": 0,
                "schema_err": 0, "unreachable": 0, "latencies": [],
                "vetoes": 0, "veto_scored": 0, "veto_hits": 0,
                "veto_pnl_eur": 0.0, "missed_gain_days": 0})
            val = str(v.get("validator") or "")
            if val == "early_stop":
                s["early_stop"] += 1
                continue                       # nicht befragt -> keine Meinung gemessen
            s["n"] += 1
            act = v.get("action")
            if act == "TRADE":
                s["trade"] += 1
            if act == agg_action:
                s["agree_aggregate"] += 1
            if val in ("reject_json", "reject_schema"):
                s["schema_err"] += 1
            if val == "reject_unreachable":
                s["unreachable"] += 1
            if v.get("latency_ms") is not None:
                s["latencies"].append(v["latency_ms"])
            if act != "TRADE" and val != "reject_unreachable":
                s["vetoes"] += 1
                if dir_ret is not None and size > 0:
                    s["veto_scored"] += 1
                    if dir_ret < 0:
                        s["veto_hits"] += 1
                    else:
                        s["missed_gain_days"] += 1
                    # Kontrafaktisch: nicht gehandelt = Richtungs-P&L vermieden,
                    # Round-Trip-Gebühr gespart.
                    s["veto_pnl_eur"] += -(dir_ret / 100) * size + fee_roundtrip

    for m, s in stats.items():
        n = s["n"] or 1
        s["trade_rate"] = round(s["trade"] / n, 3)
        s["agree_rate"] = round(s["agree_aggregate"] / n, 3)
        s["schema_err_rate"] = round(s["schema_err"] / n, 3)
        s["latency_p50"] = _percentile(s["latencies"], 50)
        s["latency_p95"] = _percentile(s["latencies"], 95)
        s["veto_hit_rate"] = (round(s["veto_hits"] / s["veto_scored"], 3)
                              if s["veto_scored"] else None)
        s["veto_pnl_eur"] = round(s["veto_pnl_eur"], 2)
        del s["latencies"]
    return {"models": stats, "runs": runs, "symbol": symbol,
            "fee_roundtrip": fee_roundtrip}


def format_report(result: dict) -> str:
    runs, models = result["runs"], result["models"]
    lines = [f"[STAT] Voting-Kalibrierung ({result['symbol']}, Folgetags-Return, "
             f"Fee {result['fee_roundtrip']:.2f} €/RT)",
             f"Läufe: {runs['total']} gesamt - {runs['with_votes']} mit Voten - "
             f"{runs['pregate']} Pre-Gate (0 LLM-Kosten) - "
             f"{runs['no_return']} ohne Folgetag"]
    if not models:
        lines.append("Noch keine Einzelvoten im Journal.")
        return "\n".join(lines)
    for m, s in sorted(models.items()):
        hit = ("--" if s["veto_hit_rate"] is None
               else f"{s['veto_hit_rate']:.0%} ({s['veto_hits']}/{s['veto_scored']})")
        lat = (f"{s['latency_p50']:.0f}/{s['latency_p95']:.0f} ms"
               if s["latency_p50"] is not None else "--")
        warn = " [!] Dauer-Zustimmer?" if s["n"] >= 10 and s["agree_rate"] > 0.95 else ""
        lines.append(
            f"- {m}: n={s['n']} - TRADE {s['trade_rate']:.0%} - "
            f"Zustimmung {s['agree_rate']:.0%}{warn} - Schema-Fehler "
            f"{s['schema_err_rate']:.0%} - unreachable {s['unreachable']} - "
            f"early_stop {s['early_stop']} - Latenz p50/p95 {lat}")
        lines.append(
            f"    Vetos: {s['vetoes']} - Trefferquote {hit} - "
            f"kontrafaktischer P&L {s['veto_pnl_eur']:+.2f} € "
            f"(verpasste Gewinntage: {s['missed_gain_days']})")
    return "\n".join(lines)


def load_ml_predictions(journal_db: str) -> list[dict]:
    """Zeilen aus ml_predictions (mlforecast.py); [] wenn Tabelle/DB fehlt.
    Je (date, symbol) zählt nur die JÜNGSTE Prognose (Re-Runs desselben Tages
    überschreiben sich sonst gegenseitig in der Statistik)."""
    try:
        con = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
        rows = con.execute(
            "SELECT ts_utc, date, symbol, direction, p_direction, model_version "
            "FROM ml_predictions ORDER BY ts_utc").fetchall()
        con.close()
    except sqlite3.OperationalError:
        return []
    latest: dict[tuple[str, str], dict] = {}
    for ts, day, sym, direction, p, ver in rows:
        latest[(day, sym)] = {"ts_utc": ts, "date": day, "symbol": sym,
                              "direction": direction, "p_direction": float(p),
                              "model_version": ver}
    return [latest[k] for k in sorted(latest)]


def evaluate_ml(journal_db: str, market_db: str, symbol: str, fee_roundtrip: float,
                threshold: float = 0.45) -> dict:
    """Kalibrierung der scikit-Prüfinstanz gegen den realisierten Folgetags-Return.

    Metriken (Design-Memo): Trefferquote der p>=0,5-Prognosen vs. Basisrate,
    Brier-Score vs. Klimatologie (konstante Basisraten-Prognose) und der
    kontrafaktische Beitrag der ML_DISAGREE-Tage (p < threshold): wäre an diesen
    Tagen nicht gehandelt worden, was wäre gespart/verpasst? Bewusst deskriptiv --
    kleine Stichprobe, keine automatische Abschaltung; auch ein Negativbefund
    wird berichtet."""
    preds = [p for p in load_ml_predictions(journal_db) if p["symbol"] == symbol]
    closes = load_daily_closes(market_db, symbol) if market_db else []
    scored: list[tuple[float, int, float]] = []   # (p, outcome, dir_ret_pct)
    pending = 0
    for pr in preds:
        ret = next_day_return_pct(closes, pr["date"]) if closes else None
        if ret is None or pr["direction"] not in ("LONG", "SHORT"):
            pending += 1
            continue
        dir_ret = ret if pr["direction"] == "LONG" else -ret
        scored.append((pr["p_direction"], 1 if dir_ret > 0 else 0, dir_ret))

    out: dict = {"n_predictions": len(preds), "n_scored": len(scored),
                 "n_pending": pending, "threshold": threshold, "symbol": symbol}
    if not scored:
        return out
    n = len(scored)
    base = sum(o for _, o, _ in scored) / n
    hits = sum(1 for p, o, _ in scored if (p >= 0.5) == (o == 1))
    brier = sum((p - o) ** 2 for p, o, _ in scored) / n
    brier_clim = sum((base - o) ** 2 for _, o, _ in scored) / n
    disagree = [(p, o, r) for p, o, r in scored if p < threshold]
    # Kontrafaktisch wie beim Veto-P&L: ML_DISAGREE-Tag nicht gehandelt =>
    # Richtungs-P&L vermieden + Round-Trip-Gebühr gespart (Größe: 1000 € Referenz
    # entfällt -- wir rechnen in %-Punkten des Richtungs-Returns + Fee separat).
    out.update({
        "base_rate": round(base, 3),
        "accuracy": round(hits / n, 3),
        "edge_vs_base": round(hits / n - max(base, 1 - base), 3),
        "brier": round(brier, 4), "brier_climatology": round(brier_clim, 4),
        "brier_skill_score": (round(1 - brier / brier_clim, 4) if brier_clim else None),
        "disagree_days": len(disagree),
        "disagree_hit_days": sum(1 for _, o, _ in disagree if o == 0),
        "disagree_hit_rate": (round(sum(1 for _, o, _ in disagree if o == 0)
                                    / len(disagree), 3) if disagree else None),
        "disagree_avoided_ret_pct": (round(-sum(r for _, _, r in disagree), 2)
                                     if disagree else None),
        "fee_roundtrip": fee_roundtrip,
    })
    return out


def format_ml_report(ml: dict) -> str:
    lines = [f"[BOT] scikit-Prüfinstanz ({ml['symbol']}, Folgetags-Return, "
             f"ML_DISAGREE-Schwelle p < {ml['threshold']})",
             f"Prognosen: {ml['n_predictions']} - bewertbar {ml['n_scored']} - "
             f"ohne Folgetag {ml['n_pending']}"]
    if not ml.get("n_scored"):
        lines.append("Noch keine bewertbaren Prognosen (ml_predictions leer oder zu jung).")
        return "\n".join(lines)
    bss = ml["brier_skill_score"]
    verdict = ("über Klimatologie" if bss is not None and bss > 0
               else "OHNE Mehrwert ggü. Klimatologie (Negativbefund -- Rolle bleibt "
                    "Risikofilter)")
    lines.append(
        f"Trefferquote {ml['accuracy']:.0%} vs. Basisrate {ml['base_rate']:.0%} "
        f"(Edge {ml['edge_vs_base']:+.3f}) - Brier {ml['brier']:.4f} vs. "
        f"Klimatologie {ml['brier_climatology']:.4f} (Skill {bss:+.4f}) -- {verdict}")
    if ml["disagree_days"]:
        lines.append(
            f"ML_DISAGREE-Tage: {ml['disagree_days']} - davon Verlusttage vermieden: "
            f"{ml['disagree_hit_days']} ({ml['disagree_hit_rate']:.0%}) - "
            f"kumulierter vermiedener Richtungs-Return {ml['disagree_avoided_ret_pct']:+.2f} % "
            f"(+ {ml['fee_roundtrip']:.2f} €/RT gespart je Tag, kontrafaktisch)")
    else:
        lines.append("Keine ML_DISAGREE-Tage im Zeitraum.")
    return "\n".join(lines)


def write_csv(result: dict, path: str) -> None:
    cols = ["model", "n", "trade_rate", "agree_rate", "schema_err_rate", "unreachable",
            "early_stop", "latency_p50", "latency_p95", "vetoes", "veto_scored",
            "veto_hits", "veto_hit_rate", "missed_gain_days", "veto_pnl_eur"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for m, s in sorted(result["models"].items()):
            w.writerow([m] + [s.get(c) for c in cols[1:]])


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Kalibrierung des Multi-LLM-Votings (B3)")
    p.add_argument("--journal-db", default="decisions.sqlite")
    p.add_argument("--market-db", default="marketdata.sqlite")
    p.add_argument("--symbol", default="^GSPC")
    p.add_argument("--fee-roundtrip", type=float, default=7.80,
                   help="Round-Trip-Fixkosten in EUR (Trader Pro, STRATEGIE.md §5)")
    p.add_argument("--csv", default=None, help="Statistik zusätzlich als CSV schreiben")
    p.add_argument("--ml-threshold", type=float, default=0.45,
                   help="ML_DISAGREE-Schwelle für die scikit-Statistik "
                        "(= agentconfig ml.threshold_disagree)")
    p.add_argument("--no-ml", action="store_true",
                   help="scikit-Prüfinstanz-Block (ml_predictions) unterdrücken")
    args = p.parse_args(argv)
    result = evaluate(args.journal_db, args.market_db, args.symbol, args.fee_roundtrip)
    print(format_report(result))
    if not args.no_ml:
        ml = evaluate_ml(args.journal_db, args.market_db, args.symbol,
                         args.fee_roundtrip, args.ml_threshold)
        print()
        print(format_ml_report(ml))
    if args.csv:
        write_csv(result, args.csv)
        print(f"\nCSV -> {args.csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
