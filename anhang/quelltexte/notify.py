#!/usr/bin/env python3
"""
notify.py -- formatiert den letzten Auditor-Entscheid als kurze Signal-Nachricht.

Liest den jüngsten Eintrag der 'decisions'-Tabelle und gibt eine kompakte,
menschenlesbare Zusammenfassung auf stdout aus (von run_cycle.sh an signal-cli
weitergereicht). Keine Secrets, keine Rohdaten -- nur das Entscheidungsergebnis.

CLI:  python3 notify.py [--journal-db decisions.sqlite]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone


def latest_message(journal_db: str) -> str:
    con = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
    try:
        row = con.execute(
            "SELECT ts_utc, date, strategy, model, latency_ms, decision_json, "
            "request_json FROM decisions ORDER BY ts_utc DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        return "Trading-Agent: noch kein Auditor-Lauf im Journal."
    finally:
        con.close()
    if not row:
        return "Trading-Agent: noch kein Auditor-Lauf im Journal."

    ts, date, strat, model, latency, dec_raw, req_raw = row
    dec = json.loads(dec_raw)
    req = json.loads(req_raw)
    sig = req.get("signal", {}).get("direction", "?")
    flags = ", ".join(req.get("audit_flags", [])) or "keine"
    inst = (req.get("instrument") or {}).get("isin", "--")
    action = dec.get("action", "?")
    icon = "[+]" if action == "TRADE" else "[o]"
    # Laufzeit (ts_utc) sichtbar machen; `date` ist der Datenstand-Bar (letzter
    # abgeschlossener Handelstag), nicht die Uhrzeit des Laufs.
    run = (ts[:16].replace("T", " ") + " UTC") if ts else date

    lines = [
        f"{icon} Trading-Agent -- Lauf {run} - Datenstand {date} ({strat})",
        f"Signal: {sig} | Instrument: {inst}",
        f"Entscheidung: {action}",
    ]
    # M2 -- Stale-Guard: dieser Report zeigt IMMER den jüngsten Journal-Eintrag.
    # Ist der nicht von HEUTE (UTC), lief der heutige Auditor nicht bis zum Journal
    # (Timeout/Preflight-Abbruch/DB-Fehler) -- dann darf der Report nicht wie ein
    # frischer Entscheid aussehen. Deutliche Kennzeichnung an erster Stelle.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not (ts or "").startswith(today):
        lines.insert(0, f"[!] VERALTET -- kein Lauf von HEUTE im Journal; angezeigt "
                        f"wird der letzte Eintrag ({run}). Auditor/Logs prüfen!")
    if action == "TRADE":
        lines.append(f"Einstieg: {dec.get('entry')} | Größe: {dec.get('size_eur')} €")
    lines.append(f"Begründung: {dec.get('reason', '')[:180]}")
    lines.append(f"Flags: {flags} | Modell: {model} | {latency} ms")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Signal-Report des letzten Auditor-Laufs")
    p.add_argument("--journal-db", default="decisions.sqlite",
                   help="Entscheidungsjournal (getrennt von den Marktdaten)")
    args = p.parse_args()
    sys.stdout.write(latest_message(args.journal_db))


if __name__ == "__main__":
    main()
