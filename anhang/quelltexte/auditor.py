#!/usr/bin/env python3
"""
auditor.py -- LLM-Auditor-Zyklus des Handelsagenten (bounded discretion).

Ablauf (STRATEGIE.md §2-3):
  DecisionRequest (strategy.py/strategyloader) -> Gemma 4 via Ollama (native API,
  Temperature 0, erzwungenes JSON-Schema) -> validate_response (fail-safe) ->
  Entscheidungsjournal (Tabelle 'decisions' in der Projekt-SQLite).

Sicherheitsprinzipien:
  * Der Auditor kann Trades nur verhindern/verkleinern/timen -- nie erzeugen:
    Jeder Fehlerpfad (Timeout, Verbindungsfehler, Schemabruch, unerwartete
    Ausnahme im Zyklus) endet in einem journalierten NO_TRADE.
  * Dieses Modul spricht NIE mit dem Broker -- es erzeugt nur die geprüfte
    Entscheidung; die Ausführung ist broker.py/orchestrate.py vorbehalten.
  * Secrets tauchen weder im Request noch im Prompt auf (credstore getrennt).

Entkopplung (Code-Review 20.07., vgl. SYSTEMDOKUMENTATION §4):
  * Der System-Prompt wird aus dem Strategienamen erzeugt (build_system_prompt)
    statt die SMA-Strategie hartzukodieren -- passend zum Plugin-System.
  * Der LLM-Zugriff liegt hinter einer Backend-Abstraktion (make_ollama_backend);
    der Anbieter ist damit austauschbar (Default: native Ollama-API).
  * Die Laufparameter sind in AuditConfig gebündelt statt als lange Positionsliste.

CLI:
  python3 auditor.py run      [--db ...] [--strategy sma] [--model gemma4:cloud]
                              [--base-url http://127.0.0.1:11434] [--timeout 90]
                              [--capital 2000] [--spread-bp N] [--mock JSON]
  python3 auditor.py history  [--db ...] [--limit 10]

Abhängigkeiten: nur Standardbibliothek (+ strategy.py/strategyloader.py-Kette).
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable

from strategy import HARD_FLAGS, SETUP, build_request, validate_response  # noqa: E402
import strategyloader as loader                                           # noqa: E402

log = logging.getLogger("auditor")

DEFAULT_BASE_URL = "http://127.0.0.1:11434"
DEFAULT_MODEL = "gemma4:cloud"
DEFAULT_TIMEOUT_S = 90

# Ein Backend nimmt (DecisionRequest, System-Prompt) und liefert
# (raw_content | None, latency_ms, error | None). So ist der LLM-Anbieter
# austauschbar; run_audit hängt nicht an Ollama-Spezifika.
Backend = Callable[[dict, str], "tuple[str | None, int, str | None]"]


def build_system_prompt(strategy_label: str) -> str:
    """Auditor-Prompt (STRATEGIE.md §3) als Zweitmeinung mit Veto. Das LLM bildet
    zuerst eine EIGENE Regime-Einschätzung (assessed_direction) und darf einem TRADE
    nur zustimmen, wenn sie mit dem Strategie-Signal übereinstimmt. Die Richtung bleibt
    fixiert und wird nie umgekehrt (bounded discretion). Der Strategiename wird
    eingesetzt statt hartkodiert."""
    return (
        "Du bist kritischer Risiko-Auditor eines regelbasierten Handelssystems. "
        f"Die Handelsrichtung ist durch die {strategy_label}-Strategie fixiert und "
        "nicht verhandelbar; du darfst sie NIEMALS umkehren. "
        "Bilde zunächst eine EIGENE Einschätzung der Regime-Richtung aus den "
        "Indikatoren (signal.meta mit sma_fast/sma_slow, last5_index_returns_pct, "
        "vol20_pct) und gib sie als \"assessed_direction\" (LONG, SHORT oder UNCLEAR) "
        "an. Vergleiche sie mit signal.direction. Deine Aufgabe ist "
        "Verlustrisiko-Minimierung: Prüfe zusätzlich Datenqualität, Marktlage und "
        "audit_flags. Enthält der Request ein ml_context-Feld, ist p_direction die "
        "kalibrierte Wahrscheinlichkeit eines statistischen Prüfmodells, dass der "
        "Folgetag in Signalrichtung schließt: Werte unter threshold_disagree (Flag "
        "ML_DISAGREE) sind ein ZUSÄTZLICHES Risikosignal wie vol20_pct/last5 -- sie "
        "erzwingen kein NO_TRADE, sprechen aber für Ablehnung oder eine kleinere "
        "Größe; das Modell ersetzt NIE deine eigene Einschätzung. "
        "Stimme einem TRADE NUR zu, wenn deine assessed_direction mit "
        "signal.direction übereinstimmt UND keine erhöhten Risiken vorliegen; "
        "andernfalls NO_TRADE. Bei Zustimmung wähle eine der entry_options (Bedeutung "
        "in entry_options_desc) und eine Positionsgröße innerhalb size_range_eur "
        "(niemals über size_suggested_eur). Wäge die Größe gegen cost_context ab: "
        "eine kleinere Größe senkt das Risiko, erhöht aber die Fixkostenquote "
        "(fee_roundtrip_eur, fee_pct_by_size_eur) -- begründe die Größenwahl. "
        "Sei skeptisch: Im Zweifel NO_TRADE. "
        "Antworte ausschließlich mit einem JSON-Objekt nach dem "
        "vorgegebenen Schema, ohne weiteren Text. Pflichtfelder exakt so benennen: "
        "\"action\" (TRADE oder NO_TRADE), \"assessed_direction\" (LONG/SHORT/UNCLEAR), "
        "\"entry\" (E1/E2/E3 oder null), \"size_eur\" (Zahl), \"reason\" (kurzer Text). "
        "Keine anderen Feldnamen."
    )


# Erzwungenes Antwortschema (Ollama structured output, 'format'-Feld)
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["TRADE", "NO_TRADE"]},
        "assessed_direction": {"type": "string",
                               "enum": ["LONG", "SHORT", "UNCLEAR"]},
        "entry": {"type": "string", "enum": ["E1", "E2", "E3"]},
        "size_eur": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["action", "assessed_direction", "reason"],
}

JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
    ts_utc      TEXT NOT NULL,
    date        TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    model       TEXT NOT NULL,
    latency_ms  INTEGER,
    request_json  TEXT NOT NULL,
    response_raw  TEXT,
    decision_json TEXT NOT NULL,
    validator   TEXT NOT NULL,
    error       TEXT
);
"""


@dataclass
class AuditConfig:
    """Laufparameter eines Audit-Zyklus (bündelt die frühere Positionsliste)."""
    db: str = "marketdata.sqlite"        # Marktdaten (nur lesen; wird per rsync gespiegelt)
    journal_db: str = "decisions.sqlite"  # Entscheidungsjournal (getrennt, rsync-sicher)
    strategy: str = "sma"
    params: dict | None = None
    capital: float = field(default_factory=lambda: SETUP["capital_eur"])
    spread_bp: float | None = None
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL
    models: list[str] | None = None     # Multi-Voting; None -> [model] (Einzelmodell)
    timeout_s: int = DEFAULT_TIMEOUT_S
    # B1 -- Early-Stop: Voting-Schleife abbrechen, sobald die NO_TRADE-Mehrheit
    # feststeht (spart Aufrufe/Latenz). False für den wöchentlichen Vollvoten-Lauf
    # (--no-early-stop), damit die Kalibrierung (evaluate_votes) Vollstatistik bekommt.
    early_stop: bool = True


# --------------------------------------------------------------------------
# LLM-Backend (native Ollama /api/chat, stdlib)
# --------------------------------------------------------------------------

def call_ollama(base_url: str, model: str, request: dict, timeout_s: int,
                system_prompt: str | None = None
                ) -> tuple[str | None, int, str | None]:
    """Returns (raw_content | None, latency_ms, error | None)."""
    if system_prompt is None:
        system_prompt = build_system_prompt("SMA")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",
             "content": "DecisionRequest:\n" + json.dumps(request,
                                                          ensure_ascii=False)},
        ],
        "stream": False,
        # think=False: Reasoning-Ausgabe abschalten. Empirisch nötig für Thinking-
        # Modelle (qwen3.5, gpt-oss) -- mit aktivem Thinking überstimmt der Reasoning-
        # Modus das erzwungene JSON-Schema (Fliesstext statt Schema, teils Endlos-
        # schleifen). Mit think=False liefern sie kompaktes, schema-konformes JSON.
        "think": False,
        "format": RESPONSE_SCHEMA,
        "options": {"temperature": 0},
    }
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/chat",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = json.loads(resp.read())
        latency = int((time.monotonic() - t0) * 1000)
        return body.get("message", {}).get("content"), latency, None
    except urllib.error.HTTPError as exc:
        latency = int((time.monotonic() - t0) * 1000)
        return None, latency, f"HTTP {exc.code}: {exc.read()[:200]!r}"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        latency = int((time.monotonic() - t0) * 1000)
        return None, latency, f"Verbindung/Timeout: {exc}"


def make_ollama_backend(base_url: str, model: str, timeout_s: int) -> Backend:
    """Default-Backend: bindet die Ollama-Zugriffsparameter und liefert ein
    (request, system_prompt) -> (raw, latency, error)-Callable."""
    def backend(request: dict, system_prompt: str):
        return call_ollama(base_url, model, request, timeout_s, system_prompt)
    return backend


def normalize_fields(raw: str) -> str:
    """Mappt häufige LLM-Feld-Synonyme auf das erwartete Schema, bevor der
    Validator prüft (empirisch: gemma4:cloud liefert teils 'decision'/'entry_option').
    Die Rohantwort bleibt fürs Journal unverändert erhalten."""
    try:
        d = json.loads(extract_json(raw))
    except (json.JSONDecodeError, TypeError):
        return raw
    synonyms = {"decision": "action", "entry_option": "entry",
                "entryOption": "entry", "size": "size_eur",
                "position_size_eur": "size_eur"}
    for src_key, dst_key in synonyms.items():
        if src_key in d and dst_key not in d:
            d[dst_key] = d[src_key]
    return json.dumps(d)


def extract_json(raw: str) -> str:
    """Defensive Extraktion: entfernt Markdown-Zäune und isoliert den ersten
    JSON-Block. LLMs verpacken JSON gern in ```json ...``` oder Begleittext --
    der Validator soll die Substanz prüfen, nicht die Verpackung.
    Findet sich kein Block, wird der Rohtext unverändert zurückgegeben
    (Validator lehnt dann regulär ab)."""
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[-1]           # erste Zaunzeile (```json) entfernen
        if s.rstrip().endswith("```"):
            s = s.rstrip()[:-3]
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end > start:
        return s[start:end + 1]
    return raw


# --------------------------------------------------------------------------
# Journal
# --------------------------------------------------------------------------

def journal(db: str, row: dict) -> None:
    con = sqlite3.connect(db, timeout=30)
    con.execute("PRAGMA busy_timeout=10000")
    con.executescript(JOURNAL_SCHEMA)
    # Spalten explizit benennen statt positional VALUES(?...): entkoppelt das INSERT
    # von der Spaltenreihenfolge in JOURNAL_SCHEMA -- spätere Spaltenergänzungen
    # brechen das Insert dann nicht mehr stillschweigend (Spalten-/Werte-Versatz).
    con.execute(
        "INSERT INTO decisions "
        "(ts_utc, date, strategy, model, latency_ms, request_json, "
        "response_raw, decision_json, validator, error) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (row["ts_utc"], row["date"], row["strategy"], row["model"],
         row["latency_ms"], row["request_json"], row["response_raw"],
         row["decision_json"], row["validator"], row["error"]))
    con.commit()
    con.close()


# --------------------------------------------------------------------------
# Audit-Zyklus
# --------------------------------------------------------------------------

def _no_trade(reason: str, validator: str) -> dict:
    return {"action": "NO_TRADE", "entry": None, "size_eur": 0.0,
            "reason": reason, "validator": validator}


def _journal_no_trade(db: str, strategy: str, request: dict, decision: dict,
                      error: str | None) -> None:
    """Fail-safe-Journalierung: schreibt ein NO_TRADE, auch wenn kein LLM-Aufruf
    zustande kam. Nutzt request['date'], falls vorhanden, sonst heute (UTC)."""
    journal(db, {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": request.get("date", date.today().isoformat()),
        "strategy": strategy, "model": "none", "latency_ms": 0,
        "request_json": json.dumps(request, ensure_ascii=False),
        "response_raw": None,
        "decision_json": json.dumps(decision, ensure_ascii=False),
        "validator": decision.get("validator", ""), "error": error,
    })


def combine_verdicts(decisions: list[dict], request: dict) -> dict:
    """Kombiniert die validierten Einzel-Voten mehrerer LLMs (voting-fähig).

    Regel: TRADE nur bei STRIKTER MEHRHEIT der TRADE-Voten über alle Modelle
    (len(TRADE)-2 > N). Gleichstand oder Veto-Mehrheit => NO_TRADE ("kein
    Mehrheitsbescheid = Veto"). Für ZWEI Modelle bedeutet das Einstimmigkeit; ein nicht
    erreichbares Modell zählt als NO_TRADE-Votum (konservativ). Harte Flags (DATA_STALE
    etc.) sind bereits je Modell in validate_response als NO_TRADE durchgesetzt und
    blocken damit unabhängig vom Mehrheitsentscheid. Bei TRADE gilt die KONSERVATIVSTE
    (kleinste) Größe. Die Felder votes_trade/votes_total werden fürs Journal gesetzt."""
    n = len(decisions)
    if n == 0:
        return {**_no_trade("VALIDATOR: keine Bewertung", "reject_no_verdict"),
                "votes_trade": 0, "votes_total": 0, "votes": []}
    votes = [_vote_row(d) for d in decisions]     # strukturierte Einzelvoten (Journal/Audit)
    trades = [d for d in decisions if d.get("action") == "TRADE"]
    if len(trades) * 2 > n:                       # strikte Mehrheit für TRADE
        base = dict(trades[0])
        base["size_eur"] = round(min(float(d.get("size_eur", 0)) for d in trades), 2)
        base["reason"] = " | ".join(d.get("reason", "") for d in decisions)[:500]
        base["validator"] = ",".join(sorted({d.get("validator", "") for d in trades})) or "ok"
        base["votes_trade"], base["votes_total"], base["votes"] = len(trades), n, votes
        base.pop("model", None)                   # Aggregat-Feld sauber halten
        return base
    veto = next((d for d in decisions if d.get("action") != "TRADE"), None)
    return {**_no_trade((veto or {}).get("reason", "VALIDATOR: keine TRADE-Mehrheit"),
                        (veto or {}).get("validator", "reject_no_majority")),
            "votes_trade": len(trades), "votes_total": n, "votes": votes}


def _vote_row(d: dict) -> dict:
    """Ein Einzelvotum kompakt fürs Journal/die Inspektion (welches Modell wie entschied)."""
    return {"model": d.get("model", "?"), "action": d.get("action"),
            "direction": d.get("assessed_direction"), "size_eur": d.get("size_eur"),
            "validator": d.get("validator"), "latency_ms": d.get("latency_ms"),
            "reason": (d.get("reason") or "")[:240]}


def run_audit(cfg: AuditConfig, mock: str | None = None,
              backend: Backend | None = None) -> dict:
    """Ein Audit-Zyklus: Request bauen -> LLM (oder Mock) -> Validator -> Journal.

    Fail-safe-Kette: (1) Ein nicht erreichbarer/fehlerhafter LLM liefert raw=None
    und führt zu NO_TRADE. (2) Jede tatsächliche Antwort läuft durch den fail-safe
    validate_response (liefert bei ungültigem JSON/Schema/Größe stets ein NO_TRADE-
    Dict und wirft nicht). (3) Ein umschließender Fallback fängt auch unerwartete
    Ausnahmen im Zyklus (z. B. build_request bei zu wenig Historie) ab und
    journalisiert ein NO_TRADE -- so bleibt die dokumentierte Garantie auch gegen
    künftige Änderungen erhalten, ohne dass ein Folgeschritt einen veralteten
    Entscheid liest. Der Fehler wird zusätzlich laut geloggt."""
    # --- Phase 1: Request aufbauen (kann scheitern) ---
    try:
        strat, plugin = loader.get_plugin(cfg.strategy, cfg.params)
        request = build_request(cfg.db, strat, plugin, cfg.capital, cfg.spread_bp)
        strategy_name = strat.name
    # Fail-safe: kein Request -> NO_TRADE. Bewusst auch SystemExit gefangen, da der
    # Loader Konfigurationsfehler (z. B. unbekannte Strategie) als SystemExit wirft;
    # strategy.py-Domänenfehler kommen als StrategyError (Exception-Unterklasse).
    except (Exception, SystemExit) as exc:  # noqa: BLE001
        log.error("Audit-Zyklus vor LLM-Aufruf abgebrochen: %s", exc)
        decision = _no_trade(f"VALIDATOR: Zyklusfehler ({exc})", "reject_exception")
        stub = {"date": date.today().isoformat(),
                "signal": {"direction": None}, "audit_flags": []}
        _journal_no_trade(cfg.journal_db, cfg.strategy, stub, decision, str(exc))
        return {"request": stub, "response_raw": None, "decision": decision,
                "latency_ms": 0, "model": "none", "error": str(exc)}

    # --- B1: Hard-Flag-Pre-Gate -- deterministische Checks VOR den LLMs ---
    # Harte Flags erzwingen in validate_response ohnehin jedes Mal NO_TRADE; das
    # Ergebnis steht damit VOR dem ersten LLM-Aufruf fest. Journalisieren und
    # zurückkehren: 0 Aufrufe, 0 Kosten, 0 Latenz an Flag-Tagen (z. B. DATA_STALE,
    # FLAT_SIGNAL). Semantik unverändert konservativ.
    hard = sorted(HARD_FLAGS.intersection(request.get("audit_flags") or []))
    if hard:
        log.info("Hard-Flag-Pre-Gate: %s -> NO_TRADE ohne LLM-Aufruf.", ",".join(hard))
        decision = {**_no_trade(f"VALIDATOR: harte Flags [{', '.join(hard)}] -- "
                                "deterministisch NO_TRADE, LLM-Aufrufe übersprungen "
                                "(Pre-Gate)", "hard_flag_pregate"),
                    "votes_trade": 0, "votes_total": 0, "votes": []}
        journal(cfg.journal_db, {
            "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "date": request["date"], "strategy": strategy_name, "model": "pregate",
            "latency_ms": 0,
            "request_json": json.dumps(request, ensure_ascii=False),
            "response_raw": None,
            "decision_json": json.dumps(decision, ensure_ascii=False),
            "validator": decision.get("validator", ""), "error": None,
        })
        return {"request": request, "response_raw": None, "decision": decision,
                "latency_ms": 0, "model": "pregate", "error": None}

    # --- Phase 2: LLM-Voten (oder Mock) + Validierung + Kombination ---
    # Ein Votum je konfiguriertem Modell; combine_verdicts bildet den Mehrheits-
    # entscheid. cfg.models=None => Einzelmodell (verhaltensneutral zum Vorstand).
    # Betriebs-Hinweis (B1): die Reihenfolge in agentconfig.auditor.models sollte
    # latenz-aufsteigend sein (schnellstes Modell zuerst) -- der Early-Stop spart dann
    # maximal.
    models = cfg.models or [cfg.model]
    raws: dict[str, str | None] = {}
    latency, errors = 0, []
    try:
        if mock is not None:
            model = "mock"
            raws["mock"] = mock
            log.info("Mock-Modus: LLM-Aufruf übersprungen.")
            verdicts = [validate_response(request, normalize_fields(mock))]
            verdicts[0]["model"] = "mock"
            verdicts[0]["latency_ms"] = 0
        else:
            model = ",".join(models)
            prompt = build_system_prompt(strategy_name.upper())
            verdicts = []
            n_models = len(models)
            stopped = False
            for m in models:
                # B1 -- Early-Stop: NO_TRADE steht fest, sobald selbst alle noch
                # ausstehenden TRADE-Voten keine strikte Mehrheit mehr ergeben können.
                # Eine bereits erreichte TRADE-Mehrheit stoppt bewusst NICHT früher:
                # weitere Voten können die Größe nur konservativer machen (min-Regel).
                if cfg.early_stop and not stopped and verdicts:
                    trades_so_far = sum(1 for v in verdicts
                                        if v.get("action") == "TRADE")
                    remaining = n_models - len(verdicts)
                    if (trades_so_far + remaining) * 2 <= n_models:
                        stopped = True
                        log.info("Early-Stop: NO_TRADE-Mehrheit steht (%d/%d) -- "
                                 "restliche Modelle werden nicht befragt.",
                                 len(verdicts) - trades_so_far, n_models)
                if stopped:
                    v = _no_trade("VALIDATOR: nicht befragt -- NO_TRADE-Mehrheit stand "
                                  "fest (Early-Stop)", "early_stop")
                    v["model"], v["latency_ms"] = m, None
                    verdicts.append(v)
                    continue
                be = backend or make_ollama_backend(cfg.base_url, m, cfg.timeout_s)
                raw_m, lat_m, err_m = be(request, prompt)
                raws[m], latency = raw_m, latency + lat_m
                if err_m:
                    errors.append(f"{m}: {err_m}")
                if raw_m is None:     # Fail-safe: Modell nicht erreichbar = Veto-Votum
                    v = _no_trade(f"VALIDATOR: {m} nicht erreichbar ({err_m})",
                                  "reject_unreachable")
                else:
                    v = validate_response(request, normalize_fields(raw_m))
                v["model"] = m        # Modell-Tag für die strukturierten Einzelvoten
                v["latency_ms"] = lat_m   # Antwortzeit dieses Modells (nicht nur Aggregat)
                verdicts.append(v)
        decision = combine_verdicts(verdicts, request)
    except Exception as exc:  # noqa: BLE001 -- Fail-safe: Verarbeitungsfehler -> NO_TRADE
        log.error("Verarbeitung/Validierung fehlgeschlagen: %s", exc)
        model, errors = "none", errors + [f"proc: {exc}"]
        decision = _no_trade(f"VALIDATOR: Verarbeitungsfehler ({exc})", "reject_exception")

    error = "; ".join(errors) or None
    raw_journal = (raws.get("mock") if mock is not None else
                   raws.get(models[0]) if len(models) == 1 else
                   json.dumps(raws, ensure_ascii=False))

    journal(cfg.journal_db, {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": request["date"], "strategy": strategy_name, "model": model,
        "latency_ms": latency,
        "request_json": json.dumps(request, ensure_ascii=False),
        "response_raw": raw_journal,
        "decision_json": json.dumps(decision, ensure_ascii=False),
        "validator": decision.get("validator", ""), "error": error,
    })
    return {"request": request, "response_raw": raw_journal, "decision": decision,
            "latency_ms": latency, "model": model, "error": error}


def show_history(db: str, limit: int) -> None:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts_utc, date, strategy, model, latency_ms, validator, "
            "decision_json FROM decisions ORDER BY ts_utc DESC LIMIT ?",
            (limit,)).fetchall()
    except sqlite3.OperationalError:
        print("Noch kein Entscheidungsjournal vorhanden.")
        return
    finally:
        con.close()
    for r in rows:
        d = json.loads(r[6])
        print(f"{r[0]}  {r[1]}  {r[2]:<10} {r[3]:<14} {r[4] or '-':>6} ms  "
              f"{d['action']:<9} size={d.get('size_eur', 0):<8} "
              f"validator={r[5]}  reason={d.get('reason', '')[:60]}")


def show_votes(db: str, limit: int) -> None:
    """Zeigt je Entscheidung die strukturierten Einzelvoten der Modelle (Voting-Audit):
    welches Modell TRADE/NO_TRADE mit welcher Richtung/Größe/Begründung, plus das
    Mehrheits-Aggregat."""
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        print("Noch kein Entscheidungsjournal vorhanden.")
        return
    try:
        rows = con.execute(
            "SELECT ts_utc, date, strategy, decision_json FROM decisions "
            "ORDER BY ts_utc DESC LIMIT ?", (limit,)).fetchall()
    except sqlite3.OperationalError:
        print("Noch kein Entscheidungsjournal vorhanden.")
        return
    finally:
        con.close()
    for ts, dt, strat, dj in rows:
        d = json.loads(dj)
        print(f"\n{ts}  {dt}  [{strat}]  -> AGGREGAT: {d.get('action')}  "
              f"(Mehrheit {d.get('votes_trade', '?')}/{d.get('votes_total', '?')} TRADE, "
              f"size={d.get('size_eur', 0)})")
        votes = d.get("votes")
        if not votes:
            print("  (keine strukturierten Einzelvoten -- Entscheidung vor dem votes-Update)")
            continue
        for v in votes:
            print(f"  - {str(v.get('model')):<20} {str(v.get('action')):<9} "
                  f"dir={v.get('direction')} size={v.get('size_eur')} [{v.get('validator')}]")
            reason = (v.get("reason") or "").strip()
            if reason:
                print(f"      {reason[:200]}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="LLM-Auditor-Zyklus (bounded discretion)")
    p.add_argument("--db", default="marketdata.sqlite",
                   help="Marktdaten (nur lesen; per rsync gespiegelt)")
    p.add_argument("--journal-db", default="decisions.sqlite",
                   help="Entscheidungsjournal (getrennt von den Marktdaten)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("run")
    sp.add_argument("--strategy", default="sma")
    sp.add_argument("--params", default=None)
    sp.add_argument("--capital", type=float, default=SETUP["capital_eur"])
    sp.add_argument("--spread-bp", type=float, default=None)
    sp.add_argument("--base-url", default=DEFAULT_BASE_URL)
    sp.add_argument("--model", default=DEFAULT_MODEL)
    sp.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    sp.add_argument("--mock", default=None,
                    help="JSON-Antwort statt LLM (Pipeline-/Replay-Test)")
    sp.add_argument("--no-early-stop", action="store_true",
                    help="B1: alle Modelle IMMER befragen (Vollvoten für die "
                         "Kalibrierung, z. B. wöchentlicher evaluate_votes-Lauf)")
    sp = sub.add_parser("history")
    sp.add_argument("--limit", type=int, default=10)
    sp = sub.add_parser("votes", help="Einzelvoten der Modelle je Entscheidung (Voting-Audit)")
    sp.add_argument("--limit", type=int, default=3)

    args = p.parse_args(argv)
    if args.cmd == "history":
        show_history(args.journal_db, args.limit)
        return
    if args.cmd == "votes":
        show_votes(args.journal_db, args.limit)
        return

    params = json.loads(args.params) if args.params else None
    # Modell-Liste + base_url + timeout_s aus agentconfig.yaml (auditor-Sektion);
    # CLI als Fallback. M1: timeout_s zentral in der Config (Mac-mini: 180 statt 90),
    # run_cycle leitet sein Gesamtbudget aus DEMSELBEN Wert ab.
    acfg = loader.load_auditor_config()
    cfg = AuditConfig(db=args.db, journal_db=args.journal_db,
                      strategy=args.strategy, params=params,
                      capital=args.capital, spread_bp=args.spread_bp,
                      base_url=acfg.get("base_url") or args.base_url,
                      model=args.model, models=acfg.get("models"),
                      timeout_s=acfg.get("timeout_s") or args.timeout,
                      early_stop=not args.no_early_stop)
    result = run_audit(cfg, mock=args.mock)
    print(json.dumps({
        "date": result["request"]["date"],
        "signal": result["request"]["signal"]["direction"],
        "audit_flags": result["request"]["audit_flags"],
        "model": result["model"], "latency_ms": result["latency_ms"],
        "decision": result["decision"],
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
