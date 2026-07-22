#!/usr/bin/env python3
"""
datacollect.py -- Datenpipeline S&P 500 für den SMA-Trading-Agenten (FOM-Projektarbeit).

Eigenständiger Datensammler (läuft als systemd-Dienst auf der collector-vm,
unabhängig vom Agenten/LLM). Erfasst Marktdaten, leitet Bars ab, sichert die
Datenqualität und ist für den unbeaufsichtigten Dauerbetrieb gehärtet.

Designprinzipien (vgl. PLAN.md):
  Rohticks speichern, Bars daraus ableiten (nie umgekehrt) - Zeitstempel durchgängig
  UTC - SQLite im WAL-Modus - Lücken transparent protokollieren.

Funktionsumfang:
  * Erfassung: Bootstrap-Historie (1m/5m/1d via yfinance) sowie Live-Sammlung über
    WebSocket (Index/ETF) und paralleles fast_info-Polling (Futures, da nicht
    gestreamt). Schreibpfad über dedizierten Writer-Thread mit Batch-Commit
    (WS-Callback enqueued nur, blockiert nie).
  * Datenqualität: Sanity-Filter (nicht-positive Preise verworfen, Preissprünge
    > SANITY_MAX_JUMP_PCT als suspect markiert); Zweitquellen-Cross-Check des
    Tagesschlusses gegen FRED (offizielle EOD-Quelle).
  * Betrieb/Härtung: Reconnect mit Backoff; Poll-Fallback (--mode auto); Stall-
    Reconnect bei stillem WS-Ausfall; selbstheilender Poll-Thread; Daten-Watchdog
    (Echtzeit-Alarm bei Totalstille); sauberer Shutdown (WS im Daemon-Thread,
    Hauptthread beendet in < 1 s auf SIGTERM -> keine systemd-Fehlalarme).
  * Wartung: Rollup+Prune alter Rohticks zu Bars inkl. Integritätsprüfung
    (PRAGMA quick_check); VACUUM; Disk-Guard. Alarme werden in
    ~/sync/collector_alerts.log geschrieben und von der agent-vm via Signal relayed.
  * Symbolquelle: explizite CLI-Angabe > data_requirements.json (Strategie-
    Datenbedarf) > Fallback.

CLI:
  python datacollect.py init        [--db pfad]
  python datacollect.py bootstrap   [--db pfad] [--symbols ...]
  python datacollect.py collect     [--db pfad] [--symbols ...] [--min-interval 5]
                                    [--mode auto|ws|poll] [--poll-interval 5]
  python datacollect.py aggregate   [--db pfad] [--interval 1m|5m] [--symbols ...]
  python datacollect.py crosscheck  [--db pfad] [--symbols ...]   # FRED-Zweitquelle
  python datacollect.py maintain    [--db pfad] [--days N] [--symbols ...]  # Rollup+Prune+quick_check
  python datacollect.py vacuum      [--db pfad]                   # nur bei pausiertem Collector
  python datacollect.py diskguard   [--db pfad] [--threshold 80]
  python datacollect.py stats       [--db pfad] [--date YYYY-MM-DD]

Abhängigkeiten: python>=3.10, sqlite>=3.25, yfinance>=0.2.50 (nur bootstrap/collect/
crosscheck-Historie); FRED-API-Key in $FRED_API_KEY oder ~/.fred_key (nur crosscheck).

Revisionshinweis: Kernpipeline (Writer-Thread, Poll-Fallback, Lückenklassifikation,
Window-Function-Aggregation, defensives WS-Parsing) gemäß KRITISCHE_PRUEFUNG_2.md §3;
spätere Härtung (Sanity, FRED, Futures-Poll, Watchdog, Wartung, sauberer Shutdown)
dokumentiert in SYSTEMDOKUMENTATION.md §2 und §7.
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import signal as os_signal
import sqlite3
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("datacollect")

# --------------------------------------------------------------------------
# Konfiguration
# --------------------------------------------------------------------------

DEFAULT_DB = "marketdata.sqlite"
DEFAULT_SYMBOLS = ["^GSPC", "SPY", "ES=F"]   # Fallback, falls kein Datenbedarf vorliegt
REQUIREMENTS_FILE = "data_requirements.json" # erzeugt von: strategy.py datareq


def resolve_symbols(cli_symbols: list[str] | None) -> list[str]:
    """Symbolquelle: explizite CLI-Angabe > data_requirements.json > Fallback.

    Die JSON-Datei wird vom Strategie-Handler erzeugt (strategy.py datareq) und
    aggregiert den deklarierten Datenbedarf aller Strategie-Plugins -- die
    Strategien geben damit dem Collector vor, was zu sammeln ist.
    """
    if cli_symbols:
        return cli_symbols
    try:
        req = json.loads(open(REQUIREMENTS_FILE, encoding="utf-8").read())
        syms = req.get("symbols") or []
        if syms:
            log.info("Symbole aus %s (Strategien: %s): %s",
                     REQUIREMENTS_FILE, ",".join(req.get("strategies", [])), syms)
            return syms
    except FileNotFoundError:
        pass
    except Exception as exc:  # noqa: BLE001
        log.warning("%s unlesbar (%s) -- nutze Fallback", REQUIREMENTS_FILE, exc)
    log.info("Kein Datenbedarf hinterlegt -- Fallback-Symbole: %s", DEFAULT_SYMBOLS)
    return DEFAULT_SYMBOLS

BOOTSTRAP_JOBS = [("1m", "7d", "bars_1m"), ("5m", "60d", "bars_5m"), ("1d", "max", "bars_1d")]

GAP_SILENCE_S = 120            # Stille > 120 s = Lücke 'silence'
GAP_SESSION_BREAK_S = 12 * 3600  # Stille > 12 h = 'session_break' (Wochenende/Feiertag)
RECONNECT_BACKOFF_S = [1, 2, 5, 10, 30, 60]
WS_FAIL_TO_POLL = 3            # auto-Modus: nach N WS-Fehlern -> Poll-Phase
POLL_PHASE_S = 300             # Dauer der Poll-Phase, danach WS-Retry
BATCH_MAX_ROWS = 500           # Writer: Batchgröße
BATCH_FLUSH_S = 1.0            # Writer: max. Latenz bis Commit
STATS_FLUSH_S = 900            # Statistik alle 15 min sichern
STALL_TIMEOUT_S = 120         # kein Tick > 120 s bei aktiver Session -> Reconnect erzwingen
STALL_MAX_S = 1800            # > 30 min ohne Tick: Markt wohl geschlossen -> nicht erzwingen (kein Spam)
WATCHDOG_ALERT_S = 600        # keine Marktdaten (alle Quellen) > 10 min -> Echtzeit-Alarm
WATCHDOG_MAX_S = 7200         # > 2 h: echte Marktpause (Wochenende/Halt) -> nicht alarmieren


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS ticks_raw (
    ts_utc_ms   INTEGER NOT NULL,      -- Exchange-/Quote-Zeitstempel (Epoch ms, UTC)
    recv_utc_ms INTEGER NOT NULL,      -- lokaler Empfang (Latenzmessung)
    symbol      TEXT    NOT NULL,
    price       REAL    NOT NULL,
    source      TEXT    NOT NULL,      -- 'ws' | 'poll'
    suspect     INTEGER NOT NULL DEFAULT 0,  -- 1 = Plausibilitäts-Flag (Sprung > Schwelle)
    PRIMARY KEY (symbol, ts_utc_ms, recv_utc_ms)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks_raw(symbol, ts_utc_ms);

CREATE TABLE IF NOT EXISTS bars_1m (
    ts_utc_ms INTEGER NOT NULL, symbol TEXT NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL, source TEXT NOT NULL,   -- 'yf_hist' | 'ticks'
    PRIMARY KEY (symbol, ts_utc_ms)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS bars_5m (
    ts_utc_ms INTEGER NOT NULL, symbol TEXT NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL, source TEXT NOT NULL,
    PRIMARY KEY (symbol, ts_utc_ms)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS bars_1d (
    ts_utc_ms INTEGER NOT NULL, symbol TEXT NOT NULL,
    open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
    volume REAL, source TEXT NOT NULL,
    PRIMARY KEY (symbol, ts_utc_ms)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS gaps (
    symbol      TEXT NOT NULL,
    from_utc_ms INTEGER NOT NULL,
    to_utc_ms   INTEGER NOT NULL,
    reason      TEXT NOT NULL,   -- 'silence' | 'session_break' | 'disconnect' | 'shutdown'
    PRIMARY KEY (symbol, from_utc_ms)
);

CREATE TABLE IF NOT EXISTS collect_stats (
    date_utc     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    tick_count   INTEGER NOT NULL DEFAULT 0,
    stored_count INTEGER NOT NULL DEFAULT 0,
    disconnects  INTEGER NOT NULL DEFAULT 0,
    first_tick_ms INTEGER,
    last_tick_ms  INTEGER,
    PRIMARY KEY (date_utc, symbol)
);

-- Zweitquellen-Cross-Check: EOD-Schlusskurs Yahoo (bars_1d) gegen offizielle
-- Zweitquelle FRED (St. Louis Fed). Hinweis: Die Spalte 'stooq_close' ist historisch
-- benannt (ursprünglich Stooq, das inzwischen bot-gesperrt ist) und enthält jetzt den
-- FRED-Schlusskurs; der Name bleibt aus Kompatibilität mit bestehenden DBs erhalten.
CREATE TABLE IF NOT EXISTS source_crosscheck (
    date_utc    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    yf_close    REAL,
    stooq_close REAL,                          -- enthält den FRED-Schlusskurs (Name historisch)
    diff_pct    REAL,
    suspect     INTEGER NOT NULL DEFAULT 0,    -- 1 = Divergenz über Schwelle
    ts_utc      TEXT NOT NULL,
    PRIMARY KEY (date_utc, symbol)
);
"""

# Zweitquelle: FRED (St. Louis Fed) -- offizielle, unabhängige EOD-Daten.
# Serie SP500 = S&P-500-Schlusskurs. API-Key (kostenlos) aus FRED_API_KEY oder
# aus Datei ~/.fred_key. Nur der Basiswert (^GSPC) wird gegengeprüft.
FRED_SERIES = {"^GSPC": "SP500"}
CROSSCHECK_THRESHOLD_PCT = 0.5    # Divergenz-Schwelle -> suspect=1 + Alarm-Kandidat

# Sanity-Filter (Tick-Eingang)
SANITY_MAX_JUMP_PCT = 5.0         # Sprung zum letzten Tick > 5 % -> suspect=1 (nicht verwerfen)
RAW_RETENTION_DAYS = 10           # Rohticks älter als N Tage: zu Bars verdichten + löschen


def open_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.executescript(SCHEMA)
    # Migration: suspect-Spalte für bereits bestehende ticks_raw-Tabellen
    try:
        con.execute("ALTER TABLE ticks_raw ADD COLUMN suspect INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass                                    # Spalte existiert bereits
    con.commit()
    return con


# --------------------------------------------------------------------------
# Writer-Thread: einziger DB-Schreiber im Collect-Betrieb
# --------------------------------------------------------------------------

class DBWriter(threading.Thread):
    """Konsumiert Events aus einer Queue und persistiert gebatcht.

    Event-Formate:
      ("tick",  (ts_ms, recv_ms, symbol, price, source, suspect))
      ("gap",   (symbol, from_ms, to_ms, reason))
      ("stats", (date_utc, symbol, tick_delta, stored_delta, disconnects,
                 first_ms, last_ms))
      ("stop",  None)
    """

    def __init__(self, db_path: str):
        super().__init__(name="db-writer", daemon=True)
        self.q: queue.Queue = queue.Queue(maxsize=100_000)
        self._db_path = db_path
        self.written = 0

    def put(self, kind: str, payload) -> None:
        try:
            self.q.put_nowait((kind, payload))
        except queue.Full:  # Backpressure: lieber Tick verlieren als Callback blocken
            log.error("Writer-Queue voll -- Event verworfen (%s)", kind)

    def run(self) -> None:
        con = sqlite3.connect(self._db_path, timeout=30)  # eigene Verbindung im Thread
        con.execute("PRAGMA busy_timeout=10000")
        buf: list[tuple] = []
        last_flush = time.monotonic()

        def flush() -> None:
            nonlocal buf, last_flush
            if buf:
                con.executemany(
                    "INSERT OR IGNORE INTO ticks_raw VALUES (?,?,?,?,?,?)", buf)
                self.written += len(buf)
                buf = []
            con.commit()
            last_flush = time.monotonic()

        running = True
        while running:
            try:
                kind, payload = self.q.get(timeout=0.25)
            except queue.Empty:
                kind, payload = None, None
            if kind == "tick":
                buf.append(payload)
            elif kind == "gap":
                con.execute("INSERT OR IGNORE INTO gaps VALUES (?,?,?,?)", payload)
            elif kind == "stats":
                con.execute(
                    """INSERT INTO collect_stats VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(date_utc, symbol) DO UPDATE SET
                         tick_count   = tick_count + excluded.tick_count,
                         stored_count = stored_count + excluded.stored_count,
                         disconnects  = excluded.disconnects,
                         first_tick_ms= COALESCE(collect_stats.first_tick_ms,
                                                 excluded.first_tick_ms),
                         last_tick_ms = excluded.last_tick_ms""",
                    payload,
                )
            elif kind == "stop":
                running = False
            if buf and (len(buf) >= BATCH_MAX_ROWS
                        or time.monotonic() - last_flush >= BATCH_FLUSH_S):
                flush()
        flush()
        con.close()


# --------------------------------------------------------------------------
# Collector (WS mit Poll-Fallback)
# --------------------------------------------------------------------------

@dataclass
class CollectorState:
    last_tick_ms: dict[str, int] = field(default_factory=dict)
    last_persist_ms: dict[str, int] = field(default_factory=dict)
    last_price: dict[str, float] = field(default_factory=dict)
    tick_count: dict[str, int] = field(default_factory=dict)
    stored_count: dict[str, int] = field(default_factory=dict)
    rejected_count: dict[str, int] = field(default_factory=dict)
    disconnects: int = 0
    running: bool = True
    first_msg_logged: bool = False
    current_ws: object = None            # aktuelle WS-Instanz (für Stall-Reconnect)
    last_force_reconnect: float = 0.0    # monotonic; Rate-Limit für erzwungene Reconnects
    stall_reconnects: int = 0
    futures_poll_restarts: int = 0       # Neustarts des Futures-Poll-Threads
    watchdog_alerted: bool = False       # Dedup: nur ein Alarm je Stille-Episode


def _parse_msg(msg: dict, now_ms: int) -> tuple[str, float, int] | None:
    """Defensives Parsing der WS-Message (Feldnamen variieren je yfinance-Version)."""
    sym = msg.get("id") or msg.get("symbol") or msg.get("ticker")
    price = msg.get("price")
    if price is None:
        price = msg.get("last") or msg.get("regularMarketPrice")
    ts = msg.get("time") or msg.get("timestamp") or now_ms
    if sym is None or price is None:
        return None
    ts = int(ts)
    if ts < 100_000_000_000:      # Sekunden statt Millisekunden -> konvertieren
        ts *= 1000
    return str(sym), float(price), ts


def _classify_gap(delta_ms: int) -> str:
    return "session_break" if delta_ms > GAP_SESSION_BREAK_S * 1000 else "silence"


def _handle_tick(state: CollectorState, writer: DBWriter, min_interval_s: float,
                 sym: str, price: float, ts_ms: int, source: str) -> None:
    # Sanity-Filter Stufe 1: harte Verwerfung offensichtlich kaputter Ticks
    if price is None or not (price > 0) or price != price:  # <=0, None, NaN
        state.rejected_count[sym] = state.rejected_count.get(sym, 0) + 1
        log.debug("Tick verworfen (Preis %r) %s", price, sym)
        return
    # Sanity-Filter Stufe 2: implausibler Sprung -> markieren (nicht verwerfen!)
    last = state.last_price.get(sym)
    suspect = 0
    if last and abs(price - last) / last * 100 > SANITY_MAX_JUMP_PCT:
        suspect = 1
        log.warning("Sprung %s: %.2f -> %.2f (%.1f%%) -- suspect", sym, last, price,
                    abs(price - last) / last * 100)
    prev = state.last_tick_ms.get(sym)
    if prev is not None and ts_ms - prev > GAP_SILENCE_S * 1000:
        writer.put("gap", (sym, prev, ts_ms, _classify_gap(ts_ms - prev)))
    state.last_tick_ms[sym] = ts_ms
    state.last_price[sym] = price
    state.tick_count[sym] = state.tick_count.get(sym, 0) + 1
    if ts_ms - state.last_persist_ms.get(sym, 0) >= min_interval_s * 1000:
        writer.put("tick",
                   (ts_ms, int(time.time() * 1000), sym, price, source, suspect))
        state.last_persist_ms[sym] = ts_ms
        state.stored_count[sym] = state.stored_count.get(sym, 0) + 1


def _flush_stats(state: CollectorState, writer: DBWriter, symbols: list[str]) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for sym in symbols:
        writer.put("stats", (
            today, sym,
            state.tick_count.pop(sym, 0), state.stored_count.pop(sym, 0),
            state.disconnects,
            state.last_tick_ms.get(sym), state.last_tick_ms.get(sym),
        ))


def _poll_phase(state: CollectorState, writer: DBWriter, symbols: list[str],
                min_interval_s: float, poll_interval_s: float,
                duration_s: float | None) -> None:
    """Fallback: fast_info-Polling. duration_s=None -> unbegrenzt (--mode poll)."""
    import yfinance as yf
    tickers = yf.Tickers(" ".join(symbols))
    t_end = None if duration_s is None else time.monotonic() + duration_s
    log.warning("Poll-Modus aktiv (Intervall %.1f s%s)", poll_interval_s,
                "" if t_end is None else f", {duration_s:.0f} s")
    while state.running and (t_end is None or time.monotonic() < t_end):
        now_ms = int(time.time() * 1000)
        for sym in symbols:
            try:
                price = tickers.tickers[sym].fast_info.last_price
            except Exception as exc:  # noqa: BLE001
                log.debug("poll %s: %s", sym, exc)
                continue
            if price is not None:
                _handle_tick(state, writer, min_interval_s, sym, float(price),
                             now_ms, "poll")
        time.sleep(poll_interval_s)


def collect(db: str, symbols: list[str], min_interval_s: float,
            mode: str, poll_interval_s: float) -> None:
    import yfinance as yf

    open_db(db).close()                 # Schema sicherstellen
    writer = DBWriter(db)
    writer.start()
    state = CollectorState()

    def on_signal(_sig, _frm):
        # WS sofort schließen, damit das blockierende ws.listen() zurückkehrt und
        # der Prozess zügig + sauber (Exit 0) endet -> keine OnFailure-Fehlalarme
        # bei geplanten Neustarts (nur echte Crashes lösen dann noch Alarm aus).
        state.running = False
        ws = state.current_ws
        if ws is not None:
            try:
                ws.close()
            except Exception:  # noqa: BLE001
                pass

    os_signal.signal(os_signal.SIGINT, on_signal)
    os_signal.signal(os_signal.SIGTERM, on_signal)

    def handle(msg: dict) -> None:
        if not state.first_msg_logged:   # Feldprüfung (Fix 7): erste Message roh loggen
            log.info("Erste WS-Message (Feldpruefung): %r", msg)
            state.first_msg_logged = True
        parsed = _parse_msg(msg, int(time.time() * 1000))
        if parsed is None:
            log.debug("WS-Message ohne symbol/price ignoriert: %r", msg)
            return
        sym, price, ts_ms = parsed
        _handle_tick(state, writer, min_interval_s, sym, price, ts_ms, "ws")

    # Symbol-Aufteilung: Futures (...=F) werden vom Yahoo-WebSocket nicht gestreamt
    # -> parallel per fast_info gepollt; der WS bedient die übrigen Symbole.
    poll_symbols = [s for s in symbols if "=F" in s]
    ws_symbols = [s for s in symbols if s not in poll_symbols]

    log.info("Collector startet: WS=%s, Poll=%s (Modus %s, Drosselung %.1f s)",
             ws_symbols, poll_symbols, mode, min_interval_s)
    last_stats = time.monotonic()
    consec_fail = 0
    backoff_idx = 0

    # Paralleles Futures-Polling (Overnight-Abdeckung): läuft dauerhaft neben dem WS.
    # Selbstheilend: jede Ausnahme startet den Poll-Loop nach 10 s neu, statt den
    # Thread still sterben zu lassen (sonst ES=F ohne Alarm stumm).
    def _futures_poll() -> None:
        import yfinance as yf
        while state.running:
            try:
                tickers = yf.Tickers(" ".join(poll_symbols))
                while state.running:
                    now_ms = int(time.time() * 1000)
                    for sym in poll_symbols:
                        try:
                            price = tickers.tickers[sym].fast_info.last_price
                        except Exception as exc:  # noqa: BLE001
                            log.debug("futures-poll %s: %s", sym, exc)
                            continue
                        if price is not None:
                            _handle_tick(state, writer, min_interval_s, sym,
                                         float(price), now_ms, "poll")
                    time.sleep(poll_interval_s)
            except Exception as exc:  # noqa: BLE001
                if not state.running:
                    break
                state.futures_poll_restarts += 1
                log.warning("Futures-Poll-Thread-Fehler (%s) -- Neustart in 10 s "
                            "(#%d)", exc, state.futures_poll_restarts)
                time.sleep(10)

    if poll_symbols and mode != "poll":
        threading.Thread(target=_futures_poll, name="futures-poll",
                         daemon=True).start()

    # Daten-Watchdog: alarmiert in Echtzeit, wenn ALLE Quellen für 10 min bis 2 h
    # verstummen (Yahoo-Ausfall). Über 2 h wird eine echte Marktpause angenommen
    # (Wochenende/Halt) und nicht alarmiert. Ein Alarm je Stille-Episode (Dedup).
    def _data_watchdog() -> None:
        while state.running:
            time.sleep(30)
            if not state.last_tick_ms:
                continue
            age_ms = time.time() * 1000 - max(state.last_tick_ms.values())
            if WATCHDOG_ALERT_S * 1000 < age_ms < WATCHDOG_MAX_S * 1000:
                if not state.watchdog_alerted:
                    state.watchdog_alerted = True
                    _emit_alert(f"KEINE Marktdaten seit {int(age_ms / 1000)}s "
                                "(Yahoo-Ausfall?) auf collector-vm.")
            elif age_ms < WATCHDOG_ALERT_S * 1000:
                state.watchdog_alerted = False   # Ticks zurück -> Reset

    threading.Thread(target=_data_watchdog, name="data-watchdog",
                     daemon=True).start()

    # Stall-Monitor (Feature 2): erkennt "Verbindung lebt, aber keine Ticks" und
    # erzwingt einen Reconnect. Prüft NUR die WS-Symbole (Futures-Poll hält
    # last_tick sonst künstlich frisch und würde echte WS-Stalls verdecken).
    def _stall_monitor() -> None:
        while state.running:
            time.sleep(10)
            ws_last = [state.last_tick_ms[s] for s in ws_symbols
                       if s in state.last_tick_ms]
            if not ws_last:
                continue
            age_ms = time.time() * 1000 - max(ws_last)
            # Nur bei kürzlich aktivem Stream erzwingen (nicht bei geschlossenem Markt)
            if STALL_TIMEOUT_S * 1000 < age_ms < STALL_MAX_S * 1000 and \
               time.monotonic() - state.last_force_reconnect > STALL_TIMEOUT_S:
                state.last_force_reconnect = time.monotonic()
                state.stall_reconnects += 1
                log.warning("Stall erkannt (%.0fs ohne WS-Tick) -- erzwinge Reconnect.",
                            age_ms / 1000)
                ws = state.current_ws
                if ws is not None:
                    try:
                        ws.close()          # macht ws.listen() zurückkehren -> Reconnect
                    except Exception as exc:  # noqa: BLE001
                        log.debug("ws.close() im Stall-Monitor: %s", exc)

    if mode != "poll" and ws_symbols:
        threading.Thread(target=_stall_monitor, name="stall-monitor",
                         daemon=True).start()

    # WS-Reconnect-Schleife als Funktion -- läuft im Daemon-Thread, damit ein
    # blockierendes ws.listen() (C-recv) den Prozess-Shutdown NICHT verzögert.
    # So beendet SIGTERM prompt und sauber (Exit 0) -> keine OnFailure-Fehlalarme
    # bei geplanten Neustarts; echte Ausfälle alarmieren weiterhin.
    def _ws_worker() -> None:
        cf, bi = 0, 0
        while state.running:
            try:
                ws = yf.WebSocket()
                state.current_ws = ws
                ws.subscribe(ws_symbols)
                cf = 0
                bi = 0
                ws.listen(handle)        # blockiert bis Abbruch/Fehler
            except Exception as exc:     # noqa: BLE001
                if not state.running:
                    break
                cf += 1
                state.disconnects += 1
                now_ms = int(time.time() * 1000)
                for sym, prev in state.last_tick_ms.items():
                    writer.put("gap", (sym, prev, now_ms, "disconnect"))
                wait = RECONNECT_BACKOFF_S[min(bi, len(RECONNECT_BACKOFF_S) - 1)]
                bi += 1
                log.warning("WebSocket-Abbruch (%s) -- Reconnect in %d s "
                            "(Fehlversuch %d)", exc, wait, cf)
                time.sleep(wait)
            if mode == "auto" and cf >= WS_FAIL_TO_POLL and state.running:
                _poll_phase(state, writer, symbols, min_interval_s,
                            poll_interval_s, POLL_PHASE_S)
                cf = 0

    if mode == "poll":
        _poll_phase(state, writer, symbols, min_interval_s, poll_interval_s, None)
    elif not ws_symbols:
        _futures_poll()                  # nur Futures -> reines Polling
    else:
        threading.Thread(target=_ws_worker, name="ws-worker", daemon=True).start()
        # Hauptthread: unterbrechbare Warteschleife + periodischer Statistik-Flush.
        # SIGTERM -> running=False -> Schleife endet in <1 s -> sauberer Exit 0.
        while state.running:
            time.sleep(1)
            if time.monotonic() - last_stats >= STATS_FLUSH_S:
                _flush_stats(state, writer, symbols)
                last_stats = time.monotonic()

    # Shutdown: offene Verbindung als Lücke markieren, Statistik sichern
    now_ms = int(time.time() * 1000)
    for sym, prev in state.last_tick_ms.items():
        writer.put("gap", (sym, prev, now_ms, "shutdown"))
    _flush_stats(state, writer, symbols)
    writer.put("stop", None)
    writer.join(timeout=10)
    log.info("Collector beendet -- %d Ticks persistiert.", writer.written)


# --------------------------------------------------------------------------
# Bootstrap (historische Bars via yfinance, idempotent)
# --------------------------------------------------------------------------

def bootstrap(db: str, symbols: list[str]) -> None:
    import yfinance as yf

    con = open_db(db)
    for symbol in symbols:
        for interval, period, table in BOOTSTRAP_JOBS:
            try:
                df = yf.download(symbol, interval=interval, period=period,
                                 auto_adjust=False, progress=False, threads=False)
            except Exception as exc:  # noqa: BLE001
                log.error("bootstrap %s %s fehlgeschlagen: %s", symbol, interval, exc)
                continue
            if df is None or df.empty:
                log.warning("bootstrap %s %s: keine Daten", symbol, interval)
                continue
            if hasattr(df.columns, "levels"):
                df.columns = df.columns.get_level_values(0)
            rows = []
            for ts, row in df.iterrows():
                ts_ms = int(ts.tz_localize("UTC").timestamp() * 1000) \
                    if ts.tzinfo is None else int(ts.timestamp() * 1000)
                rows.append((ts_ms, symbol,
                             float(row["Open"]), float(row["High"]),
                             float(row["Low"]), float(row["Close"]),
                             float(row["Volume"]) if "Volume" in row else None,
                             "yf_hist"))
            con.executemany(f"INSERT OR REPLACE INTO {table} VALUES (?,?,?,?,?,?,?,?)",
                            rows)
            con.commit()
            log.info("bootstrap %s %s -> %s: %d Bars", symbol, interval, table, len(rows))
    con.close()


# --------------------------------------------------------------------------
# Zweitquelle: Stooq-EOD-Cross-Check des Tagesschlusses (keyless)
# --------------------------------------------------------------------------

def _fred_key() -> str | None:
    """FRED-API-Key aus Umgebungsvariable FRED_API_KEY oder Datei ~/.fred_key."""
    import os
    key = os.environ.get("FRED_API_KEY")
    if key:
        return key.strip()
    f = Path.home() / ".fred_key"
    if f.exists():
        return f.read_text().strip()
    return None


def _fred_recent_closes(series_id: str, key: str) -> list[tuple[str, float]]:
    """Die letzten (bis zu 7) gültigen Tagesschlüsse einer FRED-Serie holen (JSON),
    jüngste zuerst. Feiertage liefern value='.', werden übersprungen.
    Returns [(YYYY-MM-DD, close), ...] -- leere Liste bei Fehler."""
    import json as _json
    import urllib.request
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={key}&file_type=json"
           "&sort_order=desc&limit=7")
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = _json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        log.warning("FRED-Abruf %s fehlgeschlagen: %s", series_id, exc)
        return []
    out: list[tuple[str, float]] = []
    for obs in data.get("observations", []):
        val = obs.get("value", ".")
        if val not in (".", "", None):
            try:
                out.append((obs["date"], float(val)))
            except ValueError:
                continue
    if not out:
        log.warning("FRED %s: kein gültiger Wert", series_id)
    return out


def crosscheck(db: str, symbols: list[str]) -> None:
    """Vergleicht yfinance-Tagesschlüsse (bars_1d) mit FRED (offizielle Zweitquelle)
    und protokolliert Divergenzen (source_crosscheck). Divergenz > Schwelle -> suspect=1.

    Jede FRED-Beobachtung wird unter ihrem EIGENEN Beobachtungsdatum gespeichert und
    NUR mit dem bars_1d-Schluss desselben Tages verglichen. Früher wurde der jüngste
    FRED-Wert blind unter dem jüngsten bars_1d-Datum abgelegt -- hinkte FRED einen Tag
    hinterher, verglich diff_pct zwei verschiedene Handelstage (falsche SUSPECT-Alarme)
    und das Dashboard überschrieb den echten Tagesschluss mit dem Vortageswert.
    Es werden die letzten ~7 FRED-Beobachtungen upsertet, sodass fehlerhafte
    Alt-Zeilen beim nächsten Lauf automatisch korrigiert werden. Fehlt für ein
    FRED-Datum (noch) der bars_1d-Bar (Bootstrap im Rückstand), wird die Zeile ohne
    Vergleich gespeichert -- der Dashboard-Export nutzt sie als Frische-Ergänzung."""
    key = _fred_key()
    if not key:
        log.warning("crosscheck: kein FRED-API-Key (FRED_API_KEY oder ~/.fred_key) "
                    "-- übersprungen.")
        return
    con = open_db(db)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    day_ms = 86_400_000
    for sym in symbols:
        series = FRED_SERIES.get(sym)
        if not series:
            continue
        frs = _fred_recent_closes(series, key)
        if not frs:
            continue
        newest_msg = None
        for fred_date, fred_close in frs:
            # bars_1d-Schluss DESSELBEN Tages (1d-Bars liegen auf 00:00 UTC des Tages).
            day0 = int(datetime.strptime(fred_date, "%Y-%m-%d")
                       .replace(tzinfo=timezone.utc).timestamp() * 1000)
            row = con.execute(
                "SELECT close FROM bars_1d WHERE symbol=? AND ts_utc_ms>=? AND "
                "ts_utc_ms<? ORDER BY ts_utc_ms DESC LIMIT 1",
                (sym, day0, day0 + day_ms)).fetchone()
            yf_close = row[0] if row else None
            if yf_close is not None and fred_close:
                diff_pct = round(abs(yf_close - fred_close) / fred_close * 100, 3)
                suspect = 1 if diff_pct > CROSSCHECK_THRESHOLD_PCT else 0
            else:
                diff_pct, suspect = None, 0
            con.execute(
                "INSERT OR REPLACE INTO source_crosscheck VALUES (?,?,?,?,?,?,?)",
                (fred_date, sym, yf_close, fred_close, diff_pct, suspect, now))
            if newest_msg is None:                       # jüngste Beobachtung fürs Log
                newest_msg = (fred_date, yf_close, fred_close, diff_pct, suspect)
        con.commit()
        fred_date, yf_close, fred_close, diff_pct, suspect = newest_msg
        if yf_close is None:
            log.info("crosscheck %s (%s): FRED %.2f -- kein bars_1d-Bar für diesen Tag "
                     "(Bootstrap im Rückstand?) -- als Frische-Ergänzung gespeichert.",
                     sym, fred_date, fred_close)
        else:
            lvl = log.warning if suspect else log.info
            lvl("crosscheck %s (%s): yf %.2f vs FRED %.2f -> Delta %.3f%%%s",
                sym, fred_date, yf_close, fred_close, diff_pct,
                " [!] SUSPECT" if suspect else "")
    con.close()


# --------------------------------------------------------------------------
# Wartung: Rollup + Prune (Retention) und VACUUM
# --------------------------------------------------------------------------

def maintain(db: str, retention_days: int, symbols: list[str]) -> None:
    """Rollup + Prune: Rohticks älter als retention_days werden zu 1m/5m-Bars
    verdichtet (Aggregation ist idempotent, baut Bars aus ALLEN Ticks) und
    anschließend aus ticks_raw gelöscht. Der jüngste Zeitraum bleibt tickgenau.
    Cutoff wird als Lücke 'pruned' NICHT protokolliert (bewusste Verdichtung)."""
    aggregate(db, "1m", symbols)
    aggregate(db, "5m", symbols)
    cutoff_ms = int((datetime.now(timezone.utc).timestamp()
                     - retention_days * 86_400) * 1000)
    con = open_db(db)
    n = con.execute("SELECT COUNT(*) FROM ticks_raw WHERE ts_utc_ms < ?",
                    (cutoff_ms,)).fetchone()[0]
    con.execute("DELETE FROM ticks_raw WHERE ts_utc_ms < ?", (cutoff_ms,))
    con.commit()
    # DB-Integritätsprüfung (billige Versicherung gegen unbemerkte Korruption,
    # die sich sonst über rsync/Litestream in die Replikate ausbreiten würde).
    res = con.execute("PRAGMA quick_check").fetchone()
    con.close()
    if res and res[0] != "ok":
        _emit_alert(f"DB-Integritätsprüfung FEHLGESCHLAGEN: {str(res[0])[:120]}")
        log.error("quick_check: %s", res[0])
    else:
        log.info("quick_check: ok")
    log.info("maintain: %d Rohticks älter als %d Tage verdichtet + gelöscht "
             "(Bars bleiben erhalten).", n, retention_days)


def _emit_alert(text: str) -> None:
    """Schreibt eine Alarmzeile in ~/sync/collector_alerts.log (rsync -> agent-vm ->
    Signal-Relay). Einziger Alarmkanal der collector-vm (kein signal-cli hier)."""
    line = f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {text}\n"
    alerts = Path.home() / "sync" / "collector_alerts.log"
    try:
        alerts.parent.mkdir(exist_ok=True)
        with open(alerts, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError as exc:
        log.error("Alarm-Schreiben fehlgeschlagen: %s", exc)
    log.warning("ALERT: %s", text)


def diskguard(db: str, threshold_pct: int = 80) -> None:
    """Feature 4: warnt (Alarmdatei) bei Plattenbelegung >= threshold_pct."""
    import shutil
    total, used, _ = shutil.disk_usage(Path(db).resolve().parent)
    pct = round(used / total * 100)
    if pct >= threshold_pct:
        _emit_alert(f"DISK {pct}% belegt auf collector-vm (Schwelle {threshold_pct}%).")
    else:
        log.info("diskguard: %d%% belegt (< %d%%, ok).", pct, threshold_pct)


def vacuum(db: str) -> None:
    """VACUUM -- gibt gelöschten Speicher physisch ans Dateisystem zurück.
    ACHTUNG: exklusiver Lock -- nur bei pausiertem Collector ausführen."""
    con = sqlite3.connect(db, timeout=60)
    before = Path(db).stat().st_size if Path(db).exists() else 0
    con.execute("VACUUM")
    con.close()
    after = Path(db).stat().st_size if Path(db).exists() else 0
    log.info("VACUUM: %.1f MB -> %.1f MB (%.1f MB freigegeben).",
             before / 1e6, after / 1e6, (before - after) / 1e6)


# --------------------------------------------------------------------------
# Aggregation (Ticks -> Bars, Window-Functions, idempotent)
# --------------------------------------------------------------------------

_INTERVAL_MS = {"1m": 60_000, "5m": 300_000}


def aggregate(db: str, interval: str, symbols: list[str]) -> None:
    if interval not in _INTERVAL_MS:
        raise SystemExit(f"Unbekanntes Intervall: {interval} (erlaubt: 1m, 5m)")
    step = _INTERVAL_MS[interval]
    con = open_db(db)
    for sym in symbols:
        cur = con.execute(
            f"""
            INSERT OR REPLACE INTO bars_{interval}
            SELECT DISTINCT bucket, symbol,
                   FIRST_VALUE(price) OVER w AS open,
                   MAX(price)         OVER w AS high,
                   MIN(price)         OVER w AS low,
                   LAST_VALUE(price)  OVER w AS close,
                   NULL AS volume, 'ticks' AS source
            FROM (SELECT (ts_utc_ms / {step}) * {step} AS bucket,
                         symbol, price, ts_utc_ms
                  FROM ticks_raw WHERE symbol = ?)
            WINDOW w AS (PARTITION BY bucket ORDER BY ts_utc_ms
                         ROWS BETWEEN UNBOUNDED PRECEDING
                                  AND UNBOUNDED FOLLOWING)
            """,
            (sym,),
        )
        con.commit()
        log.info("aggregate %s %s: %d Bars abgeleitet", sym, interval, cur.rowcount)
    con.close()


# --------------------------------------------------------------------------
# Statistik
# --------------------------------------------------------------------------

def stats(db: str, date: str | None) -> None:
    con = open_db(db)
    where, args = ("WHERE date_utc = ?", [date]) if date else ("", [])
    print(f"{'Datum':<12}{'Symbol':<8}{'Ticks':>10}{'Gespeichert':>12}{'Disc.':>7}"
          f"{'Letzter Tick (UTC)':>26}")
    for row in con.execute(
        f"SELECT * FROM collect_stats {where} ORDER BY date_utc, symbol", args
    ):
        last = datetime.fromtimestamp(row[6] / 1000, timezone.utc).isoformat() \
            if row[6] else "-"
        print(f"{row[0]:<12}{row[1]:<8}{row[2]:>10}{row[3]:>12}{row[4]:>7}{last:>26}")
    n_ticks = con.execute("SELECT COUNT(*) FROM ticks_raw").fetchone()[0]
    print(f"\nticks_raw gesamt: {n_ticks:,}")
    print("Lücken nach Ursache:")
    for reason, n in con.execute(
        "SELECT reason, COUNT(*) FROM gaps GROUP BY reason ORDER BY reason"
    ):
        print(f"  {reason:<15}{n:>6}")
    con.close()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Datenpipeline S&P 500 (Rev. 2)")
    p.add_argument("--db", default=DEFAULT_DB)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")
    for name in ("bootstrap", "collect", "aggregate"):
        sp = sub.add_parser(name)
        sp.add_argument("--symbols", nargs="+", default=None,
                        help="explizit; sonst data_requirements.json, sonst Fallback")
        if name == "collect":
            sp.add_argument("--min-interval", type=float, default=5.0,
                            help="Persistenz-Drosselung in s (0 = alle Ticks)")
            sp.add_argument("--mode", choices=("auto", "ws", "poll"), default="auto")
            sp.add_argument("--poll-interval", type=float, default=5.0)
        if name == "aggregate":
            sp.add_argument("--interval", choices=("1m", "5m"), default="1m")
    sp = sub.add_parser("crosscheck")
    sp.add_argument("--symbols", nargs="+", default=None)
    sp = sub.add_parser("maintain")
    sp.add_argument("--symbols", nargs="+", default=None)
    sp.add_argument("--days", type=int, default=RAW_RETENTION_DAYS)
    sub.add_parser("vacuum")
    sp = sub.add_parser("diskguard")
    sp.add_argument("--threshold", type=int, default=80)
    sp = sub.add_parser("stats")
    sp.add_argument("--date", default=None, help="YYYY-MM-DD (UTC)")

    args = p.parse_args(argv)
    if args.cmd == "init":
        open_db(args.db).close()
        print(f"Schema angelegt: {args.db}")
    elif args.cmd == "bootstrap":
        bootstrap(args.db, resolve_symbols(args.symbols))
    elif args.cmd == "collect":
        collect(args.db, resolve_symbols(args.symbols), args.min_interval,
                args.mode, args.poll_interval)
    elif args.cmd == "aggregate":
        aggregate(args.db, args.interval, resolve_symbols(args.symbols))
    elif args.cmd == "crosscheck":
        crosscheck(args.db, resolve_symbols(args.symbols))
    elif args.cmd == "maintain":
        maintain(args.db, args.days, resolve_symbols(args.symbols))
    elif args.cmd == "vacuum":
        vacuum(args.db)
    elif args.cmd == "diskguard":
        diskguard(args.db, args.threshold)
    elif args.cmd == "stats":
        stats(args.db, args.date)


if __name__ == "__main__":
    main()
