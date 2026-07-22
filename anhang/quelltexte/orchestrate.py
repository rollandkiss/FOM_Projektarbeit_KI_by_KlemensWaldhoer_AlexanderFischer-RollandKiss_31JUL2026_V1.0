#!/usr/bin/env python3
"""
orchestrate.py -- Orchestrierungsschicht zwischen Auditor-Entscheid und Broker.

Liest den jüngsten Entscheid aus dem Entscheidungs-Journal (Tabelle `decisions`,
befüllt von auditor.py) und ruft für ein TRADE-Votum die entkoppelte
broker.validate_order() auf (bis /orders/validation + Ex-Ante-Kosten; löst KEINE
Order aus). NO_TRADE -> keine Aktion.

Zweck der Auslagerung (Code-Review 20.07., vgl. SYSTEMDOKUMENTATION §5/§6):
  Der Broker soll ein reiner Ausführungs-Adapter bleiben und das Persistenzschema
  des Auditors NICHT kennen. Diese Schicht übersetzt den journalierten Entscheid in
  ein entkoppeltes Order-Objekt (depot/isin/side/qty/limit) und übergibt es dem
  Broker. Damit ist die frühere Schichtenkopplung (broker.py las das decisions-
  Journal selbst) aufgelöst.

Mengenbildung (#1): size_eur / echter Stückkurs aus der gehaltenen Seed-Position
(broker.reference_price -> currentPrice; kostenlos über den Depotabruf). Ohne Kurs wird
live blockiert. COMDIRECT_REF_PRICE ist nur noch ein optionaler Dry-Run-Demo-Override.
Depot aus COMDIRECT_DEPOT.

CLI:
  python3 orchestrate.py validate-decision [--db marketdata.sqlite]

Abhängigkeiten: Standardbibliothek + broker.py + config.py (im selben Ordner).
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import subprocess
from datetime import datetime, timezone

import broker
from config import agent_env

log = logging.getLogger("orchestrate")


def _alert(text: str) -> None:
    """Betriebs-Alarm an die Signal-Direktnummer (best-effort, analog run_cycle
    send_alert). Ein Versandfehler bricht den Orderpfad NIE ab."""
    try:
        e = agent_env()
        bot, rec = e.get("SIGNAL_BOT"), e.get("SIGNAL_RECIPIENT")
        if bot and rec:
            subprocess.run(["signal-cli", "-a", bot, "send", "-m",
                            f"[!] Trading-Agent Alarm: {text}", rec],
                           timeout=30, check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:  # noqa: BLE001
        log.warning("Alarm-Versand fehlgeschlagen (%s): %s", exc, text)


def _pause(reason: str) -> None:
    """Kill-Switch-Pause setzen (A2/A5): der nächste Zyklus handelt erst wieder nach
    menschlichem RESUME ('R'). Lazy-Import, damit orchestrate ohne control testbar bleibt."""
    try:
        import control
        control.PAUSE_FLAG.touch()
        log.error("Trading PAUSIERT: %s", reason)
    except Exception as exc:  # noqa: BLE001
        log.error("PAUSE-Flag konnte nicht gesetzt werden (%s) -- Grund war: %s", exc, reason)


def _wkn(isin: str | None) -> str | None:
    """WKN aus einer deutschen ISIN ableiten (DE000<WKN><Prüfziffer>)."""
    if isin and len(isin) == 12 and isin[:2].upper() == "DE":
        return isin[5:11]
    return None


def _resolve_depot(e: dict, account: str, depot: str | None = None) -> str | None:
    """Depot bestimmen: expliziter Parameter > COMDIRECT_DEPOT_<ACCOUNT> > COMDIRECT_DEPOT (haupt)."""
    return depot or e.get(f"COMDIRECT_DEPOT_{account.upper()}") or \
        (e.get("COMDIRECT_DEPOT") if account == "haupt" else None)


def _resolve_mode(e: dict, account: str) -> str:
    """Ausführungsmodus je Konto (A3, gehärtet 21.07.): `live` wird AUSSCHLIESSLICH aus
    dem kontospezifischen Schlüssel `EXECUTION_MODE_<ACCOUNT>` akzeptiert. Ein globales
    `EXECUTION_MODE=live` wird ignoriert (Warnung + dry_run) -- sonst würde ein später
    aktiviertes Konto ungetestet live erben, und ein in der Cron-/systemd-Umgebung
    vergessenes Env-live könnte agent.env unsichtbar übersteuern. Go-Live je Konto ist
    damit genau eine bewusste Zeile: EXECUTION_MODE_<ACCOUNT>=live."""
    acct = (e.get(f"EXECUTION_MODE_{account.upper()}") or "").strip().lower()
    if acct:
        return acct
    glob = (e.get("EXECUTION_MODE") or "dry_run").strip().lower()
    if glob == "live":
        log.warning("Globales EXECUTION_MODE=live wird IGNORIERT -- live nur noch "
                    "kontospezifisch via EXECUTION_MODE_%s=live. Fallback: dry_run.",
                    account.upper())
        return "dry_run"
    return glob


def record_valuation(journal_db: str, account: str, depot: str, mode: str = "dry_run") -> dict:
    """Depotwert-Snapshot (Positionswert + Verrechnungssaldo -> Gesamt) in die valuations-
    Tabelle schreiben -- Basis für die Wertentwicklung im Zeitvergleich. Best-effort: ohne
    aktive Session/Depot schlägt es still fehl (kein Abbruch)."""
    try:
        positions = broker.portfolio_exposure(depot, account)
        cash = broker.available_cash(depot, account)
        total = round(positions + cash, 2)
        now = datetime.now(timezone.utc)
        con = sqlite3.connect(journal_db)
        con.execute("CREATE TABLE IF NOT EXISTS valuations ("
                    "ts_utc TEXT, date TEXT, account TEXT, positions_eur REAL, "
                    "cash_eur REAL, total_eur REAL, mode TEXT)")
        con.execute("INSERT INTO valuations (ts_utc,date,account,positions_eur,cash_eur,"
                    "total_eur,mode) VALUES (?,?,?,?,?,?,?)",
                    (now.isoformat(timespec="seconds"), now.date().isoformat(), account,
                     positions, cash, total, mode))
        con.commit()
        con.close()
        log.info("Valuation %s (%s): Positionen %.2f € + Cash %.2f € = %.2f €",
                 account, mode, positions, cash, total)
        return {"account": account, "mode": mode, "positions_eur": positions,
                "cash_eur": cash, "total_eur": total}
    except Exception as exc:  # noqa: BLE001 -- Snapshot ist unkritisch
        log.warning("Valuation-Snapshot fehlgeschlagen (ignoriert): %s", exc)
        return {"error": str(exc)}


def _ensure_orders_schema(con: sqlite3.Connection) -> None:
    """orders-Schema inkl. A1-Erweiterung (decision_ts = Ausführungs-Marker, order_id =
    comdirect-Referenz). Migration idempotent: ALTER TABLE für Alt-Journale, partieller
    UNIQUE-Index nur auf Live-Zeilen (Dry-Run-Validierungen desselben Entscheids
    kollidieren nicht mit einer späteren Live-Ausführung)."""
    con.execute("CREATE TABLE IF NOT EXISTS orders ("
                "ts_utc TEXT, date TEXT, account TEXT, isin TEXT, wkn TEXT, name TEXT, "
                "side TEXT, mode TEXT, status TEXT, venue TEXT, tan_typ TEXT, "
                "qty REAL, value_eur REAL, limit_eur REAL, "
                "decision_ts TEXT, order_id TEXT)")
    for col in ("decision_ts TEXT", "order_id TEXT"):
        try:
            con.execute(f"ALTER TABLE orders ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass                                     # Spalte existiert bereits
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_decision ON orders"
                "(account, decision_ts) WHERE decision_ts IS NOT NULL AND mode='live'")


def _journal_order(journal_db: str, isin, name, side, mode, status, account,
                   venue=None, tan_typ=None, qty=None, value_eur=None, limit=None,
                   decision_ts=None, order_id=None, strict: bool = False) -> None:
    """Order-Ereignis in die orders-Tabelle des Journals anhängen -- mit den ECHTEN Werten
    (Instrument WKN/ISIN/Name, Seite, Stückzahl, Ordervolumen EUR, Limit, Status,
    Handelsplatz, Modus) plus decision_ts/order_id (A1).
    `strict=False` (Dry-Run): best-effort, ein Fehler bricht den Pfad nicht ab.
    `strict=True` (Live-Intent, A1/A3): ein Fehler WIRD geworfen -- der Aufrufer bricht
    dann VOR dem Execution-POST ab (keine Order ohne Journalzeile)."""
    try:
        con = sqlite3.connect(journal_db)
        _ensure_orders_schema(con)
        con.execute(
            "INSERT INTO orders (ts_utc,date,account,isin,wkn,name,side,mode,status,venue,"
            "tan_typ,qty,value_eur,limit_eur,decision_ts,order_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(timespec="seconds"),
             datetime.now(timezone.utc).date().isoformat(), account,
             isin, _wkn(isin), name, side, mode, status, venue, tan_typ,
             qty, value_eur, limit, decision_ts, order_id))
        con.commit()
        con.close()
    except Exception as exc:  # noqa: BLE001
        if strict:
            raise
        log.warning("Order-Journal fehlgeschlagen (ignoriert, Dry-Run): %s", exc)


def _live_order_status(journal_db: str, account: str, decision_ts: str) -> str | None:
    """A1-Marker: Status einer bereits abgewickelten LIVE-Order zu diesem Entscheid
    (intent/executed/...), sonst None. Verhindert die Doppel-Ausführung desselben
    Entscheids durch Re-Run, doppelte Cron-Zeile oder manuellen Aufruf."""
    try:
        con = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
        row = con.execute("SELECT status FROM orders WHERE account=? AND decision_ts=? "
                          "AND mode='live' ORDER BY ts_utc DESC LIMIT 1",
                          (account, decision_ts)).fetchone()
        con.close()
        return row[0] if row else None
    except sqlite3.Error:
        return None                                  # Tabelle/Spalte fehlt (Alt-Journal)


def _update_live_order(journal_db: str, account: str, decision_ts: str,
                       status: str, order_id=None) -> None:
    """Intent-Zeile (A1) nach dem Execution-Versuch fortschreiben. Best-effort --
    der Ausgang steht zu diesem Zeitpunkt fest, ein Update-Fehler wird nur geloggt."""
    try:
        con = sqlite3.connect(journal_db)
        con.execute("UPDATE orders SET status=?, order_id=COALESCE(?, order_id) "
                    "WHERE account=? AND decision_ts=? AND mode='live'",
                    (status, order_id, account, decision_ts))
        con.commit()
        con.close()
    except Exception as exc:  # noqa: BLE001
        log.error("Order-Status-Update fehlgeschlagen (%s -> %s): %s",
                  decision_ts, status, exc)


def _reconcile_after_error(depot: str, isin: str, side: str, qty: int,
                           account: str) -> tuple[str, str | None]:
    """A2: Nach unklarem Execution-Ausgang (Netzwerkfehler nach POST) das Orderbuch
    der Bank befragen. Rückgabe (status, order_id):
      executed_reconciled -- Order ist angekommen (im Orderbuch gefunden)
      failed              -- Orderbuch erreichbar, Order nicht vorhanden
      unknown             -- Orderbuch nicht abfragbar (Zustand bleibt offen)
    Matching best-effort über ISIN/Seite/Stückzahl im Order-Objekt (tolerant gegen
    Schema-Details der comdirect-Antwort)."""
    try:
        orders = broker.depot_orders(depot, account)
    except Exception as exc:  # noqa: BLE001
        log.error("Orderbuch-Abfrage fehlgeschlagen (%s) -- Orderstatus UNBEKANNT.", exc)
        return "unknown", None
    for o in orders:
        blob = json.dumps(o, ensure_ascii=False).upper()
        if isin.upper() in blob and f'"{side.upper()}"' in blob and str(qty) in blob:
            oid = o.get("orderId") or o.get("orderID") or o.get("id")
            log.warning("Reconciliation: Order im Orderbuch gefunden (orderId=%s).", oid)
            return "executed_reconciled", oid
    return "failed", None


def _latest_decision(db: str) -> tuple[dict, dict, str] | None:
    """Jüngsten (decision_json, request_json, ts_utc) aus dem Journal lesen; None,
    falls kein Eintrag/Tabelle vorhanden. ts_utc dient als Frische-Gate (A1) und als
    Ausführungs-Marker-Schlüssel (decision_ts)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT decision_json, request_json, ts_utc FROM decisions "
                          "ORDER BY ts_utc DESC LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()
    if not row:
        return None
    return json.loads(row[0]), json.loads(row[1]), row[2]


def _price_stale(price_dt: str | None, mode: str, max_age_h: float) -> str | None:
    """O6a -- Kursfrische-Gate: Begründung, falls der Referenzkurs zu alt für die
    Limit-Bildung ist, sonst None. Regeln: Zeitstempel älter als max_age_h ODER
    (live) nicht vom heutigen UTC-Tag -> stale. OHNE Zeitstempel (ältere API-
    Antworten): None mit Warnung beim Aufrufer -- bewusst KEIN Hard-Block, sonst
    wäre der Orderpfad von einem optionalen Feld abhängig (Verhaltensbruch)."""
    if not price_dt:
        return None
    now = datetime.now(timezone.utc)
    try:
        ts = datetime.fromisoformat(str(price_dt).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None                              # unlesbar = wie fehlend (Warnung)
    age_h = (now - ts).total_seconds() / 3600
    if age_h > max_age_h:
        return f"Referenzkurs {age_h:.1f} h alt (> {max_age_h:g} h, Stand {price_dt})"
    if mode == "live" and ts.astimezone(timezone.utc).date() != now.date():
        return f"Referenzkurs nicht vom heutigen UTC-Tag ({price_dt})"
    return None


_OPEN_ORDER_STATES = {"OPEN", "PENDING", "PARTIALLY_EXECUTED", "QUOTE_REQUESTED",
                      "ACCEPTED"}


def _open_orders_for(depot: str, isin: str, account: str) -> list[dict] | None:
    """O6c -- offene Orders des Depots auf diesem Instrument (Orderbuch-Blick VOR
    einer neuen Aufgabe: eine gestern unfilled gebliebene GFD-/Alt-Order und die
    heutige Neuaufgabe dürfen nicht koexistieren). Instrument-Zuordnung defensiv
    per ISIN/WKN-Blob-Match (wie _reconcile_after_error -- Orderobjekte führen je
    nach API-Version unterschiedliche Felder). Returns Liste (leer = frei) oder
    None, wenn das Orderbuch nicht lesbar war (Aufrufer entscheidet fail-safe)."""
    try:
        orders = broker.depot_orders(depot, account)
    except Exception as exc:  # noqa: BLE001
        log.warning("Orderbuch nicht lesbar (%s).", exc)
        return None
    q = (isin or "").upper()
    q_wkn = q[5:11] if len(q) == 12 and q[:2].isalpha() else None
    hits = []
    for o in orders:
        blob = json.dumps(o, ensure_ascii=False).upper()
        if q not in blob and not (q_wkn and q_wkn in blob):
            continue
        status = str(o.get("orderStatus") or o.get("status") or "").upper()
        if status in _OPEN_ORDER_STATES or "OPEN" in status:
            hits.append({"order_id": o.get("orderId") or o.get("orderID"),
                         "status": status or "?"})
    return hits


def _decision_stale(dec_ts: str, mode: str, max_age_h: float) -> str | None:
    """A1-Frische-Gate: Begründung, falls der Entscheid nicht mehr ausführbar ist,
    sonst None. Regeln: älter als max_age_h Stunden -> stale (beide Modi); im
    Live-Modus zusätzlich: nicht vom heutigen UTC-Tag -> stale. Ein unlesbarer
    Zeitstempel gilt fail-safe als stale."""
    now = datetime.now(timezone.utc)
    try:
        ts = datetime.fromisoformat(str(dec_ts))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return f"Entscheid-Zeitstempel unlesbar ({dec_ts!r})"
    age_h = (now - ts).total_seconds() / 3600
    if age_h > max_age_h:
        return f"Entscheid {age_h:.1f} h alt (> {max_age_h:g} h)"
    if mode == "live" and ts.astimezone(timezone.utc).date() != now.date():
        return f"Entscheid nicht vom heutigen UTC-Tag ({dec_ts})"
    return None


def validate_decision(journal_db: str = "decisions.sqlite", account: str = "haupt",
                      depot: str | None = None) -> dict:
    """Jüngsten Entscheid aus dem Entscheidungsjournal (`decisions.sqlite`, getrennt
    von den per rsync gespiegelten Marktdaten) holen und bei TRADE über comdirect
    abwickeln. `account`/`depot` wählen Zugang und Depot (Multi-Depot-fähig: sma auf
    'haupt', smatrend auf 'zweit'); ohne `depot` greift COMDIRECT_DEPOT bzw.
    COMDIRECT_DEPOT_<ACCOUNT> aus agent.env. Zwei Modi (agent.env `EXECUTION_MODE`,
    Default `dry_run`):
      * dry_run: broker.validate_order() -- validiert + Ex-Ante-Kosten, KEINE reale Order.
      * live:    broker.place_order(execute=True) -- REALE Orderanlage (Session-TAN).
    Preisbildung (#1): der Stückkurs kommt kostenlos aus der gehaltenen **Seed-Position**
    (broker.reference_price -> currentPrice). Fehlt der Kurs, wird live BLOCKIERT (kein
    stiller Ersatzpreis). BUY-Menge = Minimum aus (Strategie-`size_eur`, `EXECUTION_MAX_EUR`,
    verfügbarer Deckung `availableCashAmountEUR` minus `EXECUTION_CASH_BUFFER`) / Schutz-Limit
    -- es wird also passend zur Deckung KLEINER skaliert statt bei Unterdeckung nur blockiert.
    Zusätzlich der Portfolio-Exposure-Breaker (EXECUTION_MAX_EXPOSURE_EUR).
    SELL = Regime-Ausstieg: verkauft alles über dem Seed (EXECUTION_MIN_RETAIN bleibt).
    NO_TRADE / kein Entscheid -> {'skipped': ...}."""
    e = agent_env()
    depot = _resolve_depot(e, account, depot)
    if not depot:
        raise SystemExit(f"Kein Depot für Konto '{account}' -- COMDIRECT_DEPOT"
                         f"{'' if account == 'haupt' else '_' + account.upper()} in agent.env setzen.")
    latest = _latest_decision(journal_db)
    if latest is None:
        return {"skipped": "kein Entscheid im Journal"}
    dec, req, dec_ts = latest
    if dec.get("action") != "TRADE":
        return {"skipped": f"kein TRADE ({dec.get('action')})"}
    mode = _resolve_mode(e, account)

    # A1 -- Frische-Gate: veraltete Entscheide sind in KEINEM Pfad mehr ausführbar
    # (gestriger TRADE via manuellem Re-Run/zweitem Codepfad = reale Order auf alter
    # Signallage). EXECUTION_DECISION_MAX_AGE_H (Default 6 h) ist bewusst großzügig
    # für den regulären 13:25-UTC-Zyklus und blockiert alles darüber hinaus.
    max_age_h = float(e.get("EXECUTION_DECISION_MAX_AGE_H", "6"))
    stale = _decision_stale(dec_ts, mode, max_age_h)
    if stale:
        log.warning("Entscheid verworfen: %s", stale)
        return {"skipped": f"Entscheid veraltet -- {stale}"}

    isin = (req.get("instrument") or {}).get("isin")
    name = (req.get("instrument") or {}).get("name")
    direction = req.get("signal", {}).get("direction")

    # A5 -- Interim-Schutz Exit-Pfad: Das bisherige Mapping (direction != LONG -> SELL des
    # Request-Instruments) ist strukturell falsch -- bei LONG->SHORT wäre das Request-
    # Instrument das SHORT-Zertifikat (nur Seed gehalten), die Long-Position bliebe
    # unangetastet. Bis zum Soll-Positions-Umbau (MASSNAHMENPLAN A5-Vollausbau) wird ein
    # Nicht-LONG-Signal daher NICHT automatisch abgewickelt: laut stoppen statt falsch
    # handeln. Live zusätzlich PAUSE (menschlicher Ausstieg via 'F'/manuell, 'R' danach).
    if direction != "LONG":
        reason = (f"Regimewechsel/Signal {direction} -- automatischer Ausstieg deaktiviert "
                  f"(Interim-Schutz A5), manueller Eingriff nötig")
        if mode == "live":
            _pause(reason)
            _alert(f"{reason} (Konto {account}). Trading PAUSIERT -- Positionen prüfen, "
                   f"'F' zum Glattstellen, 'R' nach Bereinigung.")
        else:
            _alert(f"{reason} (Konto {account}, Dry-Run -- kein Eingriff nötig, "
                   f"aber vor Go-Live beachten).")
        log.error("%s", reason)
        return {"skipped": reason}
    side = "BUY"
    size_eur = float(dec.get("size_eur") or 0)

    # A1 -- Ausführungs-Marker: dieser Entscheid wurde live bereits abgewickelt?
    if mode == "live":
        prev = _live_order_status(journal_db, account, dec_ts)
        if prev is not None:
            log.warning("Entscheid %s bereits abgewickelt (Status %s) -- kein erneuter "
                        "Handel.", dec_ts, prev)
            return {"skipped": f"Entscheid bereits abgewickelt (Status {prev})"}
    max_eur = float(e.get("EXECUTION_MAX_EUR", "2500"))
    max_exposure = float(e.get("EXECUTION_MAX_EXPOSURE_EUR", "5000"))
    tol = float(e.get("EXECUTION_LIMIT_TOLERANCE", "0.01"))
    cash_buffer = float(e.get("EXECUTION_CASH_BUFFER", "0.02"))   # Reserve für Kosten/Slippage

    # (#1) Echter Stückkurs aus der Seed-Position -- kein stiller Default mehr.
    # O6a: bevorzugt reference_quote (Kurs + Zeitstempel); fällt defensiv auf
    # reference_price zurück (ältere Stubs/Deployments) -- dann ohne Frischeprüfung.
    try:
        quote = broker.reference_quote(depot, isin, account)
    except Exception as exc:  # noqa: BLE001
        log.warning("reference_quote fehlgeschlagen (%s) -- Fallback reference_price.", exc)
        quote = None
    if quote is None:
        p = broker.reference_price(depot, isin, account)
        quote = {"price": p, "price_datetime": None} if p else None
    price = quote["price"] if quote else None
    if not price or price <= 0:
        quote = None
        override = e.get("COMDIRECT_REF_PRICE")
        if mode != "live" and override:      # nur Dry-Run: expliziter Demo-Override
            price = float(override)
            log.warning("Kein Positionskurs für %s -- Demo-Override %.4f € (nur Dry-Run).",
                        isin, price)
        else:
            log.error("Kein Positionskurs für %s (Seed-Position fehlt?) -- BLOCKIERT.", isin)
            return {"skipped": f"kein Kurs für {isin} (Seed-Position fehlt) -- blockiert"}

    # O6a -- Kursfrische: ein Vortags-/verzögerter Referenzkurs macht das Schutz-Limit
    # wertlos (Limit +/-1 % um einen falschen Kurs). Live blockiert ein staler Kurs;
    # Dry-Run warnt nur (Demo-Betrieb). Ohne Zeitstempel: Warnung (nicht prüfbar).
    price_max_age_h = float(e.get("EXECUTION_PRICE_MAX_AGE_H", "6"))
    price_dt = (quote or {}).get("price_datetime")
    pstale = _price_stale(price_dt, mode, price_max_age_h)
    if pstale:
        if mode == "live":
            log.error("Kursfrische: %s -- BLOCKIERT (fail-safe).", pstale)
            _alert(f"Order {side} {isin} blockiert: {pstale} (Konto {account}). "
                   "Kein Handel auf veralteter Kursbasis.")
            return {"skipped": f"Referenzkurs veraltet -- {pstale} (blockiert)"}
        log.warning("Kursfrische (Dry-Run, nur Hinweis): %s", pstale)
    elif not price_dt:
        log.warning("Kursfrische nicht prüfbar (kein priceDateTime am Referenzkurs).")

    # Menge + Schutz-Limit aus dem echten Kurs. Hinweis: SELL (Regime-Ausstieg) läuft
    # seit dem A5-Interim-Schutz NICHT mehr über diesen Pfad -- der frühere Zweig
    # (verkaufe Request-Instrument über dem Seed) war für LONG->SHORT falsch adressiert.
    # Der Ausstieg kommt mit dem Soll-Positions-Umbau (MASSNAHMENPLAN A5) zurück.
    limit = round(price * (1 + tol), 4)          # Kauf: knapp über Kurs (füllt, deckelt Slippage)
    # Drei Obergrenzen -- die kleinste bindet (Sizing statt bloßer Blockade):
    caps = {"size_eur": int(size_eur // price),          # Strategie-Notional
            "max_eur": int(max_eur // price)}            # Config-Deckel je Order
    try:                                                 # verfügbare Deckung (mit Puffer)
        cash = broker.available_cash(depot, account)
        caps["deckung"] = int((cash * (1 - cash_buffer)) // limit)
    except Exception as exc:  # noqa: BLE001
        if mode == "live":                               # live: ohne Deckung kein Kauf
            log.error("Deckungsabfrage fehlgeschlagen (%s) -- BLOCKIERT (fail-safe).", exc)
            return {"skipped": f"Deckungsabfrage fehlgeschlagen: {exc} (blockiert)"}
        log.warning("Deckungsabfrage fehlgeschlagen (%s) -- Dry-Run ohne Deckungsdeckel.", exc)
    qty = min(caps.values())
    binding = min(caps, key=caps.get)
    if qty < 1:
        return {"skipped": f"Ordergröße <1 Stück (limitierend: {binding}) -- kein Kauf"}
    log.info("BUY-Sizing: qty=%s (bindend: %s; caps=%s)", qty, binding, caps)
    order_value = round(qty * price, 2)
    log.info("Order aus Entscheid: %s %s qty=%s @ Limit %.4f € (Kurs %.4f €, Wert %.2f €, "
             "mode=%s)", side, isin, qty, limit, price, order_value, mode)

    # O6c -- Orderbuch-Check VOR der Aufgabe: existiert bereits eine OFFENE Order auf
    # dem Instrument (z. B. gestern unfilled), gäbe es sonst zwei koexistierende
    # Orders zur alten + neuen Signallage. Live: offene Order ODER unlesbares
    # Orderbuch => BLOCKIERT (fail-safe, konsistent zur Deckungs-/Exposure-Logik);
    # Dry-Run: nur Hinweis (keine realen Orders zu erwarten).
    open_orders = _open_orders_for(depot, isin, account)
    if mode == "live":
        if open_orders is None:
            log.error("Orderbuch vor Neuaufgabe nicht lesbar -- BLOCKIERT (fail-safe).")
            _alert(f"Order {side} {isin} blockiert: Orderbuch nicht lesbar "
                   f"(Konto {account}) -- bitte manuell prüfen.")
            return {"skipped": "Orderbuch nicht lesbar -- blockiert (fail-safe)"}
        if open_orders:
            ids = ", ".join(f"{h['order_id'] or '?'}({h['status']})" for h in open_orders)
            log.error("Offene Order(s) auf %s: %s -- keine Neuaufgabe.", isin, ids)
            _alert(f"Order {side} {isin} blockiert: bereits OFFENE Order(s) im "
                   f"Orderbuch [{ids}] (Konto {account}) -- bitte prüfen/streichen, "
                   "dann nächster Zyklus.")
            return {"skipped": f"offene Order(s) auf {isin}: {ids} -- keine Neuaufgabe"}
    elif open_orders:
        log.warning("Dry-Run-Hinweis: offene Order(s) auf %s im Orderbuch: %s",
                    isin, open_orders)

    if mode == "live":
        # Portfolio-Exposure-Breaker (BUY): Gesamtdepotwert + Kauf gedeckelt. Fail-safe:
        # kann das Exposure nicht gelesen werden, wird blockiert.
        if side == "BUY":
            try:
                exposure = broker.portfolio_exposure(depot, account)
            except Exception as exc:  # noqa: BLE001
                log.error("Exposure-Abfrage fehlgeschlagen (%s) -- BLOCKIERT (fail-safe).", exc)
                return {"skipped": f"Exposure-Abfrage fehlgeschlagen: {exc} (blockiert)"}
            if exposure + order_value > max_exposure:
                log.error("Exposure %.0f € + Order %.0f € > EXECUTION_MAX_EXPOSURE_EUR "
                          "%.0f € -- BLOCKIERT.", exposure, order_value, max_exposure)
                return {"skipped": f"Exposure {exposure:.0f}€+{order_value:.0f}€ > "
                                   f"EXECUTION_MAX_EXPOSURE_EUR {max_exposure:.0f}€ (blockiert)"}
        log.warning("EXECUTION_MODE live (%s) -> REALE Orderanlage (Session-TAN-autorisiert).",
                    account)
        # A1/A3 -- Intent-Zeile VOR dem Execution-POST (zweiphasig): Sie ist der
        # Ausführungs-Marker (UNIQUE je account+decision_ts) und macht auch einen
        # Crash zwischen POST und Journal-Write nachvollziehbar. Schlägt der Write
        # fehl, wird VOR der Bank abgebrochen -- keine Order ohne Journalzeile.
        try:
            _journal_order(journal_db, isin, name, side, mode, "intent", account,
                           qty=qty, value_eur=order_value, limit=limit,
                           decision_ts=dec_ts, strict=True)
        except Exception as exc:  # noqa: BLE001
            log.error("Intent-Journal fehlgeschlagen (%s) -- Order NICHT ausgeführt.", exc)
            _alert(f"Intent-Journal fehlgeschlagen ({exc}) -- Order {side} {isin} "
                   f"NICHT ausgeführt (Konto {account}).")
            return {"skipped": f"Intent-Journal fehlgeschlagen: {exc} -- Order nicht ausgeführt"}

        # A2 -- Execution-POST mit unklarem Ausgang absichern: JEDER Fehler (BrokerError,
        # URLError/Timeout/OSError) führt in die Orderbuch-Reconciliation. Nie Auto-Retry.
        try:
            res = broker.place_order(depot, isin, side, qty, limit,
                                     execute=True, account=account)
        except Exception as exc:  # noqa: BLE001
            log.error("Execution-Ausgang unklar (%s) -- starte Reconciliation.", exc)
            status, oid = _reconcile_after_error(depot, isin, side, qty, account)
            _update_live_order(journal_db, account, dec_ts, status, oid)
            if status == "executed_reconciled":
                _pause("Order kam trotz Fehlerantwort an (Reconciliation) -- "
                       "Zustand prüfen, dann 'R'.")
                _alert(f"Order {side} {isin} qty={qty} wurde trotz Fehler PLATZIERT "
                       f"(orderId={oid}, Konto {account}). Trading PAUSIERT -- bitte "
                       f"Depot prüfen, danach 'R'.")
            elif status == "unknown":
                _pause("Orderstatus UNBEKANNT (Orderbuch nicht abfragbar) -- "
                       "manuell prüfen, dann 'R'.")
                _alert(f"Orderstatus UNBEKANNT für {side} {isin} qty={qty} "
                       f"(Konto {account}): {exc}. Trading PAUSIERT -- bitte Orderbuch/"
                       f"Depot manuell prüfen, danach 'R'.")
            else:  # failed -- Order nachweislich nicht angekommen; Marker bleibt bewusst
                   # stehen (kein Auto-Retry; bewusster manueller Neustart löscht die Zeile)
                _alert(f"Orderanlage FEHLGESCHLAGEN ({exc}) -- Order {side} {isin} "
                       f"qty={qty} nicht platziert (Konto {account}, Orderbuch geprüft).")
            return {"error": f"Execution unklar/fehlgeschlagen: {exc}",
                    "reconciliation": status, "order_id": oid}

        _update_live_order(journal_db, account, dec_ts,
                           f"executed:{res.get('execution_status')}", res.get("order_id"))
        return res

    # Default (dry_run): validieren + Ex-Ante-Kosten, KEINE reale Order.
    res = broker.validate_order(depot, isin, side, qty, limit, account=account)
    _journal_order(journal_db, isin, name, side, mode,
                   f"validated:{res.get('validation_status')}", account, res.get("venue"),
                   qty=qty, value_eur=order_value, limit=limit, decision_ts=dec_ts)
    return res


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(
        description="Orchestrierung: Auditor-Entscheid -> Order-Validierung")
    sub = p.add_subparsers(dest="cmd", required=True)
    for cmd in ("validate-decision", "snapshot"):
        sp = sub.add_parser(cmd)
        sp.add_argument("--journal-db", default="decisions.sqlite",
                        help="Entscheidungsjournal (getrennt von den Marktdaten)")
        sp.add_argument("--account", default="haupt",
                        help="Zugang/Konto (Default haupt; z. B. zweit fürs Zweitdepot)")
        sp.add_argument("--depot", default=None,
                        help="Depot-Override; sonst COMDIRECT_DEPOT[_<ACCOUNT>] aus agent.env")
    args = p.parse_args(argv)
    if args.cmd == "validate-decision":
        # broker.BrokerError -> Exit-Code 1 (wie zuvor der Broker-CLI-Pfad)
        try:
            print(json.dumps(validate_decision(args.journal_db, args.account, args.depot),
                             indent=2, ensure_ascii=False))
        except broker.BrokerError as exc:
            raise SystemExit(str(exc))
    elif args.cmd == "snapshot":
        e = agent_env()
        depot = _resolve_depot(e, args.account, args.depot)
        mode = _resolve_mode(e, args.account)
        print(json.dumps(record_valuation(args.journal_db, args.account, depot, mode),
                         indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
