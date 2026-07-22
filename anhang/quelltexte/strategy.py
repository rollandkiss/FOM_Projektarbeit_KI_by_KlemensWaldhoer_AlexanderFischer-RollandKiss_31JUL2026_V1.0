#!/usr/bin/env python3
"""
strategy.py -- Fachlogik des Handelsagenten (Rev. 5, entkernt).

Zerlegung (siehe STRATEGIE.md §6):
  * strategyloader.py : Betriebsschicht -- Discovery/Aktivierung der Plugins
    (agentconfig.yaml), Produktzuordnung (wknassign.yaml), Auflösung der
    gültigen Instrumente je aktiver Strategie, Datenbedarf-Aggregation.
  * strategy.py (diese Datei) : Domäne -- Strategie-Kontrakt (StrategyBase),
    Instrumentenkatalog + Eignungsregeln, Auditor-Korridor (DecisionRequest/
    Validator), Kennzahlen, Backtest, CLI (nutzt den Loader lazy).
  * plstrategy_<name>.py : Strategie-Plugins (importieren StrategyBase von hier).

Handelssperren im Korridor (HARD_FLAGS -> NO_TRADE): DATA_STALE, GAP_RECENT,
NO_INSTRUMENT, FLAT_SIGNAL, STRATEGY_INACTIVE (inaktiv laut agentconfig.yaml).

CLI:
  python3 strategy.py strategies | instruments [--direction ...] | datareq [--all] [--out ...]
  python3 strategy.py regime|request|backtest|validate  [--strategy sma] [--params JSON] ...
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Bei Direktstart (__main__) unter 'strategy' registrieren, damit Plugins und
# Loader DIESELBEN Klasseninstanzen sehen (Modul-Identität).
sys.modules.setdefault("strategy", sys.modules[__name__])

log = logging.getLogger("strategy")


class StrategyError(Exception):
    """Fachlicher Strategie-Fehler (Domäne). Bewusst von Exception abgeleitet, nicht
    SystemExit: So kann die Auditor-Fail-safe-Kette (auditor.run_audit) einen
    Zyklusfehler wie 'zu wenig Historie' abfangen und ein NO_TRADE journalisieren,
    statt den Prozess unkontrolliert zu beenden. Die CLI-Schicht (main) übersetzt
    StrategyError in Exit-Code 1."""


# --------------------------------------------------------------------------
# Setup-Einschränkungen (bindend für alle Plugins)
# --------------------------------------------------------------------------

SETUP = {
    "data_freshness_s": 90,
    "min_decision_interval_s": 60,
    "capital_eur": 2000.0,
    "size_min_eur": 1000.0,
    "fee_per_order_eur": 3.90,
    "max_roundtrips_per_day": 1,
}

VOL_TARGET_PCT = 1.0
STOP_LOSS_PCT = 3.0
TAKE_PROFIT_PCT = 2.0
SPREAD_BP_DEFAULT = 20

# Kapitalertragsteuer für den Backtest-Realismus (vgl. TRANSSKRIPT_Daily-SMA-gehebelt):
# 26,38 % KapESt (inkl. Soli) auf 70 % der Gewinne (30 % Teilfreistellung für Aktien-
# fonds). Effektiver Satz auf realisierte Gewinne ~ 18,47 %. Kirchensteuer nicht modelliert.
TAX_KEST_RATE = 0.2638
TAX_TEILFREISTELLUNG = 0.30
TAX_ON_GAINS = TAX_KEST_RATE * (1 - TAX_TEILFREISTELLUNG)

FLAG_DATA_STALE_S = 300
FLAG_SPREAD_WIDE_BP = 40
FLAG_VOL_EXTREME_FACTOR = 2.0
FLAG_REGIME_FRESH_DAYS = 3
# ML_DISAGREE-Schwelle (Default; überschreibbar via agentconfig.yaml ml.threshold_
# disagree): p_direction unterhalb => SOFT-Flag. Bewusst KEIN Hard-Flag -- die
# scikit-Prüfinstanz (mlforecast.py) wirkt wie vol20/last5 als Risiko-Input für
# die LLM-Voter; als Hard-Flag würde sie de facto zur Signalquelle (verletzt
# bounded discretion; siehe mlforecast.py-Docstring + KI_EINORDNUNG.md §3).
FLAG_ML_DISAGREE_P = 0.45
HARD_FLAGS = {"DATA_STALE", "GAP_RECENT", "NO_INSTRUMENT", "FLAT_SIGNAL",
              "STRATEGY_INACTIVE"}

# --------------------------------------------------------------------------
# Instrumentenkatalog (Metadaten) + Eignungsregeln
# --------------------------------------------------------------------------

INSTRUMENT_CATALOG: dict[str, dict] = {
    "DE000SB295Z1": {"name": "SG Faktor 2x Long S&P 500", "direction": "LONG",
                     "leverage": 2.0, "issuer": "SG", "premium_partner": True,
                     "active": True, "price_eur": None, "spread_bp": None,
                     "note": "strategischer Beginn"},
    "DE000SD5PHQ2": {"name": "SG Faktor 2x Short S&P 500", "direction": "SHORT",
                     "leverage": 2.0, "issuer": "SG", "premium_partner": True,
                     "active": True, "price_eur": None, "spread_bp": None,
                     "note": "strategischer Beginn"},
    "DE000SD0USY4": {"name": "SG Faktor 4x Long S&P 500", "direction": "LONG",
                     "leverage": 4.0, "issuer": "SG", "premium_partner": True,
                     "active": False, "price_eur": 28.50, "spread_bp": 18,
                     "note": "Risikovariante (Evaluation)"},
    "DE000SF3J181": {"name": "SG Faktor 4x Short S&P 500", "direction": "SHORT",
                     "leverage": 4.0, "issuer": "SG", "premium_partner": True,
                     "active": False, "price_eur": 0.45, "spread_bp": 220,
                     "note": "ausgeschlossen: Tick-Spread ~2,2 %"},
    "DE000PF2SP50": {"name": "BNP Faktor 2x Long S&P 500", "direction": "LONG",
                     "leverage": 2.0, "issuer": "BNP", "premium_partner": True,
                     "active": False, "price_eur": None, "spread_bp": None,
                     "note": "Kandidat Spread-Benchmark"},
    "DE000PX2PXP7": {"name": "BNP Faktor 2x Short S&P 500", "direction": "SHORT",
                     "leverage": 2.0, "issuer": "BNP", "premium_partner": True,
                     "active": False, "price_eur": None, "spread_bp": None,
                     "note": "Kandidat Spread-Benchmark"},
}

CONSTRAINTS = {
    "price_min_eur": 5.0,
    "spread_bp_max": 40,
    "require_premium_partner": True,
    "leverage_max": 2.0,
}


def eligible(isin: str, overrides: dict | None = None) -> tuple[bool, str]:
    """Prüft ein Instrument gegen die Handelbarkeits-Constraints (Aktivität, Hebel,
    Premium-Partner, Mindestkurs, Spread). Returns (geeignet, Begründung).
    `overrides` erlaubt strategie-/zuordnungsspezifische Constraint-Anpassungen."""
    inst = INSTRUMENT_CATALOG.get(isin)
    if inst is None:
        return False, "nicht im Katalog"
    c = {**CONSTRAINTS, **(overrides or {})}
    if not inst["active"]:
        return False, "inaktiv"
    if inst["leverage"] > c["leverage_max"]:
        return False, f"Hebel {inst['leverage']} > Cap {c['leverage_max']}"
    if c["require_premium_partner"] and not inst["premium_partner"]:
        return False, "kein Premium-Partner"
    if inst["price_eur"] is not None and inst["price_eur"] < c["price_min_eur"]:
        return False, f"Kurs {inst['price_eur']} € < Mindestkurs"
    if inst["spread_bp"] is not None and inst["spread_bp"] > c["spread_bp_max"]:
        return False, f"Spread {inst['spread_bp']} bp > Max"
    return True, "ok"


# --------------------------------------------------------------------------
# Strategie-Kontrakt
# --------------------------------------------------------------------------

@dataclass
class StrategySignal:
    direction: str                 # LONG | SHORT | FLAT
    meta: dict = field(default_factory=dict)


@dataclass
class DailyBar:
    ts_ms: int
    open: float
    close: float

    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d")


class StrategyBase:
    """Kontrakt für plstrategy_<name>.py -- siehe strategyloader.py für Laden/Aktivierung."""
    name = "base"
    description = ""
    underlying = "^GSPC"
    decision_interval_s = 86_400
    data_freshness_s = 6 * 3600
    min_history = 1
    default_params: dict = {}
    data_requirements: dict = {"symbols": ["^GSPC"],
                               "bootstrap": {"1d": "max"}, "live": False}

    def __init__(self, params: dict | None = None):
        self.params = {**self.default_params, **(params or {})}
        if self.decision_interval_s < SETUP["min_decision_interval_s"]:
            raise StrategyError(
                f"Strategie '{self.name}': Entscheidungsintervall "
                f"{self.decision_interval_s}s < Setup-Untergrenze "
                f"{SETUP['min_decision_interval_s']}s")
        if self.data_freshness_s < SETUP["data_freshness_s"]:
            raise StrategyError(
                f"Strategie '{self.name}': braucht Datenfrische "
                f"{self.data_freshness_s}s, Setup liefert ~{SETUP['data_freshness_s']}s")

    def decide_series(self, bars: list[DailyBar]) -> list[StrategySignal]:
        raise NotImplementedError

    def decide(self, bars: list[DailyBar]) -> StrategySignal:
        return self.decide_series(bars)[-1]


# --------------------------------------------------------------------------
# Daten / Kennzahlen
# --------------------------------------------------------------------------

def load_daily(db: str, symbol: str) -> list[DailyBar]:
    """Tagesbars (bars_1d) eines Symbols aufsteigend aus der read-only geöffneten
    Marktdaten-SQLite laden (vom Datensammler befüllt)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT ts_utc_ms, open, close FROM bars_1d WHERE symbol=? ORDER BY ts_utc_ms",
        (symbol,)).fetchall()
    con.close()
    return [DailyBar(*r) for r in rows]


def vol20_pct(bars: list[DailyBar], idx: int) -> float:
    rets = [(bars[i].close / bars[i - 1].close - 1) * 100
            for i in range(idx - 19, idx + 1)]
    return statistics.pstdev(rets)


def position_size(capital: float, vol20: float) -> float:
    """Volatilitätsskalierte Positionsgröße: skaliert das Kapital mit
    VOL_TARGET_PCT/vol20 (gedeckelt auf 1.0), niemals unter die Mindestgröße
    (size_min_eur) und nie über das Kapital."""
    factor = min(1.0, VOL_TARGET_PCT / vol20) if vol20 > 0 else 1.0
    return max(SETUP["size_min_eur"], min(capital, capital * factor))


def collect_data_quality(db: str) -> dict:
    """Datenqualität für den DecisionRequest. Zählt nur `disconnect`-Gaps (echte
    Feed-Verluste), NICHT `silence`: Der Index `^GSPC` tickt außerhalb der US-Cash-
    Session legitim nicht (nachts/Wochenende), das ist kein Datenfehler. Würde man
    `silence` mitzählen, feuerte GAP_RECENT strukturell an jedem Montag/Morgen und
    blockierte den Handel (empirischer Befund 20.07.: 76x silence auf ^GSPC,
    0x disconnect). Ein Feed-Stall während aktiver Zeiten wird weiterhin über
    last_tick_age_s -> DATA_STALE erkannt. Feinere Variante (silence nur innerhalb
    erwarteter Session-Fenster via market_calendar) ist als optionales Feature notiert."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    last = con.execute("SELECT MAX(ts_utc_ms) FROM ticks_raw").fetchone()[0]
    disconnects_24h = con.execute(
        "SELECT COUNT(*) FROM gaps WHERE to_utc_ms > ? AND reason = 'disconnect'",
        (now_ms - 86_400_000,)).fetchone()[0]
    con.close()
    return {"disconnects_24h": disconnects_24h,
            "last_tick_age_s": round((now_ms - last) / 1000) if last else None}


# --------------------------------------------------------------------------
# Auditor-Korridor: DecisionRequest + Validator
# --------------------------------------------------------------------------

def cost_context(capital: float, spread_bp: float | None = None) -> dict:
    """B4 -- deterministischer Kostenkontext für den DecisionRequest: Round-Trip-
    Fixkosten und deren prozentuale Last je Größenstufe (Min/Mitte/Kapital). Gibt dem
    Auditor die Grundlage, die Positionsgröße ökonomisch abzuwägen, statt pauschal
    die (fixkosten-teuerste) Minimalgröße zu wählen. Werte aus SETUP/STRATEGIE.md §5;
    keine LLM-Mehrkosten."""
    fee_rt = round(2 * SETUP["fee_per_order_eur"], 2)
    steps = sorted({SETUP["size_min_eur"],
                    round((SETUP["size_min_eur"] + capital) / 2, 2), float(capital)})
    return {
        "fee_roundtrip_eur": fee_rt,
        "fee_pct_by_size_eur": {f"{s:g}": round(fee_rt / s * 100, 2)
                                for s in steps if s > 0},
        "spread_bp_assumed": spread_bp if spread_bp is not None else SPREAD_BP_DEFAULT,
        "note": ("Fixkosten je Round-Trip. Kleinere Größe = geringeres Risiko, aber "
                 "höhere Fixkostenquote -- Größenwahl gegen den Break-even abwägen."),
    }


# Signal-Steuerkanal-Overrides für den ML-Modus (control.py schreibt, wir lesen;
# gleiche Datei-Konvention wie TRADING_PAUSED/GROUPS_ACCEPTED -- Projektroot).
ML_MODES_FILE = Path(__file__).resolve().parent / "ML_MODES.json"
ML_MODE_VALUES = ("live", "sidecar", "off")


def ml_mode(strategy_name: str, overrides_file: Path | None = None
            ) -> tuple[str, str]:
    """Effektiver Betriebsmodus der scikit-Prüfinstanz für eine Strategie.
    Returns (mode, quelle) mit mode in {live, sidecar, off}:
      * live    -- ml_context + Soft-Flag ML_DISAGREE gehen an die LLM-Voter.
      * sidecar -- Prognose läuft und wird journalisiert (ml_predictions via
                  run_cycle), aber NICHTS davon erreicht Request/LLMs/Validator.
      * off     -- keine Inferenz, kein Journal.
    Rangfolge: Signal-Override (ML_MODES.json, Befehle mlNy/mlNn) >
    agentconfig ml.modes.<strategie> > agentconfig ml.mode > 'live'.
    ml.enabled=false schaltet hart auf 'off' (Override wirkungslos).
    Unbekannte Werte fallen fail-safe auf 'sidecar' (kein LLM-Einfluss)."""
    try:
        import strategyloader as loader   # lazy (kein Importzyklus top-level)
        mlcfg = loader.load_ml_config()
    except Exception as exc:  # noqa: BLE001
        log.warning("ml-Config nicht lesbar -- Modus 'sidecar' (fail-safe): %s", exc)
        return "sidecar", "fehlerhafte Config"
    if not mlcfg.get("enabled", True):
        return "off", "agentconfig ml.enabled=false"
    try:
        overrides = json.loads((overrides_file or ML_MODES_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        overrides = {}
    for value, source in ((overrides.get(strategy_name), "Signal-Override"),
                          ((mlcfg.get("modes") or {}).get(strategy_name),
                           "agentconfig ml.modes"),
                          (mlcfg.get("mode"), "agentconfig ml.mode")):
        if value is not None:
            v = str(value).strip().lower()
            if v not in ML_MODE_VALUES:
                log.warning("ml_mode: unbekannter Wert %r (%s) -- 'sidecar' "
                            "(fail-safe).", value, source)
                return "sidecar", f"{source} (ungültig)"
            return v, source
    return "live", "Default"


def ml_probe(db: str, symbol: str, direction: str,
             strategy_name: str = "sma") -> tuple[dict | None, float]:
    """Best-effort-Anbindung der scikit-Prüfinstanz (mlforecast.py) für den
    DecisionRequest. Returns (ml_context | None, threshold).

    Nur im Modus 'live' der jeweiligen Strategie (ml_mode) wird ein ml_context
    geliefert -- 'sidecar'/'off' geben (None, Schwelle): der Request bleibt dann
    frei von ML-Angaben (Sidecar-Journal läuft separat über run_cycle predict).
    Fail-open by design: fehlt das Modul, das persistierte Modell, die Historie
    oder die ml-Config, gibt es ebenfalls (None, Schwelle) -- KEIN Flag, kein
    Zyklusabbruch. Die Prüfinstanz kann Trades nur schwächen, ihr Ausfall darf
    den deterministischen Kern nicht blockieren."""
    threshold = FLAG_ML_DISAGREE_P
    try:
        import strategyloader as loader   # lazy (kein Importzyklus top-level)
        mlcfg = loader.load_ml_config()
    except Exception as exc:  # noqa: BLE001
        log.warning("ml-Config nicht lesbar (ignoriert): %s", exc)
        mlcfg = {}
    if mlcfg.get("threshold_disagree") is not None:
        threshold = float(mlcfg["threshold_disagree"])
    mode, source = ml_mode(strategy_name)
    if mode != "live":
        log.info("ml_probe[%s]: Modus '%s' (%s) -- kein ml_context im Request.",
                 strategy_name, mode, source)
        return None, threshold
    try:
        import mlforecast
    except ImportError:
        log.info("mlforecast/sklearn nicht installiert -- kein ml_context (best-effort).")
        return None, threshold
    ctx = mlforecast.ml_context_for_request(
        db, symbol, direction,
        model_path=mlcfg.get("model_path") or mlforecast.MODEL_PATH_DEFAULT)
    return ctx, threshold


def build_request(db: str, strat: StrategyBase, plugin: dict, capital: float,
                  spread_bp: float | None = None) -> dict:
    bars = load_daily(db, strat.underlying)
    if len(bars) < max(strat.min_history, 21):
        raise StrategyError(f"Zu wenig Historie für '{strat.name}' "
                            f"({len(bars)} < {strat.min_history}) -- bootstrap ausführen.")
    series = strat.decide_series(bars)
    sig = series[-1]
    i = len(bars) - 1
    v20 = vol20_pct(bars, i)
    dq = collect_data_quality(db)
    inst = plugin["instruments"].get(sig.direction) if sig.direction != "FLAT" else None
    age = 1
    while i - age >= 0 and series[i - age].direction == sig.direction:
        age += 1

    flags = []
    if not plugin.get("active", True):
        flags.append("STRATEGY_INACTIVE")
    if sig.direction == "FLAT":
        flags.append("FLAT_SIGNAL")
    elif inst is None:
        flags.append("NO_INSTRUMENT")
    if dq["last_tick_age_s"] is None or dq["last_tick_age_s"] > FLAG_DATA_STALE_S:
        flags.append("DATA_STALE")
    if dq["disconnects_24h"] > 0:            # nur echte Feed-Verluste (nicht silence)
        flags.append("GAP_RECENT")
    if spread_bp is not None and spread_bp > FLAG_SPREAD_WIDE_BP:
        flags.append("SPREAD_WIDE")
    if v20 > FLAG_VOL_EXTREME_FACTOR * VOL_TARGET_PCT:
        flags.append("VOL_EXTREME")
    if age < FLAG_REGIME_FRESH_DAYS:
        flags.append("REGIME_FRESH")

    # scikit-Prüfinstanz (Predictive Inference als Prüfinstanz im Korridor):
    # p_direction < Schwelle => SOFT-Flag ML_DISAGREE (Risiko-Input wie VOL_EXTREME,
    # kein Hard-Flag). Nur im Modus 'live' (ml_mode je Strategie; Sidecar/Off =>
    # ml_ctx=None) und best-effort -- ohne Modell/sklearn bleibt der Request
    # unverändert.
    ml_ctx, ml_threshold = ml_probe(db, strat.underlying, sig.direction, strat.name)
    if ml_ctx is not None and ml_ctx["p_direction"] < ml_threshold:
        flags.append("ML_DISAGREE")

    return {
        "date": bars[i].date,
        "strategy": {"name": strat.name, "params": strat.params,
                     "active": plugin.get("active", True),
                     "plugin": plugin["files"]["strategy"],
                     "assignment": plugin["files"]["assignment"]},
        "underlying": strat.underlying,
        "signal": {"direction": sig.direction, "age_days": age, "meta": sig.meta},
        "instrument": ({"isin": inst["isin"], "name": inst["name"],
                        "leverage": inst["leverage"]} if inst else None),
        "vol20_pct": round(v20, 2), "vol_ratio": round(v20 / VOL_TARGET_PCT, 2),
        "last5_index_returns_pct": [
            round((bars[j].close / bars[j - 1].close - 1) * 100, 2)
            for j in range(i - 4, i + 1)],
        "data_quality": dq, "spread_bp_last": spread_bp,
        # Kalibrierte Folgetags-Wahrscheinlichkeit der scikit-Prüfinstanz -- NUR im
        # Modus live als Feld vorhanden. Bewusst KEIN "ml_context": null im
        # Sidecar/Off: Live-Befund 22.07. (Zyklus 13:25) -- qwen kommentierte das
        # null-Feld ("Keine ML-Daten vorhanden"); schon die EXISTENZ des Felds
        # erreicht die LLM-Voter und verletzt die Sidecar-Zusage.
        **({"ml_context": {**ml_ctx, "threshold_disagree": ml_threshold}}
           if ml_ctx is not None else {}),
        "size_range_eur": [SETUP["size_min_eur"], capital],
        "size_suggested_eur": round(position_size(capital, v20), 2),
        "cost_context": cost_context(capital, spread_bp),   # B4: Größe ökonomisch abwägbar
        # Diskrete Einstiegsoptionen inkl. Semantik (STRATEGIE.md §Ausführung), damit
        # der Auditor begründet wählen kann. Die Label-Liste bleibt für den Validator
        # (entry in entry_options); entry_options_desc erläutert sie dem LLM.
        "entry_options": ["E1", "E2", "E3"],
        "entry_options_desc": {
            "E1": "Fenster-Open (09:35 ET, nach Eröffnungsauktion)",
            "E2": "erste 1m-Bestätigung: Kurs >=/<= VWAP der ersten 15 min in Regimerichtung",
            "E3": "verzögert 10:30 ET (meidet Eröffnungsvolatilität)",
        },
        "audit_flags": flags or ["none"],
    }


def validate_response(request: dict, response_raw: str) -> dict:
    no_trade = {"action": "NO_TRADE", "entry": None, "size_eur": 0.0}
    try:
        resp = json.loads(response_raw)
    except (json.JSONDecodeError, TypeError):
        return {**no_trade, "reason": "VALIDATOR: ungültiges JSON",
                "validator": "reject_json"}
    action = resp.get("action")
    # F3 (Live-Befund 22.07.): assessed_direction in JEDES Ergebnis-Dict durchreichen,
    # sobald die Antwort parsebar ist -- auditor._vote_row/Dashboard zeigten sonst
    # dir=None je Einzelvotum, obwohl das Zweitmeinung-Gate die Richtung geprüft hat.
    assessed = resp.get("assessed_direction")
    if action not in ("TRADE", "NO_TRADE"):
        return {**no_trade, "assessed_direction": assessed,
                "reason": "VALIDATOR: action fehlt/ungültig",
                "validator": "reject_schema"}
    if action == "NO_TRADE":
        return {**no_trade, "assessed_direction": assessed,
                "reason": str(resp.get("reason", ""))[:500],
                "validator": "ok"}
    hard = HARD_FLAGS.intersection(request.get("audit_flags", []))
    if hard:
        return {**no_trade, "assessed_direction": assessed,
                "reason": f"VALIDATOR: TRADE trotz harter Flags {sorted(hard)}",
                "validator": "reject_hard_flag"}
    # Zweitmeinung-Gate (bounded discretion): Ein TRADE ist nur zulässig, wenn die
    # eigene Regime-Einschätzung des LLM (assessed_direction) mit dem Strategie-Signal
    # übereinstimmt. Uneinigkeit oder UNCLEAR => Veto (NO_TRADE); die Richtung wird
    # NIE auf die LLM-Sicht gedreht -- das LLM kann nur bestätigen oder blockieren.
    signal_dir = (request.get("signal") or {}).get("direction")
    if assessed != signal_dir:
        return {**no_trade, "assessed_direction": assessed,
                "reason": f"VALIDATOR: Veto -- Zweitmeinung {assessed} != Signal {signal_dir}",
                "validator": "reject_disagreement"}
    entry = resp.get("entry")
    interventions = []
    if entry not in request["entry_options"]:
        entry, interventions = "E1", ["entry_default"]
    lo, hi = request["size_range_eur"]
    cap = min(hi, request["size_suggested_eur"])
    try:
        size = float(resp.get("size_eur"))
    except (TypeError, ValueError):
        return {**no_trade, "assessed_direction": assessed,
                "reason": "VALIDATOR: size_eur fehlt/ungültig",
                "validator": "reject_schema"}
    clamped = max(lo, min(cap, size))
    if clamped != size:
        interventions.append(f"size_clamped:{size}->{clamped}")
    return {"action": "TRADE", "entry": entry, "size_eur": round(clamped, 2),
            "assessed_direction": assessed,
            "instrument": request["instrument"],
            "reason": str(resp.get("reason", ""))[:500],
            "validator": ",".join(interventions) or "ok"}


# --------------------------------------------------------------------------
# Backtest (plugin-generisch, auf dem Underlying der Strategie)
# --------------------------------------------------------------------------

def backtest(db: str, strat: StrategyBase, plugin: dict, capital: float,
             years: int, spread_bp: float, fee: float) -> dict:
    bars = load_daily(db, strat.underlying)
    if len(bars) < strat.min_history + 21:
        raise StrategyError("Zu wenig Historie für Backtest -- bootstrap ausführen.")
    series = strat.decide_series(bars)
    start = max(strat.min_history, 21, len(bars) - years * 252)
    rt_fix, spread = 2 * fee, spread_bp / 10_000.0
    inst = plugin["instruments"].get("LONG") or plugin["instruments"].get("SHORT")
    lev = inst["leverage"] if inst else CONSTRAINTS["leverage_max"]

    b0 = {"pnl": 0.0, "costs": 0.0, "tax": 0.0, "wins": 0, "days": 0,
          "trades": 0, "cover": 0}
    b2_pnl, b2_costs, b2_trades, b2_tax = 0.0, 0.0, 0, 0.0
    equity, peak, dd_max = capital, capital, 0.0
    prev_dir = "FLAT"
    seg_start = None                       # b2_pnl beim Eintritt ins aktuelle Segment

    for i in range(start, len(bars)):
        direction = series[i - 1].direction
        b0["days"] += 1
        if direction != "FLAT":
            d = 1.0 if direction == "LONG" else -1.0
            size = position_size(capital, vol20_pct(bars, i - 1))
            r = (bars[i].close / bars[i].open - 1) * d * lev
            r = max(-STOP_LOSS_PCT / 100, min(TAKE_PROFIT_PCT / 100, r))
            gross, costs = size * r, rt_fix + size * spread
            b0["pnl"] += gross - costs
            b0["costs"] += costs
            b0["tax"] += TAX_ON_GAINS * max(0.0, gross)   # tgl. Verkauf -> Gewinn versteuern
            b0["trades"] += 1
            b0["wins"] += gross - costs > 0
            b0["cover"] += gross > costs
            equity += gross - costs
            peak = max(peak, equity)
            dd_max = max(dd_max, (peak - equity) / peak)
            b2_pnl += capital * (bars[i].close / bars[i - 1].close - 1) * d * lev
        if direction != prev_dir:
            if prev_dir != "FLAT" and seg_start is not None:   # Verkauf: Segmentgewinn versteuern
                b2_tax += TAX_ON_GAINS * max(0.0, b2_pnl - seg_start)
            seg_start = b2_pnl if direction != "FLAT" else None
            b2_costs += rt_fix + capital * spread
            b2_trades += 1
            prev_dir = direction
    if prev_dir != "FLAT" and seg_start is not None:           # offene Position: mark-to-market
        b2_tax += TAX_ON_GAINS * max(0.0, b2_pnl - seg_start)

    b1_gain = capital * (bars[-1].close / bars[start].close - 1)
    b1_tax = TAX_ON_GAINS * max(0.0, b1_gain)
    pct = lambda x: round(100 * x / capital, 1)  # noqa: E731
    b0_net, b2_net = b0["pnl"] - b0["tax"], b2_pnl - b2_costs - b2_tax
    return {
        "strategie": {"name": strat.name, "params": strat.params,
                      "underlying": strat.underlying,
                      "instrument": inst["isin"] if inst else None},
        "zeitraum": f"{bars[start].date} - {bars[-1].date} ({b0['days']} Handelstage)",
        "kapital_eur": capital,
        "annahmen": {"spread_bp": spread_bp, "fee_eur": fee, "leverage": lev,
                     "stop_pct": STOP_LOSS_PCT, "tp_pct": TAKE_PROFIT_PCT,
                     "steuer_auf_gewinn_pct": round(100 * TAX_ON_GAINS, 2)},
        "B0_taeglicher_roundtrip": {
            "pnl_vor_steuer_eur": round(b0["pnl"], 2),
            "steuer_eur": round(b0["tax"], 2),
            "pnl_netto_eur": round(b0_net, 2), "pnl_netto_pct": pct(b0_net),
            "kosten_eur": round(b0["costs"], 2), "handelstage": b0["trades"],
            "trefferquote_pct": round(100 * b0["wins"] / b0["trades"], 1)
            if b0["trades"] else None,
            "kostendeckung_pct": round(100 * b0["cover"] / b0["trades"], 1)
            if b0["trades"] else None,
            "max_drawdown_pct": round(100 * dd_max, 1)},
        "B1_buy_hold_index": {
            "rendite_vor_steuer_pct": round(100 * b1_gain / capital, 1),
            "steuer_eur": round(b1_tax, 2),
            "rendite_netto_pct": pct(b1_gain - b1_tax)},
        "B2_wechsel_only": {
            "pnl_vor_steuer_eur": round(b2_pnl - b2_costs, 2),
            "steuer_eur": round(b2_tax, 2),
            "pnl_netto_eur": round(b2_net, 2), "pnl_netto_pct": pct(b2_net),
            "kosten_eur": round(b2_costs, 2), "wechsel": b2_trades},
    }


# --------------------------------------------------------------------------
# CLI (nutzt strategyloader lazy -- kein Importzyklus)
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    import strategyloader as loader   # lazy: Loader importiert strategy top-level

    p = argparse.ArgumentParser(description="Handelsagent: Strategie-Fachlogik + CLI")
    p.add_argument("--db", default="marketdata.sqlite")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("strategies")
    sp = sub.add_parser("instruments")
    sp.add_argument("--direction", choices=("LONG", "SHORT"), default=None)
    sp = sub.add_parser("datareq")
    sp.add_argument("--out", default="data_requirements.json")
    sp.add_argument("--all", action="store_true",
                    help="auch inaktive Strategien einbeziehen")
    for name in ("regime", "request", "backtest"):
        sp = sub.add_parser(name)
        sp.add_argument("--strategy", default="sma")
        sp.add_argument("--params", default=None)
        sp.add_argument("--capital", type=float, default=SETUP["capital_eur"])
        if name == "request":
            sp.add_argument("--spread-bp", type=float, default=None)
        if name == "backtest":
            sp.add_argument("--years", type=int, default=10)
            sp.add_argument("--spread-bp", type=float, default=SPREAD_BP_DEFAULT)
            sp.add_argument("--fee", type=float, default=SETUP["fee_per_order_eur"])
    sp = sub.add_parser("validate")
    sp.add_argument("--strategy", default="sma")
    sp.add_argument("--params", default=None)
    sp.add_argument("--capital", type=float, default=SETUP["capital_eur"])
    sp.add_argument("--response", required=True)

    args = p.parse_args(argv)

    if args.cmd == "strategies":
        for name, pl in sorted(loader.load_plugins().items()):
            cls = pl["cls"]
            print(json.dumps({
                "name": name, "aktiv": pl["active"],
                "beschreibung": cls.description, "underlying": cls.underlying,
                "handelbar": pl["assign"] is not None,
                "instrumente": {d: (i["isin"] if i else None)
                                for d, i in pl["instruments"].items()},
                "params": {**cls.default_params, **pl["config_params"]},
                "dateien": pl["files"]}, ensure_ascii=False))
        return
    if args.cmd == "instruments":
        for isin, inst in INSTRUMENT_CATALOG.items():
            if args.direction and inst["direction"] != args.direction:
                continue
            ok, why = eligible(isin)
            print(json.dumps({"isin": isin, **inst, "geeignet": ok, "grund": why},
                             ensure_ascii=False))
        return
    if args.cmd == "datareq":
        req = loader.build_data_requirements(include_inactive=args.all)
        Path(args.out).write_text(json.dumps(req, indent=2, ensure_ascii=False))
        print(json.dumps(req, indent=2, ensure_ascii=False))
        log.info("Datenbedarf geschrieben: %s", args.out)
        return

    params = json.loads(args.params) if getattr(args, "params", None) else None
    # CLI-Schicht: StrategyError -> Exit-Code 1 (Bibliotheks-Aufrufer wie auditor.py
    # erhalten die Ausnahme dagegen unverändert und fangen sie fail-safe ab).
    try:
        strat, plugin = loader.get_plugin(args.strategy, params)

        if args.cmd == "regime":
            bars = load_daily(args.db, strat.underlying)
            sig = strat.decide(bars)
            inst = plugin["instruments"].get(sig.direction) \
                if sig.direction != "FLAT" else None
            print(json.dumps({"date": bars[-1].date, "strategie": strat.name,
                              "aktiv": plugin["active"], "underlying": strat.underlying,
                              "direction": sig.direction, "meta": sig.meta,
                              "instrument": inst}, indent=2, ensure_ascii=False))
        elif args.cmd == "request":
            print(json.dumps(build_request(args.db, strat, plugin, args.capital,
                                           args.spread_bp), indent=2, ensure_ascii=False))
        elif args.cmd == "validate":
            req = build_request(args.db, strat, plugin, args.capital)
            print(json.dumps(validate_response(req, args.response), indent=2,
                             ensure_ascii=False))
        elif args.cmd == "backtest":
            print(json.dumps(backtest(args.db, strat, plugin, args.capital, args.years,
                                      args.spread_bp, args.fee), indent=2,
                             ensure_ascii=False))
    except StrategyError as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
