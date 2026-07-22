#!/usr/bin/env python3
"""
strategyloader.py -- Loader des Handelsagenten (Betriebsschicht).

Verantwortung (getrennt von der Fachlogik in strategy.py):
  1. Discovery + Kontraktprüfung der Plugins  plstrategy_<name>.py
  2. Aktivierung gemäß agentconfig.yaml (strategies.active, params-Overrides)
  3. Produktzuordnung aus wknassign.yaml lesen und für AKTIVE Strategien die
     gültigen Instrumente je Richtung BEIM LADEN auflösen (Ladezeit- statt
     Handelszeitfehler); aktive Strategie ohne handelbares Produkt -> Warnung
  4. Datenbedarf aggregieren (nur aktive Strategien) für datacollect.py

Fail-fast-Regeln:
  * Dateisuffix == Strategy.name - Zuordnungs-Underlying == Strategie-Underlying
  * ISINs müssen im Katalog existieren - Zuordnung ohne Plugin = Fehler
  * agentconfig.active mit unbekanntem Namen = Fehler
"""

from __future__ import annotations

import importlib.util
import logging
from datetime import datetime, timezone
from pathlib import Path

from strategy import INSTRUMENT_CATALOG, StrategyBase, eligible

log = logging.getLogger("strategyloader")

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "agentconfig.yaml"
ASSIGN_FILE = BASE_DIR / "wknassign.yaml"

_PLUGINS: dict[str, dict] | None = None


def _yaml_load(path: Path) -> dict:
    try:
        import yaml  # PyYAML
    except ImportError:
        raise SystemExit(f"PyYAML fehlt für {path.name} -- installieren mit: "
                         "pip3 install pyyaml") from None
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise SystemExit(f"{path.name}: YAML-Syntaxfehler -- {exc}") from exc


def _import_file(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------
# Konfiguration + Zuordnung
# --------------------------------------------------------------------------

def load_config() -> dict:
    """agentconfig.yaml; fehlt sie, gelten alle Plugins als aktiv (mit Warnung)."""
    if not CONFIG_FILE.exists():
        log.warning("%s fehlt -- alle gefundenen Strategien gelten als aktiv. "
                    "Für den Betrieb Datei anlegen!", CONFIG_FILE.name)
        return {"strategies": {"active": None, "params": {}}}
    data = _yaml_load(CONFIG_FILE)
    strategies = data.get("strategies") or {}
    active = strategies.get("active")
    if active is not None and not isinstance(active, list):
        raise SystemExit(f"{CONFIG_FILE.name}: strategies.active muss Liste sein")
    return {"strategies": {"active": active,
                           "params": strategies.get("params") or {}}}


def load_auditor_config() -> dict:
    """auditor-Sektion aus agentconfig.yaml lesen (Modell-Liste fürs Multi-Voting +
    base_url). Fehlt sie, wird {} zurückgegeben -> auditor.py nutzt seine CLI-/Default-
    Werte (Einzelmodell, verhaltensneutral). `models` darf String oder Liste sein."""
    if not CONFIG_FILE.exists():
        return {}
    a = (_yaml_load(CONFIG_FILE).get("auditor") or {})
    models = a.get("models")
    if isinstance(models, str):
        models = [models]
    if models is not None and (not isinstance(models, list)
                               or not all(isinstance(m, str) for m in models)):
        raise SystemExit(f"{CONFIG_FILE.name}: auditor.models muss Liste von Strings sein")
    # M1 -- timeout_s je Modell-Aufruf (Sekunden); run_cycle + auditor lesen BEIDE
    # diesen Wert (eine Quelle der Wahrheit fürs Zeitbudget, Mac-mini-relevant).
    timeout_s = a.get("timeout_s")
    if timeout_s is not None:
        try:
            timeout_s = int(timeout_s)
        except (TypeError, ValueError):
            raise SystemExit(f"{CONFIG_FILE.name}: auditor.timeout_s muss eine "
                             "ganze Zahl (Sekunden) sein") from None
        if timeout_s <= 0:
            raise SystemExit(f"{CONFIG_FILE.name}: auditor.timeout_s muss > 0 sein")
    return {"models": models or None, "base_url": a.get("base_url"),
            "timeout_s": timeout_s}


def load_ml_config() -> dict:
    """ml-Sektion aus agentconfig.yaml (scikit-Prüfinstanz, mlforecast.py).
    Fehlt sie, gilt {} -> strategy.ml_probe/ml_mode nutzen Defaults (live,
    Schwelle 0,45, ml_model.pkl, logreg). Felder: enabled (bool),
    mode ('live'|'sidecar'|'off', Default für alle Strategien),
    modes (Mapping Strategie->Modus), model_path (str),
    threshold_disagree (float in (0, 1)), algo ('logreg'|'gb').
    Laufzeit-Overrides je Strategie setzt der Signal-Steuerkanal in
    ML_MODES.json (Befehle mlNy/mlNn) -- Rangfolge siehe strategy.ml_mode."""
    if not CONFIG_FILE.exists():
        return {}
    m = (_yaml_load(CONFIG_FILE).get("ml") or {})
    thr = m.get("threshold_disagree")
    if thr is not None:
        try:
            thr = float(thr)
        except (TypeError, ValueError):
            raise SystemExit(f"{CONFIG_FILE.name}: ml.threshold_disagree muss eine "
                             "Zahl sein") from None
        if not 0.0 < thr < 1.0:
            raise SystemExit(f"{CONFIG_FILE.name}: ml.threshold_disagree muss "
                             "zwischen 0 und 1 liegen")
    enabled = m.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        raise SystemExit(f"{CONFIG_FILE.name}: ml.enabled muss true/false sein")
    valid_modes = ("live", "sidecar", "off")
    mode = m.get("mode")
    if mode is not None and str(mode).strip().lower() not in valid_modes:
        raise SystemExit(f"{CONFIG_FILE.name}: ml.mode muss live|sidecar|off sein")
    modes = m.get("modes")
    if modes is not None:
        if not isinstance(modes, dict):
            raise SystemExit(f"{CONFIG_FILE.name}: ml.modes muss ein Mapping "
                             "Strategie->Modus sein")
        for k, v in modes.items():
            if str(v).strip().lower() not in valid_modes:
                raise SystemExit(f"{CONFIG_FILE.name}: ml.modes.{k} muss "
                                 "live|sidecar|off sein")
    algo = m.get("algo")
    if algo is not None and str(algo).strip().lower() not in ("logreg", "gb"):
        raise SystemExit(f"{CONFIG_FILE.name}: ml.algo muss logreg|gb sein")
    out: dict = {}
    if enabled is not None:
        out["enabled"] = enabled
    if mode is not None:
        out["mode"] = str(mode).strip().lower()
    if modes is not None:
        out["modes"] = {k: str(v).strip().lower() for k, v in modes.items()}
    if thr is not None:
        out["threshold_disagree"] = thr
    if m.get("model_path"):
        out["model_path"] = str(m["model_path"])
    if algo is not None:
        out["algo"] = str(algo).strip().lower()
    return out


def load_deployments() -> list[dict]:
    """deployments-Sektion aus agentconfig.yaml (Strategie->Konto->Depot, Multi-Depot).
    Jede Einheit: {strategy, account, journal, depot}. `depot=None` -> orchestrate löst
    COMDIRECT_DEPOT[_<ACCOUNT>] aus agent.env auf. Fehlt die Sektion, wird je aktiver
    Strategie eine Default-Einheit auf 'haupt' abgeleitet (rückwärtskompatibel: die erste
    nutzt decisions.sqlite)."""
    data = _yaml_load(CONFIG_FILE) if CONFIG_FILE.exists() else {}
    raw = data.get("deployments")
    if not raw:
        active = (load_config()["strategies"]["active"]) or ["sma"]
        return [{"strategy": s, "account": "haupt", "depot": None,
                 "journal": "decisions.sqlite" if i == 0 else f"decisions_{s}.sqlite"}
                for i, s in enumerate(active)]
    if not isinstance(raw, list):
        raise SystemExit(f"{CONFIG_FILE.name}: deployments muss eine Liste sein")
    out = []
    for d in raw:
        if not isinstance(d, dict) or not d.get("strategy"):
            raise SystemExit(f"{CONFIG_FILE.name}: jeder deployments-Eintrag braucht 'strategy'")
        acc = d.get("account", "haupt")
        out.append({
            "strategy": d["strategy"], "account": acc, "depot": d.get("depot"),
            "journal": d.get("journal") or ("decisions.sqlite" if acc == "haupt"
                                            else f"decisions_{acc}.sqlite"),
        })
    return out


def load_assignments() -> dict[str, dict]:
    if not ASSIGN_FILE.exists():
        log.warning("%s fehlt -- keine Strategie ist handelbar.", ASSIGN_FILE.name)
        return {}
    data = _yaml_load(ASSIGN_FILE)
    assignments = data.get("assignments") or {}
    if not isinstance(assignments, dict):
        raise SystemExit(f"{ASSIGN_FILE.name}: 'assignments' muss ein Mapping sein")
    for name, a in assignments.items():
        if not isinstance(a, dict) or "underlying" not in a:
            raise SystemExit(f"{ASSIGN_FILE.name}: Eintrag '{name}' unvollständig "
                             f"(underlying fehlt)")
        for isin in [*a.get("LONG", []), *a.get("SHORT", [])]:
            if not isinstance(isin, str) or isin not in INSTRUMENT_CATALOG:
                raise SystemExit(f"{ASSIGN_FILE.name}: '{name}': ISIN {isin!r} "
                                 f"nicht im Katalog")
    return assignments


def _resolve_instruments(assign: dict | None) -> dict:
    """Erstes geeignetes Instrument je Richtung (Präferenzreihenfolge)."""
    out = {"LONG": None, "SHORT": None}
    if not assign:
        return out
    overrides = assign.get("constraints") or {}
    for direction in ("LONG", "SHORT"):
        for isin in assign.get(direction, []):
            ok, _ = eligible(isin, overrides)
            if ok:
                out[direction] = {"isin": isin, **INSTRUMENT_CATALOG[isin]}
                break
    return out


# --------------------------------------------------------------------------
# Plugin-Laden (Discovery + Aktivierung + Auflösung)
# --------------------------------------------------------------------------

def load_plugins() -> dict[str, dict]:
    global _PLUGINS
    if _PLUGINS is not None:
        return _PLUGINS

    config = load_config()
    assignments = load_assignments()
    plugins: dict[str, dict] = {}

    for path in sorted(BASE_DIR.glob("plstrategy_*.py")):
        suffix = path.stem[len("plstrategy_"):]
        try:
            mod = _import_file(path)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Plugin {path.name} nicht ladbar: {exc}") from exc
        cls = getattr(mod, "Strategy", None)
        if cls is None or not issubclass(cls, StrategyBase):
            raise SystemExit(f"{path.name}: erwartet Klasse Strategy(StrategyBase)")
        if cls.name != suffix:
            raise SystemExit(f"{path.name}: Strategy.name='{cls.name}' != "
                             f"Dateisuffix '{suffix}' -- Einheit verletzt")
        assign = assignments.get(suffix)
        if assign is not None and assign.get("underlying") != cls.underlying:
            raise SystemExit(
                f"{ASSIGN_FILE.name}: '{suffix}': underlying "
                f"'{assign.get('underlying')}' != Strategie-Underlying "
                f"'{cls.underlying}' -- Einheit verletzt")
        plugins[suffix] = {
            "cls": cls,
            "assign": assign,
            "instruments": _resolve_instruments(assign),
            "config_params": (config["strategies"]["params"] or {}).get(suffix) or {},
            "files": {"strategy": path.name,
                      "assignment": ASSIGN_FILE.name if assign else None},
        }

    if not plugins:
        raise SystemExit(f"Keine Plugins (plstrategy_*.py) in {BASE_DIR} gefunden.")

    orphans = set(assignments) - set(plugins)
    if orphans:
        raise SystemExit(f"{ASSIGN_FILE.name}: Zuordnungen ohne Plugin: "
                         f"{sorted(orphans)} -- Tippfehler oder Plugin fehlt")

    active = config["strategies"]["active"]
    if active is None:
        active = sorted(plugins)
    unknown = set(active) - set(plugins)
    if unknown:
        raise SystemExit(f"{CONFIG_FILE.name}: aktive Strategien ohne Plugin: "
                         f"{sorted(unknown)}")
    for name, p in plugins.items():
        p["active"] = name in active
        if p["active"]:
            missing = [d for d, inst in p["instruments"].items() if inst is None]
            if p["assign"] is None:
                log.warning("Aktive Strategie '%s' hat keine Produktzuordnung -- "
                            "nicht handelbar (NO_INSTRUMENT).", name)
            elif missing:
                log.warning("Aktive Strategie '%s': kein geeignetes Instrument "
                            "für %s (Katalog/Constraints prüfen).", name, missing)

    _PLUGINS = plugins
    return plugins


def get_plugin(name: str, cli_params: dict | None = None):
    """Instanziiert Strategie mit Parameter-Rang: CLI > agentconfig > Defaults."""
    plugins = load_plugins()
    if name not in plugins:
        raise SystemExit(f"Unbekannte Strategie '{name}'. Verfügbar: "
                         f"{', '.join(sorted(plugins))}")
    p = plugins[name]
    if not p["active"]:
        log.warning("Strategie '%s' ist NICHT aktiv (agentconfig.yaml) -- "
                    "Analyse möglich, Handel blockiert.", name)
    params = {**p["config_params"], **(cli_params or {})}
    return p["cls"](params or None), p


# --------------------------------------------------------------------------
# Datenbedarf (nur aktive Strategien)
# --------------------------------------------------------------------------

def build_data_requirements(include_inactive: bool = False) -> dict:
    plugins = load_plugins()
    symbols: list[str] = []
    bootstrap: dict[str, str] = {}
    live = False
    used = []
    for name, p in sorted(plugins.items()):
        if not include_inactive and not p["active"]:
            continue
        used.append(name)
        req = p["cls"].data_requirements
        for s in req.get("symbols", []):
            if s not in symbols:
                symbols.append(s)
        bootstrap.update(req.get("bootstrap", {}))
        live = live or bool(req.get("live"))
    return {"generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "strategies": used, "symbols": symbols,
            "bootstrap": bootstrap, "live": live}
