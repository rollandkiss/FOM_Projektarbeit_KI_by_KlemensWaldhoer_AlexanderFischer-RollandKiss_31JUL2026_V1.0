#!/usr/bin/env python3
"""
control.py -- Signal-Kill-Switch und Steuerkanal des Handelsagenten (OP2, Echtzeit).

Der Mensch kann jederzeit per Signal in den autonomen Betrieb eingreifen. Der Empfang
läuft im ständig aktiven `signal_dispatcher.py` (systemd-Service), der die Befehle in
Sekunden verarbeitet. Dieses Modul liefert dafür die Kernlogik (`handle_command`,
`dispatch_once`) und für run_cycle den reinen Zustandsblick (`check_pause`).

Da `signal-cli receive` destruktiv auf EINER Queue arbeitet, tritt der Dispatcher
während eines Logins zurück: der Login hält den Signal-RX-Lock (config.SIGNAL_RX_LOCK),
solange er auf die TAN-Freigabe wartet, und der Dispatcher pausiert sein `receive`.

Befehle der verifizierten Nummer (SIGNAL_RECIPIENT) -- Ein-Buchstabe oder Vollwort,
Groß-/Kleinschreibung egal, optional mit KONTONUMMER als Suffix (z. B. L1, S2, P1).
Die Nummer bezeichnet die Position in SIGNAL_ACCOUNTS (agent.env, Default "haupt";
Position = Nummer, NUR ANHÄNGEN, NIE UMSORTIEREN -- sonst träfe z. B. F2 das falsche
Depot). Exaktes Matching -- kollidiert NICHT mit den Login-Tokens 'K'/'TC':
  P / PAUSE      -> Handel stoppen; ohne Nummer ALLE Konten (globales Flag
                   TRADING_PAUSED, der "große rote Knopf"), Pn nur Konto n
                   (TRADING_PAUSED_<konto>). Der nächste Zyklus wird übersprungen,
                   BEVOR Strategie/Auditor laufen -- keine neue Order.
  R / RESUME     -> fortsetzen; ohne Nummer alle Flags (global + je Konto), Rn nur
                   Konto n (bei aktiver globaler Pause mit explizitem Hinweis).
  S / STATUS     -> Zustand (Pause? Modus? Broker-Login?); ohne Nummer Übersicht
                   aller Konten, Sn nur Konto n.
  F / FLATTEN    -> Pause + aktives Glattstellen (OP6, execute-gegatet je Konto via
                   EXECUTION_MODE_<KONTO>); ohne Nummer alle Konten, Fn nur Konto n.
  L / LOGIN      -> Broker-Login prüfen; ohne stehende Session wird der Login als
                   ABGEKOPPELTER Hintergrundprozess angestoßen (photoTAN-Freigabe +
                   'K' laufen dann über den bestehenden RX-Lock-Mechanismus). NIE
                   synchron: ein blockierender Login würde den Kill-Switch
                   minutenlang taub machen. Ln für Konto n; ohne Nummer nur bei
                   genau EINEM konfigurierten Konto ausführend, sonst Übersicht.
                   Logins sind SERIALISIERT (eine Queue -> ein TAN-Fenster): ein
                   zweites L während eines laufenden Logins wird abgewiesen.
  ML / MLn / MLnY / MLnN -> scikit-Prüfinstanz (mlforecast): Status bzw. je
                   Strategie live (Prognose fließt in die LLM-Bewertung ein)
                   oder Sidecar (läuft + journalisiert, ohne Einfluss) schalten.
                   EIGENER Nummernraum: n = Position in ML_STRATEGIES (agent.env,
                   Default 'sma'; nur anhängen, nie umsortieren) -- zählt
                   STRATEGIEN, nicht Konten. Persistiert in ML_MODES.json
                   (Rang: Override > agentconfig ml.modes > ml.mode).
  COMMANDS/HILFE -> Übersicht aller Befehle mit Kurzbeschreibung (ohne Nummer).

'K' (Freigabe bestätigen) und 'TC' (abbrechen) bleiben bewusst UNNUMMERIERT und
kontextbezogen: da immer nur ein Login zugleich im TAN-Fenster sein kann, sind sie
stets eindeutig; verarbeitet werden sie vom Login-Prozess selbst (broker.py).

Kanal-Regel: Befehle wirken NUR im Direkt-Chat mit der verifizierten Nummer. ALLE
Gruppen sind vollständig passiv -- Gruppen-Nachrichten lösen weder Befehle noch
Feedback aus (niemand kann dort versehentlich 'F' auslösen; normaler Gruppen-Chat
erzeugt keinen Antwort-Spam). Nicht erkannte DIREKT-Nachrichten werden beantwortet
('[?] Unbekannter Befehl ...' mit Verweis auf COMMANDS; K/TC außerhalb eines
Login-Fensters mit Kontexthinweis); fremde Absender bleiben still.

Gruppen-Policy: Erlaubt sind die Report-Gruppe (SIGNAL_GROUP) und per 'A'
freigegebene Gruppen (GROUPS_ACCEPTED.json -- zusätzliche Report-Empfänger von
run_cycle send_group). Jede fremde Gruppe (Einladung ODER Direkt-Aufnahme) wird als
offene Entscheidung im Direkt-Chat angezeigt ([MSG], mit 'A'/'D'); ohne Antwort lehnt
der Sweep nach INVITE_TTL_S (60 min) automatisch ab -- fail-safe ist NEIN. Bis zur
Entscheidung erhält die Gruppe weder Reports noch reagiert der Agent dort.

Der Broker-Login-STATUS wird bewusst READ-ONLY und OHNE broker-Import ermittelt
(session_<konto>.json: expires_at/obtained_at; .dead-Marker des Keepalive) -- der
Kill-Switch bleibt damit unabhängig von Broker-/Credstore-Umgebung und API-Latenz.

WICHTIG -- Wirkung auf Orders: Der Kill-Switch verhindert das ANLEGEN neuer Orders und
(bei F) stellt offene Positionen glatt. Er storniert KEINE bereits eingereichten,
offenen Orders (dafür Task #32, EOD-Flatten/Reconciliation).

CLI:
  python3 control.py check-pause [--account <konto>]
      # NUR Flags lesen; Exit 0 = aktiv, 1 = pausiert (global ODER Konto).
      # (Empfang/Verarbeitung läuft im signal_dispatcher.)

Abhängigkeiten: Standardbibliothek + config.py (agent.env).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from config import SIGNAL_RX_LOCK, agent_env  # noqa: F401 -- Lock: Login-läuft-Erkennung

PROJECT_DIR = Path(__file__).resolve().parent
PAUSE_FLAG = PROJECT_DIR / "TRADING_PAUSED"          # global: pausiert ALLE Konten

# Basisbefehl (Kürzel + Vollwort) -> kanonischer Befehl; darf mit Kontonummer-Suffix
# kombiniert werden (p1, status2, l1 ...). Exaktes Matching, damit die Login-Tokens
# 'k' (bestätigen) und 'tc' (abbrechen) NICHT als Kill-Switch-Befehl gelten. Neue
# Aliasse dürfen NIE mit brokers CONFIRM_KEYWORDS (k/ok/ja/j/tan/go/freigabe/...) oder
# ABORT_KEYWORDS (tc/abbruch/stop/cancel) kollidieren.
BASE_ALIASES = {"p": "pause", "pause": "pause",
                "r": "resume", "resume": "resume",
                "s": "status", "status": "status",
                "f": "flatten", "flatten": "flatten",
                "l": "login", "login": "login"}
# Nur ohne Nummer gültig. A/D sind -- wie K/TC -- bewusst kontextbezogen und
# unnummeriert: es ist immer nur EINE Gruppeneinladung zur Entscheidung aktiv (FIFO).
UNNUMBERED_ALIASES = {"commands": "commands", "befehle": "commands",
                      "hilfe": "commands", "help": "commands", "?": "commands",
                      "a": "accept", "accept": "accept", "annehmen": "accept",
                      "d": "deny", "deny": "deny", "ablehnen": "deny"}

# --- Broker-Login-Sicht (read-only, bewusst OHNE broker-Import) ---------------------
# Pfadkonvention identisch zu broker._session_file (dupliziert, damit STATUS/L auch
# funktionieren, wenn der broker-Import scheitern würde -- Kill-Switch bleibt autark).
SESSION_DIR = Path.home() / ".fom-agent"
LOGIN_PID = PROJECT_DIR / "LOGIN_L.pid"      # PID des zuletzt per 'L' gestarteten Logins
LOGIN_LOG = PROJECT_DIR / "login_l.log"      # Ausgabe der per 'L' gestarteten Logins
LOGIN_LOCK_MAX_AGE_S = 600                   # wie signal_dispatcher.LOCK_MAX_AGE_S


def accounts(env: dict) -> list[str]:
    """Nummernstabile Kontoliste aus SIGNAL_ACCOUNTS (Position = Nummer; Default
    'haupt'). Bewusst NICHT aus agentconfig-deployments abgeleitet -- deren
    Reihenfolge ist nicht nummernstabil."""
    raw = (env.get("SIGNAL_ACCOUNTS") or "haupt").strip()
    accs = [a.strip() for a in raw.split(",") if a.strip()]
    return accs or ["haupt"]


def ml_strategies(env: dict) -> list[str]:
    """Nummernstabile Strategieliste für die ML-Befehle (mlN...): ML_STRATEGIES aus
    agent.env (Position = Nummer, Default 'sma'). Wie SIGNAL_ACCOUNTS: NUR
    ANHÄNGEN, NIE UMSORTIEREN -- sonst schaltet ml2n die falsche Strategie.
    Bewusst NICHT aus agentconfig strategies.active abgeleitet (nicht
    nummernstabil, siehe accounts()). ACHTUNG: ML-Nummern zählen STRATEGIEN,
    Konto-Nummern (P1/S2 ...) zählen KONTEN -- getrennte Räume."""
    raw = (env.get("ML_STRATEGIES") or "sma").strip()
    strats = [s.strip() for s in raw.split(",") if s.strip()]
    return strats or ["sma"]


def parse_command(body: str) -> tuple[str, int | None] | None:
    """Nachrichtentext -> (kanonischer Befehl, Nummer|None) oder None (ignorieren).
    'l1' -> ('login', 1); 's' -> ('status', None); 'k'/'tc'/Freitext -> None.
    ML-Schalter (eigene Grammatik mlN[y|n], da Suffix NACH der Nummer):
    'ml' -> ('ml_status', None); 'ml1' -> ('ml_status', 1);
    'ml1y' -> ('ml_on', 1); 'ml1n' -> ('ml_off', 1); 'mly'/'mln' -> ohne Nummer
    (nur bei genau EINER konfigurierten Strategie ausführend). Die Nummer zählt
    STRATEGIEN (ML_STRATEGIES), nicht Konten."""
    b = body.strip().lower()
    if b in UNNUMBERED_ALIASES:
        return UNNUMBERED_ALIASES[b], None
    # ML-Grammatik VOR der Basis-Grammatik: 'ml1y' passt nicht auf ([a-z]+)(\d+)?,
    # 'ml'/'ml1' würden dort zwar matchen, aber als unbekannt enden.
    m = re.fullmatch(r"ml(\d+)?([yn])?", b)
    if m:
        num = int(m.group(1)) if m.group(1) else None
        if m.group(2) == "y":
            return "ml_on", num
        if m.group(2) == "n":
            return "ml_off", num
        return "ml_status", num
    m = re.fullmatch(r"([a-z]+)(\d+)?", b)
    if not m:
        return None
    cmd = BASE_ALIASES.get(m.group(1))
    if cmd is None:
        return None
    return cmd, (int(m.group(2)) if m.group(2) else None)


def _pause_flag(account: str) -> Path:
    """Kontospezifisches Pause-Flag (Pn/Rn); das globale PAUSE_FLAG gilt zusätzlich."""
    return PROJECT_DIR / f"TRADING_PAUSED_{account}"


def _ml_modes_file() -> Path:
    """Signal-Overrides des ML-Modus je Strategie ({strategie: 'live'|'sidecar'}).
    strategy.ml_mode liest dieselbe Datei (Rang: Override > agentconfig)."""
    return PROJECT_DIR / "ML_MODES.json"


def _ml_effective(strat: str) -> tuple[str, str]:
    """Effektiver ML-Modus (mode, quelle) via strategy.ml_mode -- lazy importiert
    und fehlertolerant, damit der Kill-Switch nie an strategy/PyYAML hängt."""
    try:
        import strategy  # lazy: stdlib-Import, yaml erst in ml_mode selbst (lazy)
        return strategy.ml_mode(strat, overrides_file=_ml_modes_file())
    except Exception as exc:  # noqa: BLE001
        return "unbekannt", f"nicht lesbar ({exc})"


def _ml_set(env: dict, num: int | None, mode: str) -> str:
    """mlNy/mlNn: Signal-Override für den ML-Modus einer Strategie setzen.
    y = live (ml_context + Soft-Flag ML_DISAGREE gehen an die LLM-Voter),
    n = sidecar (Prognose läuft und wird weiter journalisiert -- Kalibrierung
    geht nicht verloren --, beeinflusst aber weder Request noch Entscheidung).
    Komplett AUS nur per agentconfig (ml.enabled/ml.mode: off)."""
    strats = ml_strategies(env)
    if num is None:
        if len(strats) != 1:
            hint = " / ".join(f"ml{i + 1}={s}" for i, s in enumerate(strats))
            return f"[i] Mehrere ML-Strategien -- bitte nummerieren ({hint})."
        num = 1
    if not (1 <= num <= len(strats)):
        known = ", ".join(f"{i + 1}={s}" for i, s in enumerate(strats))
        return f"ML-Strategie {num} nicht konfiguriert. Konfiguriert: {known}."
    strat = strats[num - 1]
    overrides = _load_json(_ml_modes_file(), {})
    if not isinstance(overrides, dict):
        overrides = {}
    overrides[strat] = mode
    _save_json(_ml_modes_file(), overrides)
    eff, source = _ml_effective(strat)
    if eff == "off":
        return (f"[!] ML[{num}={strat}] Override '{mode}' gespeichert, aber die "
                f"Prüfinstanz ist per {source} hart AUS -- Override wirkungslos, "
                "bis agentconfig ml.enabled/ml.mode das zulässt.")
    if mode == "live":
        return (f"[BOT] ML[{num}={strat}] -> LIVE: p_direction geht ab dem NÄCHSTEN "
                "Zyklus als ml_context + Soft-Flag ML_DISAGREE an die LLM-Voter "
                "(kann Trades nur schwächen, nie erzeugen). "
                f"'ml{num}n' schaltet zurück auf Sidecar.")
    return (f"[BOT] ML[{num}={strat}] -> SIDECAR: Prognose läuft und wird weiter "
            "journalisiert (Kalibrierung), beeinflusst aber ab dem NÄCHSTEN "
            f"Zyklus weder Request noch LLMs. 'ml{num}y' schaltet live.")


def _ml_status_text(env: dict, num: int | None) -> str:
    """ml / mlN: Modus-Übersicht der ML-Prüfinstanz (Nummern = STRATEGIEN)."""
    strats = ml_strategies(env)
    if num is not None and not (1 <= num <= len(strats)):
        known = ", ".join(f"{i + 1}={s}" for i, s in enumerate(strats))
        return f"ML-Strategie {num} nicht konfiguriert. Konfiguriert: {known}."
    show = [strats[num - 1]] if num is not None else strats
    lines = ["[BOT] ML-Prüfinstanz (scikit-Prognose; Nummern = Strategien, "
             "NICHT Konten):"]
    for s in show:
        i = strats.index(s) + 1
        eff, source = _ml_effective(s)
        desc = {"live": "LIVE -- fließt in die LLM-Bewertung ein",
                "sidecar": "SIDECAR -- läuft + journalisiert, ohne Einfluss",
                "off": "AUS -- keine Inferenz, kein Journal"}.get(eff, eff)
        lines.append(f"[ml{i}={s}] {desc} (Quelle: {source}). "
                     f"Schalten: ml{i}y=live, ml{i}n=sidecar.")
    return "\n".join(lines)


def _mode(env: dict, account: str) -> str:
    """Effektiver Ausführungsmodus des Kontos -- A3-konform: live gilt NUR
    kontospezifisch (EXECUTION_MODE_<KONTO>), nie über ein globales EXECUTION_MODE."""
    return ((env.get(f"EXECUTION_MODE_{account.upper()}") or "").strip().lower()
            or "dry_run")


def _session_file(account: str = "haupt") -> Path:
    """Sitzungsdatei je Konto -- Konvention identisch zu broker._session_file."""
    name = "session.json" if account == "haupt" else f"session_{account}.json"
    return SESSION_DIR / name


def login_status(account: str = "haupt") -> tuple[str, str]:
    """Read-only-Blick auf den Broker-Login (KEIN API-Call, KEIN broker-Import).
    Verlässlich, weil der Keepalive-Timer alle 5 min refresht: expires_at in der
    Zukunft = stehende Session. Priorität: session.json vor .dead-Marker (nach
    einem Neu-Login ist die frische session.json maßgeblich).
    Returns (zustand, meldung) mit zustand in {aktiv, abgelaufen, tot, fehlt, unbekannt}."""
    try:
        sf = _session_file(account)
        if sf.exists():
            d = json.loads(sf.read_text())
            left = float(d.get("expires_at", 0.0)) - time.time()
            renewed = time.time() - float(d.get("obtained_at", 0.0))
            if left > 0:
                return ("aktiv",
                        f"Broker-Login[{account}]: AKTIV (Token noch {int(left)}s, "
                        f"zuletzt erneuert vor {int(renewed)}s; Keepalive verlängert).")
            return ("abgelaufen",
                    f"Broker-Login[{account}]: ABGELAUFEN (seit {int(-left)}s -- "
                    "Keepalive greift nicht). 'L' für neuen Login.")
        if sf.with_suffix(".dead").exists():
            return ("tot",
                    f"Broker-Login[{account}]: VERLOREN (Keepalive-Refresh "
                    "fehlgeschlagen). 'L' für neuen Login.")
        return ("fehlt",
                f"Broker-Login[{account}]: KEINE Session. 'L' für Login.")
    except Exception as exc:  # noqa: BLE001 -- STATUS darf nie sterben
        return "unbekannt", f"Broker-Login[{account}]: Status nicht lesbar ({exc})."


def _login_running() -> bool:
    """True, wenn bereits ein Login läuft: frischer Signal-RX-Lock (Login wartet auf
    die TAN-Freigabe) ODER der zuletzt per 'L' gestartete Prozess lebt noch (Fenster
    zwischen Spawn und Lock-Übernahme). GLOBAL über alle Konten -- die eine
    Signal-Queue erlaubt nur EIN TAN-Fenster zugleich; parallele Logins würden sich
    die TAN-Challenges gegenseitig verbrennen."""
    try:
        if (time.time() - SIGNAL_RX_LOCK.stat().st_mtime) < LOGIN_LOCK_MAX_AGE_S:
            return True
    except OSError:
        pass
    try:
        pid = int(LOGIN_PID.read_text().strip())
        os.kill(pid, 0)          # Signal 0: reine Existenzprüfung
        return True
    except (OSError, ValueError):
        return False


def _spawn_login(env: dict, account: str = "haupt") -> str:  # noqa: ARG001
    """Login als ABGEKOPPELTEN Hintergrundprozess starten (start_new_session) und
    sofort antworten. NIE synchron: broker.login blockiert bis ~4 min auf die
    photoTAN-Freigabe -- synchron wäre der Kill-Switch so lange taub. Der
    Login-Prozess nimmt selbst den SIGNAL_RX_LOCK; der Dispatcher tritt dann
    zurück, sodass 'K'/'TC' beim Login ankommen (bestehender Mechanismus)."""
    try:
        with LOGIN_LOG.open("ab") as lf:
            proc = subprocess.Popen(
                [sys.executable, str(PROJECT_DIR / "broker.py"),
                 "--account", account, "login"],
                stdout=lf, stderr=subprocess.STDOUT,
                cwd=str(PROJECT_DIR), start_new_session=True)
        LOGIN_PID.write_text(str(proc.pid))
        return (f"[AUTH] Kein stehender Broker-Login[{account}] -- Login angestoßen. "
                "Bitte photoTAN in der comdirect-App freigeben und DANACH 'K' "
                "antworten ('TC' bricht ab).")
    except Exception as exc:  # noqa: BLE001
        return (f"FEHLER beim Anstoßen des Logins[{account}] ({exc}). Manuell auf "
                f"der VM: python3 broker.py --account {account} login")


def _flatten(env: dict, account: str = "haupt") -> str:
    """Schließt offene Positionen des Kontos über den Broker (OP6). execute nur bei
    EXECUTION_MODE_<KONTO>=live (A3-konform, nie über ein globales EXECUTION_MODE) --
    sonst Dry-Run (protokolliert, verkauft nichts). Depot: COMDIRECT_DEPOT für
    'haupt' (rückwärtskompatibel), sonst COMDIRECT_DEPOT_<KONTO>. broker wird lazy
    importiert, damit control.py ohne Broker-/Credstore-Umgebung testbar bleibt.
    Fehler brechen den Kill-Switch NICHT ab -- die Pause ist bereits gesetzt."""
    try:
        import broker  # lazy: kein Import beim reinen Kill-Switch-Check/Test
        if account == "haupt":
            depot = env.get("COMDIRECT_DEPOT", "")
        else:
            depot = env.get(f"COMDIRECT_DEPOT_{account.upper()}", "")
            if not depot:
                return (f"FEHLER: kein Depot für Konto '{account}' konfiguriert "
                        f"(COMDIRECT_DEPOT_{account.upper()} in agent.env).")
        live = _mode(env, account) == "live"
        retain = float(env.get("EXECUTION_MIN_RETAIN", "1"))
        r = broker.flatten_positions(depot, execute=live, retain=retain,
                                     account=account)
        mode = "VERKAUFT" if live else "DRY-RUN (nichts verkauft)"
        msg = f"{len(r['sold'])}/{r['count']} Position(en) {mode} (Seed {retain} bleibt)."
        if r["errors"]:
            msg += f" {len(r['errors'])} Fehler -- bitte im Depot prüfen."
        return msg
    except Exception as exc:  # noqa: BLE001
        return f"FEHLER ({exc}). Positionen ggf. offen -- bitte manuell im Depot prüfen."


def _help_text(accs: list[str]) -> str:
    konten = ", ".join(f"{i + 1}={a}" for i, a in enumerate(accs))
    return (
        "[HILFE] Befehle (Groß-/Kleinschreibung egal; optional mit Kontonummer, "
        "z. B. L1, S2):\n"
        "P / PAUSE -- Handel stoppen; ohne Nummer ALLE Konten, Pn nur Konto n\n"
        "R / RESUME -- Handel fortsetzen; ohne Nummer alle, Rn nur Konto n\n"
        "S / STATUS -- Zustand (Pause, Modus, Broker-Login); Sn nur Konto n\n"
        "F / FLATTEN -- Pause + offene Positionen glattstellen; Fn nur Konto n\n"
        "L / LOGIN -- Broker-Login prüfen, ohne stehende Session anstoßen "
        "(photoTAN); Ln für Konto n\n"
        "A / D -- offene Gruppeneinladung annehmen (Gruppe wird Report-Empfänger) "
        "oder ablehnen; ohne Antwort automatische Ablehnung nach 60 min\n"
        "ML -- Status der scikit-Prüfinstanz; MLnY = live schalten (fließt in "
        "die LLM-Bewertung ein), MLnN = Sidecar (läuft + journalisiert, ohne "
        "Einfluss). ACHTUNG: n zählt hier STRATEGIEN (ml1=sma, ...), nicht "
        "Konten!\n"
        "COMMANDS / HILFE -- diese Übersicht\n"
        f"Konten: {konten} (Zuordnung via SIGNAL_ACCOUNTS, nummernstabil)\n"
        "Nur während eines laufenden Logins: K = photoTAN-Freigabe bestätigen "
        "(ohne Nummer -- gilt für den gerade laufenden Login), TC = Login "
        "abbrechen. In dieser Phase (max. ~4 min) werden andere Befehle nicht "
        "verarbeitet."
    )


def handle_command(cmd: str, env: dict, num: int | None = None) -> str:
    """Führt einen kanonischen Befehl (optional nummeriert) aus und liefert die
    Antwort für den Nutzer. Setzt/entfernt Pause-Flags; FLATTEN stößt zusätzlich das
    Glattstellen an; LOGIN prüft/startet den Broker-Login.

    ACHTUNG Nummernräume: mlN... zählt STRATEGIEN (ML_STRATEGIES) und wird deshalb
    VOR der Konto-Validierung behandelt -- sonst würde z. B. 'ml2y' bei nur einem
    Konto fälschlich mit 'Konto 2 nicht konfiguriert' abgewiesen."""
    if cmd == "ml_status":
        return _ml_status_text(env, num)
    if cmd in ("ml_on", "ml_off"):
        return _ml_set(env, num, "live" if cmd == "ml_on" else "sidecar")

    accs = accounts(env)
    acc: str | None = None
    if num is not None:
        if not (1 <= num <= len(accs)):
            konten = ", ".join(f"{i + 1}={a}" for i, a in enumerate(accs))
            return f"Konto {num} nicht konfiguriert. Konfiguriert: {konten}."
        acc = accs[num - 1]

    if cmd == "pause":
        if acc is None:
            PAUSE_FLAG.touch()
            return ("[PAUSE] Trading PAUSIERT (ALLE Konten) -- keine neuen Orders. "
                    "'R' zum Fortsetzen.")
        _pause_flag(acc).touch()
        return (f"[PAUSE] Konto {num} ({acc}) PAUSIERT -- keine neuen Orders. "
                f"'R{num}' zum Fortsetzen.")

    if cmd == "resume":
        if acc is None:
            PAUSE_FLAG.unlink(missing_ok=True)
            for a in accs:
                _pause_flag(a).unlink(missing_ok=True)
            return "[>] Trading FORTGESETZT (alle Konten)."
        _pause_flag(acc).unlink(missing_ok=True)
        if PAUSE_FLAG.exists():
            return (f"[>] Konto {num} ({acc}) fortgesetzt -- GLOBAL weiterhin "
                    "PAUSIERT ('R' für alle).")
        return f"[>] Konto {num} ({acc}) FORTGESETZT."

    if cmd == "flatten":
        if acc is None:
            PAUSE_FLAG.touch()
            parts = [f"[{a}] {_flatten(env, a)}" for a in accs]
            return ("[STOP] PAUSIERT (alle Konten) -- keine neuen Orders. Flatten: "
                    + " | ".join(parts))
        _pause_flag(acc).touch()
        return (f"[STOP] Konto {num} ({acc}) PAUSIERT -- keine neuen Orders. "
                f"Flatten: {_flatten(env, acc)}")

    if cmd == "status":
        show = [acc] if acc is not None else accs
        glob = "PAUSIERT (global)" if PAUSE_FLAG.exists() else "aktiv"
        konten = ", ".join(f"{i + 1}={a}" for i, a in enumerate(accs))
        lines = [f"[i] Trading-Status: {glob}. Konten: {konten}."]
        for a in show:
            n = accs.index(a) + 1
            st = "PAUSIERT" if check_pause(a) else "aktiv"
            lines.append(f"[{n}={a}] {st}, Modus={_mode(env, a)} (live nur "
                         f"kontospezifisch). {login_status(a)[1]}")
        ml_strats = ml_strategies(env)
        ml_parts = []
        for i, s in enumerate(ml_strats):
            eff, _src = _ml_effective(s)
            ml_parts.append(f"ml{i + 1}={s}:{eff}")
        lines.append(f"[BOT] ML-Prüfinstanz: {', '.join(ml_parts)} "
                     "('ml' für Details; mlNy/mlNn schaltet live/sidecar).")
        pend = pending_invites()
        if pend:
            p0 = pend[0]
            more = f" (+{len(pend) - 1} weitere)" if len(pend) > 1 else ""
            lines.append(f"[MSG] Offene Gruppeneinladung: "
                         f"{_gid_short(p0['id'], p0.get('name'))}{more} -- 'A' "
                         f"annehmen / 'D' ablehnen (Auto-Ablehnung nach "
                         f"{INVITE_TTL_S // 60} min).")
        extra = accepted_groups()
        if extra:
            names = ", ".join(_gid_short(g["id"], g.get("name")) for g in extra)
            lines.append(f"[ALARM] Zusätzliche Report-Gruppen: {names}.")
        return "\n".join(lines)

    if cmd == "login":
        # Reihenfolge: läuft-bereits VOR Status -- während eines laufenden Logins ist
        # die alte Session naturgemäß noch nicht aktiv, ein zweiter Spawn wäre falsch.
        # Global über alle Konten (eine Queue -> ein TAN-Fenster).
        if _login_running():
            return ("[...] Ein Login läuft bereits -- bitte photoTAN in der "
                    "comdirect-App freigeben und 'K' antworten ('TC' bricht ab).")
        if acc is None:
            if len(accs) == 1:
                acc = accs[0]           # eindeutig -> wie L1
            else:
                lines = [login_status(a)[1] for a in accs]
                hint = " / ".join(f"L{i + 1}={a}" for i, a in enumerate(accs))
                return ("[i] Mehrere Konten -- Login bitte nummeriert anstoßen "
                        f"({hint}).\n" + "\n".join(lines))
        state, txt = login_status(acc)
        if state == "aktiv":
            return f"[AUTH] {txt} Kein neuer Login nötig."
        return _spawn_login(env, acc)

    if cmd in ("accept", "deny"):
        # Kontextbezogen wie K/TC: wirkt auf die ÄLTESTE offene Einladung (FIFO).
        pend = pending_invites()
        if not pend:
            return ("[i] Keine offene Gruppeneinladung. ('A'/'D' sind nur aktiv, wenn "
                    "eine [MSG]-Anfrage vorliegt.)")
        inv = pend[0]
        bot = env.get("SIGNAL_BOT", "")
        label = _gid_short(inv["id"], inv.get("name"))
        if cmd == "deny":
            ok = _quit_group(bot, inv["id"])
            _HANDLED_GROUPS.add(inv["id"])
            _save_json(GROUP_INVITES, pend[1:])
            msg = (f"[SCHUTZ] Gruppe {label} abgelehnt/verlassen." if ok else
                   f"[SCHUTZ] Ablehnen von {label} FEHLGESCHLAGEN -- bitte manuell prüfen "
                   f"(quitGroup).")
        else:
            if _accept_group(bot, inv["id"]):
                acc_list = accepted_groups()
                acc_list.append({"id": inv["id"], "name": inv.get("name", "")})
                _save_json(GROUPS_ACCEPTED, acc_list)
                _save_json(GROUP_INVITES, pend[1:])
                msg = (f"[OK] Gruppe {label} angenommen -- erhält ab jetzt die "
                       f"Gruppen-Reports. Befehle bleiben dort deaktiviert.")
            else:
                return (f"[X] Annehmen von {label} FEHLGESCHLAGEN (Einladung "
                        f"abgelaufen/zurückgezogen oder signal-cli-Fehler). Anfrage "
                        f"bleibt offen -- 'D' zum Ablehnen.")
        rest = pending_invites()
        if rest:
            nxt = rest[0]
            msg += (f"\n[MSG] Nächste offene Einladung: "
                    f"{_gid_short(nxt['id'], nxt.get('name'))} -- 'A'/'D'.")
        return msg

    if cmd == "commands":
        return _help_text(accs)

    return f"unbekannter Befehl: {cmd}"


def _receive(bot: str) -> tuple[list[tuple[str, str, bool]], set[str]]:
    """Einmalig Signal-Nachrichten holen -> (Nachrichten, gesehene Gruppen-IDs).
    Nachrichten: Liste (absender, text_lower, ist_gruppe). ist_gruppe unterscheidet
    Direktnachrichten von Gruppen-Chat -- Befehle wirken NUR direkt. Die Gruppen-IDs
    werden aus ALLEN Envelopes gesammelt (auch solchen ohne Text, z. B.
    Gruppen-Updates/Einladungen) und speisen die Gruppen-Policy."""
    out = subprocess.run(
        ["signal-cli", "-a", bot, "-o", "json", "receive", "--timeout", "5"],
        capture_output=True, text=True, timeout=30).stdout
    msgs: list[tuple[str, str, bool]] = []
    groups: set[str] = set()
    for line in out.splitlines():
        try:
            env = json.loads(line).get("envelope", {})
        except json.JSONDecodeError:
            continue
        src = env.get("source") or env.get("sourceNumber")
        data = env.get("dataMessage") or {}
        gid = ((data.get("groupInfo") or {}).get("groupId") or "").strip()
        if gid:
            groups.add(gid)
        body = (data.get("message") or "").strip().lower()
        if src and body:
            msgs.append((src, body, data.get("groupInfo") is not None))
    return msgs, groups


def _unknown_feedback(body: str) -> str:
    """Antwort auf eine nicht erkannte Direktnachricht. K/TC außerhalb eines
    Login-Fensters bekommen einen kontextbezogenen Hinweis statt 'unbekannt'."""
    b = body.strip().lower()
    if b in ("k", "tc"):
        return (f"[i] '{b.upper()}' ist nur während eines laufenden Logins gültig -- "
                "aktuell wartet keiner auf Freigabe. Login anstoßen: 'L'.")
    short = body.strip()
    if len(short) > 40:
        short = short[:40] + "..."
    return f"[?] Unbekannter Befehl: '{short}'. 'COMMANDS' zeigt alle Befehle."


def _send(bot: str, recipient: str, msg: str) -> None:
    subprocess.run(["signal-cli", "-a", bot, "send", "-m", msg, recipient],
                   timeout=30, check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# --- Gruppen-Policy: nur Report-Gruppe + explizit freigegebene Gruppen --------------
# Fremde Gruppen (Einladung ODER Direkt-Aufnahme) werden NICHT mehr sofort verlassen,
# sondern als offene Entscheidung im Direkt-Chat angezeigt: 'A' = annehmen (Gruppe
# wird Report-Empfänger, persistiert in GROUPS_ACCEPTED.json), 'D' = ablehnen.
# Ohne Entscheidung räumt der Sweep nach INVITE_TTL_S auf (Auto-Ablehnung, fail-safe).
# Bis zur Entscheidung: KEINE Reports an die Gruppe, KEINE Befehle (wie überall).
# Zwei Erkennungslinien: (1) Gruppen-IDs aus dem laufenden Empfang (dispatch_once),
# (2) periodischer Sweep über listGroups im Dispatcher (fängt stille Einladungen).

GROUP_INVITES = PROJECT_DIR / "GROUP_INVITES.json"    # offene A/D-Entscheidungen (FIFO)
GROUPS_ACCEPTED = PROJECT_DIR / "GROUPS_ACCEPTED.json"  # per 'A' freigegebene Gruppen
INVITE_TTL_S = 3600                 # 60 min bis Auto-Ablehnung durch den Sweep

_HANDLED_GROUPS: set[str] = set()   # abgelehnte/verlassene IDs (Prozess-Lebensdauer,
#                                     verhindert Re-Registrierung + Alarm-Spam)


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return default


def _save_json(path: Path, data) -> None:
    try:
        path.write_text(json.dumps(data, ensure_ascii=False))
    except OSError:
        pass


def pending_invites() -> list[dict]:
    """Offene Gruppen-Entscheidungen, FIFO ([{'id','name','ts'}, ...])."""
    return _load_json(GROUP_INVITES, [])


def accepted_groups() -> list[dict]:
    """Per 'A' freigegebene Gruppen ([{'id','name'}, ...]) -- zusätzliche
    Report-Empfänger (run_cycle send_group liest diese Datei)."""
    return _load_json(GROUPS_ACCEPTED, [])


def _allowed_groups(env: dict) -> set[str]:
    """Erlaubte Gruppen: Report-Gruppe (SIGNAL_GROUP) + per 'A' freigegebene."""
    g = (env.get("SIGNAL_GROUP") or "").strip()
    allowed = {g} if g else set()
    allowed |= {a["id"] for a in accepted_groups() if a.get("id")}
    return allowed


def _gid_short(gid: str, name: str | None = None) -> str:
    if name:
        return f""{name}""
    return gid if len(gid) <= 24 else gid[:24] + "..."


def _try_quit(bot: str, gid: str) -> bool:
    """Roher Austritt/Einladungs-Ablehnung: erst mit --delete (entfernt auch die
    lokalen Gruppendaten -> kein erneutes Auftauchen im Sweep), bei älterem
    signal-cli ohne --delete als Fallback."""
    for args in (["quitGroup", "-g", gid, "--delete"], ["quitGroup", "-g", gid]):
        try:
            r = subprocess.run(["signal-cli", "-a", bot, *args],
                               capture_output=True, text=True, timeout=60)
            if r.returncode == 0:
                return True
        except Exception:  # noqa: BLE001
            pass
    return False


def _quit_group(bot: str, gid: str) -> bool:
    """Gruppe verlassen bzw. Einladung ablehnen (best-effort) -- mit Selbstheilung
    der Last-Admin-Sperre (live gefunden 22.07., Fall "TradingFOM"): Ist die
    Bot-Nummer einziger Admin einer Gruppe mit weiteren Mitgliedern (nur bei
    selbst angelegten Gruppen möglich), verweigert Signal den Austritt. Dann
    werden erst alle ANDEREN Mitglieder entfernt (= vollständige Auflösung der
    Gruppe, Muster aus cleanup_groups.py) und der Austritt wiederholt. Ohne
    Admin-Rechte scheitert das Entfernen folgenlos (rc!=0) -> False wie bisher."""
    if _try_quit(bot, gid):
        return True
    try:
        for g in _list_groups(bot):
            if (g.get("id") or "").strip() != gid:
                continue
            others = []
            for m in g.get("members") or []:
                ident = (m.get("number") or m.get("uuid")) if isinstance(m, dict) else m
                if ident and ident != bot:
                    others.append(ident)
            if not others:
                break               # keine anderen Mitglieder -> Ursache ist anders
            r = subprocess.run(
                ["signal-cli", "-a", bot, "updateGroup", "-g", gid, "-r", *others],
                capture_output=True, text=True, timeout=120)
            if r.returncode == 0:
                return _try_quit(bot, gid)
            break                   # kein Admin -> nicht weiter versuchen
    except Exception:  # noqa: BLE001
        pass
    return False


def _accept_group(bot: str, gid: str) -> bool:
    """Gruppeneinladung annehmen: updateGroup akzeptiert eine offene v2-Einladung.
    Bei Direkt-Aufnahme (bereits Mitglied) ist nichts zu senden -- Annahme = reine
    Freigabe; dann genügt der Mitgliedschafts-Check über listGroups."""
    try:
        r = subprocess.run(["signal-cli", "-a", bot, "updateGroup", "-g", gid],
                           capture_output=True, text=True, timeout=60)
        if r.returncode == 0:
            return True
    except Exception:  # noqa: BLE001
        pass
    return any((g.get("id") or "").strip() == gid and g.get("isMember")
               for g in _list_groups(bot))


def _register_invite(env: dict, gid: str, name: str | None = None,
                     *, send: bool = True) -> str | None:
    """Fremde Gruppe als offene A/D-Entscheidung registrieren (einmalig) und die
    verifizierte Nummer informieren. Returns die Meldung oder None (schon bekannt)."""
    bot, recipient = env.get("SIGNAL_BOT"), env.get("SIGNAL_RECIPIENT")
    gid = (gid or "").strip()
    if not gid or gid in _allowed_groups(env) or gid in _HANDLED_GROUPS:
        return None
    pend = pending_invites()
    for p in pend:
        if p["id"] == gid:
            if name and not p.get("name"):       # Name nachtragen (Sweep kennt ihn)
                p["name"] = name
                _save_json(GROUP_INVITES, pend)
            return None
    pend.append({"id": gid, "name": name or "", "ts": time.time()})
    _save_json(GROUP_INVITES, pend)
    pos = f" (Warteschlange: {len(pend)}.)" if len(pend) > 1 else ""
    note = (f"[MSG] Gruppeneinladung/-aufnahme erkannt: {_gid_short(gid, name)}. "
            f"'A' = annehmen (Gruppe erhält dann die Handelsreports -- Mitglieder "
            f"können sich später ändern!), 'D' = ablehnen. Ohne Antwort automatische "
            f"Ablehnung in {INVITE_TTL_S // 60} min. Befehle bleiben in Gruppen "
            f"immer deaktiviert.{pos}")
    if send and bot and recipient:
        _send(bot, recipient, note)
    return note


def enforce_group_policy(env: dict, group_ids, *, send: bool = True) -> list[str]:
    """Empfangs-Linie der Gruppen-Policy: jede fremde Gruppen-ID aus dem laufenden
    Empfang wird als offene A/D-Entscheidung registriert (kein sofortiges Verlassen
    mehr -- der Mensch entscheidet; der Sweep räumt nach Timeout auf)."""
    notes: list[str] = []
    for gid in group_ids:
        note = _register_invite(env, gid, send=send)
        if note:
            notes.append(note)
    return notes


def _list_groups(bot: str) -> list[dict]:
    """Alle Gruppen der Bot-Nummer (signal-cli listGroups, JSON)."""
    out = subprocess.run(["signal-cli", "-a", bot, "-o", "json", "listGroups"],
                         capture_output=True, text=True, timeout=60).stdout
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def group_sweep(env: dict, *, send: bool = True) -> list[str]:
    """Periodische Gruppen-Hygiene (Dispatcher: beim Start + alle 10 min).
    1) Offene A/D-Entscheidungen älter als INVITE_TTL_S -> Auto-Ablehnung (quitGroup)
       + Meldung. 2) Neue fremde Gruppen/Einladungen aus listGroups -> als offene
    Entscheidung registrieren (mit Gruppenname). Bereits verlassene Altlasten
    (weder Mitglied noch eingeladen) werden übersprungen."""
    bot, recipient = env.get("SIGNAL_BOT"), env.get("SIGNAL_RECIPIENT")
    if not bot:
        return []
    notes: list[str] = []
    # 1) Timeout-Ablehnung offener Entscheidungen (fail-safe: Default ist NEIN).
    pend = pending_invites()
    keep = []
    for p in pend:
        if time.time() - float(p.get("ts", 0)) >= INVITE_TTL_S:
            ok = _quit_group(bot, p["id"])
            _HANDLED_GROUPS.add(p["id"])
            note = (f"[SCHUTZ] Gruppeneinladung {_gid_short(p['id'], p.get('name'))} nach "
                    f"{INVITE_TTL_S // 60} min ohne Antwort automatisch abgelehnt."
                    if ok else
                    f"[SCHUTZ] Auto-Ablehnung von {_gid_short(p['id'], p.get('name'))} "
                    f"FEHLGESCHLAGEN -- bitte manuell prüfen.")
            notes.append(note)
            if send and recipient:
                _send(bot, recipient, note)
        else:
            keep.append(p)
    if len(keep) != len(pend):
        _save_json(GROUP_INVITES, keep)
    # 2) Neue fremde Gruppen/Einladungen registrieren (Name aus listGroups).
    for g in _list_groups(bot):
        gid = (g.get("id") or "").strip()
        if not gid:
            continue
        if g.get("isMember") is False and not g.get("pendingMembers"):
            continue        # weder Mitglied noch Einladung offen -> nichts zu tun
        note = _register_invite(env, gid, g.get("name") or None, send=send)
        if note:
            notes.append(note)
    return notes


def dispatch_once(env: dict, *, send: bool = True) -> list[str]:
    """Einmal Signal lesen und die Befehle der verifizierten Nummer ausführen
    (Echtzeit-Kern; der signal_dispatcher ruft dies in einer Schleife auf). Nur
    P/R/S/F/L/COMMANDS (optional nummeriert) der SIGNAL_RECIPIENT-Nummer werden
    ausgeführt -- und zwar NUR aus dem Direkt-Chat: Gruppen-Nachrichten (Report-
    Gruppe) lösen weder Befehle noch Feedback aus (dort kann niemand versehentlich
    'F' auslösen, und normaler Chat erzeugt keinen Antwort-Spam). Nicht erkannte
    DIREKT-Nachrichten der Nummer erhalten Feedback ('[?] Unbekannter Befehl ...';
    K/TC außerhalb eines Logins einen Kontexthinweis); fremde Absender bleiben
    still. Fehler beim Empfang brechen NICHT ab.
    Returns: die gesendeten Antworttexte (für Tests)."""
    bot, recipient = env.get("SIGNAL_BOT"), env.get("SIGNAL_RECIPIENT")
    replies: list[str] = []
    if not (bot and recipient):
        return replies
    try:
        res = _receive(bot)
    except Exception as exc:  # noqa: BLE001 -- Empfang best-effort
        print(f"control: Signal-Empfang fehlgeschlagen: {exc}", file=sys.stderr)
        return replies
    if isinstance(res, tuple):
        msgs, groups = res
    else:                       # ältere/gestubbte _receive-Form: nur Nachrichtenliste
        msgs, groups = res, set()
    # Gruppen-Policy ZUERST (Sicherheit vor Komfort): fremde Gruppen im Empfang ->
    # sofort verlassen/ablehnen, [SCHUTZ]-Alarm an die Direktnummer.
    replies += enforce_group_policy(env, groups, send=send)
    for msg in msgs:
        src, body = msg[0], msg[1]
        is_group = bool(msg[2]) if len(msg) > 2 else False
        if src != recipient or is_group:
            continue            # Gruppe: KEINE Ausführung, KEIN Feedback (nur Direkt-Chat)
        parsed = parse_command(body)
        if parsed is None:
            reply = _unknown_feedback(body)
        else:
            cmd, num = parsed
            reply = handle_command(cmd, env, num)
        replies.append(reply)
        if send:
            _send(bot, recipient, reply)
    return replies


def check_pause(account: str | None = None) -> bool:
    """Nur-Lesen: True, wenn das globale Pause-Flag (TRADING_PAUSED) gesetzt ist ODER
    -- bei gegebenem Konto -- dessen Konto-Flag (TRADING_PAUSED_<konto>). Das Empfangen
    und Ausführen der Signal-Befehle übernimmt der ständig laufende signal_dispatcher
    (Echtzeit); run_cycle liest hier lediglich den Zustand -- KEIN `signal-cli receive`
    mehr, damit es keinen Queue-Konflikt mit dem Login gibt."""
    if PAUSE_FLAG.exists():
        return True
    return bool(account) and _pause_flag(account).exists()


def main(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]
    if args and args[0] == "check-pause":
        acc = None
        if len(args) >= 3 and args[1] == "--account":
            acc = args[2]
        sys.exit(1 if check_pause(acc) else 0)
    print("Nutzung: control.py check-pause [--account <konto>] "
          "(Empfang läuft im signal_dispatcher)", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
