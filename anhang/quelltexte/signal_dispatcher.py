#!/usr/bin/env python3
"""
signal_dispatcher.py -- ständig laufender Signal-Empfänger für den Echtzeit-Kill-Switch (OP2).

Betreibt als EINZIGER Empfänger die destruktive `signal-cli receive`-Queue und führt
die Befehle der verifizierten Nummer (P/R/S/F/L/COMMANDS) in Sekunden aus -- unabhängig
vom täglichen Handelszyklus. Während eines Logins (morgens per Cron ODER per 'L'
angestoßen) tritt er zurück: hält broker.login den Signal-RX-Lock
(config.SIGNAL_RX_LOCK), pausiert dieser Dispatcher sein `receive`, damit die
TAN-Freigabe (K/TC) beim Login ankommt und nicht weggefangen wird.

Betrieb als systemd-Service (Restart=always) auf der agent-vm -- siehe
signal_dispatcher.service.

CLI:
  python3 signal_dispatcher.py            # Endlosschleife (Service-Modus)
  python3 signal_dispatcher.py --once     # genau eine Iteration (Test/Debug)

Abhängigkeiten: Standardbibliothek + control.py + config.py.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time

import control
from config import SIGNAL_RX_LOCK, agent_env

POLL_INTERVAL_S = 2      # Pause zwischen zwei Empfangsrunden
LOCK_WAIT_S = 1          # Schlafzeit, solange der Login den RX-Lock hält
LOCK_MAX_AGE_S = 600     # Login-Lock gilt nur frisch (<10 min) -- Schutz vor hängendem
#                          Lock, falls der Login-Prozess ohne finally stirbt (SIGKILL).
GROUP_SWEEP_INTERVAL_S = 600  # Gruppen-Hygiene (control.group_sweep): beim Start +
#                               alle 10 min -- verlässt fremde Gruppen, lehnt stille
#                               Einladungen ab (nur SIGNAL_GROUP ist erlaubt).

_running = True


def _stop(signum, frame):  # noqa: ARG001 -- SIGTERM/SIGINT sauber beenden
    global _running
    _running = False


def _login_active() -> bool:
    """True, wenn der Signal-RX-Lock existiert UND frisch ist. Ein veralteter Lock
    (Login gecrasht) wird ignoriert, damit der Kill-Switch nicht dauerhaft blockiert."""
    try:
        return (time.time() - SIGNAL_RX_LOCK.stat().st_mtime) < LOCK_MAX_AGE_S
    except OSError:      # nicht vorhanden / nicht lesbar -> kein aktiver Login
        return False


def run(once: bool = False) -> None:
    """Empfangsschleife: liest Signal, führt Kill-Switch-Befehle aus. Tritt zurück,
    solange der Login-Lock steht. Stirbt an keiner einzelnen Nachricht (per-Iteration
    gefangen)."""
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    print("signal_dispatcher: gestartet (Echtzeit-Kill-Switch aktiv).", flush=True)
    last_sweep = 0.0        # 0 -> erster Sweep sofort beim Start (räumt Altlasten auf)
    while _running:
        # Login-Fenster: Queue dem Login überlassen (kein konkurrierendes receive).
        if _login_active():
            time.sleep(LOCK_WAIT_S)
            if once:
                break
            continue
        if time.time() - last_sweep >= GROUP_SWEEP_INTERVAL_S:
            try:
                for note in control.group_sweep(agent_env()):
                    print(f"signal_dispatcher: Gruppen-Policy -> {note}", flush=True)
            except Exception as exc:  # noqa: BLE001 -- Hygiene darf nie killen
                print(f"signal_dispatcher: Gruppen-Sweep-Fehler (ignoriert): {exc}",
                      file=sys.stderr, flush=True)
            last_sweep = time.time()
        try:
            for reply in control.dispatch_once(agent_env()):
                print(f"signal_dispatcher: ausgeführt -> {reply}", flush=True)
        except Exception as exc:  # noqa: BLE001 -- nie an einer Nachricht sterben
            print(f"signal_dispatcher: Fehler (ignoriert): {exc}", file=sys.stderr,
                  flush=True)
        if once:
            break
        time.sleep(POLL_INTERVAL_S)
    print("signal_dispatcher: beendet.", flush=True)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Echtzeit-Signal-Kill-Switch-Dispatcher")
    p.add_argument("--once", action="store_true", help="nur eine Iteration (Debug/Test)")
    args = p.parse_args(argv)
    run(once=args.once)


if __name__ == "__main__":
    main()
