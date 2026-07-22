#!/usr/bin/env python3
"""
config.py -- gemeinsames Laden der agent.env-Konfiguration.

Zentraler Ort für das Parsen von agent.env (SIGNAL_*, COMDIRECT_*), damit die
Logik nicht in mehreren Modulen dupliziert wird. Genutzt von broker.py (Signal-
Kanal der TAN-Freigabe) und orchestrate.py (COMDIRECT_DEPOT/COMDIRECT_REF_PRICE).

agent.env liegt im Projektordner neben den Skripten; bestehende Prozess-
Umgebungsvariablen haben Vorrang (setdefault überschreibt sie nicht).
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

# Signal-Empfangs-Lock: Der Login (broker.py) hält diese Datei, während er die
# comdirect-TAN-Freigabe per Signal abwartet. Der ständig laufende signal_dispatcher
# pausiert sein destruktives `signal-cli receive`, solange der Lock existiert -- so
# konkurrieren Login (K/TC) und Kill-Switch (P/R/S/F) NICHT um die gemeinsame Queue.
SIGNAL_RX_LOCK = PROJECT_DIR / "SIGNAL_RX.lock"


@contextlib.contextmanager
def signal_rx_lock():
    """Hält den Signal-Empfangs-Lock (best-effort -- ein Dateifehler darf den Login
    nicht verhindern)."""
    try:
        SIGNAL_RX_LOCK.touch()
    except OSError:
        pass
    try:
        yield
    finally:
        try:
            SIGNAL_RX_LOCK.unlink(missing_ok=True)
        except OSError:
            pass


def agent_env() -> dict:
    """Prozess-Umgebung, ergänzt um KEY=VALUE-Zeilen aus agent.env (falls vorhanden).
    Kommentare (#) und Zeilen ohne '=' werden übersprungen; umschließende Quotes
    an den Werten werden entfernt. Bereits gesetzte Variablen bleiben unangetastet."""
    env = dict(os.environ)
    envfile = PROJECT_DIR / "agent.env"
    if envfile.exists():
        for line in envfile.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env
