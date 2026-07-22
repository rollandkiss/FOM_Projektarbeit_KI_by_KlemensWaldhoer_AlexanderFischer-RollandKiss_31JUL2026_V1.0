#!/usr/bin/env python3
"""
market_calendar.py -- deutscher Börsenkalender (Xetra/Frankfurt), strategieunabhängig.

Allgemeine Utility (kein Strategie-Bezug): liefert Handelstage der deutschen
Börse und daraus abgeleitet Wartungsfenster. Genutzt von:
  * Order-/Audit-Schicht  -> an Börsenfeiertagen kein Handel/keine Validierung
  * Collector-Wartung     -> maintain/vacuum bevorzugt an handelsfreien Tagen
  * Lückenklassifikation  -> Feiertage nicht als 'silence' fehldeuten

Xetra-Feiertage (ganztägig geschlossen): Neujahr, Karfreitag, Ostermontag,
Tag der Arbeit (1. Mai), Heiligabend, 1./2. Weihnachtstag, Silvester.
(Reguläre Handelszeit Xetra: 09:00-17:30 MEZ/MESZ.)

Keine externen Abhängigkeiten (Ostern via Gauß-Algorithmus).

CLI:
  python3 market_calendar.py today
  python3 market_calendar.py check 2026-12-25
  python3 market_calendar.py next            # nächster Handelstag
  python3 market_calendar.py holidays 2026
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

XETRA_OPEN = "09:00"
XETRA_CLOSE = "17:30"   # Ortszeit Europe/Berlin


def _easter(year: int) -> date:
    """Ostersonntag (Gauß/Anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month, day = divmod(h + m - 7 * n + 114, 31)
    return date(year, month, day + 1)


def holidays(year: int) -> dict[date, str]:
    """Xetra-Feiertage eines Jahres (ganztägig geschlossen)."""
    easter = _easter(year)
    return {
        date(year, 1, 1): "Neujahr",
        easter - timedelta(days=2): "Karfreitag",
        easter + timedelta(days=1): "Ostermontag",
        date(year, 5, 1): "Tag der Arbeit",
        date(year, 12, 24): "Heiligabend",
        date(year, 12, 25): "1. Weihnachtstag",
        date(year, 12, 26): "2. Weihnachtstag",
        date(year, 12, 31): "Silvester",
    }


def is_trading_day(d: date) -> bool:
    """True, wenn die deutsche Börse an d handelt (kein Wochenende/Feiertag)."""
    if d.weekday() >= 5:            # 5=Sa, 6=So
        return False
    return d not in holidays(d.year)


def holiday_name(d: date) -> str | None:
    return holidays(d.year).get(d)


def next_trading_day(d: date | None = None) -> date:
    """Nächster Handelstag ab (ausschließlich) d (Default heute)."""
    cur = (d or date.today()) + timedelta(days=1)
    while not is_trading_day(cur):
        cur += timedelta(days=1)
    return cur


def prev_trading_day(d: date | None = None) -> date:
    cur = (d or date.today()) - timedelta(days=1)
    while not is_trading_day(cur):
        cur -= timedelta(days=1)
    return cur


def is_maintenance_day(d: date | None = None) -> bool:
    """Wartungsfenster: handelsfreier Tag (Wochenende/Feiertag) -- ideal für
    VACUUM/Rollup ohne Handelsbetrieb."""
    return not is_trading_day(d or date.today())


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "today"
    if cmd == "today":
        d = date.today()
        print(f"{d}: {'Handelstag' if is_trading_day(d) else 'handelsfrei'}"
              f"{' (' + holiday_name(d) + ')' if holiday_name(d) else ''}"
              f" | Wartungstag: {is_maintenance_day(d)}")
    elif cmd == "check" and len(sys.argv) > 2:
        d = date.fromisoformat(sys.argv[2])
        print(f"{d}: {'Handelstag' if is_trading_day(d) else 'handelsfrei'}"
              f"{' (' + holiday_name(d) + ')' if holiday_name(d) else ''}")
    elif cmd == "next":
        print(next_trading_day())
    elif cmd == "holidays" and len(sys.argv) > 2:
        for d, name in sorted(holidays(int(sys.argv[2])).items()):
            print(f"{d}  {name}")
    else:
        print(__doc__)
        sys.exit("Verwendung: today | check YYYY-MM-DD | next | holidays JAHR")


if __name__ == "__main__":
    main()
