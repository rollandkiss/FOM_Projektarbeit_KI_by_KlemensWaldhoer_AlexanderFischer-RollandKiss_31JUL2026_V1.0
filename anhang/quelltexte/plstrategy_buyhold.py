"""plstrategy_buyhold.py -- Benchmark-Strategie: immer LONG.

Bewusst OHNE wknassign_buyhold.py: demonstriert den definierten Zustand
'backtestbar, aber nicht handelbar' (Flag NO_INSTRUMENT -> NO_TRADE).
"""

from strategy import StrategyBase, StrategySignal


class Strategy(StrategyBase):
    name = "buyhold"
    description = "Benchmark: immer LONG (nicht handelbar -- keine Produktzuordnung)"
    underlying = "^GSPC"
    min_history = 1
    data_requirements = {
        "symbols": ["^GSPC"],
        "bootstrap": {"1d": "max"},
        "live": False,
    }

    def decide_series(self, bars):
        return [StrategySignal("LONG", {}) for _ in bars]
