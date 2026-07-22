"""plstrategy_smacross.py -- ereignisbasierte SMA-Crossover-Strategie.

Handelt nur an Kreuzungstagen (Golden/Death Cross) plus Haltefenster, sonst FLAT.
Einheit mit wknassign_smacross.py.
"""

from strategy import StrategyBase, StrategySignal
from plstrategy_sma import Strategy as SMARegime


class Strategy(StrategyBase):
    name = "smacross"
    description = ("SMA-Crossover (ereignisbasiert): handelt nur an Kreuzungstagen "
                   "+/- Haltefenster, sonst FLAT")
    underlying = "^GSPC"
    default_params = {"fast": 50, "slow": 200, "hold_days": 5}
    min_history = 201
    data_requirements = {
        "symbols": ["^GSPC"],
        "bootstrap": {"1d": "max"},
        "live": False,             # EOD genügt für Ereigniserkennung
    }

    def decide_series(self, bars):
        base = SMARegime({"fast": self.params["fast"],
                          "slow": self.params["slow"]}).decide_series(bars)
        out, hold, cur, prev = [], 0, "FLAT", None
        for sig in base:
            if sig.direction in ("LONG", "SHORT"):
                if prev in ("LONG", "SHORT") and sig.direction != prev:
                    cur, hold = sig.direction, self.params["hold_days"]
                prev = sig.direction
            if hold > 0:
                out.append(StrategySignal(cur, {"hold_left": hold}))
                hold -= 1
            else:
                out.append(StrategySignal("FLAT", {}))
        return out
