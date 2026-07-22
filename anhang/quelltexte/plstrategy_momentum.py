"""plstrategy_momentum.py -- Zeitreihen-Momentum (Beispiel-Erweiterung).

Demonstriert die Erweiterbarkeit: LONG wenn Kurs > Kurs vor N Tagen, sonst SHORT.
Einheit mit wknassign_momentum.py.
"""

from strategy import StrategyBase, StrategySignal


class Strategy(StrategyBase):
    name = "momentum"
    description = "Zeitreihen-Momentum: LONG wenn Kurs > Kurs vor N Tagen, sonst SHORT"
    underlying = "^GSPC"
    default_params = {"lookback": 90}
    min_history = 91
    data_requirements = {
        "symbols": ["^GSPC"],
        "bootstrap": {"1d": "max"},
        "live": False,
    }

    def decide_series(self, bars):
        lb = self.params["lookback"]
        out = []
        for i in range(len(bars)):
            if i < lb:
                out.append(StrategySignal("FLAT", {"reason": "history"}))
            else:
                mom = bars[i].close / bars[i - lb].close - 1
                out.append(StrategySignal("LONG" if mom > 0 else "SHORT",
                                          {"momentum_pct": round(mom * 100, 2)}))
        return out
