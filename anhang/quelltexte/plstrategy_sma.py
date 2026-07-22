"""plstrategy_sma.py -- SMA-Regimefilter (Default-Strategie des Handelsagenten).

Einheit: dieses Plugin + wknassign_sma.py (Produktzuordnung).
Regel: LONG wenn SMA_fast(t) > SMA_slow(t) auf Tagesschlusskursen des Underlyings,
sonst SHORT (STRATEGIE.md §1). Täglicher Round-Trip gemäß Ausführungsregeln.
"""

from strategy import StrategyBase, StrategySignal


def _sma(closes, n, idx):
    return sum(closes[idx - n + 1: idx + 1]) / n


class Strategy(StrategyBase):
    name = "sma"
    description = ("SMA-Regimefilter: LONG wenn SMA_fast > SMA_slow, sonst SHORT; "
                   "täglicher Round-Trip (max. 1/Tag)")
    underlying = "^GSPC"
    default_params = {"fast": 50, "slow": 200}
    min_history = 201
    decision_interval_s = 86_400
    data_freshness_s = 6 * 3600
    data_requirements = {
        "symbols": ["^GSPC", "SPY", "ES=F"],   # Index - Proxy - Randzeiten-Futures
        "bootstrap": {"1m": "7d", "5m": "60d", "1d": "max"},
        "live": True,
    }

    def decide_series(self, bars):
        f, s = self.params["fast"], self.params["slow"]
        closes = [b.close for b in bars]
        out = []
        for i in range(len(bars)):
            if i < s - 1:
                out.append(StrategySignal("FLAT", {"reason": "history"}))
            else:
                sf, ss = _sma(closes, f, i), _sma(closes, s, i)
                out.append(StrategySignal(
                    "LONG" if sf > ss else "SHORT",
                    {"sma_fast": round(sf, 2), "sma_slow": round(ss, 2)}))
        return out
