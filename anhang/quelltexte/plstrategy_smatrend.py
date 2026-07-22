"""plstrategy_smatrend.py -- SMA-Trendfilter (Regime LONG/CASH) für gehebelte Produkte.

Extern validierte Regel (vgl. TRANSSKRIPT_Daily-SMA-gehebelt.md): Investiert (LONG im
2x-Long-Produkt), solange der Tagesschlusskurs des Referenzindex ÜBER seinem SMA(N)
liegt; darunter -> FLAT (Cash, KEINE Short-Position). Gehandelt wird nur bei Kreuzung
(Regimewechsel), sonst gehalten -- der Renditevorteil entsteht durch Nicht-Handeln.
Filtertiefe N im robusten Plateau 200-400 (Ausgangswert 325; historisch bestes, aber
in-sample -- bewusst nicht verabsolutieren).

Unterschied zu `plstrategy_sma` (Crossover SMA_fast/SMA_slow, LONG/SHORT, tgl. Round-
Trip): hier EIN SMA gegen den Index, LONG/CASH statt LONG/SHORT, Handel nur bei
Kreuzung. Einheit: dieses Plugin + Eintrag 'smatrend' in wknassign.yaml (nur LONG).
"""

from strategy import StrategyBase, StrategySignal


def _sma(closes, n, idx):
    return sum(closes[idx - n + 1: idx + 1]) / n


class Strategy(StrategyBase):
    name = "smatrend"
    description = ("SMA-Trendfilter: LONG solange Index-Close > SMA(N), sonst CASH "
                  "(FLAT, kein Short); Handel nur bei Regimewechsel")
    underlying = "^GSPC"
    default_params = {"period": 325}       # robustes Plateau 200-400
    min_history = 401                      # deckt das ganze Plateau ab
    decision_interval_s = 86_400
    data_freshness_s = 6 * 3600
    data_requirements = {
        "symbols": ["^GSPC", "SPY", "ES=F"],   # Index - Proxy - Randzeiten-Futures
        "bootstrap": {"1m": "7d", "5m": "60d", "1d": "max"},
        "live": True,
    }

    def decide_series(self, bars):
        n = int(self.params["period"])
        closes = [b.close for b in bars]
        out = []
        for i in range(len(bars)):
            if i < n - 1:
                out.append(StrategySignal("FLAT", {"reason": "history"}))
            else:
                sma = _sma(closes, n, i)
                out.append(StrategySignal(
                    "LONG" if closes[i] > sma else "FLAT",
                    {"sma": round(sma, 2), "close": round(closes[i], 2)}))
        return out
