#!/usr/bin/env python3
"""
mlforecast.py -- scikit-learn-Prognose als Prüfinstanz im Auditor-Korridor.

Rolle (bounded discretion, STRATEGIE.md §2; Design-Entscheid 22.07.):
  Das Modell ist PRÜFINSTANZ, NIE Signalquelle. Es liefert eine kalibrierte
  Wahrscheinlichkeit p_direction = p(Folgetags-Return liegt in Regimerichtung)
  aus bars_1d-Features. strategy.build_request hängt sie als NEUES Feld
  `ml_context` an den DecisionRequest und setzt bei p_direction < Schwelle
  (agentconfig.yaml ml.threshold_disagree, Default 0,45) das SOFT-Flag
  ML_DISAGREE -- es wirkt wie vol20/last5 als Risiko-Input für die LLM-Voter,
  ist bewusst KEIN Hard-Flag (sonst würde das ML de facto Signalquelle).
  Eine Prognose kann Trades damit nur verhindern/schwächen, nie erzeugen --
  das Periodensystem-Element Predictive Inference [Pi] wird so als eng
  begrenzte Prüfinstanz besetzt (KI_EINORDNUNG.md §3).

Betriebsmodi (strategy.ml_mode je Strategie; Rang: Signal-Override ML_MODES.json
[Befehle mlNy/mlNn] > agentconfig ml.modes.<strategie> > ml.mode):
  * sidecar -- Prognose läuft jeden Zyklus und journalisiert (ml_predictions),
    erreicht aber weder Request noch LLMs noch Entscheidung (Schattenbetrieb
    für den Kalibrierungsnachweis).
  * live    -- zusätzlich ml_context + Soft-Flag im DecisionRequest.
  * off     -- keine Inferenz, kein Journal (run_cycle überspringt predict).

Methodik (bewusst einfach -- Einwände a/b aus dem Design-Memo):
  * Tagesrichtung ist kaum prognostizierbar (Basisrate ~53 % Up-Tage). Das
    Modul ist daher ehrlich als RISIKOFILTER formuliert, nicht als Alpha;
    auch ein Negativbefund (keine Prognosekraft) wird berichtet (eval).
  * Gegen Overfitting/Data-Snooping: wenige Features, einfaches Modell
    (Logistische Regression, Default) und Walk-Forward ohne Lookahead --
    Training strikt auf Daten < t, Label t braucht den Folgetag t+1.
  * Features sind REGIMEGERICHTET (x dir): ein Modell für LONG- und
    SHORT-Regime, Label = 1 wenn der Folgetags-Return in Regimerichtung
    positiv war. dir folgt der SMA-Konvention (fast > slow -> LONG).

Persistenz:
  * Modellstand: ml_model.pkl (Pipeline + Metadaten; Version = Algo + Datum
    'trainiert bis' + Hash) -- wöchentliches Nachtraining per Cron (So.),
    tägliche Inferenz lädt nur die Datei (Mac-mini-tauglich, CPU, <1 s).
  * Journal: Tabelle ml_predictions in der Journal-DB (decisions.sqlite,
    GETRENNT von der rsync-gespiegelten Marktdaten-DB) -- eine Zeile je
    Inferenz; Grundlage der Kalibrierung in evaluate_votes.py.

CLI:
  python3 mlforecast.py train    [--db marketdata.sqlite] [--symbol ^GSPC]
                                 [--model-path ml_model.pkl] [--algo logreg|gb]
  python3 mlforecast.py predict  [--db ...] [--journal-db decisions.sqlite]
                                 [--direction LONG|SHORT] [--no-journal]
  python3 mlforecast.py eval     [--db ...] [--min-train 750] [--retrain-every 21]
                                 [--csv ml_walkforward.csv]
  python3 mlforecast.py history  [--journal-db ...] [--limit 10]

Cron (agent-vm, So. VOR dem evaluate_votes-Lauf 16:30 UTC):
  0 15 * * 0  cd /home/me/fom-ki-project && python3 mlforecast.py train >> logs/mlforecast.log 2>&1

Abhängigkeiten: scikit-learn (+ numpy). Alle Aufrufer (strategy.build_request)
nutzen das Modul BEST-EFFORT: fehlt sklearn oder das Modell, gibt es keinen
ml_context und kein Flag -- der Zyklus läuft unverändert (fail-open by design:
Ausfall der Prüfinstanz darf den deterministischen Kern nicht blockieren).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import pickle
import sqlite3
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("mlforecast")

MODEL_PATH_DEFAULT = "ml_model.pkl"
SMA_FAST, SMA_SLOW = 50, 200          # SMA-Konvention der Default-Strategie (sma)
N_RET_LAGS = 5                        # Returns-Lags r1..r5 (wie last5 im Request)
MIN_TRAIN_DEFAULT = 750               # ~3 Handelsjahre Mindesttraining (Walk-Forward)
RETRAIN_EVERY_DEFAULT = 21            # Walk-Forward: monatliches Nachtraining
FEATURE_NAMES = [f"ret_lag{k}" for k in range(1, N_RET_LAGS + 1)] + [
    "vol20", "sma_gap_fast", "sma_gap_slow", "sma_spread", "regime_age_log"]

ML_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS ml_predictions (
    ts_utc        TEXT NOT NULL,
    date          TEXT NOT NULL,      -- Handelstag der Entscheidung (= letzter Bar)
    symbol        TEXT NOT NULL,
    direction     TEXT NOT NULL,      -- Regimerichtung, auf die sich p bezieht
    p_direction   REAL NOT NULL,      -- p(Folgetag in Regimerichtung)
    model_version TEXT NOT NULL,
    features_json TEXT NOT NULL,
    mode          TEXT                -- Betriebsmodus zum Prognosezeitpunkt
                                      -- (live/sidecar; NULL = vor dem Feld/manuell).
                                      -- Historische Modus-Spur: belegt je Tag, ob die
                                      -- Prognose Einfluss hatte; Dashboard-Export
                                      -- liest die jüngste Zeile.
);
"""


class MLUnavailable(Exception):
    """Prognose nicht verfügbar (Modell/Abhängigkeit/Historie fehlt) -- Aufrufer
    behandeln das best-effort (kein ml_context, kein Flag), nie als Zyklusfehler."""


# --------------------------------------------------------------------------
# Daten + Features (stdlib; sklearn erst in train/predict nötig)
# --------------------------------------------------------------------------

def load_closes(db: str, symbol: str) -> list[tuple[str, float]]:
    """(date, close) aufsteigend aus bars_1d (read-only, wie strategy.load_daily)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts_utc_ms, close FROM bars_1d WHERE symbol=? ORDER BY ts_utc_ms",
            (symbol,)).fetchall()
    finally:
        con.close()
    return [(datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
             float(c)) for ms, c in rows]


def _sma(closes: list[float], n: int, idx: int) -> float:
    return sum(closes[idx - n + 1: idx + 1]) / n


def regime_dir(closes: list[float], idx: int) -> int:
    """+1 (LONG) wenn SMA_fast > SMA_slow, sonst -1 (SHORT) -- SMA-Konvention."""
    return 1 if _sma(closes, SMA_FAST, idx) > _sma(closes, SMA_SLOW, idx) else -1


def _features(closes: list[float], idx: int, d: int, age: int) -> list[float]:
    """Feature-Vektor am Bar `idx` -- nutzt AUSSCHLIESSLICH closes[: idx+1]
    (Walk-Forward-Garantie: kein Lookahead).

    Regimegerichtete Skalierung (x dir): >>Rückenwind in Regimerichtung<< sieht
    für LONG- und SHORT-Regime gleich aus -> ein gemeinsames Modell."""
    rets = [(closes[i] / closes[i - 1] - 1) * 100
            for i in range(idx - N_RET_LAGS + 1, idx + 1)]     # r5..r1 (alt->neu)
    lags = [r * d for r in reversed(rets)]                     # r1 (jüngster) zuerst
    vol20 = statistics.pstdev(
        [(closes[i] / closes[i - 1] - 1) * 100 for i in range(idx - 19, idx + 1)])
    sf, ss = _sma(closes, SMA_FAST, idx), _sma(closes, SMA_SLOW, idx)
    gap_fast = (closes[idx] / sf - 1) * 100 * d
    gap_slow = (closes[idx] / ss - 1) * 100 * d
    spread = (sf / ss - 1) * 100 * d       # per Konstruktion >= 0 in Regimerichtung
    return [*lags, vol20, gap_fast, gap_slow, spread, math.log1p(age)]


def feature_row(closes: list[float], idx: int) -> tuple[list[float], int]:
    """(features, dir) am Bar `idx` (Einzel-Inferenz). Das Regime-Alter zählt ab dem
    ersten Bar mit vollem SMA-Fenster (konsistent zu build_dataset)."""
    if idx < SMA_SLOW:                      # braucht SMA200 + Lag-/Vortagsfenster
        raise MLUnavailable(f"zu wenig Historie für Features (idx {idx} < {SMA_SLOW})")
    d = regime_dir(closes, idx)
    age = 1                                 # Regime-Alter in Handelstagen
    while idx - age >= SMA_SLOW and regime_dir(closes, idx - age) == d:
        age += 1
    return _features(closes, idx, d, age), d


def build_dataset(closes: list[float]) -> tuple[list[list[float]], list[int], list[int]]:
    """(X, y, idx) über alle Bars mit vollständigem Feature-Fenster UND Folgetag.
    Label y[i] = 1 <-> Return des Folgetags in Regimerichtung positiv. Das Regime-
    Alter wird inkrementell geführt (identische Semantik wie feature_row, ohne
    den O(n^2)-Rückwärtslauf je Zeile)."""
    X, y, idxs = [], [], []
    prev_d, age = None, 0
    for i in range(SMA_SLOW, len(closes)):
        d = regime_dir(closes, i)
        age = age + 1 if d == prev_d else 1
        prev_d = d
        if i >= len(closes) - 1:            # jüngster Bar: Folgetag fehlt -> kein Label
            break
        nxt = (closes[i + 1] / closes[i] - 1) * d
        X.append(_features(closes, i, d, age))
        y.append(1 if nxt > 0 else 0)
        idxs.append(i)
    return X, y, idxs


# --------------------------------------------------------------------------
# Modell (sklearn) + Persistenz
# --------------------------------------------------------------------------

def _make_pipeline(algo: str):
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    if algo == "gb":
        from sklearn.ensemble import GradientBoostingClassifier
        clf = GradientBoostingClassifier(n_estimators=100, max_depth=2,
                                         learning_rate=0.05, random_state=0)
    else:                                   # Default: einfach + gut kalibrierbar
        from sklearn.linear_model import LogisticRegression
        clf = LogisticRegression(C=1.0, max_iter=1000)
    return Pipeline([("scaler", StandardScaler()), ("clf", clf)])


def train_model(db: str, symbol: str, model_path: str, algo: str = "logreg") -> dict:
    """Trainiert auf ALLEN verfügbaren, vollständig gelabelten Tagen (< heute)
    und persistiert Pipeline + Metadaten. Kein Lookahead: der jüngste Bar hat
    keinen Folgetag und ist nie Teil des Trainings."""
    rows = load_closes(db, symbol)
    closes = [c for _, c in rows]
    X, y, idxs = build_dataset(closes)
    if len(X) < MIN_TRAIN_DEFAULT:
        raise MLUnavailable(f"zu wenig Trainingstage ({len(X)} < {MIN_TRAIN_DEFAULT})")
    pipe = _make_pipeline(algo)
    pipe.fit(X, y)
    trained_until = rows[idxs[-1]][0]
    blob = pickle.dumps(pipe)
    version = (f"{algo}-{trained_until}-"
               f"{hashlib.sha256(blob).hexdigest()[:8]}")
    meta = {"version": version, "algo": algo, "symbol": symbol,
            "trained_until": trained_until, "n_samples": len(X),
            "base_rate": round(sum(y) / len(y), 4),
            "features": FEATURE_NAMES, "sma": [SMA_FAST, SMA_SLOW],
            "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    Path(model_path).write_bytes(pickle.dumps({"meta": meta, "pipeline_pkl": blob}))
    log.info("Modell %s gespeichert -> %s (n=%d, Basisrate %.1f %%)",
             version, model_path, len(X), 100 * meta["base_rate"])
    return meta


def load_model(model_path: str):
    """Returns (pipeline, meta). MLUnavailable, wenn Datei/sklearn fehlt."""
    p = Path(model_path)
    if not p.exists():
        raise MLUnavailable(f"Modelldatei fehlt: {model_path} -- 'train' ausführen")
    try:
        bundle = pickle.loads(p.read_bytes())
        pipe = pickle.loads(bundle["pipeline_pkl"])
    except Exception as exc:  # noqa: BLE001 -- defekte Datei = nicht verfügbar
        raise MLUnavailable(f"Modelldatei unlesbar: {exc}") from exc
    return pipe, bundle["meta"]


# --------------------------------------------------------------------------
# Inferenz + Journal
# --------------------------------------------------------------------------

def journal_prediction(journal_db: str, row: dict) -> None:
    con = sqlite3.connect(journal_db, timeout=30)
    try:
        con.execute("PRAGMA busy_timeout=10000")
        con.executescript(ML_JOURNAL_SCHEMA)
        try:                                     # idempotente Migration bestehender
            con.execute("ALTER TABLE ml_predictions ADD COLUMN mode TEXT")
        except sqlite3.OperationalError:         # Tabellen (Spalte existiert schon)
            pass
        con.execute(
            "INSERT INTO ml_predictions "
            "(ts_utc, date, symbol, direction, p_direction, model_version, "
            "features_json, mode) VALUES (?,?,?,?,?,?,?,?)",
            (row["ts_utc"], row["date"], row["symbol"], row["direction"],
             row["p_direction"], row["model_version"], row["features_json"],
             row.get("mode")))
        con.commit()
    finally:
        con.close()


def predict(db: str, symbol: str, model_path: str,
            journal_db: str | None = None, mode: str | None = None) -> dict:
    """Inferenz auf dem JÜNGSTEN Bar: p_direction für die aktuelle Regimerichtung.
    Schreibt best-effort ins ml_predictions-Journal (journal_db=None -> nur Rechnen).
    `mode` (live/sidecar) wird mitjournaliert -- historische Modus-Spur je Tag,
    die auch der Dashboard-Export liest (run_cycle übergibt --mode)."""
    pipe, meta = load_model(model_path)
    rows = load_closes(db, symbol)
    closes = [c for _, c in rows]
    if not closes:
        raise MLUnavailable(f"keine bars_1d für {symbol}")
    i = len(closes) - 1
    feats, d = feature_row(closes, i)
    p = float(pipe.predict_proba([feats])[0][1])
    direction = "LONG" if d > 0 else "SHORT"
    out = {"date": rows[i][0], "symbol": symbol, "direction": direction,
           "p_direction": round(p, 4), "model_version": meta["version"],
           "trained_until": meta["trained_until"], "mode": mode}
    if journal_db:
        try:
            journal_prediction(journal_db, {
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "date": out["date"], "symbol": symbol, "direction": direction,
                "p_direction": out["p_direction"], "model_version": meta["version"],
                "features_json": json.dumps(dict(zip(FEATURE_NAMES,
                                                     [round(f, 6) for f in feats]))),
                "mode": mode})
        except sqlite3.Error as exc:      # Journal best-effort -- Inferenz gilt trotzdem
            log.warning("ml_predictions-Journal fehlgeschlagen: %s", exc)
    return out


def ml_context_for_request(db: str, symbol: str, direction: str,
                           model_path: str = MODEL_PATH_DEFAULT,
                           journal_db: str | None = None) -> dict | None:
    """Einstiegspunkt für strategy.build_request (BEST-EFFORT, wirft nie).

    Prüft, dass die Regimerichtung des Modells zum Strategie-Signal passt
    (sonst kein Kontext -- z. B. smatrend-FLAT-Phasen oder abweichende Regeln);
    bei FLAT gibt es nichts zu prüfen. Liefert das ml_context-Dict für den
    DecisionRequest oder None."""
    if direction not in ("LONG", "SHORT"):
        return None
    try:
        res = predict(db, symbol, model_path, journal_db)
        if res["direction"] != direction:
            log.info("ml_context: Modell-Regime %s != Signal %s -- kein Kontext.",
                     res["direction"], direction)
            return None
        return {"p_direction": res["p_direction"],
                "model_version": res["model_version"],
                "trained_until": res["trained_until"],
                "note": ("Kalibrierte Wahrscheinlichkeit, dass der Folgetags-Return "
                         "in Regimerichtung positiv ist (scikit-Prüfinstanz; "
                         "Risiko-Input wie vol20/last5, keine Signalquelle).")}
    except MLUnavailable as exc:
        log.info("ml_context nicht verfügbar (best-effort, kein Flag): %s", exc)
        return None
    except Exception as exc:  # noqa: BLE001 -- Prüfinstanz darf den Zyklus nie brechen
        log.warning("ml_context unerwartet fehlgeschlagen (ignoriert): %s", exc)
        return None


# --------------------------------------------------------------------------
# Walk-Forward-Evaluation (ehrliche Prüfung inkl. möglichem Negativbefund)
# --------------------------------------------------------------------------

def walk_forward(closes: list[float], algo: str = "logreg",
                 min_train: int = MIN_TRAIN_DEFAULT,
                 retrain_every: int = RETRAIN_EVERY_DEFAULT) -> dict:
    """Expanding-Window-Walk-Forward: Vorhersage für Tag t nutzt ein Modell, das
    ausschließlich auf Tagen < t trainiert wurde (Nachtraining alle `retrain_every`
    Tage). Metriken: Trefferquote vs. Basisrate, Brier-Score vs. Klimatologie
    (konstante Basisraten-Prognose), Kalibrierungs-Bins."""
    X, y, _ = build_dataset(closes)
    if len(X) <= min_train:
        raise MLUnavailable(f"zu wenig Daten für Walk-Forward ({len(X)} <= {min_train})")
    preds: list[tuple[float, int]] = []
    pipe = None
    for t in range(min_train, len(X)):
        if pipe is None or (t - min_train) % retrain_every == 0:
            pipe = _make_pipeline(algo)
            pipe.fit(X[:t], y[:t])          # strikt < t -- kein Lookahead
        preds.append((float(pipe.predict_proba([X[t]])[0][1]), y[t]))

    n = len(preds)
    base = sum(y[min_train:]) / n                     # Basisrate im Testfenster
    hits = sum(1 for p, yy in preds if (p >= 0.5) == (yy == 1))
    brier = sum((p - yy) ** 2 for p, yy in preds) / n
    brier_clim = sum((base - yy) ** 2 for p, yy in preds) / n
    bins: dict[str, dict] = {}
    for p, yy in preds:
        k = f"{min(int(p * 10), 9) / 10:.1f}"         # 0.0-0.9 Dezile
        b = bins.setdefault(k, {"n": 0, "hit": 0, "p_sum": 0.0})
        b["n"] += 1
        b["hit"] += yy
        b["p_sum"] += p
    calibration = {k: {"n": b["n"], "p_mean": round(b["p_sum"] / b["n"], 3),
                       "obs_rate": round(b["hit"] / b["n"], 3)}
                   for k, b in sorted(bins.items())}
    return {"n_test": n, "algo": algo, "min_train": min_train,
            "retrain_every": retrain_every,
            "base_rate": round(base, 4),
            "accuracy": round(hits / n, 4),
            "edge_vs_base": round(hits / n - max(base, 1 - base), 4),
            "brier": round(brier, 4), "brier_climatology": round(brier_clim, 4),
            "brier_skill_score": round(1 - brier / brier_clim, 4) if brier_clim else None,
            "calibration": calibration,
            "verdict": ("Prognosekraft über Basisrate/Klimatologie nachgewiesen"
                        if brier < brier_clim else
                        "KEINE Prognosekraft über Klimatologie -- Negativbefund "
                        "(erwartbar; Rolle bleibt Risikofilter)")}


def show_history(journal_db: str, limit: int) -> None:
    try:
        con = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM ml_predictions ORDER BY ts_utc DESC "
                           "LIMIT ?", (limit,)).fetchall()
        con.close()
    except sqlite3.OperationalError:
        print("Noch keine ml_predictions vorhanden.")
        return
    for r in rows:
        mode = r["mode"] if "mode" in r.keys() else None
        print(f"{r['ts_utc']}  {r['date']}  {r['symbol']:<7} {r['direction']:<5} "
              f"p={r['p_direction']:.3f}  {mode or '--':<7} {r['model_version']}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(
        description="scikit-Prognose als Prüfinstanz (train/predict/eval)")
    p.add_argument("--db", default="marketdata.sqlite")
    p.add_argument("--symbol", default="^GSPC")
    p.add_argument("--model-path", default=MODEL_PATH_DEFAULT)
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("train")
    sp.add_argument("--algo", choices=("logreg", "gb"), default=None,
                    help="Default: agentconfig ml.algo, sonst logreg")
    sp = sub.add_parser("predict")
    sp.add_argument("--journal-db", default="decisions.sqlite")
    sp.add_argument("--no-journal", action="store_true")
    sp.add_argument("--mode", choices=("live", "sidecar", "off"), default=None,
                    help="Betriebsmodus fürs Journal (run_cycle übergibt "
                         "strategy.ml_mode; Dashboard liest die Spur)")
    sp.add_argument("--direction", choices=("LONG", "SHORT"), default=None,
                    help="nur prüfen: erwartete Regimerichtung (Warnung bei Abweichung)")
    sp = sub.add_parser("eval")
    sp.add_argument("--algo", choices=("logreg", "gb"), default="logreg")
    sp.add_argument("--min-train", type=int, default=MIN_TRAIN_DEFAULT)
    sp.add_argument("--retrain-every", type=int, default=RETRAIN_EVERY_DEFAULT)
    sp.add_argument("--csv", default=None)
    sp = sub.add_parser("history")
    sp.add_argument("--journal-db", default="decisions.sqlite")
    sp.add_argument("--limit", type=int, default=10)

    args = p.parse_args(argv)
    try:
        if args.cmd == "train":
            algo = args.algo
            if algo is None:                 # Fallback: agentconfig ml.algo (best-effort)
                try:
                    import strategyloader as loader
                    algo = loader.load_ml_config().get("algo") or "logreg"
                except Exception:  # noqa: BLE001
                    algo = "logreg"
            meta = train_model(args.db, args.symbol, args.model_path, algo)
            print(json.dumps(meta, indent=2, ensure_ascii=False))
        elif args.cmd == "predict":
            res = predict(args.db, args.symbol, args.model_path,
                          None if args.no_journal else args.journal_db,
                          mode=args.mode)
            if args.direction and res["direction"] != args.direction:
                log.warning("Regimerichtung des Modells (%s) != erwartet (%s).",
                            res["direction"], args.direction)
            print(json.dumps(res, indent=2, ensure_ascii=False))
        elif args.cmd == "eval":
            closes = [c for _, c in load_closes(args.db, args.symbol)]
            res = walk_forward(closes, args.algo, args.min_train, args.retrain_every)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            if args.csv:
                import csv as _csv
                with open(args.csv, "w", newline="", encoding="utf-8") as f:
                    w = _csv.writer(f)
                    w.writerow(["bin", "n", "p_mean", "obs_rate"])
                    for k, b in res["calibration"].items():
                        w.writerow([k, b["n"], b["p_mean"], b["obs_rate"]])
                print(f"Kalibrierung -> {args.csv}", file=sys.stderr)
        elif args.cmd == "history":
            show_history(args.journal_db, args.limit)
    except MLUnavailable as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
