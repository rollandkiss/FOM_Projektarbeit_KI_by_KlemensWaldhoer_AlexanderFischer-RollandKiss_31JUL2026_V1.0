#!/usr/bin/env bash
# run_cycle.sh -- täglicher Trading-Zyklus mit Signal-Reporting (System-Cron-Runner).
#
# Ablauf: strategy.py -> Gemma-Auditor -> Validator -> decisions-Journal (auditor.py),
# danach Report an die Signal-Gruppe; bei Betriebsstörungen zusätzlich Alarm an
# die Direktnummer. Reporting ist best-effort -- ein Signal-Fehler bricht weder
# den Audit noch das Journaling ab (Orderpfad bleibt deterministisch, kein
# Chat-Kommando löst Trades aus).
#
# Konfiguration in agent.env (nicht im Code):
#   SIGNAL_BOT="+49XXXXXXXXXXX"
#   SIGNAL_GROUP="<Base64-Gruppen-ID>"        # Ziel der Tagesreports
#   SIGNAL_RECIPIENT="+49..."                 # Ziel der Störungs-Alarme
#
# Cron (agent-vm, UTC):
#   25 13 * * 1-5  /home/me/fom-ki-project/run_cycle.sh >> /home/me/fom-ki-project/logs/cron.log 2>&1

set -uo pipefail   # bewusst kein -e: Reporting-Fehler dürfen den Zyklus nicht killen

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

STRATEGY="${STRATEGY:-sma}"
MODEL="${MODEL:-gemma4:cloud}"
CAPITAL="${CAPITAL:-2000}"
DB="${DB:-marketdata.sqlite}"              # Marktdaten (per rsync vom collector-vm gespiegelt)
JOURNAL_DB="${JOURNAL_DB:-decisions.sqlite}"  # Entscheidungsjournal, GETRENNT vom rsync
ACCOUNT="${ACCOUNT:-haupt}"               # Zugang/Konto (Multi-Depot; haupt = Default)
DEPOT="${DEPOT:-}"                         # Depot-Override (leer -> COMDIRECT_DEPOT[_<ACCOUNT>])

# A1 -- Prozess-Lock je Konto: verhindert, dass derselbe Zyklus doppelt läuft (doppelte
# Cron-Zeile, manueller Re-Run parallel zum Cron). Verschiedene Konten (run_all) blockieren
# sich nicht. flock ist Linux (GNU util-linux); auf macOS wäre ein mkdir-Lock nötig.
exec 9>"$PROJECT_DIR/.cycle_${ACCOUNT}.lock"
if ! flock -n 9; then
  echo "[$STAMP] Zyklus für Konto '$ACCOUNT' läuft bereits (Lock aktiv) -- übersprungen."
  exit 0
fi
# Wichtig: Das Journal darf NICHT in $DB liegen -- der minütliche rsync ersetzt $DB und
# würde die decisions-Tabelle sonst laufend überschreiben.

# Konfiguration laden (falls vorhanden)
[[ -f agent.env ]] && source agent.env
SIGNAL_BOT="${SIGNAL_BOT:-}"
SIGNAL_GROUP="${SIGNAL_GROUP:-}"
SIGNAL_RECIPIENT="${SIGNAL_RECIPIENT:-}"

# Report-Ziele: SIGNAL_GROUP + alle per Signal-Befehl 'A' freigegebenen Gruppen
# (GROUPS_ACCEPTED.json, gepflegt von control.py). Befehle nimmt KEINE Gruppe an --
# Gruppen sind reine Report-Empfänger.
_report_groups() {
  [[ -n "$SIGNAL_GROUP" ]] && echo "$SIGNAL_GROUP"
  [[ -f GROUPS_ACCEPTED.json ]] && python3 - <<'PYEOF' 2>/dev/null
import json
try:
    for g in json.load(open("GROUPS_ACCEPTED.json")):
        gid = (g.get("id") or "").strip()
        if gid:
            print(gid)
except Exception:
    pass
PYEOF
  return 0
}

send_group() {  # $1 = Nachricht -> an alle Report-Gruppen (best-effort, dedupliziert)
  [[ -n "$SIGNAL_BOT" ]] || return 0
  local g sent=""
  while IFS= read -r g; do
    [[ -n "$g" ]] || continue
    case "$sent" in *"|$g|"*) continue ;; esac
    sent="$sent|$g|"
    signal-cli -a "$SIGNAL_BOT" send -g "$g" -m "$1" >/dev/null 2>&1 || true
  done < <(_report_groups)
  return 0
}

send_alert() {  # $1 = Alarmtext (nur Betriebsstörungen)
  [[ -n "$SIGNAL_BOT" && -n "$SIGNAL_RECIPIENT" ]] || return 0
  signal-cli -a "$SIGNAL_BOT" send -m "[!] Trading-Agent Alarm: $1" "$SIGNAL_RECIPIENT" \
    >/dev/null 2>&1 || echo "[$STAMP] WARN: Alarm-Versand fehlgeschlagen."
}

heartbeat_cycle() {  # M4 -- Zyklus-Stempel: "der Zyklus LIEF heute". Wird an JEDEM
  # Ausgang geschrieben (auch Feiertag/Pause/Preflight-Abbruch), damit cycle_watch
  # auf der collector-vm nur bei echter NICHT-Ausführung (Crontab kaputt, flock
  # hängt, Rechte) alarmiert. Spiegelung best-effort.
  date -u +%FT%TZ > last_cycle.ts
  rsync -az last_cycle.ts me@10.128.0.3:/home/me/sync/agent_last_cycle.ts 2>/dev/null || true
}

# M1 -- Auditor-Budget + Endpoint aus agentconfig.yaml (EINE Quelle der Wahrheit;
# Mac mini: nur timeout_s in der Config erhöhen, das Gesamtbudget folgt).
read -r AUD_BASE AUD_N AUD_TIMEOUT <<<"$(python3 -c "
import strategyloader as l
a = l.load_auditor_config()
print(a.get('base_url') or 'http://127.0.0.1:11434',
      len(a.get('models') or [1]), a.get('timeout_s') or 90)" 2>/dev/null \
  || echo 'http://127.0.0.1:11434 3 90')"
AUDIT_BUDGET=$(( AUD_N * AUD_TIMEOUT + 90 ))


# Deutscher Börsenfeiertag? -> Zyklus überspringen (SG-Order am dt. Handelsplatz
# nicht möglich). market_calendar.py ist die allgemeine Kalender-Utility.
if [[ -f market_calendar.py ]] && \
   ! python3 -c "import market_calendar as m, datetime, sys; \
sys.exit(0 if m.is_trading_day(datetime.date.today()) else 1)"; then
  HOL="$(python3 -c "import market_calendar as m, datetime; \
print(m.holiday_name(datetime.date.today()) or 'handelsfrei')" 2>/dev/null)"
  echo "[$STAMP] Deutscher Börsenfeiertag ($HOL) -- Zyklus übersprungen."
  send_group "[KAL] Kein Handel heute: $HOL (deutscher Börsenfeiertag). Agent pausiert."
  heartbeat_cycle
  exit 0
fi

# photoTAN-Sicherheitsnetz: wartende Challenge an Direktnummer schicken
if [[ -f tan/pending ]]; then
  ./notify_tan.sh && mv -f tan/pending tan/pending.sent 2>/dev/null
fi

echo "[$STAMP] Zyklusstart -- Strategie=$STRATEGY Modell=$MODEL Kapital=$CAPITAL"

# OP2 Kill-Switch: NUR die Pause-Flags lesen (kein receive hier -- das übernimmt der
# ständig laufende signal_dispatcher in Echtzeit). Global (TRADING_PAUSED) ODER
# kontospezifisch (TRADING_PAUSED_<konto>, Signal-Befehl Pn) pausiert (Exit 1) ->
# den ganzen Zyklus dieses Kontos stoppen.
if ! python3 control.py check-pause --account "$ACCOUNT"; then
  echo "[$STAMP] Handel PAUSIERT (Kill-Switch, Konto $ACCOUNT) -- Zyklus übersprungen."
  send_group "[PAUSE] Agent pausiert (Kill-Switch, Konto $ACCOUNT). Kein Handel. 'R' zum Fortsetzen."
  heartbeat_cycle
  exit 0
fi

# Vorabprüfung: ohne DB kein Lauf (Alarm an Direktnummer)
if [[ ! -f "$DB" ]]; then
  echo "[$STAMP] ABBRUCH: $DB nicht gefunden (rsync von collector-vm prüfen)."
  send_alert "Datenbank $DB fehlt -- rsync von collector-vm prüfen."
  heartbeat_cycle
  exit 1
fi

# M3 -- Ollama-Preflight: ein toter Endpoint würde sonst n_models x timeout_s Budget
# verbrennen und einen unpräzisen Alarm liefern. /api/tags ist billig und wärmt die
# Verbindung; Abbruch VOR den LLM-Aufrufen ist fail-safe (keine Entscheidung = keine
# Order; notify-Stale-Guard kennzeichnet den Report als VERALTET).
if ! python3 -c "import urllib.request; \
urllib.request.urlopen('$AUD_BASE/api/tags', timeout=5)" 2>/dev/null; then
  echo "[$STAMP] ABBRUCH: Ollama-Endpoint $AUD_BASE nicht erreichbar (Preflight)."
  send_alert "Ollama-Endpoint $AUD_BASE nicht erreichbar -- LLM-Voting unmöglich, Zyklus abgebrochen (fail-safe: keine Order). ollama-Dienst prüfen."
  heartbeat_cycle
  exit 1
fi

# scikit-Prüfinstanz: Tagesprognose ins ml_predictions-Journal schreiben (Basis der
# Kalibrierung in evaluate_votes --ml). Läuft in den Modi live UND sidecar (Sidecar =
# journalisieren ohne Einfluss); nur 'off' überspringt komplett. Best-effort mit
# hartem Zeitdeckel -- fehlt sklearn/Modell, läuft der Zyklus unverändert weiter
# (fail-open; den ml_context im Request rechnet build_request selbst und NUR im
# Modus live). Modusquelle: strategy.ml_mode (Signal-Override ML_MODES.json >
# agentconfig ml.modes > ml.mode).
ML_MODE="$(python3 -c "import strategy; print(strategy.ml_mode('$STRATEGY')[0])" 2>/dev/null || echo off)"
ML_LINE="ML[$STRATEGY]: $ML_MODE"
if [[ "$ML_MODE" != "off" ]]; then
  ML_OUT="$(timeout 60 python3 mlforecast.py --db "$DB" predict --journal-db "$JOURNAL_DB" --mode "$ML_MODE" 2>/dev/null)" \
    || echo "[$STAMP] Hinweis: mlforecast-Prognose übersprungen (best-effort)."
  ML_P="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(f\"p={d['p_direction']:.3f} ({d['direction']})\")" "$ML_OUT" 2>/dev/null)"
  [[ -n "$ML_P" ]] && ML_LINE="$ML_LINE, $ML_P"
fi

# Auditor-Zyklus (Marktdaten aus $DB, Journal nach $JOURNAL_DB)
# OP5+M1: Gesamt-Timeout aus agentconfig abgeleitet (n_models x timeout_s + 90 s
# für Request-Bau/Validator/Journal) -- vorher waren 3x90 s je Modell gegen einen
# harten 300-s-Kill inkonsistent (Kill NACH LLM-Aufrufen, VOR dem Journal-Eintrag
# möglich). Der Fail-safe behandelt einen Abbruch als Fehler (AUDIT_OK=0 -> Alarm,
# orchestrate wird übersprungen, keine Order). Exit-Code 124 = Timeout.
echo "[$STAMP] Auditor-Budget: $AUD_N Modell(e) x ${AUD_TIMEOUT}s + 90s = ${AUDIT_BUDGET}s."
if timeout "$AUDIT_BUDGET" python3 auditor.py --db "$DB" --journal-db "$JOURNAL_DB" run \
     --strategy "$STRATEGY" --model "$MODEL" --capital "$CAPITAL"; then
  AUDIT_OK=1
else
  rc=$?          # A4: MUSS die erste Zeile des Zweigs sein -- jede andere Anweisung
  AUDIT_OK=0     # (auch AUDIT_OK=0) würde $? überschreiben und rc wäre immer 0.
  echo "[$STAMP] FEHLER: auditor.py Exit $rc (124 = Timeout)."
  send_alert "auditor.py-Lauf fehlgeschlagen (Exit $rc; siehe logs/cron.log)."
fi

# Depotwert-Snapshot fürs Dashboard (Wertentwicklung im Zeitvergleich) -- jeden Zyklus,
# unabhängig vom Entscheid. Best-effort: ohne aktive Session still übersprungen.
python3 orchestrate.py snapshot --journal-db "$JOURNAL_DB" --account "$ACCOUNT" \
  ${DEPOT:+--depot "$DEPOT"} >/dev/null 2>&1 || true

# Bei TRADE-Votum: Order gegen comdirect validieren (bis /orders/validation,
# inkl. Ex-Ante-Kosten) -- löst KEINE Order aus. Ergebnis dem Report anhängen.
ORDER_REPORT=""
if [[ "$AUDIT_OK" == "1" ]]; then
  ACTION="$(python3 - "$JOURNAL_DB" <<'PY' 2>/dev/null
import json, sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
r = c.execute("SELECT decision_json FROM decisions ORDER BY ts_utc DESC LIMIT 1").fetchone()
print(json.loads(r[0]).get("action", "") if r else "")
PY
)"
  if [[ "$ACTION" == "TRADE" ]]; then
    # Orchestrierungsschicht: liest den Entscheid aus dem Journal und ruft die
    # entkoppelte broker.validate_order() (der Broker kennt das Journal nicht).
    # F4 (Live-Befund 22.07.): stdout (JSON) und stderr (Logging) TRENNEN -- das
    # frühere 2>&1 mischte Log-Zeilen ins JSON, der Parser fiel dadurch jedes Mal
    # auf "siehe Log" zurück. stderr läuft jetzt regulär ins cron.log durch; das
    # JSON geht über eine Temp-Datei (statt fragilem '''$VAL'''-Heredoc, das an
    # Anführungszeichen in der Begründung scheitern konnte) und wird defensiv
    # aus dem ersten..letzten Brace extrahiert.
    VAL_FILE="$(mktemp)"
    python3 orchestrate.py validate-decision --journal-db "$JOURNAL_DB" \
      --account "$ACCOUNT" ${DEPOT:+--depot "$DEPOT"} >"$VAL_FILE"
    ORDER_REPORT="$(python3 - "$VAL_FILE" <<'PY' 2>/dev/null
import json, sys
try:
    raw = open(sys.argv[1], encoding="utf-8").read()
    start, end = raw.find("{"), raw.rfind("}")
    d = json.loads(raw[start:end + 1] if start != -1 and end > start else raw)
    st = d.get("validation_status")
    if st in (200, 201):
        v = d["cost_indication"]["values"][0]
        print(f"Order validiert @ {v.get('venueName')} | Kauf {v['purchaseCosts']['sum']['value']} EUR | Status {st}")
    else:
        print(f"Order-Validierung Status {st}: {d.get('validation_body') or d.get('skipped')}")
except Exception:
    print("Order-Validierung: siehe Log")
PY
)"
    rm -f "$VAL_FILE"
  fi
fi

# A3 -- effektiven Ausführungsmodus sichtbar machen: die Scharfschaltung (live) steht
# damit täglich belegt im Report. _resolve_mode ist die eine Quelle der Wahrheit.
MODE_EFF="$(python3 -c "import orchestrate, config; \
print(orchestrate._resolve_mode(config.agent_env(), '$ACCOUNT'))" 2>/dev/null || echo '?')"

# Report bauen und an Gruppe senden
REPORT="$(python3 notify.py --journal-db "$JOURNAL_DB" 2>/dev/null)"
REPORT="$REPORT
Modus[$ACCOUNT]: $MODE_EFF
$ML_LINE"
[[ -n "$ORDER_REPORT" ]] && REPORT="$REPORT
$ORDER_REPORT"
[[ -n "$REPORT" ]] && send_group "$REPORT"

# Betriebs-Alarm, falls Auditor das LLM/den Endpoint nicht erreicht hat
if [[ "$AUDIT_OK" == "1" ]]; then
  VALIDATOR="$(python3 - "$JOURNAL_DB" <<'PY' 2>/dev/null
import json, sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
r = c.execute("SELECT validator, error FROM decisions ORDER BY ts_utc DESC LIMIT 1").fetchone()
print((r[0] or "") + "|" + (r[1] or "") if r else "")
PY
)"
  case "$VALIDATOR" in
    reject_unreachable*|*"|"?*) send_alert "Auditor-Problem: $VALIDATOR" ;;
  esac
fi

echo "[$STAMP] Zyklus beendet. Letzter Journal-Eintrag:"
python3 auditor.py --journal-db "$JOURNAL_DB" history --limit 1

# OP2+M4 Heartbeat: Zyklus-Stempel schreiben + spiegeln (heartbeat_cycle -- auch an
# allen frühen Ausgängen gesetzt; cycle_watch auf der collector-vm prüft werktags
# 14:30 UTC, ob der Stempel von HEUTE ist).
heartbeat_cycle

# OP3: Journal-Backup (konsistenter .backup-Snapshot) auf die collector-vm spiegeln
# (best-effort, einweg agent->collector; bricht den Zyklus nicht ab).
if sqlite3 "$JOURNAL_DB" ".backup decisions_sync.sqlite" 2>/dev/null \
   && rsync -az decisions_sync.sqlite me@10.128.0.3:/home/me/sync/decisions_backup.sqlite 2>/dev/null; then
  echo "[$STAMP] Journal-Backup zur collector-vm ok."
else
  echo "[$STAMP] WARN: Journal-Backup-Sync zur collector-vm fehlgeschlagen (best-effort)."
fi
