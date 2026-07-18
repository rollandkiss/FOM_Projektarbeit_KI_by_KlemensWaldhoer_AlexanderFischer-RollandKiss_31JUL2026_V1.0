#!/usr/bin/env bash
# =============================================================================
#  Compliance-Check fuer die FOM-LaTeX-Vorlage
#
#  Vor jeder Abgabe ausfuehren:
#      $ ./scripts/check.sh
#
#  Prueft die wichtigsten formalen Anforderungen aus dem FOM-Leitfaden
#  Kap. 7.4 ("Qualitaetssicherung") sowie typische Latex-Fehler.
# =============================================================================

set -u
cd "$(dirname "$0")/.."

PASS=0
FAIL=0
WARN=0

ok()    { echo "  [ OK ] $1"; PASS=$((PASS+1)); }
fail()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
warn()  { echo "  [WARN] $1"; WARN=$((WARN+1)); }

echo "=========================================================="
echo "  FOM-Compliance-Check"
echo "=========================================================="

# 1) Existieren alle Pflichtdateien?
echo ""
echo "[1] Projektstruktur"
for f in main.tex config.md verzeichnisse/references.bib main.xmpdata \
         titelblatt/titelblatt.tex sperrvermerk/sperrvermerk.tex \
         verzeichnisse/abkuerzungen.tex verzeichnisse/abkuerzungen-defs.tex \
         verzeichnisse/glossar.tex verzeichnisse/glossar-defs.tex \
         verzeichnisse/symbolverzeichnis.tex verzeichnisse/ki-verzeichnis.tex \
         anhang/anhang.tex anhang/ki-prompts.tex \
         anhang/ki-prompts/tool-chatgpt.tex anhang/ki-prompts/tool-claude.tex \
         anhang/eigenstaendigkeitserklaerung.tex; do
    if [[ -f "$f" ]]; then ok "$f vorhanden"
    else fail "$f fehlt"
    fi
done

# 2) Kompiliert das Dokument?
echo ""
echo "[2] LaTeX-Kompilierung"
if command -v latexmk >/dev/null 2>&1; then
    if latexmk -pdf -shell-escape -interaction=nonstopmode -halt-on-error \
        main.tex >/tmp/fom_check.log 2>&1; then
        ok "latexmk -pdf laeuft fehlerfrei durch"
    else
        fail "latexmk ist mit Fehler abgebrochen (siehe /tmp/fom_check.log)"
    fi
else
    warn "latexmk nicht installiert -- Kompilierungs-Test uebersprungen"
fi

# 3) Unaufgeloeste Verweise?
echo ""
echo "[3] Quer- und Quellverweise"
if [[ -f main.log ]]; then
    if grep -q "There were undefined references" main.log; then
        fail "main.log: 'undefined references'"
    else
        ok "Keine 'undefined references'"
    fi
    if grep -q "Citation .* on page .* undefined" main.log; then
        fail "main.log: undefinierte Citation"
    else
        ok "Alle \\cite{} aufgeloest"
    fi
    if grep -q "There were multiply-defined labels" main.log; then
        fail "main.log: doppelt definierte Labels"
    else
        ok "Keine doppelten Labels"
    fi
else
    warn "main.log nicht vorhanden -- Verweis-Check uebersprungen"
fi

# 4) TODO/FIXME-Marker im Quelltext?
echo ""
echo "[4] Offene Markierungen (TODO/FIXME/XXX)"
TODO_COUNT=$(grep -rIn -E 'TODO|FIXME|XXX' \
    --include='*.tex' --include='*.bib' \
    . 2>/dev/null | grep -v '^\./scripts/' | wc -l | tr -d ' ')
if [[ "$TODO_COUNT" -eq 0 ]]; then
    ok "Keine offenen Markierungen"
else
    warn "Es bestehen $TODO_COUNT offene TODO/FIXME/XXX-Markierungen"
    grep -rIn -E 'TODO|FIXME|XXX' --include='*.tex' --include='*.bib' \
        . 2>/dev/null | grep -v '^\./scripts/' | head -5
fi

# 5) Pflichtangaben im Titelblatt
echo ""
echo "[5] Pflichtangaben Titelblatt (Leitfaden Kap. 5.1, Anhang I)"
for cmd in arbeitTitel arbeitArt arbeitStudiengang autorVorname \
           autorNachname autorMatrikel erstgutachter abgabedatum; do
    if grep -q "newcommand{\\\\$cmd}" main.tex; then
        ok "\\$cmd definiert"
    else
        fail "\\$cmd fehlt"
    fi
done

# 6) Eigenstaendigkeitserklaerung-Wortlaut?
echo ""
echo "[6] Eigenstaendigkeitserklaerung (Leitfaden Kap. 7.2)"
if grep -q "Hiermit versichere ich" anhang/eigenstaendigkeitserklaerung.tex; then
    ok "Originalwortlaut der Erklaerung erkannt"
else
    fail "Erklaerungs-Wortlaut weicht vom FOM-Standard ab"
fi

# 7) Bibliographie / KI-Verzeichnis
echo ""
echo "[7] Quellen- und KI-Verzeichnis (Kap. 6.3 / 6.2.4)"
if [[ -f verzeichnisse/references.bib ]]; then
    ENTRIES=$(grep -c '^@' verzeichnisse/references.bib)
    KI_ENTRIES=$(grep -c 'keywords *= *{ki}' verzeichnisse/references.bib || true)
    ok "verzeichnisse/references.bib: $ENTRIES Eintraege ($KI_ENTRIES davon KI)"
else
    fail "verzeichnisse/references.bib fehlt"
fi

# 8) Akzentlinien / Goldener-Schnitt-Werte
echo ""
echo "[8] Goldener-Schnitt-Konsistenz"
if grep -q "0.618cm" main.tex && grep -q "8.35cm" main.tex; then
    ok "Einrueckung 0,618 cm und Akzentlinie 8,35 cm gesetzt"
else
    warn "Akzentlinien-Werte wurden manuell veraendert"
fi

# Zusammenfassung
echo ""
echo "=========================================================="
echo "  Ergebnis: $PASS OK | $WARN WARN | $FAIL FAIL"
echo "=========================================================="

if [[ "$FAIL" -gt 0 ]]; then exit 1; fi
exit 0
