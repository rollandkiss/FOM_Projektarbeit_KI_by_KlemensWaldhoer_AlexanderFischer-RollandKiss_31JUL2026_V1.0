# config.md — Konfiguration und Datei-Hinweise

**Projekt:** FOM-Projektarbeit „OpenClaw als Trading Agent" · Fischer / Kiss · Abgabe 31.07.2026
**Erstgutachter:** Prof. Dr. Klemens Waldhör · **Studiengang:** Informatik (B.Sc.), Hochschulzentrum DLS
**Vorlagenherkunft:** LaTeX-Gerüst der BPMN-Thesis bei Prof. Peter Preuss, für diese Arbeit angepasst
**Stand dieser Datei:** 25.07.2026

---

## 0 Wozu diese Datei

Jede `.tex`- und `.bib`-Datei des Projekts trägt im Kopf nur zwei Zeilen:

```latex
%  Datei-Hinweise & Konfiguration: siehe config.md
%  -> Abschnitt "kapitel/02_grundlagen.tex"
```

Der eigentliche Erklärungstext steht hier. Das hält die Quelldateien schlank und verhindert, dass dieselbe Begründung an fünf Stellen gepflegt werden muss. **Kapitel 4 dieser Datei enthält für jede Projektdatei einen gleichnamigen Abschnitt** — der Verweis im Dateikopf ist also wörtlich als Sprungmarke zu lesen.

Ausführliche Begründungen einzelner Einstellungen stehen weiterhin als Inline-Kommentar direkt an der betreffenden Zeile in `main.tex`. Diese Datei erklärt das *Warum* auf Projektebene, `main.tex` das *Warum* auf Zeilenebene.

**Zur Vorlagenherkunft:** Das Gerüst stammt aus der BPMN-Thesis bei Prof. Peter Preuss. Übernommen wurden Verzeichnisaufbau, KOMA-Optionen, die Verzeichnis-Auslagerung unter `verzeichnisse/` und das Prinzip, jede Formatentscheidung gegen den FOM-Leitfaden zu belegen. Ersetzt wurden Zitierstil (jetzt Chicago Notes statt Autor-Jahr im Text), Schriftfamilie, Farbschema und sämtliche fachlichen Inhalte. Reste der Vorlage, die noch zu bereinigen sind, stehen in Kapitel 8.

---

## 1 Projektstruktur

```
├── main.tex                      Präambel, Metadaten, Dokumentgerüst
├── main.xmpdata                  PDF/A-Metadaten (Pflicht für pdfx)
├── titelblatt/
│   └── titelblatt.tex
├── kapitel/
│   ├── 01_einleitung.tex         Problemstellung, Zielsetzung, Methodik
│   ├── 02_grundlagen.tex         Agentenbegriff, Periodensystem, Abgrenzung
│   ├── 03_umsetzung.tex          Fallstudie: Architektur → Strategie →
│   │                             Implementierung → Evaluation → Würdigung
│   └── 04_schluss.tex            Fazit, Diskussion, Ausblick
├── verzeichnisse/
│   ├── references.bib            EINZIGE Quelle für Literatur- UND KI-Verzeichnis
│   ├── abbildungsverzeichnis.tex
│   ├── abkuerzungen.tex          Ausgabe   ─┐
│   ├── abkuerzungen-defs.tex     Einträge  ─┘ Paar
│   ├── formelzeichen.tex         Ausgabe   ─┐
│   ├── formelverzeichnis-defs.tex Einträge ─┘ Paar
│   ├── glossar.tex               Ausgabe   ─┐
│   ├── glossar-defs.tex          Einträge  ─┘ Paar
│   ├── tabellenverzeichnis.tex
│   └── ki-verzeichnis.tex        Kap. 6.2.4, direkt hinter der Literatur
├── anhang/
│   ├── anhang.tex                Klammer, bindet die drei Anhänge ein
│   ├── quellcodearchiv.tex       Anhang I   (Rahmentext, handgepflegt)
│   ├── archiv_meta.tex           Anhang I   GENERIERT
│   ├── archiv_quellcode.tex      Anhang I   GENERIERT
│   ├── archiv_tests.tex          Anhang I   GENERIERT
│   ├── archiv_daten.tex          Anhang I   GENERIERT
│   ├── evaluationsbelege.tex     Anhang II
│   ├── belege/                   Rohprotokolle für Anhang II
│   ├── ki-prompts.tex            Anhang III, definiert \kiwerkzeug
│   ├── ki-prompts/tool-claude.tex   Claude Opus 5      (III.I)
│   ├── ki-prompts/tool-fable.tex    Claude Fable 5     (III.II)
│   ├── ki-prompts/tool-gemini.tex   Gemini 3.5 Flash   (III.III)
│   └── eigenstaendigkeitserklaerung.tex   ohne Seitenzahl, nicht im ToC
├── abbildungen/                  ausschließlich SVG
└── scripts/
    ├── check.sh                  Compliance-Check vor jeder Abgabe
    └── config.md                 diese Datei
```

**Nicht im Repository:** `sperrvermerk/sperrvermerk.tex` (in `main.tex` auskommentiert vorbereitet, nur bei vertraulichen Arbeiten anlegen) und das Quelltextverzeichnis (bewusst deaktiviert, siehe Abschnitt 4.1).

---

## 2 Build

### 2.1 Overleaf (Arbeitsumgebung)

**Regel: drei Durchläufe plus biber, bis das PDF konvergiert.** Zwei aufeinanderfolgende Läufe mit identischem Ergebnis gelten als konvergiert.

Ohne biber verschwinden Literatur- und KI-Verzeichnis **stillschweigend** — das PDF wird zwei Seiten kürzer, ohne dass eine Fehlermeldung erscheint. Die Zahlen im Inhaltsverzeichnis können einen Lauf hinterherhinken.

Overleaf-Einstellungen: Compiler `pdfLaTeX`, `--shell-escape` ist dort für das `svg`-Paket vorkonfiguriert.

### 2.2 Lokal

```bash
latexmk -pdf -shell-escape main.tex
```

`--shell-escape` ist zwingend: Das `svg`-Paket ruft Inkscape auf, um die Vektorgrafiken zu konvertieren. Ohne die Option bricht der Lauf ab.

### 2.3 Kompilierumgebung neu aufsetzen (Container/CI)

```bash
apt-get install -y texlive-bibtex-extra texlive-fonts-extra \
                   texlive-fonts-recommended texlive-lang-german \
                   texlive-latex-extra texlive-latex-recommended \
                   texlive-plain-generic texlive-science cm-super biber
apt-get update && apt-get install -y inkscape
```

Zwei Stolperstellen, beide real aufgetreten:

- **`tracklang.sty not found`** — fehlt in der Minimalinstallation, steckt in `texlive-latex-recommended`.
- **Inkscape-Installation schlägt mit 404 fehl**, wenn `apt-get update` nicht unmittelbar davor lief.

---

## 3 Globale Konfiguration (`main.tex`)

Jede Einstellung ist gegen den FOM-Leitfaden belegt; die Kapitelangaben stehen als Inline-Kommentar an der jeweiligen Zeile.

| Bereich | Einstellung | Leitfaden |
|---|---|---|
| Dokumentklasse | `scrreprt`, A4, 11 pt, einseitig, `parskip=half-`, `numbers=noenddot` | 5.2.1 / 5.2.3 / 5.3 |
| Ränder | oben 4 cm, unten 2 cm, links 4 cm, rechts 2 cm | 5.2.1 |
| Zeilenabstand | 1,5-zeilig (`\onehalfspacing`) | 5.2.2 |
| Schrift | Open Sans 11 pt (`scale=0.97`), Inconsolata als Fixtype | 5.2.2 / 5.9.2 |
| Sprache | `main=ngerman`, Zusatzsprache `english`; `\enquote{}` via csquotes | 2.2 |
| Fußnoten | 10 pt, durchlaufend über das ganze Dokument (`\counterwithout`) | 5.4 |
| Abbildungen | Unterschrift **unter** dem Bild, Quellenzeile linksbündig | 2.3 / 5.7 |
| Tabellen | Titel **über** der Tabelle, `booktabs` | 2.4 / 5.6 |
| Formeln | je Kapitel nummeriert, Variablen kursiv (`mathastext`) | 5.8 |
| Listings | Zeilennummern links, Titel über dem Listing, Rahmen | 5.9.2 |
| Querverweise | `cleveref` mit ausgeschriebenen Bezeichnern | 2.3 / 2.4 |
| Zitierstil | biblatex-chicago, **Notes & Bibliography**, `sorting=nyt` | 6.2 / 6.3 |
| PDF-Ausgabe | PDF/A-2b via `pdfx` | 5.10.2 |
| Umbruch | `\clubpenalty`/`\widowpenalty` = 10000 | 7.4 |

### 3.1 Akzentlinien — die Goldener-Schnitt-Familie

Alle Zierlinien folgen einer gemeinsamen φ-Familie, damit die Maße nicht willkürlich wirken:

| Linie | Länge | Herleitung |
|---|---|---|
| Seitenzahl-Einrückung | 0,618 cm | 1/φ |
| Akzentlinie Seitenzahl (rechts) | 1,5 cm | Basis |
| Fußnoten-Trennlinie | 6,354 cm | 1,5 · φ³ |
| Kapitellinie (links) | 9,35 cm | 1,5 · φ³ + 3 cm |

Strichstärke durchgehend 0,8 pt, Farbe `FOMakzent` (#239F91, FOM-Petrolgrün). `scripts/check.sh` prüft die beiden Schlüsselwerte 0,618 cm und 9,35 cm und warnt, wenn sie von Hand verändert wurden.

### 3.2 Reihenfolge der Pakete — nicht umsortieren

1. `pdfx` **lädt hyperref intern.** Ein separates `\usepackage{hyperref}` davor oder danach bricht den Lauf. Konfiguration nur über `\hypersetup{}`.
2. `glossaries-extra` **muss nach** hyperref und biblatex geladen werden.
3. Der `\mkbibfootnote`-Patch (siehe 3.3) muss **nach** biblatex-chicago stehen, weil er dessen Definition überschreibt.

### 3.3 Der Fußnoten-Patch

`footmisc[multiple]` setzt zwischen direkt aufeinanderfolgenden Fußnotenmarken einen Trenner, damit aus „¹²" ein „¹ ²" wird. Dazu platziert das Paket hinter jeder Marke einen Kern-Marker. biblatex' Standard-`\mkbibfootnote` beginnt jedoch mit `\unspace` und löscht genau diesen Marker — die Erkennung schlägt fehl.

`main.tex` definiert `\mkbibfootnote` deshalb ohne das führende `\unspace` neu. Der Patch wirkt für `\footcite`, `\autocite` und damit auch für die Wrapper `\zit` und `\zitw`.

**Praktische Folge:** `\zit[S.~15]{bitkom2018}\zit{hammond2016}` erzeugt zwei sauber getrennte Marken. Diese Konstruktion wird in `kapitel/02_grundlagen.tex` genutzt.

### 3.4 Glossar-Infrastruktur

Drei Listen über dieselbe `glossaries-extra`-Infrastruktur:

| Typ | Verzeichnis | Definitionen | Style |
|---|---|---|---|
| `main` | Glossar | `glossar-defs.tex` | `fomgloss` |
| `abbreviations` | Abkürzungsverzeichnis | `abkuerzungen-defs.tex` | `fomabbr` |
| `symbols` | Formelzeichenverzeichnis | `formelverzeichnis-defs.tex` | `fomabbr` |

`\makenoidxglossaries` statt `\makeglossaries`: verzichtet auf externes `makeglossaries`/`bib2gls` und erlaubt reines pdfLaTeX-Kompilieren — das ist der Overleaf-Default. Sortiert wird beim Drucken durch TeX.

Die Styles `fomabbr` und `fomgloss` sind in `main.tex` per `\newglossarystyle` selbst definiert, damit sie nicht von `stylemods` abhängen. Ohne `stylemods=list` würde `style=altlist` mit „Undefined control sequence" abbrechen.

---

## 4 Datei-Hinweise

### 4.1 `main.tex`

Präambel, Metadaten und Dokumentgerüst. Fachlicher Inhalt gehört ausschließlich in die eingebundenen Dateien.

**Metadatenblock** (ab „METADATEN"): Titel, Untertitel, Art der Arbeit, Studiengang, beide Autoren mit Matrikelnummern, Gutachter, Abgabedatum, Ort. `scripts/check.sh` prüft die Existenz aller Pflichtmakros.

> **Vor der finalen Abgabe** `\abgabedatum` auf das tatsächliche Datum festsetzen, damit es bei späteren Re-Compiles nicht mitwandert.

**Dokumentreihenfolge:** Titelblatt (Seite i) → Inhaltsverzeichnis → Vorspann-Verzeichnisse → Hauptteil (arabisch ab 1) → Literaturverzeichnis → KI-Verzeichnis → Anhang → Eigenständigkeitserklärung.

**Vorspann-Verzeichnisse alphabetisch:** Abbildungs- → Abkürzungs- → Formelzeichen- → Glossar → Tabellenverzeichnis. Zwischen ihnen **kein** `\clearpage` — jedes Verzeichnis ruft intern `\chapter*` auf und beginnt damit selbst eine neue Seite; ein zusätzliches `\clearpage` erzeugt eine leere Zwischenseite mit der Kopfmarke des Vorgängers.

**Anhang-Nummerierung:** große römische Ziffern auf allen Ebenen (I, I.I, I.I.I).

> **Anhang-Kapitel NIE mit `\cref` referenzieren.** cleveref rendert je nach Version „Kapitel I" statt „Anhang I". Versionsunabhängig ist `Anhang~\ref{label}`.

**ToC-Umbruch vor dem Anhang:** `\addtocontents{toc}{\protect\newpage}` direkt vor `\include{anhang/anhang}` sorgt dafür, dass die Anhang-Einträge im Inhaltsverzeichnis auf einer neuen Seite beginnen. Wächst der Hauptteil weiter und wird die erste ToC-Seite dadurch zu leer, genügt es, **diese eine Zeile auszukommentieren**.

**Quelltextverzeichnis deaktiviert:** Der Leitfaden stuft es als optional ein (Kap. 5.1). Seit Anhang I nur noch auf das Quellcodearchiv verweist, enthält die Arbeit ein einziges Listing ohne `\caption` — `\lstlistoflistings` wäre leer. Die Aktivierungszeilen stehen auskommentiert bereit.

**Deaktivierte Verzeichnisse:** Algorithmen- und mathematisches Formelverzeichnis wurden vollständig entfernt; das Symbolverzeichnis existiert als Quelldatei nicht.

### 4.2 `references.bib`

**Einzige Quelle** für Literatur- **und** KI-Verzeichnis. Getrennt wird über das Schlüsselwort:

```latex
\defbibfilter{nichtKI}{not keyword=ki}   % → Literaturverzeichnis (Kap. 6.3)
\defbibfilter{nurKI}{keyword=ki}         % → KI-Verzeichnis (Kap. 6.2.4)
```

Nur Werkzeuge, die bei der **Erstellung der Arbeit** eingesetzt wurden, tragen `keywords = {ki}`. Modelle, die Gegenstand der Untersuchung sind, gehören ins normale Literaturverzeichnis.

**Gliederung:** thematische Blöcke, getrennt durch `% --- Kategorie ---`. Nicht bestandene Quellen stehen auskommentiert am Dateiende.

**Schlüsselkonvention:** Kurzform führend (`brock1992`), abweichende Schlüssel aus zugelieferten Bib-Dateien als `ids`-Alias — beide Zitierweisen funktionieren, niemand muss seinen Text anpassen.

**Vier Fallstricke, alle teuer gelernt:**

1. **`\url{...}` in einem `note`-Feld zerstört die Feldtrennung.** Symptom: ab dem betroffenen Eintrag bluten Felder (note, version, edition, subtitle, doi, url) in **alle alphabetisch folgenden** Einträge über. Das `.bbl` ist dabei korrekt — der Fehler entsteht erst beim Satz und sieht aus wie ein Klammerfehler. **URLs im note-Feld als Klartext schreiben.**
2. **`\autocite{}` in einem Feld** erzeugt eine Fußnote mitten im Literaturverzeichnis. Querverweise als Klartext formulieren.
3. **Das Feld `version` wird von biblatex-chicago bei `@online` nicht ausgegeben.** Versionsangaben zusätzlich ins `note`-Feld.
4. `\^{}` extrahiert unsauber — `\textasciicircum{}` verwenden.

**Datumslose `@online`-Einträge** erzeugen nur bei `biber --tool --validate-datamodel` Warnungen, nicht im normalen Lauf. Sie sind gewollt: Wo eine Seite keinen Stand ausweist, wird nach Abrufdatum zitiert statt ein Jahr zu erfinden.

**Nach dem Import aus einem Reference Manager prüfen:** Crossref führt bei Fama (1970) fälschlich Malkiel als Ko-Autor und weist zum Pardo-DOI abweichend 2012/1st ed. aus. Beide Korrekturen stehen im jeweiligen `note`-Feld und dürfen nicht überschrieben werden.

### 4.3 `titelblatt/titelblatt.tex`

Rein darstellend; sämtliche Inhalte kommen aus den Metadaten-Makros in `main.tex`. Logo als SVG über `\includesvg`, Akzentlinien in `FOMakzent`. Enthält keine Unterschrift — die steht ausschließlich in der Eigenständigkeitserklärung.

### 4.4 `kapitel/01_einleitung.tex`

Problemstellung, Zielsetzung und Relevanz, Vorgehen und Methodik (Leitfaden Kap. 5.1).

Der Beleg zum Element der prozesssteuernden Kontrolle zitiert `bitkom2018`, S. 18. Die Quelle formuliert dort „z. B. **automatisierter Handel**" — nicht „Börsenhandel". Der Fließtext ist entsprechend gefasst; wer ihn ändert, muss den Beleg mitprüfen.

### 4.5 `kapitel/02_grundlagen.tex`

Agentenbegriff, Einordnung über das Periodensystem, Abgrenzung regelbasiert/lernend.

Seitenanker in `bitkom2018`: S. 15 f. (Grundgedanke, LEGO-Metapher, 28 Elemente, Tripel Assess–Infer–Respond), S. 17 f. (Tabelle 1 mit allen Elementen und Gruppen), S. 19 f. (Einsatzszenarien: vergleichen, Reifegrad, organisationale Wirkung). Die Doppelfußnote in Zeile 88 belegt Sekundär- und Primärquelle nebeneinander.

### 4.6 `kapitel/03_umsetzung.tex`

Fallstudie in fester Reihenfolge: Architektur → Strategie → Implementierung → Evaluation → kritische Würdigung.

Alle Kennzahlen stammen aus **einem** Backtest-Lauf (Datenstand 22.07.2026). Kapitel und Beleg in Anhang II müssen stets derselbe Lauf sein — bei jeder Zahlenänderung beides gemeinsam anfassen.

Bindet `abbildungen/zielarchitektur.svg` ein. Das SVG ist attributbasiert gestylt (kein `<style>`-Block), damit die Inkscape-Konvertierung verlustfrei arbeitet.

### 4.7 `kapitel/04_schluss.tex`

Fazit, Diskussion, Ausblick. Enthält keine neuen Belege — alle Aussagen sind in Kap. 3 nachgewiesen.

### 4.8 `verzeichnisse/abbildungsverzeichnis.tex`

Kapselt `\listoffigures`, damit alle Vorspann-Verzeichnisse einheitlich unter `verzeichnisse/` liegen und in `main.tex` gleichartig per `\input` eingebunden werden. Der ToC-Eintrag entsteht automatisch aus der KOMA-Option `listof=totoc`. `\markboth` setzt die Kopfmarke explizit, weil es sich um ein einseitiges Verzeichnis handelt.

### 4.9 `verzeichnisse/abkuerzungen.tex` und `verzeichnisse/abkuerzungen-defs.tex`

Ausgabe- und Definitionsdatei. `-defs` enthält ausschließlich `\newabbreviation`-Einträge, die Ausgabedatei nur `\glsaddall` plus `\printnoidxglossary` im Style `fomabbr` (linke Spalte 2,5 cm fett, 0,7 cm Spaltenabstand, rechte Spalte Flattersatz).

`\glsaddall` nimmt alle Einträge auf, unabhängig davon, ob sie im Fließtext per `\gls` verwendet wurden.

Im Text `\glsxtrshort{api}` bzw. `\glsxtrlong{api}` verwenden; `\gls{api}` liefert beim ersten Auftreten automatisch „Application Programming Interface (API)" (Leitfaden Kap. 2.5).

### 4.10 `verzeichnisse/formelzeichen.tex` und `verzeichnisse/formelverzeichnis-defs.tex`

Alle in nummerierten Gleichungen verwendeten Formelzeichen mit Kurzerklärung. `\glsaddall` ist hier zwingend, weil Formelzeichen in Gleichungen als reine Mathematik gesetzt werden und nie per `\gls` auftauchen.

### 4.11 `verzeichnisse/glossar.tex` und `verzeichnisse/glossar-defs.tex`

Fachbegriffe im Style `fomgloss`: Begriff fett als eigener Absatz, Beschreibung eingerückt. `sort=word`.

### 4.12 `verzeichnisse/tabellenverzeichnis.tex`

Bis V1_7 ausgeblendet, weil die Arbeit keine Tabellen enthielt. Seit den Anhängen „Evaluationsbelege" (vier Tabellen) und „Projektstruktur und Quellcodearchiv" (vier Tabellen) nach Leitfaden Kap. 2.4 erforderlich. Aufbau analog zum Abbildungsverzeichnis.

### 4.13 `verzeichnisse/ki-verzeichnis.tex`

Gibt `\printbibliography[filter=nurKI]` aus, direkt hinter dem Literaturverzeichnis (Leitfaden Kap. 6.2.4). Die Einträge selbst stehen in `references.bib` mit `keywords = {ki}`; ins Verzeichnis gelangen sie über `\nocite{<key>}` in der jeweiligen Tool-Datei unter `anhang/ki-prompts/`.

### 4.14 `anhang/anhang.tex`

Reine Klammer, bindet die drei Anhänge in fester Reihenfolge ein: Quellcodearchiv (I) → Evaluationsbelege (II) → KI-Prompts (III). Kein eigener Inhalt.

### 4.15 `anhang/quellcodearchiv.tex` und die vier `archiv_*.tex`

Anhang I. **`quellcodearchiv.tex` ist handgepflegt; `archiv_meta.tex`, `archiv_quellcode.tex`, `archiv_tests.tex` und `archiv_daten.tex` sind generiert und dürfen nicht von Hand editiert werden.**

Regenerierung:

1. `archiv_manifest.csv` im Projektordner anpassen (neue Datei = neue Zeile; Status „geplant" → „aktiv").
2. `python3 build_quellcode_archiv.py --version V3 --datum <TTMONJJJJ>`
3. Die vier generierten Dateien aus `archiv_tex/` nach Overleaf in `anhang/` hochladen und ersetzen.

Das Skript setzt deterministische Zip-Zeitstempel, damit die SHA-256-Summe reproduzierbar bleibt; sie wandert automatisch in `archiv_meta.tex` und von dort in den Anhangstext.

> Journal-Belege **vor** der Aufnahme ins Archiv manuell redigieren (Depot-, Konto- und Order-IDs).

### 4.16 `anhang/evaluationsbelege.tex`

Anhang II: reproduzierbare Belege der in Kap. 3.4 genannten Kennzahlen. Protokolle unverändert von den Produktionssystemen, nur typografisch aufbereitet. Umfangreiche Rohdaten werden durch Herkunftsangaben referenziert statt abgedruckt.

Die `\lstset`-Literate-Tabelle am Dateianfang ist erforderlich, weil pdfLaTeX plus `listings` UTF-8-Mehrbytezeichen (ä, ö, ü, ß, €, §) nur über diese Zuordnung verarbeitet.

### 4.17 `anhang/ki-prompts.tex`

Anhang III, Rahmentext und Definition des Makros `\kiwerkzeug{Tool}{Version}{URL}`. Nach Leitfaden Kap. 6.2.4 werden die Prompts in Kurzform dokumentiert — ohne die generierten Antworten. Bindet die drei Werkzeugdateien in fester Reihenfolge ein: Opus 5 (III.I) → Fable 5 (III.II) → Gemini (III.III).

Der Einleitungsabsatz nennt die Zwecke je Werkzeug. **Wird eine Werkzeugdatei geändert, ist dieser Absatz mitzuführen** — er ist die einzige Stelle, an der die Aufgabenteilung zusammenhängend beschrieben wird.

> **Festlegung:** Prompts zur Kapitelerstellung bleiben im KI-Anhang unerwähnt. Dokumentiert werden ausschließlich die unten je Werkzeug genannten Zwecke.

### 4.18 `anhang/ki-prompts/tool-claude.tex`

**Anthropic Claude Opus 5** (`claude-opus-5`, 24.07.2026), `\nocite{claudeOpus2026}`.

Zwecke: Glossar, Formelzeichenverzeichnis, Aufbau des Anhangs, Anpassung des Extraktionsskripts für die Code- und Datenquellen der Cloud-Umgebung, Erzeugung der Testskripte zur Ergebniskontrolle.

Der Abschnitt zum Extraktionsskript hält ausdrücklich fest, dass das Skript aus vorangegangenen Projekten bereits vorlag und nur angepasst wurde — diese Einordnung nicht streichen, sie betrifft die Eigenständigkeit.

### 4.19 `anhang/ki-prompts/tool-fable.tex`

**Anthropic Claude Fable 5** (`claude-fable-5`, 09.06.2026), `\nocite{claudeFable2026}`.

Zwecke: Unterstützung bei Python- und Shell-Skripten, Cloud-Run-Anwendung des öffentlichen Dashboards, Verifikation der mit Opus 5 erzeugten Testskripte, Erzeugung der beiden als KI-generiert gekennzeichneten Abbildungen (`fig:zielarchitektur`, `fig:projektstruktur`).

Die Arbeitsteilung bei den Tests ist bewusst dokumentiert: **Opus 5 entwirft, Fable 5 prüft gegen.** Wer das ändert, muss beide Dateien anfassen.

### 4.20 `anhang/ki-prompts/tool-gemini.tex`

**Google Gemini 3.5 Flash** (`gemini-3.5-flash`, Model Card 19.05.2026), `\nocite{geminiFlash2026}`.

Zwecke: Rechtschreib- und Interpunktionsprüfung, Quellenrecherche, Abgleich der automatischen Tests, Erstellung des Abkürzungsverzeichnisses.

> Die Modellbezeichnung lautet „Gemini 3.5 Flash", nicht „Gemini Flash 3.5". Nicht verwechseln mit 3.5 Flash-Lite, 3.5 Flash Cyber und 3.6 Flash (alle 21.07.2026).

### 4.21 `anhang/eigenstaendigkeitserklaerung.tex`

Letztes Blatt, ohne Seitenzahl und ohne Eintrag im Inhaltsverzeichnis (Leitfaden Kap. 7.2). `scripts/check.sh` prüft, ob der Originalwortlaut („Hiermit versichere ich") erhalten ist.

> **Offener Punkt:** Nur `abbildungen/unterschrift_rolland.svg` liegt vor und ist eingebunden. Für den zweiten Autor steht in Zeile 56 ein `\rule{0pt}{1.1cm}`-Platzhalter; die passende `\includesvg`-Zeile ist darüber auskommentiert vorbereitet. Sobald `unterschrift_alexander.svg` vorliegt, Platzhalter durch die auskommentierte Zeile ersetzen.

---

## 5 Konventionen

### 5.1 Zitieren

Zwei Wrapper statt `\footcite` direkt:

```latex
\zit[S.~24]{schluessel}    % → Fußnote mit vorangestelltem "Vgl."
\zitw[S.~24]{schluessel}   % → ohne "Vgl." (wörtliches Zitat)
```

Chicago Notes liefert bei der Erstnennung den Vollbeleg, danach automatisch die Kurzform. Seitenangaben immer mit geschütztem Leerzeichen: `S.~15`. Folgeseite als `S.~17\,f.`, mehrere Stellen als `S.~15, 17\,f.`.

> **Seitenzahlen mehrfach verifizieren.** Ein Beleg, der auf die falsche Seite zeigt, ist schlimmer als kein Beleg.

### 5.2 Abbildungen

Ausschließlich SVG in `abbildungen/`, eingebunden per `\includesvg`. Jede Caption schließt mit einem Quellenmakro:

```latex
\caption[Kurztitel]{Langtitel.\quelleeigen}                    % eigene Darstellung
\caption[Kurztitel]{Langtitel.\quelleeigenki}                  % KI-generiert (Kap. 6.2.4)
\caption[Kurztitel]{Langtitel.\quellefremd{Autor (Jahr, S.~X)}}
```

Der Kurztitel in eckigen Klammern erscheint im Abbildungsverzeichnis, der Langtitel unter dem Bild.

### 5.3 Neue Systemkomponenten

Werden Komponenten erstmals erwähnt, gehören sie in die Setup-Beschreibung in Kap. 1 — nicht beiläufig mitten in Kap. 3.

### 5.4 Schemata und Zahlen

Datenschemata, Feldnamen und Kennzahlen niemals aus dem Gedächtnis rekonstruieren, sondern aus Quelltext oder Protokoll übernehmen.

---

## 6 Qualitätssicherung

`./scripts/check.sh` vor jeder Abgabe ausführen. Geprüft werden: Vollständigkeit der Pflichtdateien, fehlerfreier `latexmk`-Durchlauf, undefinierte Referenzen und Zitate, doppelte Labels, offene TODO/FIXME/XXX-Marker, Pflichtangaben des Titelblatts, Wortlaut der Eigenständigkeitserklärung, Einträge in `references.bib` inklusive KI-Anteil, Goldener-Schnitt-Werte der Akzentlinien.

Rückgabewert 0 bei bestandener Prüfung, 1 bei mindestens einem `[FAIL]`.

Ergänzend nach jeder größeren Bib-Änderung:

```bash
biber --tool --validate-datamodel --output-file=/dev/null verzeichnisse/references.bib
```

---

## 7 Formale Endkontrolle vor Abgabe

- [ ] `\abgabedatum` fest eingetragen
- [ ] `./scripts/check.sh` ohne `[FAIL]`
- [ ] Drei Läufe plus biber bis Konvergenz, PDF unverändert
- [ ] Literatur- **und** KI-Verzeichnis im PDF vorhanden (Indikator für gelaufenes biber)
- [ ] Wortbilanz im Zielband
- [ ] Unterschriften auf Titelblatt und Eigenständigkeitserklärung geklärt
- [ ] SHA-256 des Quellcodearchivs im Anhang stimmt mit der ausgelieferten Zip überein
- [ ] Nach Abgabe: Credentials und Bucket-Namen rotieren (im Archiv enthalten)

---

## 8 Offene Vorlagenreste

Alle aus dem BPMN-Gerüst stammenden Textreste sind beseitigt (Stand 25.07.2026):

| Ort | Rest | Status |
|---|---|---|
| `anhang/ki-prompts/tool-gemini.tex` | Recherche-Prompts zu BPMN 2.0 und ERP | **erledigt** — durch die tatsächlich verwendeten Prompts ersetzt |
| `main.tex`, Dateikopf | Hinweis auf „BPMN-Modelle" | **erledigt** |
| `main.tex`, `\svgsetup`-Zeile | Kommentar nannte „BPMN-Modelle" | **erledigt** — jetzt „Architektur- und Strukturgrafiken" |

Eine Volltextsuche über das kompilierte PDF findet weder „BPMN" noch „ERP".

Nicht aus der Vorlage, aber weiterhin offen: der Unterschriften-Platzhalter des zweiten Autors in `anhang/eigenstaendigkeitserklaerung.tex` (siehe Abschnitt 4.21).

`scripts/check.sh` meldet unverändert **einen** `[WARN]` zu einer angeblich offenen `XXX`-Markierung. Es handelt sich um einen Fehlalarm: Getroffen wird die redigierte Telefonnummer `+49XXXXXXXXXXX` in `anhang/quellcodearchiv.tex`, kein Bearbeitungsvermerk.
