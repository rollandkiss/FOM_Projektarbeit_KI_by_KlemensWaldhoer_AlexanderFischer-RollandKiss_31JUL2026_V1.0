#!/usr/bin/env python3
"""
broker.py -- comdirect-REST-API-Anbindung des Handelsagenten.

Implementiert den dokumentierten Ablauf (comdirect, 2026):
  Auth (Kap. 2):  ROPC -> Session-Objekt -> Session-TAN validieren ->
                  Session-TAN aktivieren (P_TAN_PUSH) -> CD Secondary Flow
  Keep-Alive:     Refresh-Token-Flow (Kap. 3.1.1)
  Read-only:      Kontosalden (Kap. 4), Depots/Positionen (Kap. 5),
                  Instrument (Kap. 6)
  Order:          Prevalidation -> **Validation** -> Ex-Ante-Kosten (Kap. 7)

ORDER-AUSFÜHRUNG (EXECUTION_MODE-gegatet):
  `place_order()` ist implementiert (Validation -> Execution via aktiver Session-TAN),
  aber standardmäßig im DRY-RUN: es validiert und protokolliert, POSTet aber NICHT
  `/orders`. Eine reale Orderanlage erfolgt nur mit `execute=True`, das die
  Orchestrierung ausschließlich bei `EXECUTION_MODE=live` (agent.env) setzt. Auslieferung
  = dry_run; die Scharfschaltung (Echtgeld) ist eine bewusste Nutzerhandlung in
  Eigenverantwortung. Autorisierung des Handelstags = einmalige photoTAN-Freigabe beim
  Login (Session-TAN, danach `TAN_FREI`).

Sicherheit:
  * Zugangsdaten ausschließlich über credstore.get_credentials() (nie im Klartext,
    nie in Logs/Prompts). Tokens in ~/.fom-agent/session.json (0600), kurzlebig.
  * Session-TAN-Freigabe per photoTAN-Push -> der TanConfirmer alarmiert den Nutzer;
    broker.py erhält/verarbeitet die TAN selbst niemals.

Architektur (Code-Review 20.07., vgl. SYSTEMDOKUMENTATION §5):
  * BrokerSession kapselt den veränderlichen Sitzungszustand (Tokens, sessionId,
    identifier, Ablauf) samt Header-Erzeugung, Gültigkeit/Refresh, Persistenz und
    Auth-Flow. Das On-Disk-Format von session.json bleibt kompatibel.
  * TanConfirmer entkoppelt die photoTAN-Freigabe vom konkreten Kanal (Signal/
    Terminal/Test-Stub) und ist in activate_tan injizierbar.
  * Fehler werden als BrokerError geworfen (Ausnahmehierarchie statt SystemExit an
    der Fehlerstelle): So ist das Modul ohne Prozessabbruch als Bibliothek nutzbar;
    erst die CLI-Schicht (main) übersetzt BrokerError in Exit-Code 1.
  * Die Verbindung Auditor-Entscheid -> Order-Validierung liegt bewusst NICHT hier,
    sondern in orchestrate.py -- der Broker kennt das Auditor-Journal nicht.

CLI:
  python3 broker.py login              # kompletter Auth-Flow inkl. TAN-Freigabe
  python3 broker.py refresh            # Access-Token per Refresh erneuern
  python3 broker.py balances           # Kontosalden (read-only)
  python3 broker.py depots             # Depots + Positionen (read-only)
  python3 broker.py instrument WKN     # Instrument-Stammdaten
  python3 broker.py validate-order --isin ... --side BUY --qty N --limit X [--depot ID]

Abhängigkeiten: Standardbibliothek + credstore.py + config.py (im selben Ordner).
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path

from config import agent_env as _agent_env
from config import signal_rx_lock
from credstore import get_credentials

log = logging.getLogger("broker")

BASE = "https://api.comdirect.de"
API = f"{BASE}/api"
PROJECT_DIR = Path(__file__).resolve().parent
STATE_DIR = Path.home() / ".fom-agent"
STATE_FILE = STATE_DIR / "session.json"


def _session_file(account: str = "haupt") -> Path:
    """Sitzungsdatei je Konto. 'haupt' bleibt session.json (rückwärtskompatibel);
    weitere Konten (z. B. 'zweit') -> session_<account>.json."""
    return STATE_FILE if account == "haupt" else STATE_DIR / f"session_{account}.json"


TOKEN_DEFAULT_TTL_S = 599        # Fallback-Gültigkeit, falls expires_in fehlt
TOKEN_REFRESH_SKEW_S = 30        # so lange vor Ablauf wird proaktiv erneuert
TAN_CONFIRM_TIMEOUT_S = 240      # Gesamtwartezeit auf Freigabe-Bestätigung
TAN_POLL_INTERVAL_S = 5
CONFIRM_KEYWORDS = {"k", "ok", "ready", "ja", "j", "tan", "enter", "go", "freigabe"}
# 'tc' = TransactionsCanceled. Bewusst NICHT 'pa': ein Tippfehler nahe 'p' (Kill-Switch
# PAUSE) soll den Login nicht versehentlich abbrechen; 'tc' liegt weit weg von P/R/S/F.
ABORT_KEYWORDS = {"tc", "abbruch", "stop", "cancel"}


class BrokerError(Exception):
    """Fachlicher Broker-Fehler. Wird an der Fehlerstelle geworfen; die CLI-Schicht
    (main) übersetzt ihn in Exit-Code 1. Bei Import als Bibliothek bleibt der
    aufrufende Prozess erhalten (kein SystemExit tief im Stack)."""


# --------------------------------------------------------------------------
# Signal-Messaging (Benachrichtigung/Bestätigung der TAN-Freigabe)
# --------------------------------------------------------------------------

def _signal_send(msg: str, group: bool = False) -> None:
    e = _agent_env()
    bot = e.get("SIGNAL_BOT")
    target = e.get("SIGNAL_GROUP") if group else e.get("SIGNAL_RECIPIENT")
    if not bot or not target:
        return
    cmd = ["signal-cli", "-a", bot, "send", "-m", msg]
    cmd += (["-g", target] if group else [target])
    try:
        subprocess.run(cmd, timeout=30, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:  # noqa: BLE001
        log.warning("Signal-Versand fehlgeschlagen: %s", exc)


def _signal_wait_confirm(timeout_s: int) -> str:
    """Wartet auf eine Antwort des Nutzers via Signal. Ersetzt das Terminal-ENTER.
    Returns: 'confirm' (Freigabe-Bestätigung, z. B. 'K'), 'abort' ('TC') oder
    'timeout'.

    Caveat: `signal-cli receive` leert die gemeinsame Nachrichtenwarteschlange
    des Bot-Kontos destruktiv. Läuft der Login zeitgleich mit einem anderen
    Empfänger (notify.py, alert_relay.sh), können Nachrichten konsumiert werden,
    bevor der jeweils andere sie sieht. Der Login-Flow ist deshalb bewusst der
    einzige aktive Empfänger während der TAN-Freigabe."""
    e = _agent_env()
    bot, recipient = e.get("SIGNAL_BOT"), e.get("SIGNAL_RECIPIENT")
    if not bot or not recipient:
        return "timeout"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            out = subprocess.run(
                ["signal-cli", "-a", bot, "-o", "json", "receive", "--timeout", "5"],
                capture_output=True, text=True, timeout=20).stdout
        except Exception as exc:  # noqa: BLE001
            log.warning("Signal-Empfang fehlgeschlagen: %s", exc)
            time.sleep(TAN_POLL_INTERVAL_S)
            continue
        for line in out.splitlines():
            try:
                env = json.loads(line).get("envelope", {})
            except json.JSONDecodeError:
                continue
            src = env.get("source") or env.get("sourceNumber")
            body = ((env.get("dataMessage") or {}).get("message") or "").strip().lower()
            if src != recipient or not body:
                continue
            if body in ABORT_KEYWORDS:
                return "abort"
            if body in CONFIRM_KEYWORDS:
                return "confirm"
    return "timeout"


# --------------------------------------------------------------------------
# TanConfirmer -- entkoppelt die photoTAN-Freigabe vom Benachrichtigungskanal
# --------------------------------------------------------------------------

class TanConfirmer:
    """Schnittstelle für die photoTAN-Freigabe-Bestätigung. Entkoppelt
    BrokerSession.activate_tan vom konkreten Kanal, sodass Signal, Terminal und
    ein Test-Stub austauschbar sind (bessere Testbarkeit)."""

    def confirm(self, attempt: int, total: int) -> str:
        """Wartet auf die Freigabe. Returns 'confirm' | 'abort' | 'timeout'."""
        raise NotImplementedError

    def notify(self, msg: str) -> None:
        """Nachricht an den Nutzer (Standard: kein Kanal -> still)."""


class SignalConfirmer(TanConfirmer):
    """photoTAN-Freigabe über Signal ('K' bestätigt, 'TC' bricht ab)."""

    def confirm(self, attempt: int, total: int) -> str:
        _signal_send(
            f"[AUTH] comdirect TAN-Freigabe ({attempt}/{total}): Bitte JETZT in der "
            "photoTAN-App bestätigen, danach 'K' antworten. Abbruch mit 'TC'.")
        log.info("Warte auf Signal-Antwort ('K' bzw. 'TC') ... Versuch %d/%d",
                 attempt, total)
        res = _signal_wait_confirm(TAN_CONFIRM_TIMEOUT_S)
        if res == "abort":
            _signal_send("[STOP] Login abgebrochen (TC). Es wurde keine Session aufgebaut.")
            return "abort"
        if res == "timeout":
            log.warning("Keine Signal-Antwort (Timeout).")
            if attempt < total:
                _signal_send("[...] Keine Antwort erhalten. Neuer Versuch -- bitte "
                             "bestätigen ('K') oder abbrechen ('TC').")
            return "timeout"
        return "confirm"

    def notify(self, msg: str) -> None:
        _signal_send(msg)


class TerminalConfirmer(TanConfirmer):
    """photoTAN-Freigabe interaktiv am Terminal (ENTER nach App-Bestätigung)."""

    def confirm(self, attempt: int, total: int) -> str:
        try:
            input(f"-> Freigabe in der comdirect photoTAN-App erteilen, dann "
                  f"ENTER (Versuch {attempt}/{total}) ...")
        except EOFError:
            time.sleep(TAN_POLL_INTERVAL_S)
        return "confirm"


def _default_confirmer() -> TanConfirmer:
    """Signal, sobald ein SIGNAL_RECIPIENT konfiguriert ist; sonst Terminal."""
    return SignalConfirmer() if _agent_env().get("SIGNAL_RECIPIENT") \
        else TerminalConfirmer()


# --------------------------------------------------------------------------
# HTTP-Helfer (stdlib)
# --------------------------------------------------------------------------

def _http(method: str, url: str, *, headers: dict, data=None,
          form: bool = False) -> tuple[int, dict, dict]:
    """Returns (status, response_headers, json_body). Wirft bei Netzfehler."""
    if form:
        body = urllib.parse.urlencode(data).encode()
    elif data is not None:
        body = json.dumps(data).encode()
    else:
        body = None
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            payload = json.loads(raw) if raw else {}
            return resp.status, dict(resp.headers), payload
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw[:300].decode(errors="replace")}
        return exc.code, dict(exc.headers), payload


def _oauth(data: dict) -> dict:
    """OAuth2-Token-Endpunkt (x-www-form-urlencoded). Gemeinsam für ROPC,
    CD Secondary Flow und Refresh (Kap. 2/3.1.1)."""
    status, _, body = _http(
        "POST", f"{BASE}/oauth/token",
        headers={"Accept": "application/json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data=data, form=True)
    if status != 200:
        raise BrokerError(f"OAuth fehlgeschlagen (HTTP {status}): {body}")
    return body


def primary_token(account: str = "haupt") -> dict:
    """Schritt 1: ROPC (Kap. 2.1) -- Passwort-Grant mit Zugangsnummer/PIN des Kontos."""
    c = get_credentials(account)
    for req in ("client_id", "client_secret", "zugangsnummer", "pin"):
        if not c.get(req):
            raise BrokerError(f"credstore[{account}]: Feld '{req}' fehlt -- credstore.py init.")
    return _oauth({
        "client_id": c["client_id"], "client_secret": c["client_secret"],
        "grant_type": "password",
        "username": c["zugangsnummer"], "password": c["pin"],
    })


def secondary_token(token: str, account: str = "haupt") -> dict:
    """Schritt 5: CD Secondary Flow (Kap. 2.5) -> Token mit vollen Rechten."""
    c = get_credentials(account)
    return _oauth({
        "client_id": c["client_id"], "client_secret": c["client_secret"],
        "grant_type": "cd_secondary", "token": token,
    })


# --------------------------------------------------------------------------
# BrokerSession -- gekapselter Sitzungszustand (Tokens, sessionId), 0600
# --------------------------------------------------------------------------

@dataclass
class BrokerSession:
    """Kapselt den veränderlichen comdirect-Sitzungszustand und alle darauf
    operierenden Schritte: Header-Erzeugung, Gültigkeit/Refresh, Persistenz und
    den Auth-Flow (Kap. 2). Ersetzt das frühere Durchreichen von (token,
    session_id) und die losen Zustands-Dictionaries.

    session_id  : je Login neu erzeugte UUID (x-http-request-info, Kap. 1.2.2)
    identifier  : comdirect-Session-Identifier (aus dem Session-Objekt, Kap. 2.2)
    access_token/refresh_token/expires_at/obtained_at : Token-Lebenszyklus
    """
    session_id: str
    access_token: str = ""
    refresh_token: str | None = None
    identifier: str | None = None
    expires_at: float = 0.0
    obtained_at: float = 0.0
    account: str = "haupt"          # NICHT persistiert -- steuert Datei/Credentials je Konto

    # ---- Erzeugung / Persistenz (session.json-Format bleibt kompatibel) ----

    @classmethod
    def new(cls, account: str = "haupt") -> "BrokerSession":
        """Frische Sitzung mit neuer sessionId (noch ohne Token) für den Login."""
        return cls(session_id=uuid.uuid4().hex, account=account)

    @classmethod
    def load(cls, account: str = "haupt") -> "BrokerSession | None":
        """Persistierte Sitzung des Kontos lesen; None, falls keine da."""
        sf = _session_file(account)
        if not sf.exists():
            return None
        d = json.loads(sf.read_text())
        return cls(session_id=d.get("session_id", ""),
                   access_token=d.get("access_token", ""),
                   refresh_token=d.get("refresh_token"),
                   identifier=d.get("identifier"),
                   expires_at=d.get("expires_at", 0.0),
                   obtained_at=d.get("obtained_at", 0.0),
                   account=account)

    def save(self) -> None:
        """Sitzung 0600 nach session_<account>.json schreiben (unverändertes Schlüsselset,
        das 'account'-Feld wird NICHT persistiert -- damit alte und neue Codestände dieselbe
        Datei lesen/schreiben können)."""
        STATE_DIR.mkdir(mode=0o700, exist_ok=True)
        sf = _session_file(self.account)
        sf.write_text(json.dumps({
            "session_id": self.session_id,
            "identifier": self.identifier,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
            "obtained_at": self.obtained_at,
        }))
        sf.chmod(0o600)

    @staticmethod
    def clear(account: str = "haupt") -> None:
        """Lokalen Sitzungszustand des Kontos entfernen (session-Datei + .dead-Marker)."""
        sf = _session_file(account)
        for f in (sf, sf.with_suffix(".dead")):
            try:
                f.unlink()
            except FileNotFoundError:
                pass

    # ---- Header / Requests ----

    def request_info(self) -> str:
        """x-http-request-info-Header (comdirect, 2026, Kap. 1.2.2). Jeder Call
        erhält eine frische requestId."""
        request_id = time.strftime("%H%M%S") + f"{int(time.time() * 1000) % 1000:03d}"
        return json.dumps({"clientRequestId": {"sessionId": self.session_id,
                                               "requestId": request_id}})

    def auth_headers(self, extra: dict | None = None) -> dict:
        """Standard-Header mit Bearer-Token + frischer x-http-request-info."""
        h = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-http-request-info": self.request_info(),
        }
        if extra:
            h.update(extra)
        return h

    def get(self, path: str) -> dict:
        """Read-only-GET auf einen API-Pfad (Kap. 4-7)."""
        status, _, body = _http("GET", f"{API}{path}", headers=self.auth_headers())
        if status != 200:
            raise BrokerError(f"GET {path} fehlgeschlagen (HTTP {status}): {body}")
        return body

    # ---- Gültigkeit / Erneuerung ----

    def is_expiring(self, skew_s: int = TOKEN_REFRESH_SKEW_S) -> bool:
        return time.time() > self.expires_at - skew_s

    def ensure_valid(self) -> "BrokerSession":
        """Stellt einen gültigen Access-Token sicher; erneuert proaktiv kurz vor
        Ablauf. Ohne Token -> Login nötig."""
        if not self.access_token:
            raise BrokerError("Keine aktive Session -- 'broker.py login' ausführen.")
        if self.is_expiring():
            log.info("Token abgelaufen/kurz davor -- refresh ...")
            self.refresh()
        return self

    def refresh(self) -> "BrokerSession":
        """Refresh-Token-Flow (Kap. 3.1.1) -- hält Session + Session-TAN am Leben.
        Aktualisiert die Felder und persistiert."""
        if not self.refresh_token:
            raise BrokerError("Kein Refresh-Token -- erst 'broker.py login'.")
        c = get_credentials(self.account)
        tok = _oauth({
            "client_id": c["client_id"], "client_secret": c["client_secret"],
            "grant_type": "refresh_token", "refresh_token": self.refresh_token,
        })
        self.access_token = tok["access_token"]
        self.refresh_token = tok.get("refresh_token", self.refresh_token)
        self.expires_at = time.time() + tok.get("expires_in", TOKEN_DEFAULT_TTL_S)
        self.obtained_at = time.time()
        self.save()
        log.info("Token erneuert -- gültig +%ds.", int(tok.get("expires_in",
                                                             TOKEN_DEFAULT_TTL_S)))
        return self

    def revoke(self) -> None:
        """Token-Revoke (Kap. 3.1.2) -- invalidiert Access-/Refresh-Token UND
        Session-TAN serverseitig sofort; danach lokalen Zustand entfernen."""
        status, _, _ = _http(
            "DELETE", f"{BASE}/oauth/revoke",
            headers={"Authorization": f"Bearer {self.access_token}",
                     "Accept": "application/json"})
        if status in (200, 204):
            log.info("Session serverseitig widerrufen (Token + Session-TAN ungültig).")
        else:
            log.warning("Revoke-Status %s -- Token läuft andernfalls von selbst ab.",
                        status)
        self.clear()

    # ---- Auth-Flow (Kap. 2), operiert auf self ----

    def fetch_session(self) -> dict:
        """Schritt 2: Session-Objekt abrufen (Kap. 2.2). Setzt self.identifier."""
        status, _, body = _http(
            "GET", f"{API}/session/clients/user/v1/sessions",
            headers=self.auth_headers())
        if status != 200:
            raise BrokerError(f"Session-Abruf fehlgeschlagen (HTTP {status}): {body}")
        obj = body[0] if isinstance(body, list) and body else body
        self.identifier = obj["identifier"]
        return obj

    def request_tan_challenge(self) -> dict:
        """Schritt 3: TAN-Challenge erzeugen (Kap. 2.3). Liefert Challenge-Info."""
        status, headers, _ = _http(
            "POST",
            f"{API}/session/clients/user/v1/sessions/{self.identifier}/validate",
            headers=self.auth_headers(),
            data={"identifier": self.identifier, "sessionTanActive": True,
                  "activated2FA": True})
        if status not in (200, 201):
            raise BrokerError(f"TAN-Validierung fehlgeschlagen (HTTP {status})")
        info = headers.get("x-once-authentication-info")
        return json.loads(info) if info else {}

    def activate_tan(self, challenge_id: str, tan: str | None,
                     confirmer: TanConfirmer | None = None) -> None:
        """Schritt 4: Session-TAN aktivieren (Kap. 2.4). P_TAN_PUSH: erst Freigabe
        abwarten (über den injizierten TanConfirmer), dann GENAU EIN PATCH --
        wiederholtes Pollen verbrennt die Challenge ('expired', origin id). Jeder
        Call mit frischer Request-Id. Bei gegebener `tan` (manuell) entfällt die
        Freigabe-Warte und es wird einmal gepatcht."""
        body = {"identifier": self.identifier, "sessionTanActive": True,
                "activated2FA": True}

        def do_patch():
            auth_info = {"id": challenge_id}
            if tan:
                auth_info["tan"] = tan
            headers = self.auth_headers(
                {"x-once-authentication-info": json.dumps(auth_info)})
            return _http("PATCH",
                         f"{API}/session/clients/user/v1/sessions/{self.identifier}",
                         headers=headers, data=body)

        manual = tan is not None
        if not manual:
            confirmer = confirmer or _default_confirmer()
        attempts = 1 if manual else 3
        for i in range(attempts):
            if not manual:
                res = confirmer.confirm(i + 1, attempts)
                if res == "abort":
                    raise BrokerError("Login vom Nutzer abgebrochen (TC).")
                if res == "timeout":
                    continue
            status, _, resp = do_patch()
            if status in (200, 204):
                log.info("Session-TAN aktiviert.")
                return
            code = (resp or {}).get("code", "")
            if code == "expired":
                if not manual:
                    confirmer.notify("[X] TAN-Challenge abgelaufen. Bitte Login neu "
                                     "starten (broker.py login).")
                raise BrokerError("TAN-Challenge abgelaufen -- 'broker.py login' neu starten.")
            log.warning("Freigabe noch nicht bestätigt (HTTP %s, %s).", status, code)
            if not manual and i < attempts - 1:
                confirmer.notify("[!] Freigabe noch nicht erkannt. Bitte ZUERST in der "
                                 "comdirect-App bestätigen, DANN 'K' antworten (Abbruch 'TC').")
        if not manual:
            confirmer.notify("[X] Login fehlgeschlagen: TAN-Freigabe nicht erkannt. "
                             "Bitte erneut versuchen (broker.py login).")
        raise BrokerError("TAN-Aktivierung fehlgeschlagen -- Freigabe nicht erkannt.")


def _notify_tan_push(challenge: dict) -> None:
    """photoTAN-Push: Nutzer per Signal zur Freigabe auffordern (notify_tan.sh)."""
    typ = challenge.get("typ", "?")
    msg = f"comdirect Session-TAN ({typ}) freigeben."
    script = Path(__file__).resolve().parent / "notify_tan.sh"
    if script.exists():
        try:
            subprocess.run(["bash", str(script), "", msg], timeout=30, check=False)
        except Exception as exc:  # noqa: BLE001
            log.warning("notify_tan.sh nicht ausführbar: %s", exc)
    else:
        log.warning("notify_tan.sh fehlt -- TAN-Freigabe manuell anstoßen.")


# --------------------------------------------------------------------------
# Auth-Flow-Orchestrierung + Sitzungszugriff (CLI-Ebene)
# --------------------------------------------------------------------------

def login(account: str = "haupt", confirmer: TanConfirmer | None = None) -> BrokerSession:
    """Kompletter Auth-Flow für ein Konto. Persistiert Tokens + sessionId in die
    kontoeigene Sitzungsdatei. Der TanConfirmer ist injizierbar (Standard: Signal,
    sonst Terminal)."""
    sess = BrokerSession.new(account)
    tok = primary_token(account)
    sess.access_token = tok["access_token"]

    session = sess.fetch_session()
    log.info("Session %s -- sessionTanActive=%s", sess.identifier,
             session.get("sessionTanActive"))

    challenge = sess.request_tan_challenge()
    typ = challenge.get("typ")
    log.info("TAN-Challenge: typ=%s id=%s", typ, challenge.get("id"))
    if typ == "P_TAN_PUSH":
        # activate_tan holt die Freigabe über den TanConfirmer ('K'/'TC'). Während dieses
        # Empfangsfensters hält der Login den Signal-RX-Lock, damit der ständig laufende
        # signal_dispatcher sein `receive` pausiert und die K/TC-Antwort nicht wegfängt.
        with signal_rx_lock():
            sess.activate_tan(challenge["id"], None, confirmer)
    elif typ in ("P_TAN", "M_TAN"):
        _signal_send(f"[X] Login nicht möglich: TAN-Typ {typ} erfordert manuelle "
                     "Eingabe. In der comdirect-App auf photoTAN-Push umstellen.")
        raise BrokerError(
            f"TAN-Typ {typ} erfordert manuelle TAN-Eingabe und ist für den "
            "autonomen Betrieb ungeeignet. In der comdirect-App auf photoTAN-Push "
            "(P_TAN_PUSH) umstellen.")
    else:
        raise BrokerError(f"Unerwarteter TAN-Typ: {typ}")

    final = secondary_token(sess.access_token, account)
    sess.access_token = final["access_token"]
    sess.refresh_token = final.get("refresh_token")
    sess.expires_at = time.time() + final.get("expires_in", TOKEN_DEFAULT_TTL_S)
    sess.obtained_at = time.time()
    sess.save()
    # Reaktivierung: ein .dead-Marker des Keepalive (frühere verlorene Session) ist
    # nach erfolgreichem Neu-Login obsolet -- aufräumen, damit Status-Sichten
    # (control.login_status, 'S'/'L') nicht fälschlich "VERLOREN" melden.
    _session_file(account).with_suffix(".dead").unlink(missing_ok=True)
    valid_s = int(final.get("expires_in", TOKEN_DEFAULT_TTL_S))
    log.info("Login (%s) erfolgreich -- Token gültig bis +%ds.", account, valid_s)
    _signal_send(f"[OK] comdirect-Login ({account}) erfolgreich. Session aktiv "
                 f"(Token +{valid_s}s). Keepalive übernimmt die Verlängerung.")
    return sess


def refresh(account: str = "haupt") -> BrokerSession:
    """CLI-Refresh: persistierte Sitzung des Kontos laden und Access-Token erneuern."""
    sess = BrokerSession.load(account)
    if sess is None or not sess.refresh_token:
        raise BrokerError("Kein Refresh-Token -- erst 'broker.py login'.")
    return sess.refresh()


def revoke(account: str = "haupt") -> None:
    """CLI-Logout: Token + Session-TAN serverseitig widerrufen, lokal aufräumen."""
    sess = BrokerSession.load(account)
    if sess is None or not sess.access_token:
        log.info("Keine aktive Session (%s) -- nichts zu widerrufen.", account)
        BrokerSession.clear(account)
        return
    sess.revoke()


def _active_session(account: str = "haupt") -> BrokerSession:
    """Persistierte, gültige Sitzung des Kontos liefern (mit proaktivem Refresh)."""
    sess = BrokerSession.load(account)
    if sess is None:
        raise BrokerError(f"Keine aktive Session (Konto {account}) -- "
                          "'broker.py login' ausführen.")
    return sess.ensure_valid()


# --------------------------------------------------------------------------
# Read-only (Kap. 4-6)
# --------------------------------------------------------------------------

def _get(path: str, account: str = "haupt") -> dict:
    return _active_session(account).get(path)


def balances(account: str = "haupt") -> dict:
    return _get("/banking/clients/user/v2/accounts/balances", account)


def depots(account: str = "haupt") -> dict:
    return _get("/brokerage/clients/user/v3/depots", account)


def depot_positions(depot_id: str, account: str = "haupt") -> dict:
    return _get(f"/brokerage/v3/depots/{depot_id}/positions", account)


def instrument(wkn_or_isin: str, account: str = "haupt") -> dict:
    return _get(f"/brokerage/v1/instruments/{wkn_or_isin}", account)


def depot_orders(depot_id: str, account: str = "haupt") -> list[dict]:
    """GET /brokerage/depots/{id}/v3/orders -- Orderbuch des Depots (read-only, Kap. 7).
    Grundlage der A2-Reconciliation: Nach einem unklaren Execution-Ausgang
    (Netzwerkfehler nach POST) prüft die Orchestrierung hierüber, ob die Order bei der
    Bank angekommen ist. Liefert die Order-Liste (values) oder wirft BrokerError."""
    data = _get(f"/brokerage/depots/{depot_id}/v3/orders", account)
    return data.get("values") or []


# --------------------------------------------------------------------------
# Order-Validierung (Kap. 7) -- Demonstrationsgrenze
# --------------------------------------------------------------------------

def order_dimensions(internal_instrument_id: str, account: str = "haupt") -> dict:
    """GET /brokerage/v3/orders/dimensions (Kap. 7.1.1) -- zulässige Handelsplätze
    etc. ERWARTET DIE INTERNE instrumentId (nicht ISIN/WKN)."""
    return _get(f"/brokerage/v3/orders/dimensions?instrumentId={internal_instrument_id}",
                account)


def _resolve_instrument_id(isin: str, account: str = "haupt") -> str:
    """ISIN/WKN -> interne comdirect-instrumentId (aus Kap. 6)."""
    vals = instrument(isin, account).get("values") or []
    if not vals:
        raise BrokerError(f"Instrument {isin} nicht gefunden.")
    return vals[0]["instrumentId"]


def _pick_venue(internal_id: str, side: str, order_type: str, account: str = "haupt"):
    """Handelsplatz wählen (Seite + Ordertyp). Bevorzugt den außerbörslichen
    Direkthandel (type=OFF, LiveTrading beim Emittenten) -- dort entfällt das
    Börsenplatzentgelt (belegt: 5 € Ersparnis/Round-Trip ggü. Stuttgart) --
    und nutzt Börsen (EXCHANGE) nur als Fallback. Hinweis: Der Direkthandel
    verdient am Geld/Brief-Spread, der in den Ex-Ante-Kosten NICHT ausgewiesen
    ist (siehe Spread-Benchmark, Kap. 4)."""
    dims = order_dimensions(internal_id, account)
    venues = (dims.get("values") or [{}])[0].get("venues", [])
    eligible = [v for v in venues
                if side.upper() in v.get("sides", [])
                and order_type in (v.get("orderTypes") or {})]
    if not eligible:
        return (venues[0].get("venueId"), venues[0].get("name")) if venues else (None, None)
    eligible.sort(key=lambda v: 0 if v.get("type") == "OFF" else 1)
    return eligible[0].get("venueId"), eligible[0].get("name")


def _build_order(depot_id: str, isin: str, side: str, qty: float,
                 limit: float | None, venue_id: str | None,
                 account: str = "haupt") -> tuple[dict, str, str]:
    """Baut den Order-Body (Auflösung ISIN->instrumentId, Handelsplatzwahl falls nötig).
    Gemeinsam für validate_order und place_order -> identische Order-Struktur."""
    internal_id = _resolve_instrument_id(isin, account)
    order_type = "LIMIT" if limit is not None else "MARKET"
    if not venue_id:
        venue_id, vname = _pick_venue(internal_id, side, order_type, account)
        log.info("Handelsplatz gewählt: %s (%s)", venue_id, vname)
    order = {
        "depotId": depot_id,
        "instrumentId": internal_id,
        "orderType": order_type,
        "side": side.upper(),
        "venueId": venue_id,
        # O6b -- Gültigkeit EXPLIZIT Good-for-Day: ohne validityType hinge die
        # Order am Platz-Default; eine unbemerkt über Nacht offene Limit-Order
        # würde am Folgetag zur alten Signallage ausgeführt. GFD deckt sich mit
        # der Tageslogik des Agenten (1 Entscheid/Tag, EOD-Verfall).
        "validityType": "GFD",
        # unit "XXX" = ISO-4217-Platzhalter für "Stück/Nominal ohne Währung".
        "quantity": {"value": str(qty), "unit": "XXX"},
    }
    if limit is not None:
        order["limit"] = {"value": str(limit), "unit": "EUR"}
    return order, venue_id, internal_id


def validate_order(depot_id: str, isin: str, side: str, qty: float,
                   limit: float | None, venue_id: str | None = None,
                   account: str = "haupt") -> dict:
    """POST /brokerage/v3/orders/validation (Kap. 7.1.5) + Ex-Ante-Kosten.
    Löst KEINE Order aus -- prüft Zulässigkeit und weist Kosten aus. Signatur bewusst
    stabil (cost_probe.py und orchestrate.py rufen sie positional auf; account ist ein
    trailing Default-Parameter)."""
    sess = _active_session(account)
    order, venue_id, internal_id = _build_order(depot_id, isin, side, qty, limit,
                                                venue_id, account)
    status, _, body = _http(
        "POST", f"{API}/brokerage/v3/orders/validation",
        headers=sess.auth_headers(), data=order)
    result = {"venue": venue_id, "instrumentId": internal_id,
              "validation_status": status, "validation_body": body}
    if status in (200, 201):
        cst, _, cost_body = _http(
            "POST", f"{API}/brokerage/v3/orders/costindicationexante",
            headers=sess.auth_headers(), data=order)
        result["cost_status"] = cst
        result["cost_indication"] = cost_body
    return result


def place_order(depot_id: str, isin: str, side: str, qty: float,
                limit: float | None, venue_id: str | None = None, *,
                execute: bool = False, account: str = "haupt") -> dict:
    """Orderanlage über den comdirect-Flow (comdirect 2026, §3.2): Validation ->
    Execution. Nutzt die bei der morgendlichen Anmeldung aktivierte **Session-TAN**
    (Validation liefert typ `TAN_FREI` -- keine erneute TAN je Order; die menschliche
    Tagesfreigabe erfolgte einmalig per photoTAN/"K").

    SICHERHEIT -- execute steuert alles:
      * execute=False (Default, Dry-Run): validiert und protokolliert die Order, POSTet
        aber NICHT `/orders`. Es wird keine reale Order angelegt.
      * execute=True (Live): legt die Order real an. Wird ausschließlich von der
        Orchestrierung gesetzt, wenn EXECUTION_MODE=live in agent.env steht (siehe
        orchestrate.py). Ohne aktive Session-TAN (typ != TAN_FREI) wird die Ausführung
        verweigert (fail-safe -> neuer Login nötig)."""
    sess = _active_session(account)
    order, venue_id, internal_id = _build_order(depot_id, isin, side, qty, limit,
                                                venue_id, account)

    # 1) Validation -> Challenge-ID + TAN-Typ aus dem Response-Header
    vstatus, vheaders, vbody = _http(
        "POST", f"{API}/brokerage/v3/orders/validation",
        headers=sess.auth_headers(), data=order)
    if vstatus not in (200, 201):
        raise BrokerError(f"Order-Validation fehlgeschlagen (HTTP {vstatus}): {vbody}")
    auth_raw = vheaders.get("x-once-authentication-info")
    challenge = json.loads(auth_raw) if auth_raw else {}
    result = {"venue": venue_id, "instrumentId": internal_id, "order": order,
              "validation_status": vstatus, "tan_typ": challenge.get("typ")}

    if not execute:
        log.info("DRY-RUN: Order NICHT platziert (execute=False): %s %s qty=%s @ %s",
                 side.upper(), isin, qty, venue_id)
        return {**result, "dry_run": True}

    # Fail-safe: ohne aktive Session-TAN (TAN_FREI) keine Ausführung ohne echte TAN
    if challenge.get("typ") != "TAN_FREI":
        raise BrokerError(
            f"Order erfordert eine TAN (typ={challenge.get('typ')}) -- Session-TAN nicht "
            "aktiv/abgelaufen. Bitte 'broker.py login' (photoTAN) neu ausführen.")

    # 2) Execution -- POST /orders mit Challenge-ID (TAN_FREI dank Session-TAN)
    ex_headers = sess.auth_headers(
        {"x-once-authentication-info": json.dumps({"id": challenge.get("id")})})
    estatus, _, ebody = _http(
        "POST", f"{API}/brokerage/v3/orders", headers=ex_headers, data=order)
    if estatus not in (200, 201):
        raise BrokerError(f"Orderanlage fehlgeschlagen (HTTP {estatus}): {ebody}")
    log.info("[OK] Order platziert (HTTP %s): %s %s qty=%s @ %s",
             estatus, side.upper(), isin, qty, venue_id)
    # orderId für Journal/Reconciliation extrahieren (best-effort -- Body-Schema kann
    # variieren; fehlende Id bricht nichts, die Order ist zu diesem Zeitpunkt platziert).
    try:
        parsed = json.loads(ebody) if isinstance(ebody, str) else (ebody or {})
        order_id = parsed.get("orderId") or parsed.get("orderID") or parsed.get("id")
    except Exception:  # noqa: BLE001
        order_id = None
    return {**result, "execution_status": estatus, "execution_body": ebody,
            "order_id": order_id, "dry_run": False}


# --------------------------------------------------------------------------
# OP6 -- Positions-Auflösung, Exposure, aktives Flatten (Not-Aus-Wirkung von 'F')
# --------------------------------------------------------------------------

def _num(obj) -> float:
    """Zahl robust aus $AmountValue/{value,...} oder Skalar lesen (fehlend -> 0.0)."""
    try:
        return float((obj or {}).get("value")) if isinstance(obj, dict) else float(obj or 0)
    except (TypeError, ValueError):
        return 0.0


def _position_list(depot_id: str, account: str = "haupt") -> list[dict]:
    """Offene Depotpositionen als robuste Liste. Extrahiert defensiv aus der comdirect-
    Antwort (Feldnamen können je API-Version leicht abweichen -- fehlende Felder werden zu
    0/None statt zu einem Fehler). qty<=0 wird verworfen. `price_eur` ist der Stückkurs
    (currentPrice, $Price.price) mit Fallback Positionswert/Stück."""
    out = []
    for p in (depot_positions(depot_id, account).get("values") or []):
        qty = _num(p.get("availableQuantity") or p.get("quantity"))
        if qty <= 0:
            continue
        value_eur = _num(p.get("currentValue"))
        cp = p.get("currentPrice")
        # $Price kapselt den Kurs unter 'price'; manche Antworten liefern direkt {value}.
        price_eur = _num(cp.get("price", cp)) if isinstance(cp, dict) else 0.0
        # O6a -- Kurs-Zeitstempel ($Price.priceDateTime) defensiv mitführen: Basis der
        # Frischeprüfung in orchestrate (Vortags-/verzögerter Kurs -> Limit daneben).
        price_dt = (cp.get("priceDateTime") or cp.get("dateTime")
                    or cp.get("timestamp")) if isinstance(cp, dict) else None
        if price_eur <= 0 and qty > 0:            # Fallback: Positionswert / Stück
            price_eur = round(value_eur / qty, 4)
            price_dt = None                       # abgeleiteter Kurs: kein Zeitstempel
        out.append({
            "wkn": p.get("wkn"),
            "isin": p.get("isin"),
            # place_order löst WKN/ISIN -> instrumentId auf; WKN ist immer vorhanden.
            "key": p.get("isin") or p.get("wkn"),
            "quantity": qty,
            "price_eur": price_eur,
            "price_datetime": price_dt,
            "value_eur": value_eur,
            "pl_abs_eur": _num(p.get("profitLossPurchaseAbs")),
        })
    return out


def position_info(depot_id: str, isin: str, account: str = "haupt") -> dict | None:
    """Positionsdatensatz zu einer ISIN/WKN (Stückkurs, gehaltene Menge) oder None,
    wenn nicht (mehr) im Depot. Basis für kostenloses Pricing über die Seed-Position.

    comdirect-Positionen führen oft NUR die `wkn` (kein `isin`/`instrumentId`). Deutsche
    ISINs betten die 6-stellige WKN ein (`DE000` + WKN + Prüfziffer), daher wird zusätzlich
    WKN<->ISIN abgeglichen (z. B. Abfrage 'DE000SB295Z1' trifft Position wkn='SB295Z')."""
    q = (isin or "").upper()
    q_wkn = q[5:11] if len(q) == 12 and q[:2].isalpha() else None
    for p in _position_list(depot_id, account):
        w = (p.get("wkn") or "").upper()
        pi = (p.get("isin") or "").upper()
        if q and q in (pi, w):            # exakte ISIN- oder WKN-Übereinstimmung
            return p
        if q_wkn and w and q_wkn == w:    # ISIN bettet die Positions-WKN ein
            return p
    return None


def reference_price(depot_id: str, isin: str, account: str = "haupt") -> float | None:
    """Aktueller Stückkurs (EUR) einer gehaltenen Position -- kostenlose Kursquelle über
    den Depotabruf (currentPrice). Setzt voraus, dass mind. eine Seed-Position gehalten
    wird. None, wenn nicht gehalten oder kein Kurs verfügbar -> Aufrufer entscheidet
    fail-safe (kein stiller Ersatzpreis)."""
    q = reference_quote(depot_id, isin, account)
    return q["price"] if q else None


def reference_quote(depot_id: str, isin: str, account: str = "haupt") -> dict | None:
    """O6a -- wie reference_price, aber MIT Kurs-Zeitstempel: {'price': float,
    'price_datetime': str | None}. Der Zeitstempel ($Price.priceDateTime) erlaubt dem
    Aufrufer die Frischeprüfung (Vortags-/verzögerter Kurs -> Limit läge daneben);
    None-Zeitstempel = nicht prüfbar (Aufrufer entscheidet)."""
    p = position_info(depot_id, isin, account)
    if p and (p.get("price_eur") or 0) > 0:
        return {"price": p["price_eur"], "price_datetime": p.get("price_datetime")}
    return None


def portfolio_exposure(depot_id: str, account: str = "haupt") -> float:
    """Summe der aktuellen Positionswerte (EUR) -- Basis für den Exposure-Breaker."""
    return round(sum(p["value_eur"] for p in _position_list(depot_id, account)), 2)


def available_cash(depot_id: str, account: str = "haupt") -> float:
    """Verfügbarer Verrechnungssaldo (EUR, `availableCashAmountEUR`) des zum Depot
    gehörenden Settlement-Kontos -- Basis für deckungsbewusstes Sizing. Das Konto wird
    über das Depot (defaultSettlementAccountId) bestimmt; Fallback: erstes EUR-Konto."""
    settle_id = None
    try:
        for d in (depots(account).get("values") or []):
            if d.get("depotId") == depot_id:
                settle_id = d.get("defaultSettlementAccountId")
                break
    except Exception:  # noqa: BLE001 -- Fallback auf erstes Konto
        pass
    accounts = balances(account).get("values") or []

    def _avail(a: dict) -> float:
        return _num(a.get("availableCashAmountEUR") or a.get("availableCashAmount"))

    for a in accounts:
        if settle_id and a.get("accountId") == settle_id:
            return _avail(a)
    return _avail(accounts[0]) if accounts else 0.0


def flatten_positions(depot_id: str, *, execute: bool = False,
                      retain: float = 1.0, account: str = "haupt") -> dict:
    """Schließt offene Positionen per MARKET-SELL (Wirkung der Kill-Switch-Aktion 'F'),
    lässt aber je Position `retain` Stück als **Seed** stehen (Preisquelle bleibt erhalten;
    Default 1). execute wie bei place_order:
      * execute=False (Default, Dry-Run): protokolliert je Position, was verkauft würde.
      * execute=True (Live): legt reale Verkaufsorders an (nur bei EXECUTION_MODE=live).
    Best-effort je Position -- ein Fehler stoppt die übrigen nicht. Returns {count, sold,
    kept, errors}."""
    positions = _position_list(depot_id, account)
    sold, errors, kept = [], [], []
    for p in positions:
        sell_qty = p["quantity"] - retain
        if sell_qty <= 0:                         # nur Seed gehalten -> nichts verkaufen
            kept.append({"wkn": p["wkn"], "qty": p["quantity"]})
            continue
        try:
            res = place_order(depot_id, p["key"], "SELL", sell_qty, None,
                              execute=execute, account=account)
            sold.append({"wkn": p["wkn"], "qty": sell_qty, "retained": retain,
                         "dry_run": res.get("dry_run", True),
                         "execution_status": res.get("execution_status")})
        except Exception as exc:  # noqa: BLE001 -- Not-Aus: nächste Position trotzdem versuchen
            log.warning("Flatten %s fehlgeschlagen: %s", p["wkn"], exc)
            errors.append({"wkn": p["wkn"], "error": str(exc)})
    log.info("Flatten %s: %d Position(en), %d verkauft (Seed %s gehalten), %d Fehler "
             "(dry_run=%s)", depot_id, len(positions), len(sold), retain, len(errors),
             not execute)
    return {"count": len(positions), "sold": sold, "kept": kept, "errors": errors,
            "dry_run": not execute}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="comdirect-Broker-Anbindung (bis Validation)")
    p.add_argument("--account", default="haupt",
                   help="Konto/Zugang (Default haupt; z. B. zweit für das Zweitdepot)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("login")
    sub.add_parser("refresh")
    sub.add_parser("logout")
    sub.add_parser("balances")
    sub.add_parser("depots")
    sp = sub.add_parser("instrument")
    sp.add_argument("wkn")
    sp = sub.add_parser("validate-order")
    sp.add_argument("--depot", required=True)
    sp.add_argument("--isin", required=True)
    sp.add_argument("--side", choices=("BUY", "SELL"), required=True)
    sp.add_argument("--qty", type=float, required=True)
    sp.add_argument("--limit", type=float, default=None)
    sp.add_argument("--venue", default=None)
    sp = sub.add_parser("flatten", help="Offene Positionen glattstellen (OP6, Seed bleibt)")
    sp.add_argument("--depot", required=True)
    sp.add_argument("--retain", type=float, default=1.0,
                    help="Seed-Stück je Position behalten (Default 1; 0 = voll schließen)")
    sp.add_argument("--execute", action="store_true",
                    help="REAL verkaufen (ohne Flag: Dry-Run, nichts wird verkauft)")

    args = p.parse_args(argv)
    # CLI-Schicht: BrokerError -> Exit-Code 1 (Bibliotheks-Aufrufer erhalten die
    # Ausnahme dagegen unverändert und behalten die Kontrolle über den Prozess).
    acc = args.account
    try:
        if args.cmd == "login":
            login(acc)
        elif args.cmd == "refresh":
            refresh(acc)
        elif args.cmd == "logout":
            revoke(acc)
        elif args.cmd == "balances":
            print(json.dumps(balances(acc), indent=2, ensure_ascii=False))
        elif args.cmd == "depots":
            print(json.dumps(depots(acc), indent=2, ensure_ascii=False))
        elif args.cmd == "instrument":
            print(json.dumps(instrument(args.wkn, acc), indent=2, ensure_ascii=False))
        elif args.cmd == "validate-order":
            print(json.dumps(
                validate_order(args.depot, args.isin, args.side, args.qty,
                               args.limit, args.venue, acc), indent=2, ensure_ascii=False))
        elif args.cmd == "flatten":
            print(json.dumps(
                flatten_positions(args.depot, execute=args.execute, retain=args.retain,
                                  account=acc), indent=2, ensure_ascii=False))
    except BrokerError as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
