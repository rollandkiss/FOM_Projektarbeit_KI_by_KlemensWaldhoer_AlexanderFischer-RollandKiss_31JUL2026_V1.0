#!/usr/bin/env python3
"""
credstore.py -- Verschlüsselte Ablage der comdirect-Zugangsdaten (für broker.py).

Prinzip (PLAN.md, Hürde 9: Secrets nie im Klartext, nie im LLM-Kontext):
  * Werte liegen NUR verschlüsselt in  comdirect.secrets.enc  (Fernet/AES-128-CBC+HMAC).
  * Der Schlüssel liegt getrennt im macOS-Schlüsselbund (Keychain, Service
    'comdirect-agent'); auf Linux/Docker alternativ Umgebungsvariable CREDSTORE_KEY.
  * Eingabe der Werte erfolgt lokal über getpass (kein Terminal-Echo, keine
    Shell-History) -- niemals Werte als CLI-Argument übergeben.
  * broker.py nutzt ausschließlich:  from credstore import get_credentials

Einrichtung (einmalig, auf dem Mac im Projektordner):
  pip3 install cryptography
  python3 credstore.py init          # fragt client_id, client_secret, Zugangsnr., PIN
  python3 credstore.py check         # Ablage prüfen (zeigt nur maskierte Werte)

Betrieb auf GCP e2-medium (bevorzugter Produktionspfad, KEIN Schlüssel auf Disk):
  Zugangsdaten liegen im GCP Secret Manager (Secret 'comdirect-agent-credentials',
  Payload = JSON mit denselben Feldern); die VM liest sie über ihren Service-
  Account (Metadata-Server, reine Standardbibliothek -- kein SDK nötig).
  Einrichtung von einer Maschine mit gcloud:
    printf '{"client_id":"...","client_secret":"...","zugangsnummer":"...","pin":"..."}' \
      | gcloud secrets create comdirect-agent-credentials --data-file=-
    gcloud secrets add-iam-policy-binding comdirect-agent-credentials \
      --member="serviceAccount:<VM-SA>" --role="roles/secretmanager.secretAccessor"
  Rotation: neue Version anlegen -- gcloud secrets versions add ... --data-file=-
  (broker.py liest 'latest'; kein Neustart/Deployment nötig).

Auflösungsreihenfolge von get_credentials():
  1. GCP Secret Manager (nur wenn Metadata-Server erreichbar, d. h. auf GCP-VM)
  2. comdirect.secrets.enc + macOS-Keychain
  3. comdirect.secrets.enc + $CREDSTORE_KEY (Docker/Linux-Fallback)

Abhängigkeit: cryptography (nur für die lokale Datei-Variante 2/3).
"""

from __future__ import annotations

import getpass
import json
import os
import platform
import stat
import subprocess
import sys
from pathlib import Path

SECRETS_FILE = Path(__file__).resolve().parent / "comdirect.secrets.enc"
KEYCHAIN_SERVICE = "comdirect-agent"
KEYCHAIN_ACCOUNT = "fernet-key"
ENV_KEY = "CREDSTORE_KEY"
GCP_SECRET_NAME = os.environ.get("GCP_SECRET_NAME", "comdirect-agent-credentials")
GCP_METADATA = "http://metadata.google.internal/computeMetadata/v1"

FIELDS = [
    ("client_id", "comdirect client_id"),
    ("client_secret", "comdirect client_secret"),
    ("zugangsnummer", "comdirect Zugangsnummer (für OAuth-ROPC; leer = später)"),
    ("pin", "comdirect PIN (leer = später)"),
]


def _fernet():
    try:
        from cryptography.fernet import Fernet  # noqa: PLC0415
        return Fernet
    except ImportError:
        sys.exit("Paket 'cryptography' fehlt -- installieren mit: "
                 "pip3 install cryptography")


# --------------------------------------------------------------------------
# Schlüsselverwaltung (Keychain auf macOS, sonst Umgebungsvariable)
# --------------------------------------------------------------------------

def _keychain_get() -> str | None:
    try:
        out = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE,
             "-a", KEYCHAIN_ACCOUNT, "-w"],
            capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None if out.returncode == 0 else None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def _keychain_set(key: str) -> bool:
    try:
        out = subprocess.run(
            ["security", "add-generic-password", "-s", KEYCHAIN_SERVICE,
             "-a", KEYCHAIN_ACCOUNT, "-w", key, "-U"],
            capture_output=True, text=True, timeout=10)
        return out.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _get_key(create: bool = False) -> bytes:
    if os.environ.get(ENV_KEY):                    # Linux/Docker-Pfad
        return os.environ[ENV_KEY].encode()
    key = _keychain_get()
    if key:
        return key.encode()
    if not create:
        sys.exit("Kein Schlüssel gefunden (weder macOS-Keychain noch "
                 f"${ENV_KEY}). Erst 'python3 credstore.py init' ausführen.")
    Fernet = _fernet()
    new_key = Fernet.generate_key().decode()
    if platform.system() == "Darwin" and _keychain_set(new_key):
        print("Neuer Schlüssel im macOS-Schlüsselbund abgelegt "
              f"(Service '{KEYCHAIN_SERVICE}').")
    else:
        print(f"WARNUNG: Keychain nicht verfügbar. Schlüssel selbst sichern und "
              f"als Umgebungsvariable setzen:\n  export {ENV_KEY}='{new_key}'")
    return new_key.encode()


# --------------------------------------------------------------------------
# GCP Secret Manager (Produktionspfad auf e2-medium; reine Standardbibliothek)
# --------------------------------------------------------------------------

def _gcp_metadata(path: str, timeout: float = 1.5) -> str | None:
    import urllib.error
    import urllib.request
    req = urllib.request.Request(f"{GCP_METADATA}/{path}",
                                 headers={"Metadata-Flavor": "Google"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()
    except (urllib.error.URLError, OSError):
        return None


def _gcp_secret(secret_name: str) -> dict | None:
    """Liest das benannte Secret über den Service-Account der VM. None = kein GCP-Umfeld."""
    project = _gcp_metadata("project/project-id")
    if project is None:
        return None                                     # nicht auf einer GCP-VM
    token_raw = _gcp_metadata("instance/service-accounts/default/token")
    if token_raw is None:
        sys.exit("GCP-VM erkannt, aber kein Service-Account-Token -- "
                 "SA der VM prüfen.")
    import base64
    import urllib.request
    token = json.loads(token_raw)["access_token"]
    url = (f"https://secretmanager.googleapis.com/v1/projects/{project}"
           f"/secrets/{secret_name}/versions/latest:access")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read())["payload"]["data"]
        return json.loads(base64.b64decode(payload))
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Secret Manager-Zugriff fehlgeschlagen ({exc}) -- Secret "
                 f"'{secret_name}' und IAM-Rolle secretAccessor prüfen.")


# --------------------------------------------------------------------------
# API für broker.py
# --------------------------------------------------------------------------

def _secret_name(account: str) -> str:
    """GCP-Secret-Name je Konto. 'haupt' bleibt beim bestehenden Namen (rückwärts-
    kompatibel); andere Konten: env GCP_SECRET_NAME_<ACCOUNT> oder comdirect-<account>-credentials."""
    if account == "haupt":
        return GCP_SECRET_NAME
    return os.environ.get(f"GCP_SECRET_NAME_{account.upper()}",
                          f"comdirect-{account}-credentials")


def _secrets_file(account: str) -> Path:
    """Verschlüsselte Datei je Konto. 'haupt' bleibt comdirect.secrets.enc."""
    if account == "haupt":
        return SECRETS_FILE
    return SECRETS_FILE.parent / f"comdirect-{account}.secrets.enc"


def get_credentials(account: str = "haupt") -> dict:
    """Entschlüsselt die Zugangsdaten des Kontos (`haupt` oder z. B. `zweit`) und liefert
    sie als dict. Quelle je Konto: GCP-Secret `_secret_name(account)` bzw. Datei
    `_secrets_file(account)`. Der Fernet-Schlüssel (Keychain/$CREDSTORE_KEY) ist geteilt.

    Aufrufkontext beachten: Rückgabewerte niemals loggen und niemals in
    LLM-Prompts oder DecisionRequests aufnehmen (Sicherheitslinie der Architektur).
    """
    gcp = _gcp_secret(_secret_name(account))    # 1) GCP Secret Manager (e2-medium)
    if gcp is not None:
        return gcp
    sf = _secrets_file(account)
    if not sf.exists():
        sys.exit(f"{sf.name} fehlt -- 'python3 credstore.py init --account {account}' ausführen.")
    Fernet = _fernet()
    token = sf.read_bytes()
    try:
        return json.loads(Fernet(_get_key()).decrypt(token))
    except Exception:  # noqa: BLE001 -- InvalidToken u. a.
        sys.exit("Entschlüsselung fehlgeschlagen -- falscher Schlüssel? "
                 "(Keychain-Eintrag bzw. $CREDSTORE_KEY prüfen)")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _init(account: str = "haupt") -> None:
    Fernet = _fernet()
    key = _get_key(create=True)
    sf = _secrets_file(account)
    print(f"Zugangsdaten für Konto '{account}' eingeben (Eingabe unsichtbar):")
    data = {}
    for field_name, prompt in FIELDS:
        value = getpass.getpass(f"  {prompt}: ").strip()
        if value:
            data[field_name] = value
    if "client_id" not in data or "client_secret" not in data:
        sys.exit("client_id und client_secret sind Pflicht -- Abbruch.")
    sf.write_bytes(Fernet(key).encrypt(json.dumps(data).encode()))
    sf.chmod(stat.S_IRUSR | stat.S_IWUSR)     # 0600
    print(f"Verschlüsselt gespeichert: {sf.name} "
          f"({len(data)} Felder, Dateirechte 0600).")


def _check(account: str = "haupt") -> None:
    data = get_credentials(account)
    print(f"Konto '{account}' ({_secrets_file(account).name}) entschlüsselbar -- Felder:")
    for k, v in data.items():
        masked = v[:4] + "..." + v[-2:] if len(v) > 8 else "..."
        print(f"  {k}: {masked} ({len(v)} Zeichen)")


def main() -> None:
    args = sys.argv[1:]
    cmd = args[0] if args else ""
    account = "haupt"
    if "--account" in args:                             # optionaler Kontowahl
        account = args[args.index("--account") + 1]
    if cmd == "init":
        _init(account)
    elif cmd == "check":
        _check(account)
    else:
        print(__doc__)
        sys.exit("Verwendung: python3 credstore.py init|check [--account haupt|zweit]")


if __name__ == "__main__":
    main()
