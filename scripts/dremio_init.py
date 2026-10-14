#!/usr/bin/env python3
"""
Proyecto 2 FYHBD | Persona 3 | dremio-init — configuración automática de Dremio

Lo ejecuta el servicio `dremio-init` del docker-compose cuando se hace `docker compose up`
(mismo patrón que `minio-init` crea los buckets): se ejecuta UNA vez, deja Dremio listo y
termina con exit 0.

Qué deja hecho:
  1. Primer usuario de Dremio  (admin / proyecto2fyhbd, configurable por variables)
  2. El source `nessie_source` (Nessie + S3/MinIO) → Silver y Gold consultables desde Dremio
  3. Una consulta de validación sobre gold.user_engagement (best-effort: si Gold aún no está
     cargada avisa, pero no falla)

Idempotente: re-ejecutar `docker compose up` no duplica nada; si el source ya existe y
funciona, se conserva; si existe pero no responde, se recrea con la configuración correcta.

Configuración por variables (todas con default):
  DREMIO_URL, DREMIO_USER, DREMIO_PASS, NESSIE_CATALOG_ENDPOINT, S3_ENDPOINT,
  MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, AWS_ROOT_PATH, EVIDENCIAS_DIR

TRAMPAS DE DREMIO + MINIO (aprendidas probando):
  * fs.s3a.endpoint va SIN esquema (minio:9000) y el esquema aparte (fs.protocol=http);
    con "http://" adentro, el SDK v1 reintenta indefinidamente y el job queda colgado
    en METADATA_RETRIEVAL.
  * dremio.s3.compat=true salta la verificación contra STS de AWS (MinIO no tiene STS).
  * el secreto se llama awsAccessSecret (no awsSecretKey) y awsRootPath debe incluir el bucket.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

DREMIO = os.getenv("DREMIO_URL", "http://dremio:9047").rstrip("/")
USER = os.getenv("DREMIO_USER", "admin")
PASS = os.getenv("DREMIO_PASS", "proyecto2fyhbd")
NESSIE_EP = os.getenv("NESSIE_CATALOG_ENDPOINT", "http://nessie:19120/api/v2")
S3_EP = os.getenv("S3_ENDPOINT", "minio:9000")          # sin esquema (ver docstring)
S3_KEY = os.getenv("MINIO_ROOT_USER", "admin")
S3_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "password")
ROOT = os.getenv("AWS_ROOT_PATH", "/bucket-iceberg")
SOURCE = "nessie_source"

SOURCE_CONFIG = {
    "entityType": "source", "type": "NESSIE", "name": SOURCE,
    "config": {
        "nessieEndpoint": NESSIE_EP, "nessieAuthType": "NONE", "asyncEnabled": True,
        "credentialType": "ACCESS_KEY", "awsAccessKey": S3_KEY, "awsAccessSecret": S3_SECRET,
        "awsRootPath": ROOT, "secure": False,
        "propertyList": [
            {"name": "fs.s3a.endpoint", "value": S3_EP},
            {"name": "fs.protocol", "value": "http"},
            {"name": "fs.s3a.path.style.access", "value": "true"},
            {"name": "dremio.s3.compat", "value": "true"},
        ],
    },
    "metadataPolicy": {
        "authTTLMs": 86400000, "namesRefreshMs": 3600000,
        "datasetRefreshAfterMs": 3600000, "datasetExpireAfterMs": 10800000,
        "datasetUpdateMode": "INLINE", "deleteUnavailableDatasets": True,
        "autoPromoteDatasets": True,
    },
}


def http(method: str, url: str, payload: dict | None = None, token: str | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"{DREMIO}{url}", data=data, method=method)
    req.add_header("Content-Type", "application/json")

    if token:
        req.add_header("Authorization", f"_dremio {token}")

    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read().decode()
            return r.status, json.loads(body) if body else {}

    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body) if body else {}
        except ValueError:
            return e.code, {"errorMessage": body[:200]}

    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        return 503, {"errorMessage": str(e)}

def wait_for_dremio(seconds: int = 180) -> bool:
    """Espera a que la API de Dremio responda (al arrancar tarda ~1-2 min)."""
    for i in range(seconds // 3):
        status, _ = http("GET", "/apiv2/server_info")
        if status < 500:
            return True
        print(f"[dremio-init] Dremio aún no responde (intento {i + 1})...", flush=True)
        time.sleep(3)
    return False


def login() -> str | None:
    status, body = http("POST", "/apiv2/login", {"userName": USER, "password": PASS})
    return body.get("token") if status == 200 else None


def ensure_user() -> str:
    """Login; si no existe ningún usuario, crea el primero (como hace la pantalla /setup)."""
    token = login()
    if token:
        return token
    status, body = http("PUT", "/apiv2/bootstrap/firstuser", {
        "userName": USER, "firstName": "Grupo", "lastName": "6",
        "email": "grupo6@fyhbd.local", "createdAt": int(time.time() * 1000), "password": PASS,
    })
    if status == 200:
        print(f"[dremio-init] usuario '{USER}' creado", flush=True)
    else:
        print(f"[dremio-init] aviso al crear el usuario ({status}): {body.get('errorMessage', '')}", flush=True)
    token = login()
    if not token:
        raise SystemExit(f"[dremio-init] ERROR: no se pudo iniciar sesión como '{USER}'. "
                         f"Revise DREMIO_USER/DREMIO_PASS.")
    return token


def find_source(token: str) -> str | None:
    status, body = http("GET", "/api/v3/catalog", token=token)
    for c in body.get("data", []):
        if c.get("path") == [SOURCE]:
            return c["id"]
    return None


def validate_gold(token: str) -> int | None:
    """Best-effort: COUNT(*) de gold.user_engagement. None si Gold aún no está cargada."""
    status, body = http("POST", "/api/v3/sql", {"sql": f"SELECT COUNT(*) AS filas FROM {SOURCE}.gold.user_engagement"}, token=token)
    job = body.get("id")
    if not job:
        return None
    state = None
    for _ in range(40):
        time.sleep(3)
        _, j = http("GET", f"/api/v3/job/{job}", token=token)
        state = j.get("jobState")
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
    if state != "COMPLETED":
        return None
    _, res = http("GET", f"/api/v3/job/{job}/results", token=token)
    rows = res.get("rows") or []
    return rows[0].get("filas") if rows else None


def guardar_evidencia(creado: bool, filas: int | None) -> None:
    ev = os.getenv("EVIDENCIAS_DIR", "/evidencias")
    try:
        os.makedirs(ev, exist_ok=True)
        with open(os.path.join(ev, "dremio_init.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "fecha_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source": SOURCE, "source_creado_en_esta_corrida": creado,
                "gold_user_engagement_filas": filas,
            }, ensure_ascii=False) + "\n")
    except OSError:
        pass  # sin carpeta de evidencias: no es crítico


def main() -> int:
    print(f"[dremio-init] configurando Dremio en {DREMIO} ...", flush=True)
    if not wait_for_dremio():
        print("[dremio-init] ERROR: Dremio no respondió a tiempo", flush=True)
        return 1

    token = ensure_user()

    creado = False
    sid = find_source(token)
    if sid:
        filas = validate_gold(token)
        if filas is not None:
            print(f"[dremio-init] source '{SOURCE}' ya existe y funciona "
                  f"(gold.user_engagement = {filas} filas): se conserva", flush=True)
            guardar_evidencia(False, filas)
            return 0
        print(f"[dremio-init] source '{SOURCE}' existe pero no responde bien: se recrea", flush=True)
        http("DELETE", f"/api/v3/catalog/{sid}", token=token)

    status, body = http("POST", "/api/v3/catalog", SOURCE_CONFIG, token=token)
    if status != 200:
        print(f"[dremio-init] ERROR al crear el source ({status}): {body.get('errorMessage', '')}", flush=True)
        return 1
    creado = True
    print(f"[dremio-init] source '{SOURCE}' creado (Nessie + S3/MinIO)", flush=True)

    filas = validate_gold(token)
    if filas is None:
        print("[dremio-init] source listo. Gold aún no está cargada — "
              "ejecute scripts/gold_agg.py y vuelva a validar", flush=True)
    else:
        print(f"[dremio-init] validación OK: gold.user_engagement = {filas} filas "
              f"(debe coincidir con la réplica de Trino)", flush=True)
    guardar_evidencia(creado, filas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
