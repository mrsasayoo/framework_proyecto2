#!/usr/bin/env python
"""
Proyecto 2 FYHBD | Persona 1 | Capa BRONZE (Parquet crudo en MinIO)

    Fuente (S3 publico de ClickHouse) -> dlt -> Parquet -> MinIO (bucket 'bronze')

Contrato de rutas (lo consumen Silver/Gold, NO cambiar sin avisar al equipo):

    s3://bronze/post/2020/post_2020.parquet
    s3://bronze/post/2021/post_2021.parquet
    s3://bronze/users/2020/users_2020.parquet
    s3://bronze/users/2021/users_2021.parquet      <- el unico que corre por Airflow

Uso (CLI):
    python scripts/bronze_ingest.py --dataset users --year 2021      # lo que llama Airflow
    python scripts/bronze_ingest.py --manual                         # post 2020/2021 + users 2020
    python scripts/bronze_ingest.py --dataset post --year 2020
    python scripts/bronze_ingest.py --list

Uso (Python / Airflow / notebook):
    from bronze_ingest import run_bronze
    run_bronze("users", 2021)

Bronze = datos crudos, Parquet, SOBRESCRITURA permitida: write_disposition="replace"
(re-ejecutar no duplica; reemplaza el archivo del dataset/anio).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

# Bronze es crudo: conservar los nombres de columna originales (PostTypeId, OwnerUserId...)
# y NO convertirlos a snake_case. Debe fijarse ANTES de importar dlt.
os.environ.setdefault("SCHEMA__NAMING", "direct")

import dlt                      # noqa: E402
import fsspec                   # noqa: E402
import pyarrow as pa            # noqa: E402
import pyarrow.compute as pc    # noqa: E402
import pyarrow.parquet as pq    # noqa: E402
import s3fs                     # noqa: E402
from tqdm import tqdm           # noqa: E402

# --------------------------------------------------------------------------- config
BASE_URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"
BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
BATCH_ROWS = int(os.getenv("BRONZE_BATCH_ROWS", "100000"))   # RAM acotada (Body de posts es pesado)
# Limite opcional de filas por archivo/anio (0 = sin limite). Ej: BRONZE_MAX_ROWS=200000
MAX_ROWS = int(os.getenv("BRONZE_MAX_ROWS", "0"))

# nombre en Bronze -> url de la fuente y columna de fecha para filtrar por anio
# (date_col=None: la fuente ya viene un archivo por anio)
DATASETS = {
    "post":      dict(url="{base}/posts/{year}.parquet",    date_col=None),
    "users":     dict(url="{base}/users.parquet",           date_col="CreationDate"),  # archivo unico -> se filtra
    "votes":     dict(url="{base}/votes/{year}.parquet",    date_col=None),
    "comments":  dict(url="{base}/comments/{year}.parquet", date_col=None),
    "badges":    dict(url="{base}/badges.parquet",          date_col="Date"),
    "postlinks": dict(url="{base}/postlinks.parquet",       date_col="CreationDate"),
}

# Carga manual previa a la clase (todo menos users_2021, que va por el pipeline)
MANUAL_LOADS = [("post", 2020), ("post", 2021), ("users", 2020)]


def _minio_cfg() -> dict:
    """Credenciales: variables de entorno > .dlt/secrets.toml [minio] > defaults del compose."""
    cfg = {"endpoint": "http://minio:9000", "access_key": "admin", "secret_key": "password"}
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        f = base / ".dlt" / "secrets.toml"
        if f.exists():
            cfg.update(tomllib.loads(f.read_text()).get("minio", {}))
            break
    cfg["endpoint"] = os.getenv("MINIO_ENDPOINT", cfg["endpoint"])
    cfg["access_key"] = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", cfg["access_key"]))
    cfg["secret_key"] = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", cfg["secret_key"]))
    return cfg


def minio_fs() -> s3fs.S3FileSystem:
    c = _minio_cfg()
    return s3fs.S3FileSystem(key=c["access_key"], secret=c["secret_key"],
                             client_kwargs={"endpoint_url": c["endpoint"]})


def source_url(dataset: str, year: int, override: str | None = None) -> str:
    return override or DATASETS[dataset]["url"].format(base=BASE_URL, year=year)


def target_path(dataset: str, year: int) -> str:
    """Ruta final esperada (sin esquema s3://)."""
    return f"{BUCKET}/{dataset}/{year}/{dataset}_{year}.parquet"


def _row_groups_del_anio(pf: pq.ParquetFile, date_col: str, year: int) -> list[int]:
    """Row groups cuyo rango [min, max] de date_col puede contener el anio pedido (usa las estadisticas
    del Parquet, asi NO se descarga ni se lee el resto del archivo). Sin estadisticas -> se incluye."""
    md = pf.metadata
    idx = next((i for i in range(md.num_columns)
                if md.row_group(0).column(i).path_in_schema == date_col), None)
    if idx is None:
        return list(range(md.num_row_groups))
    elegidos = []
    for g in range(md.num_row_groups):
        st = md.row_group(g).column(idx).statistics
        if st is None or not st.has_min_max or st.min.year <= year <= st.max.year:
            elegidos.append(g)
    return elegidos


# --------------------------------------------------------------------------- dlt
def _make_resource(dataset: str, year: int, url: str, stats: dict):
    date_col = DATASETS[dataset]["date_col"]
    table = f"{dataset}_{year}"

    @dlt.resource(name=table, write_disposition="replace")
    def leer_fuente():
        with fsspec.open(url, "rb") as f:
            pf = pq.ParquetFile(f)
            stats["filas_fuente"] = pf.metadata.num_rows
            print(f"[fuente] {url}\n[fuente] {pf.metadata.num_rows:,} filas x "
                  f"{pf.metadata.num_columns} columnas", flush=True)
            rgs = None
            total = pf.metadata.num_rows
            if date_col:      # saltar los row groups que no son del anio (mucho mas rapido)
                rgs = _row_groups_del_anio(pf, date_col, year)
                total = sum(pf.metadata.row_group(g).num_rows for g in rgs)
                print(f"[fuente] row groups a leer para {year}: {len(rgs)} de {pf.metadata.num_row_groups} "
                      f"(~{total:,} filas a revisar)", flush=True)
            if MAX_ROWS:
                print(f"[fuente] limite BRONZE_MAX_ROWS={MAX_ROWS:,} filas", flush=True)
            cargadas = 0
            for batch in tqdm(pf.iter_batches(batch_size=BATCH_ROWS, row_groups=rgs),
                              total=total,
                              desc=f"[fuente] {dataset}_{year}",
                              unit="filas", unit_scale=True, mininterval=1.0,
                              dynamic_ncols=True):
                tbl = pa.Table.from_batches([batch])
                if date_col:  # fuente unica (users/badges...): quedarse solo con el anio pedido
                    tbl = tbl.filter(pc.equal(pc.year(tbl[date_col]), year))
                if tbl.num_rows == 0:
                    continue
                if MAX_ROWS and cargadas + tbl.num_rows > MAX_ROWS:
                    tbl = tbl.slice(0, MAX_ROWS - cargadas)
                cargadas += tbl.num_rows
                yield tbl
                if MAX_ROWS and cargadas >= MAX_ROWS:
                    break
            stats["filas_cargadas"] = cargadas
            print(f"[fuente] filas a cargar en Bronze ({year}): {cargadas:,}", flush=True)

    return leer_fuente


def run_bronze(dataset: str, year: int, url: str | None = None) -> dict:
    """Ingesta un dataset/anio a Bronze. Idempotente (replace). Devuelve el resumen de la corrida."""
    if dataset not in DATASETS:
        raise ValueError(f"dataset '{dataset}' no soportado. Opciones: {list(DATASETS)}")
    t0 = time.time()
    c = _minio_cfg()
    fs = minio_fs()
    if not fs.exists(BUCKET):                       # bucket idempotente
        fs.mkdir(BUCKET)
        print(f"[minio] bucket '{BUCKET}' creado")

    stats: dict = {}
    # dlt escribe en <bucket_url>/<dataset_name>/<layout>:
    #   s3://bronze/<dataset> / <anio> / <dataset>_<anio>.parquet
    destino = dlt.destinations.filesystem(
        bucket_url=f"s3://{BUCKET}/{dataset}",
        layout="{table_name}.{ext}",
        credentials={
            "aws_access_key_id": c["access_key"],
            "aws_secret_access_key": c["secret_key"],
            "endpoint_url": c["endpoint"],
            "region_name": "us-east-1",
        },
    )
    pipeline = dlt.pipeline(pipeline_name=f"bronze_{dataset}_{year}",
                            destination=destino,
                            dataset_name=str(year))
    info = pipeline.run(_make_resource(dataset, year, source_url(dataset, year, url), stats),
                        loader_file_format="parquet")
    print(info)

    # ---- verificacion: releer metadatos DESDE MinIO
    fs.invalidate_cache()
    ruta = target_path(dataset, year)
    if not fs.exists(ruta):
        raise FileNotFoundError(f"No aparecio {ruta} en MinIO")
    with fs.open(ruta, "rb") as fh:
        md = pq.read_metadata(fh)
    esperado = stats.get("filas_cargadas")
    assert md.num_rows == esperado, f"{ruta}: MinIO={md.num_rows} vs cargadas={esperado}"
    resumen = {
        "dataset": dataset, "anio": year, "ruta": f"s3://{ruta}",
        "filas": md.num_rows, "columnas": md.num_columns,
        "tamano_mb": round(fs.size(ruta) / 1e6, 2),
        "segundos": round(time.time() - t0, 1),
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    print(f"[OK] {json.dumps(resumen, ensure_ascii=False)}")
    _guardar_evidencia(resumen)
    return resumen


def _guardar_evidencia(resumen: dict) -> None:
    ev = Path(os.getenv("EVIDENCIAS_DIR", "/evidencias"))
    try:
        ev.mkdir(parents=True, exist_ok=True)
        with (ev / "bronze_ingest.log").open("a", encoding="utf-8") as f:
            f.write(json.dumps(resumen, ensure_ascii=False) + "\n")
    except OSError:
        pass  # sin carpeta de evidencias (p.ej. dentro de Airflow): no es critico


def listar_bronze() -> None:
    fs = minio_fs()
    fs.invalidate_cache()
    print(f"Contenido de s3://{BUCKET}/")
    for p in sorted(fs.find(BUCKET)):
        if p.endswith(".parquet"):
            print(f"  {p}  ({fs.size(p)/1e6:,.1f} MB)")


# --------------------------------------------------------------------------- CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Ingesta Bronze StackOverflow -> MinIO (dlt)")
    ap.add_argument("--dataset", choices=list(DATASETS))
    ap.add_argument("--year", type=int)
    ap.add_argument("--url", help="fuente alterna (p.ej. archivo local) en vez de la publica")
    ap.add_argument("--manual", action="store_true",
                    help="carga manual previa: post 2020, post 2021, users 2020")
    ap.add_argument("--list", action="store_true", help="lista los parquet de Bronze")
    a = ap.parse_args(argv)

    if a.list:
        listar_bronze()
        return 0
    if a.manual:
        for ds, y in MANUAL_LOADS:
            run_bronze(ds, y)
        listar_bronze()
        return 0
    if not (a.dataset and a.year):
        ap.error("indique --dataset y --year (o use --manual / --list)")
    run_bronze(a.dataset, a.year, a.url)
    return 0


if __name__ == "__main__":
    sys.exit(main())