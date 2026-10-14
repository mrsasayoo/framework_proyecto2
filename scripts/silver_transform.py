#!/usr/bin/env python
"""
Proyecto 2 FYHBD | Persona 2 | Capa SILVER (PySpark + Iceberg + MERGE, catalogo Nessie)

    Bronze (Parquet en MinIO) -> PySpark (limpieza, normalizacion, deduplicacion)
        -> MERGE INTO -> Silver Iceberg (nessie.silver.*) -> consultable desde Trino

Tablas (historicas, mas de un anio):
    nessie.silver.users_hist   <- bronze/users/2020 + bronze/users/2021   (la que corre por Airflow)
    nessie.silver.post_hist    <- bronze/post/2020  + bronze/post/2021    (consolidacion manual)

Clave del MERGE: id (Id de StackOverflow).  Ambas tablas tienen `fecha_cargue`.

Uso (dentro del cluster Spark; asi lo llama la linea de comandos / Airflow):
    docker exec spark_master /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/scripts/silver_transform.py --table users_hist
    ... --table post_hist [--limit 500000]
    ... --table all --years 2020,2021

Uso (Python / notebook):
    import silver_transform as s
    s.run_silver("users_hist", years=(2020, 2021))
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

# --------------------------------------------------------------------------- config
BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
CATALOG = "nessie"
NAMESPACE = "silver"
DEFAULT_YEARS = (2020, 2021)

POST_TYPES = {1: "Question", 2: "Answer", 3: "Wiki", 4: "TagWikiExcerpt", 5: "TagWiki",
              6: "ModeratorNomination", 7: "WikiPlaceholder", 8: "PrivilegeWiki"}


def bronze_uri(dataset: str, year: int) -> str:
    """Ruta exacta del contrato de Bronze (NO leer la carpeta con comodin: dlt deja _dlt_* y .jsonl)."""
    return f"s3a://{BUCKET}/{dataset}/{year}/{dataset}_{year}.parquet"


# --------------------------------------------------------------------------- Spark
def get_spark(master: str | None = None, app_name: str = "silver_transform") -> SparkSession:
    """SparkSession contra el cluster (spark-master) con catalogo Iceberg REST de Nessie y S3 (MinIO)."""
    master = master or os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "admin"))
    sk = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password"))
    region = os.getenv("AWS_REGION", "us-east-1")
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg")
    warehouse = os.getenv("NESSIE_WAREHOUSE", "warehouse")   # nombre del warehouse en Nessie (compose)
    cat = f"spark.sql.catalog.{CATALOG}"

    b = (SparkSession.builder.appName(app_name).master(master)
         # --- Iceberg + catalogo Nessie (REST)
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config(cat, "org.apache.iceberg.spark.SparkCatalog")
         .config(f"{cat}.type", "rest")
         .config(f"{cat}.uri", nessie_uri)
         .config(f"{cat}.warehouse", warehouse)
         .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config(f"{cat}.s3.endpoint", endpoint)
         .config(f"{cat}.s3.path-style-access", "true")
         .config(f"{cat}.s3.access-key-id", ak)
         .config(f"{cat}.s3.secret-access-key", sk)
         .config(f"{cat}.client.region", region)
         # --- lectura de Bronze (s3a://) en MinIO
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.endpoint", endpoint)
         .config("spark.hadoop.fs.s3a.access.key", ak)
         .config("spark.hadoop.fs.s3a.secret.key", sk)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         # --- varios
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")))

    if master.startswith("spark://"):                      # el worker del compose tiene 2 cores / 3g
        b = (b.config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
              .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "2")))
    driver_host = os.getenv("SPARK_DRIVER_HOST")           # el worker debe poder llamar de vuelta al driver
    if driver_host:
        b = (b.config("spark.driver.host", driver_host)
              .config("spark.driver.bindAddress", "0.0.0.0")
              .config("spark.driver.port", "7078")
              .config("spark.blockManager.port", "7079"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _exists(spark: SparkSession, uri: str) -> bool:
    jpath = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return bool(fs.exists(jpath))


# --------------------------------------------------------------------------- helpers de limpieza
def _col(df: DataFrame, name: str, dtype: str):
    """Columna casteada; si no existe en este Parquet devuelve NULL (tolera cambios de esquema).

    El Parquet de ClickHouse guarda algunos textos (p.ej. Reputation LowCardinality(String)) como BINARY
    sin anotacion UTF8. Spark no castea BINARY -> BIGINT/TIMESTAMP directo, asi que se pasa por string."""
    if name not in df.columns:
        return F.lit(None).cast(dtype)
    c = F.col(name)
    if isinstance(df.schema[name].dataType, BinaryType) and dtype != "string":
        c = c.cast("string")
        if dtype in ("long", "int", "bigint"):
            c = F.trim(c)                      # "  123 " -> "123" (si no es numerico queda NULL)
    return c.cast(dtype)


def _txt(df: DataFrame, name: str):
    """Texto normalizado: trim, espacios multiples colapsados, '' -> NULL."""
    if name not in df.columns:
        return F.lit(None).cast("string")
    c = F.regexp_replace(F.trim(F.col(name).cast("string")), r"\s+", " ")
    return F.when(c == "", None).otherwise(c)


def _ts(df: DataFrame, name: str, optional: bool = True):
    """Timestamp; en columnas opcionales ClickHouse usa 1970-01-01 como 'vacio' -> NULL."""
    c = _col(df, name, "timestamp")
    return F.when(F.year(c) <= 1970, None).otherwise(c) if optional else c


def _id(df: DataFrame, name: str):
    """Id opcional: ClickHouse usa 0 como 'sin valor' -> NULL."""
    c = _col(df, name, "long")
    return F.when(c == 0, None).otherwise(c)


def _cnt(df: DataFrame, name: str, dtype: str = "int"):
    """Contador >= 0 (nulos y negativos -> 0)."""
    zero = F.lit(0).cast(dtype)
    return F.greatest(_col(df, name, dtype), zero)


# --------------------------------------------------------------------------- transformaciones
def curate_users(df: DataFrame, source_year: int) -> DataFrame:
    """Limpieza + normalizacion de Users (un archivo Bronze)."""
    out = df.select(
        _col(df, "Id", "long").alias("id"),
        _cnt(df, "Reputation", "long").alias("reputation"),
        _ts(df, "CreationDate", optional=False).alias("creation_date"),
        _txt(df, "DisplayName").alias("display_name"),
        _ts(df, "LastAccessDate").alias("last_access_date"),
        _txt(df, "Location").alias("location"),
        _cnt(df, "Views", "long").alias("views"),
        _cnt(df, "UpVotes", "long").alias("up_votes"),
        _cnt(df, "DownVotes", "long").alias("down_votes"),
        F.lower(_txt(df, "WebsiteUrl")).alias("website_url"),
        _col(df, "AccountId", "long").alias("account_id"),
    )
    return (out.where(F.col("id").isNotNull() & F.col("creation_date").isNotNull())
               .withColumn("creation_year", F.year("creation_date"))
               .withColumn("source_year", F.lit(source_year).cast("int")))


def curate_posts(df: DataFrame, source_year: int) -> DataFrame:
    """Limpieza + normalizacion de Posts (un archivo Bronze). Body se descarta (pesado, no se usa en Gold)."""
    # PostTypeId puede venir numerico (1/2) o como etiqueta (Question/Answer) segun el export
    raw = F.col("PostTypeId").cast("string") if "PostTypeId" in df.columns else F.lit(None).cast("string")
    by_num = F.create_map(*[x for k, v in POST_TYPES.items() for x in (F.lit(str(k)), F.lit(v))])
    by_name = F.create_map(*[x for k, v in POST_TYPES.items() for x in (F.lit(v), F.lit(k))])
    post_type_id = F.coalesce(raw.cast("int"), by_name[raw].cast("int"))
    post_type = F.coalesce(by_num[raw], raw)

    # Tags: "<python><pandas>" (o "python|pandas", o ya un array) -> array<string>
    if "Tags" in df.columns and df.schema["Tags"].dataType.simpleString().startswith("array"):
        tags_arr = F.col("Tags").cast("array<string>")
    else:
        t = _txt(df, "Tags")
        tags_arr = F.split(F.regexp_replace(t, r"^<|>$", ""), r"><|\|")
    # ClickHouse guarda "|python|pandas|": el split deja '' al inicio y al final -> se filtran los vacios
    tag_list = F.coalesce(F.filter(tags_arr, lambda x: x.isNotNull() & (x != "")),
                          F.array().cast("array<string>"))

    out = df.select(
        _col(df, "Id", "long").alias("id"),
        post_type_id.alias("post_type_id"),
        post_type.alias("post_type"),
        _id(df, "ParentId").alias("parent_id"),
        _id(df, "AcceptedAnswerId").alias("accepted_answer_id"),
        _ts(df, "CreationDate", optional=False).alias("creation_date"),
        _ts(df, "LastActivityDate").alias("last_activity_date"),
        _ts(df, "LastEditDate").alias("last_edit_date"),
        _ts(df, "ClosedDate").alias("closed_date"),
        _ts(df, "CommunityOwnedDate").alias("community_owned_date"),
        _id(df, "OwnerUserId").alias("owner_user_id"),
        _txt(df, "OwnerDisplayName").alias("owner_display_name"),
        _id(df, "LastEditorUserId").alias("last_editor_user_id"),
        _txt(df, "Title").alias("title"),
        tag_list.alias("tag_list"),
        _cnt(df, "Score", "int").alias("score"),
        _cnt(df, "ViewCount", "long").alias("view_count"),
        _cnt(df, "AnswerCount", "int").alias("answer_count"),
        _cnt(df, "CommentCount", "int").alias("comment_count"),
        _cnt(df, "FavoriteCount", "int").alias("favorite_count"),
        _txt(df, "ContentLicense").alias("content_license"),
    )
    return (out.where(F.col("id").isNotNull() & F.col("creation_date").isNotNull())
               .withColumn("tags", F.concat_ws("|", "tag_list"))
               .withColumn("is_question", F.col("post_type_id") == 1)
               .withColumn("has_accepted_answer", F.col("accepted_answer_id").isNotNull())
               .withColumn("creation_year", F.year("creation_date"))
               .withColumn("source_year", F.lit(source_year).cast("int")))


def finalize(df: DataFrame, order_cols: list, load_ts: datetime) -> DataFrame:
    """Deduplica por id (conserva el registro mas reciente), calcula row_hash y agrega fecha_cargue."""
    w = Window.partitionBy("id").orderBy(*order_cols)
    dedup = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    data_cols = list(dedup.columns)
    return (dedup
            .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in data_cols]), 256))
            .withColumn("fecha_cargue", F.lit(load_ts).cast("timestamp")))


# --------------------------------------------------------------------------- Iceberg: crear + MERGE
def ensure_table(spark: SparkSession, fq: str, df: DataFrame) -> bool:
    """Crea la tabla Iceberg (v2, particionada por anio de creacion) si no existe. True si la creo."""
    if spark.catalog.tableExists(fq):
        return False
    (df.limit(0).writeTo(fq).using("iceberg")
       .partitionedBy(F.col("creation_year"))
       .tableProperty("format-version", "2")
       .create())
    return True


def merge_into(spark: SparkSession, fq: str, src: DataFrame) -> None:
    """MERGE (no overwrite): inserta ids nuevos y actualiza solo los que cambiaron (row_hash distinto)."""
    src.createOrReplaceTempView("_silver_src")
    spark.sql(f"""
        MERGE INTO {fq} AS t
        USING _silver_src AS s
        ON t.id = s.id
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _build(spark, table: str, years, limit):
    """Lee Bronze (un archivo por anio), curata y devuelve (df_union, archivos_leidos)."""
    dataset, curate = {"users_hist": ("users", curate_users), "post_hist": ("post", curate_posts)}[table]
    parts, leidos = [], []
    for y in years:
        uri = bronze_uri(dataset, y)
        if not _exists(spark, uri):
            print(f"[bronze] AVISO: no existe {uri} (se omite)", flush=True)
            continue
        raw = spark.read.parquet(uri)                       # exacto: un archivo, sin comodines
        if limit:
            raw = raw.limit(limit)
        parts.append(curate(raw, y))
        leidos.append(uri)
        print(f"[bronze] leido {uri}", flush=True)
    if not parts:
        raise FileNotFoundError(f"No hay Parquet en Bronze para {table} y anios {list(years)}. "
                                f"Ejecute scripts/bronze_ingest.py primero.")
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p, allowMissingColumns=True)
    return df, leidos


def order_cols(table: str) -> list:
    """Que registro gana cuando un id aparece en mas de un archivo/anio (se arma en runtime: F.col necesita SparkContext)."""
    first = {"users_hist": "last_access_date", "post_hist": "last_activity_date"}[table]
    return [F.col(first).desc_nulls_last(), F.col("source_year").desc()]


def run_silver(table: str, years=DEFAULT_YEARS, limit: int | None = None,
               spark: SparkSession | None = None) -> dict:
    """Construye/actualiza una tabla Silver con MERGE. Idempotente. Devuelve el resumen de la corrida."""
    if table not in ("users_hist", "post_hist"):
        raise ValueError(f"tabla '{table}' no soportada. Opciones: users_hist, post_hist")
    t0 = time.time()
    own = spark is None
    spark = spark or get_spark(app_name=f"silver_{table}")
    fq = f"{CATALOG}.{NAMESPACE}.{table}"
    load_ts = datetime.now(timezone.utc).replace(microsecond=0)   # UTC con zona (aware): un solo valor por corrida
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        raw_df, leidos = _build(spark, table, years, limit)
        src = finalize(raw_df, order_cols(table), load_ts).persist(StorageLevel.MEMORY_AND_DISK)
        filas_src = src.count()

        creada = ensure_table(spark, fq, src)
        antes = 0 if creada else spark.table(fq).count()
        merge_into(spark, fq, src)
        tbl = spark.table(fq)
        despues = tbl.count()

        # ---- verificaciones de calidad sobre lo que quedo en Iceberg
        dup = tbl.groupBy("id").count().where("count > 1").count()
        sin_fecha = tbl.where("fecha_cargue IS NULL").count()
        assert dup == 0, f"{fq}: {dup} ids duplicados tras el MERGE"
        assert sin_fecha == 0, f"{fq}: {sin_fecha} filas sin fecha_cargue"
        por_anio = {str(r["creation_year"]): r["n"] for r in
                    tbl.groupBy("creation_year").agg(F.count("*").alias("n")).orderBy("creation_year").collect()}
        snap = spark.sql(f"SELECT snapshot_id, committed_at, operation, summary FROM {fq}.snapshots "
                         f"ORDER BY committed_at DESC LIMIT 1").collect()
        snap = snap[0].asDict() if snap else {}
        src.unpersist()

        resumen = {
            "tabla": fq, "archivos_bronze": leidos, "limit": limit,
            "tabla_creada_en_esta_corrida": creada,
            "filas_fuente_deduplicadas": filas_src, "filas_antes": antes, "filas_despues": despues,
            "filas_nuevas": despues - antes, "duplicados_id": dup,
            "filas_por_anio_creacion": por_anio,
            "ultimo_snapshot": {k: (v if k != "summary" else dict(v)) for k, v in snap.items()},
            "fecha_cargue": load_ts.isoformat(), "segundos": round(time.time() - t0, 1),
        }
        print("[OK] " + json.dumps(resumen, ensure_ascii=False, default=str), flush=True)
        _guardar_evidencia(resumen)
        return resumen
    finally:
        if own:
            spark.stop()


def _guardar_evidencia(resumen: dict) -> None:
    ev = Path(os.getenv("EVIDENCIAS_DIR", "/evidencias"))
    try:
        ev.mkdir(parents=True, exist_ok=True)
        with (ev / "silver_transform.log").open("a", encoding="utf-8") as f:
            f.write(json.dumps(resumen, ensure_ascii=False, default=str) + "\n")
    except OSError:
        pass  # sin carpeta de evidencias: no es critico


# --------------------------------------------------------------------------- CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Silver StackOverflow: Bronze (Parquet) -> Iceberg (MERGE) con Spark")
    ap.add_argument("--table", choices=["users_hist", "post_hist", "all"], default="all")
    ap.add_argument("--years", default=",".join(map(str, DEFAULT_YEARS)),
                    help="anios de Bronze a consolidar, p.ej. 2020,2021")
    ap.add_argument("--limit", type=int, default=None, help="limitar filas leidas por archivo (opcional)")
    ap.add_argument("--master", default=None, help="p.ej. local[2]; por defecto SPARK_MASTER_URL")
    a = ap.parse_args(argv)

    years = tuple(int(y) for y in a.years.split(",") if y.strip())
    tablas = ["users_hist", "post_hist"] if a.table == "all" else [a.table]
    spark = get_spark(a.master)
    try:
        for t in tablas:
            run_silver(t, years, a.limit, spark=spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())