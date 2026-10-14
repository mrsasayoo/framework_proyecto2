#!/usr/bin/env python3
"""
Proyecto 2 FYHBD | Persona 3 | Capa GOLD (PySpark + Iceberg + MERGE, catálogo Nessie)

    Silver (nessie.silver.*) → PySpark (reglas de negocio + agregaciones)
        → MERGE INTO → Gold Iceberg (nessie.gold.*) → consultable desde Dremio / Trino

Tablas (grano y clave del MERGE):
    nessie.gold.user_engagement       ← users_hist LEFT JOIN post_hist (clave: user_id)  [la tabla del DAG]
    nessie.gold.post_counts_by_user   ← post_hist por usuario          (clave: user_id)
    nessie.gold.top_tags              ← preguntas por etiqueta         (clave: tag)
    nessie.gold.post_activity_monthly ← actividad por mes              (clave: mes)

Todas: Iceberg v2, escritura con MERGE (no override), `row_hash` para detectar cambios,
`fecha_cargue` (timestamp UTC de la corrida). Idempotentes: re-ejecutar no duplica.

Uso (dentro del cluster Spark; así lo llama la línea de comandos / Airflow):
    docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/gold_agg.py
    ... --tables user_engagement      (solo la tabla del pipeline)
    ... --tables all                  (por defecto: las 4 tablas)

Uso (Python / notebook / Airflow):
    from gold_agg import run_gold
    run_gold()                        # todas; o run_gold(["user_engagement"])
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

from silver_transform import get_spark          # mismo bloque de configuración de Silver (contrato P2)

# --------------------------------------------------------------------------- config
CATALOG = "nessie"
NAMESPACE = "gold"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.{NAMESPACE}"

TABLES = ("user_engagement", "post_counts_by_user", "top_tags", "post_activity_monthly")
# clave de negocio de cada tabla (MERGE ON) y columnas que definen el orden estable
MERGE_KEY = {"user_engagement": "user_id", "post_counts_by_user": "user_id",
             "top_tags": "tag", "post_activity_monthly": "mes"}


# --------------------------------------------------------------------------- agregaciones
def build_user_engagement(spark: SparkSession) -> DataFrame:
    """Métricas por usuario cruzando las DOS tablas históricas de Silver (la tabla del pipeline).

    Regla de negocio: un usuario sin posts también cuenta (LEFT JOIN) con sus métricas en 0;
    preguntas/respuestas según post_type_id; las fechas primera/última publicación quedan
    NULL si nunca publicó."""
    u = spark.table(f"{SILVER}.users_hist").select(
        F.col("id").alias("user_id"), "display_name", "location", "reputation")
    p = spark.table(f"{SILVER}.post_hist").select(
        F.col("id").alias("post_id"), "owner_user_id", "post_type_id",
        "has_accepted_answer", "score", "view_count", "creation_date")
    return (u.join(p, p.owner_user_id == u.user_id, "left")
             .groupBy("user_id", "display_name", "location", "reputation")
             .agg(F.count(F.when(F.col("post_type_id") == 1, 1)).cast("long").alias("num_questions"),
                  F.count(F.when(F.col("post_type_id") == 2, 1)).cast("long").alias("num_answers"),
                  F.count("post_id").cast("long").alias("num_posts"),
                  F.coalesce(F.sum("score"), F.lit(0)).cast("long").alias("total_score"),
                  F.count(F.when(F.col("has_accepted_answer"), 1)).cast("long").alias("questions_with_accepted_answer"),
                  F.coalesce(F.sum("view_count"), F.lit(0)).cast("long").alias("total_views"),
                  F.min("creation_date").alias("first_post_date"),
                  F.max("creation_date").alias("last_post_date")))


def build_post_counts_by_user(spark: SparkSession) -> DataFrame:
    """Preguntas y respuestas publicadas por usuario (solo usuarios que publicaron)."""
    p = spark.table(f"{SILVER}.post_hist").where(F.col("owner_user_id").isNotNull())
    return (p.groupBy(F.col("owner_user_id").alias("user_id"))
             .agg(F.count(F.when(F.col("post_type_id") == 1, 1)).cast("long").alias("num_questions"),
                  F.count(F.when(F.col("post_type_id") == 2, 1)).cast("long").alias("num_answers")))


def build_top_tags(spark: SparkSession) -> DataFrame:
    """Ranking de etiquetas por número de preguntas, con score promedio."""
    p = (spark.table(f"{SILVER}.post_hist")
           .where(F.col("post_type_id") == 1)
           .select("score", F.explode("tag_list").alias("tag")))
    base = (p.groupBy("tag")
              .agg(F.count("*").cast("long").alias("num_questions"),
                   F.round(F.avg("score"), 2).alias("avg_score")))
    w = Window.orderBy(F.col("num_questions").desc(), F.col("tag"))
    return base.withColumn("tag_rank", F.row_number().over(w).cast("int"))


def build_post_activity_monthly(spark: SparkSession) -> DataFrame:
    """Preguntas y respuestas por mes (tabla analítica de evolución)."""
    p = spark.table(f"{SILVER}.post_hist")
    return (p.groupBy(F.date_trunc("month", F.col("creation_date")).alias("mes"))
             .agg(F.count(F.when(F.col("post_type_id") == 1, 1)).cast("long").alias("preguntas"),
                  F.count(F.when(F.col("post_type_id") == 2, 1)).cast("long").alias("respuestas")))


BUILDERS = {"user_engagement": build_user_engagement,
            "post_counts_by_user": build_post_counts_by_user,
            "top_tags": build_top_tags,
            "post_activity_monthly": build_post_activity_monthly}


# --------------------------------------------------------------------------- Iceberg: crear + MERGE
def finalize(df: DataFrame, load_ts: datetime) -> DataFrame:
    """Agrega row_hash (SHA-256 de las columnas de datos) y fecha_cargue."""
    data_cols = list(df.columns)
    return (df.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in data_cols]), 256))
              .withColumn("fecha_cargue", F.lit(load_ts).cast("timestamp")))


def ensure_table(spark: SparkSession, fq: str, df: DataFrame) -> bool:
    """Crea la tabla Iceberg v2 (sin particiones: son agregados pequeños) si no existe."""
    if spark.catalog.tableExists(fq):
        return False
    (df.limit(0).writeTo(fq).using("iceberg")
       .tableProperty("format-version", "2")
       .create())
    return True


def merge_into(spark: SparkSession, fq: str, src: DataFrame, key: str) -> None:
    """MERGE (no overwrite): inserta claves nuevas y actualiza solo las que cambiaron (row_hash)."""
    src.createOrReplaceTempView("_gold_src")
    spark.sql(f"""
        MERGE INTO {fq} AS t
        USING _gold_src AS s
        ON t.{key} = s.{key}
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# --------------------------------------------------------------------------- orquestación por tabla
def run_gold(tables=TABLES, spark: SparkSession | None = None) -> list[dict]:
    """Construye/actualiza las tablas Gold con MERGE. Idempotente. Devuelve los resúmenes."""
    bad = [t for t in tables if t not in TABLES]
    if bad:
        raise ValueError(f"tablas no soportadas: {bad}. Opciones: {', '.join(TABLES)}")
    own = spark is None
    spark = spark or get_spark(app_name="gold_agg")
    resumenes = []
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD}")
        # una sola fecha_cargue para toda la corrida (misma evidencia de cargue en las 4 tablas)
        load_ts = datetime.now(timezone.utc).replace(microsecond=0)
        for t in tables:
            t0 = time.time()
            fq = f"{GOLD}.{t}"
            key = MERGE_KEY[t]
            src = finalize(BUILDERS[t](spark), load_ts).persist(StorageLevel.MEMORY_AND_DISK)
            filas_src = src.count()

            creada = ensure_table(spark, fq, src)
            antes = 0 if creada else spark.table(fq).count()
            merge_into(spark, fq, src, key)
            tbl = spark.table(fq)
            despues = tbl.count()

            # ---- verificaciones de calidad sobre lo que quedó en Iceberg
            dup = tbl.groupBy(key).count().where("count > 1").count()
            sin_fecha = tbl.where("fecha_cargue IS NULL").count()
            assert dup == 0, f"{fq}: {dup} claves '{key}' duplicadas tras el MERGE"
            assert sin_fecha == 0, f"{fq}: {sin_fecha} filas sin fecha_cargue"
            snap = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {fq}.snapshots "
                             f"ORDER BY committed_at DESC LIMIT 1").collect()
            snap = snap[0].asDict() if snap else {}
            src.unpersist()

            resumen = {
                "tabla": fq, "clave_merge": key, "tabla_creada_en_esta_corrida": creada,
                "filas_fuente": filas_src, "filas_antes": antes, "filas_despues": despues,
                "filas_nuevas": despues - antes, "claves_duplicadas": dup,
                "ultimo_snapshot": {k: str(v) for k, v in snap.items()},
                "fecha_cargue": load_ts.isoformat(), "segundos": round(time.time() - t0, 1),
            }
            print("[OK] " + json.dumps(resumen, ensure_ascii=False, default=str), flush=True)
            _guardar_evidencia(resumen)
            resumenes.append(resumen)
        return resumenes
    finally:
        if own:
            spark.stop()


def _guardar_evidencia(resumen: dict) -> None:
    ev = Path(os.getenv("EVIDENCIAS_DIR", "/evidencias"))
    try:
        ev.mkdir(parents=True, exist_ok=True)
        with (ev / "gold_agg.log").open("a", encoding="utf-8") as f:
            f.write(json.dumps(resumen, ensure_ascii=False, default=str) + "\n")
    except OSError:
        pass  # sin carpeta de evidencias: no es crítico


# --------------------------------------------------------------------------- CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Gold StackOverflow: Silver (Iceberg) → agregaciones Iceberg con MERGE")
    ap.add_argument("--tables", default="all",
                    help="'all' o lista separada por comas, p. ej. user_engagement,top_tags")
    ap.add_argument("--master", default=None, help="p. ej. local[2]; por defecto SPARK_MASTER_URL")
    a = ap.parse_args(argv)
    tables = list(TABLES) if a.tables == "all" else [t.strip() for t in a.tables.split(",") if t.strip()]
    run_gold(tables, spark=get_spark(a.master, app_name="gold_agg"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
