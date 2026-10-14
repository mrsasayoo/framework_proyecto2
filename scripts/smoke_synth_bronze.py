#!/usr/bin/env python3
"""
Proyecto 2 FYHBD | Utilidad de prueba | Datos sintéticos de Bronze (sin descargas)

Genera Parquet CRUDOS pequeos en las rutas exactas del contrato de Bronze
(s3a://bronze/{dataset}/{year}/{dataset}_{year}.parquet) para probar Silver → Gold
en cualquier maquina SIN esperar las descargas del dataset real (1.3–2.9 GB por archivo).

Los datos imitan las columnas del export de ClickHouse (StackOverflow) que consume
silver_transform.py: mismos nombres de columna originales (Id, PostTypeId, OwnerUserId, ...).

Uso (dentro del contenedor jupyter, que tiene PySpark + jars S3):
    docker exec -w /workspace jupyter_proyecto python scripts/smoke_synth_bronze.py
    docker exec -w /workspace jupyter_proyecto python scripts/smoke_synth_bronze.py --users 300 --posts 600
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

BUCKET = "bronze"
YEARS = (2020, 2021)


def get_spark() -> SparkSession:
    endpoint = "http://minio:9000"
    return (SparkSession.builder.master("local[2]").appName("smoke_synth_bronze")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", "admin")
            .config("spark.hadoop.fs.s3a.secret.key", "password")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


USERS_SCHEMA = StructType([
    StructField("Id", LongType()), StructField("Reputation", LongType()),
    StructField("CreationDate", TimestampType()), StructField("DisplayName", StringType()),
    StructField("LastAccessDate", TimestampType()), StructField("Location", StringType()),
    StructField("Views", LongType()), StructField("UpVotes", LongType()), StructField("DownVotes", LongType()),
    StructField("WebsiteUrl", StringType()), StructField("AccountId", LongType()),
])

POSTS_SCHEMA = StructType([
    StructField("Id", LongType()), StructField("PostTypeId", IntegerType()),
    StructField("ParentId", LongType()), StructField("AcceptedAnswerId", LongType()),
    StructField("CreationDate", TimestampType()), StructField("LastActivityDate", TimestampType()),
    StructField("LastEditDate", TimestampType()), StructField("ClosedDate", TimestampType()),
    StructField("CommunityOwnedDate", TimestampType()), StructField("OwnerUserId", LongType()),
    StructField("OwnerDisplayName", StringType()), StructField("LastEditorUserId", LongType()),
    StructField("Title", StringType()), StructField("Tags", StringType()),
    StructField("Score", IntegerType()), StructField("ViewCount", LongType()),
    StructField("AnswerCount", IntegerType()), StructField("CommentCount", IntegerType()),
    StructField("FavoriteCount", IntegerType()), StructField("ContentLicense", StringType()),
    StructField("Body", StringType()),
])

TAGS = ["python", "pandas", "spark", "sql", "pyspark", "iceberg", "docker", "airflow", "minio", "trino"]
LOCS = ["Cali, Colombia", "Bogotá, Colombia", "Medellín, Colombia", None, "Buenos Aires, Argentina"]


def ts(y, m, d, h=12):
    return datetime(y, m, min(d, 28), h, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)


def gen_users(spark, year, n, ids):
    rnd = random.Random(1000 + year)
    rows = [{
        "Id": i,
        "Reputation": rnd.randint(1, 50000),
        "CreationDate": ts(year, rnd.randint(1, 12), rnd.randint(1, 28)),
        "DisplayName": f"user_{i}",
        "LastAccessDate": ts(year, 12, rnd.randint(1, 28)),
        "Location": rnd.choice(LOCS),
        "Views": rnd.randint(0, 5000),
        "UpVotes": rnd.randint(0, 800), "DownVotes": rnd.randint(0, 120),
        "WebsiteUrl": rnd.choice(["https://example.com", "", None]),
        "AccountId": i + 900000,
    } for i in sorted(rnd.sample(ids, n))]
    return spark.createDataFrame(rows, USERS_SCHEMA)


def gen_posts(spark, year, n, users_this_year):
    rnd = random.Random(2000 + year)
    rows = []
    for k in range(n):
        is_q = rnd.random() < 0.45
        owner = rnd.choice(users_this_year) if rnd.random() < 0.9 else None   # algunos sin dueño
        tags = rnd.sample(TAGS, rnd.randint(1, 3))
        rows.append({
            "Id": year * 100000 + k,
            "PostTypeId": 1 if is_q else 2,
            "ParentId": None if is_q else year * 100000 + rnd.randint(0, max(n // 2 - 1, 0)),
            "AcceptedAnswerId": (year * 100000 + k + 1) if (is_q and rnd.random() < 0.5) else 0,
            "CreationDate": ts(year, rnd.randint(1, 12), rnd.randint(1, 28), rnd.randint(0, 23)),
            "LastActivityDate": ts(year, 12, rnd.randint(1, 28)),
            "LastEditDate": ts(year, rnd.randint(6, 12), rnd.randint(1, 28)) if rnd.random() < 0.5 else None,
            "ClosedDate": ts(year, 11, rnd.randint(1, 28)) if rnd.random() < 0.05 else None,
            "CommunityOwnedDate": None,
            "OwnerUserId": owner if owner else 0,
            "OwnerDisplayName": None if owner else f"anon_{k}",
            "LastEditorUserId": rnd.choice(users_this_year),
            "Title": f"¿Cómo hacer X_{k} en {tags[0]}?" if is_q else None,
            "Tags": "".join(f"<{t}>" for t in tags) if is_q else None,
            "Score": rnd.randint(-2, 120), "ViewCount": rnd.randint(0, 40000),
            "AnswerCount": rnd.randint(0, 9) if is_q else 0,
            "CommentCount": rnd.randint(0, 15), "FavoriteCount": rnd.randint(0, 30) if is_q else 0,
            "ContentLicense": "CC BY-SA 4.0",
            "Body": f"cuerpo del post {k}",
        })
    return spark.createDataFrame(rows, POSTS_SCHEMA)


def main():
    ap = argparse.ArgumentParser(description="Carga Bronze sintético (prueba local sin descargas)")
    ap.add_argument("--users", type=int, default=300, help="usuarios por año")
    ap.add_argument("--posts", type=int, default=600, help="posts por año")
    a = ap.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    # ids de usuarios con traslape entre años (para probar dedup/MERGE de Silver)
    pool = list(range(1, a.users * 3))
    for y in YEARS:
        users = gen_users(spark, y, a.users, pool)
        users.repartition(1).write.mode("overwrite").parquet(f"s3a://{BUCKET}/users/{y}/users_{y}.parquet")
        users_ids = [r["Id"] for r in users.select("Id").collect()]
        posts = gen_posts(spark, y, a.posts, users_ids)
        posts.repartition(1).write.mode("overwrite").parquet(f"s3a://{BUCKET}/post/{y}/post_{y}.parquet")
        print(f"[OK] bronze {y}: users={a.users} posts={a.posts}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
