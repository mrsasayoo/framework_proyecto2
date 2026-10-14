## 2. Capa Silver, Spark y Trino

**Responsable:** Persona 2 · **Entregables:** `scripts/silver_transform.py`, `notebooks/silver_transform.ipynb`, `spark/Dockerfile`, `trino/catalog/iceberg.properties`, `sql/replicate_gold_from_silver.sql`, este documento.

```
Bronze Parquet (MinIO) → PySpark (limpieza · normalización · deduplicación) → MERGE → Silver Iceberg → Nessie
                                                                                         ↓
                                                                      Trino (consulta Silver / replica Gold)
```

### 2.1 Qué hace Spark
Apache Spark (PySpark 3.5.3) es el motor de transformación de Silver: lee los Parquet crudos de Bronze, los limpia y normaliza, elimina duplicados y escribe tablas **Iceberg** en MinIO con `MERGE INTO`, registradas en el catálogo **Nessie**. Los jars de Iceberg (`iceberg-spark-runtime`, `iceberg-aws-bundle`) y de S3 (`hadoop-aws`, `aws-java-sdk-bundle`) vienen en `spark/Dockerfile`.

### 2.2 Spark Master y Spark Worker
| Servicio | Contenedor | Puerto host | Función |
|---|---|---|---|
| `spark-master` | `spark_master` | 8082 (UI) · 7077 (cluster) | Coordina el cluster; desde aquí se lanza `spark-submit` |
| `spark-worker` | `spark_worker` | 8083 (UI) | Ejecuta las tareas (2 cores, 3 GB) |

El *driver* corre en `spark_master` (script por `spark-submit`) o en `jupyter_proyecto` (notebook); en ambos casos los *executors* viven en el worker. UI del master: `http://localhost:8082` (debe mostrar 1 worker `ALIVE` y la aplicación `silver_*` mientras corre).

```bash
docker compose up -d --build minio minio-init nessie jupyter spark-master spark-worker trino
docker compose ps
```

### 2.3 Cómo lee Bronze
Lee **el archivo exacto** del contrato de Bronze por `s3a://` (sin comodines, porque dlt deja carpetas `_dlt_*` junto al Parquet):

```
s3a://bronze/users/2020/users_2020.parquet     s3a://bronze/users/2021/users_2021.parquet
s3a://bronze/post/2020/post_2020.parquet       s3a://bronze/post/2021/post_2021.parquet
```
Si un año no existe, avisa y lo omite; si no hay ninguno, falla con un mensaje claro. Spark solo lee las columnas que se usan (el `Body` pesado de post nunca se lee). Opción `--limit N` para limitar filas por archivo.

### 2.4 Transformaciones realizadas
| Qué | Detalle |
|---|---|
| **Limpieza** | `trim` y espacios múltiples colapsados; texto vacío → `NULL`; filas sin `Id` o sin `CreationDate` se descartan; contadores nulos/negativos → 0 |
| **Normalización** | Nombres a `snake_case` (`OwnerUserId` → `owner_user_id`); tipos fijos (`long`, `int`, `timestamp` UTC); `PostTypeId` → `post_type_id` + `post_type` (Question/Answer…); `Tags` (`<a><b>`) → `tag_list array<string>` + `tags` (`a\|b`); `website_url` en minúsculas |
| **Valores "vacíos" de ClickHouse** | Ids opcionales con `0` (`ParentId`, `AcceptedAnswerId`, `OwnerUserId`…) y fechas `1970-01-01` → `NULL` |
| **Derivadas** | `creation_year`, `source_year`, `is_question`, `has_accepted_answer` |
| **Deduplicación** | Un solo registro por `id` entre archivos/años: gana el de `last_access_date` (users) o `last_activity_date` (post) más reciente, y luego el año de fuente más reciente |
| **Control de cambios** | `row_hash` (SHA-256 de todas las columnas de datos) para saber si una fila cambió |
| **Carga** | `fecha_cargue` (timestamp UTC de la corrida) |
| **Descartado** | `Body` y `AboutMe` (HTML pesado, no se usan en Gold) |

### 2.5 Clave utilizada para MERGE
**`id`** (el `Id` de StackOverflow) en `users_hist` y en `post_hist`. Es única tras la deduplicación; el script verifica `COUNT(*) = COUNT(DISTINCT id)` al terminar.

### 2.6 Cómo funciona el MERGE
```sql
MERGE INTO nessie.silver.users_hist AS t
USING _silver_src AS s
ON t.id = s.id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *   -- cambió: se actualiza (y su fecha_cargue)
WHEN NOT MATCHED THEN INSERT *                                  -- nuevo: se inserta
```
- **No es overwrite:** las filas existentes no se borran; los ids nuevos se agregan y los cambiados se actualizan.
- **Idempotente:** re-ejecutar no duplica ni modifica filas sin cambios (conservan su `fecha_cargue`; Iceberg puede reescribir el archivo de datos, pero los valores quedan iguales).
- **Histórico:** cada tabla Silver es una tabla histórica que consolida 2020 + 2021 (`creation_year`/`source_year`), y cada MERGE queda registrado como un *commit* en Nessie. Nota: en este despliegue (Nessie como catálogo REST) `$snapshots` solo muestra el snapshot vigente, por eso no se usa `FOR VERSION AS OF`; el historial de cambios se verifica en los commits de Nessie.

### 2.7 Tablas Iceberg generadas
Formato Iceberg v2, particionadas por `creation_year`, en `s3://bucket-iceberg/` (catálogo `nessie`, namespace `silver`).

| Tabla | Contenido | Columnas principales |
|---|---|---|
| `silver.users_hist` | Usuarios 2020 + 2021 | `id, reputation, creation_date, display_name, last_access_date, location, views, up_votes, down_votes, website_url, account_id, creation_year, source_year, row_hash, fecha_cargue` |
| `silver.post_hist` | Posts 2020 + 2021 | `id, post_type_id, post_type, parent_id, accepted_answer_id, creation_date, last_activity_date, …, owner_user_id, title, tag_list, tags, score, view_count, answer_count, comment_count, favorite_count, is_question, has_accepted_answer, creation_year, source_year, row_hash, fecha_cargue` |

**Ejecutar** (en el cluster; esto es lo que debe llamar Airflow para la tarea Silver):
```bash
# users_hist (la tabla del pipeline)
docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/silver_transform.py --table users_hist

# post_hist (consolidación manual de los dos Parquet; --limit opcional)
docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/silver_transform.py --table post_hist --limit 500000
```
Desde Python/Airflow: `from silver_transform import run_silver; run_silver("users_hist")`. **Notebook de respaldo:** `notebooks/silver_transform.ipynb` (Run All Cells).

### 2.8 Cómo verificarla en Nessie
- **Nessie UI:** `http://localhost:19130` → rama `main` → namespace `silver` → `users_hist`, `post_hist` (cada MERGE aparece como *commit*).
- **API REST:** `curl http://localhost:19130/iceberg/v1/namespaces/silver/tables` (si el prefijo del catálogo difiere: `curl http://localhost:19130/iceberg/v1/config`).
- **MinIO:** `http://localhost:9021` → bucket `bucket-iceberg` → carpetas `silver/…/data` y `metadata`.
- **Spark:** `SHOW TABLES IN nessie.silver` y `SELECT * FROM nessie.silver.users_hist.snapshots`.

### 2.9 Cómo conectarse desde Trino
Catálogo `iceberg` definido en `trino/catalog/iceberg.properties` (tipo REST → Nessie, datos en MinIO; las credenciales salen de las variables del `.env`). UI: `http://localhost:8081`.
```bash
docker compose up -d trino
docker exec -it trino_proyecto trino --catalog iceberg --schema silver
trino:silver> SHOW TABLES;
```

### 2.10 Consultas SQL sobre Silver
En `sql/replicate_gold_from_silver.sql`: descubrimiento de tablas, volumen por año, snapshots, **replica de Gold desde Silver** (métricas por usuario cruzando `users_hist` y `post_hist`, `post_counts_by_user`, `top_tags`, actividad por mes) y chequeo de duplicados. Las columnas de la tabla Gold las define Persona 3: contrastar la consulta 3.1 con su tabla y ajustar si hace falta.
```bash
docker exec -i trino_proyecto trino < sql/replicate_gold_from_silver.sql
```

### 2.11 Evidencias de Silver
Guardar en `evidencias/` (capturas) para la entrega:

- [ ] `docker compose ps` con `spark_master`, `spark_worker`, `trino_proyecto` arriba.
- [ ] Spark Master UI (`localhost:8082`) con el worker `ALIVE` y la app `silver_*` completada.
- [ ] Salida de `spark-submit … --table users_hist` y `--table post_hist` (línea `[OK]` con filas antes/después).
- [ ] Segunda ejecución de `users_hist`: `filas_nuevas = 0`, `duplicados_id = 0` (prueba de MERGE idempotente).
- [ ] Nessie UI con `silver.users_hist` y `silver.post_hist`, y la lista de commits de la rama `main` (`curl.exe -s http://localhost:19130/api/v2/trees/main/history`).
- [ ] MinIO con `bucket-iceberg` (archivos `data/` y `metadata/`).
- [ ] Trino: `SHOW TABLES`, `DESCRIBE` y consultas de `replicate_gold_from_silver.sql`.
- [ ] `evidencias/silver_transform.log` (resumen JSON generado automáticamente).

### 2.12 Notas para el equipo
- **Airflow (tarea Silver):** el contrato es el comando de 2.7 / `run_silver("users_hist")`. El contenedor de Airflow no trae Spark ni Java: usar `SparkSubmitOperator` (master `spark://spark-master:7077`) con Spark+Java en su imagen, o coordinar otra vía. Las variables `MINIO_*`, `NESSIE_URI` ya están en `x-lake-env`.
- **Gold (Persona 3):** leer `nessie.silver.post_hist` y `nessie.silver.users_hist` con el mismo bloque de configuración Spark (`get_spark()` en `silver_transform.py`) y también escribir con MERGE.
- **Cambios al trabajo de Persona 1:** `docker-compose.yml` (servicios Spark y variables de Jupyter), `jupyter/Dockerfile` y `jupyter/requirements.txt` (Java 17 + `pyspark==3.5.3` + jars para que el notebook sea driver de Spark).
