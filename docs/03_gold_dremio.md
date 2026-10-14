## 3. Capa Gold + Dremio

**Responsable:** Persona 3 · **Entregables:** `scripts/gold_agg.py`, `notebooks/gold_agg.ipynb`, este documento, las réplicas exactas en `sql/replicate_gold_from_silver.sql` (§5) y la **configuración de Dremio** para consultar Gold (§3.8–3.9).

```
Silver Iceberg (nessie.silver.users_hist + post_hist) → PySpark (reglas de negocio · agregaciones)
    → MERGE → Gold Iceberg (nessie.gold.*) → Nessie → Dremio (consulta de Gold) / Trino (Silver y réplicas)
```

### 3.1 Qué hace Gold
Gold es la capa analítica: **métricas, KPIs y tablas analíticas** listas para consultar. `gold_agg.py` recibe las tablas Silver (`post_hist`, `users_hist`), hace el **JOIN**, aplica reglas de negocio y agrega con PySpark (misma sesión/configuración Spark que Silver: `get_spark()` de `silver_transform.py`), y escribe 4 tablas **Iceberg** con `MERGE INTO` en el catálogo **Nessie** (namespace `gold`). La tabla del pipeline (la que corre el DAG en clase) es `user_engagement`, que cruza las dos históricas — exactamente lo que pide la aclaración de alcance del proyecto.

### 3.2 Tablas Silver utilizadas, JOIN y tablas Gold resultantes
| Tabla Silver usada | Aporta |
|---|---|
| `nessie.silver.users_hist` | `id`, `display_name`, `location`, `reputation` (dimensiones del usuario) |
| `nessie.silver.post_hist` | `post_type_id`, `score`, `view_count`, `has_accepted_answer`, `tag_list`, `creation_date`, `owner_user_id` (hechos) |

**JOIN realizado:** `users_hist LEFT JOIN post_hist ON post_hist.owner_user_id = users_hist.id` (un usuario sin posts también queda en Gold — regla de negocio).

| Tabla Gold resultante | Grano (clave MERGE) | Contenido |
|---|---|---|
| `gold.user_engagement` ★ | usuario (`user_id`) | Métricas por usuario cruzando las dos históricas: preguntas, respuestas, total posts, score acumulado, preguntas con respuesta aceptada, vistas acumuladas, primera/última publicación |
| `gold.post_counts_by_user` | usuario (`user_id`) | Preguntas y respuestas publicadas por usuario (solo quien publicó) |
| `gold.top_tags` | etiqueta (`tag`) | Ranking de tags por número de preguntas + score promedio (`tag_rank`) |
| `gold.post_activity_monthly` | mes (`mes`) | Preguntas y respuestas por mes (evolución temporal) |

Todas: Iceberg v2, columnas `row_hash` + `fecha_cargue` (timestamp UTC de la corrida). Como son agregados pequeños, no se particionan.

### 3.3 KPIs generados
Preguntas y respuestas por usuario · volumen total de publicaciones · score y vistas acumuladas por usuario · preguntas con respuesta aceptada (calidad) · fecha de primera/última publicación (actividad) · ranking de etiquetas con score promedio · evolución mensual preguntas vs respuestas. Cubren los ejemplos que propone el profesor ("posts por usuario, top de tags, engagement"); los de votos y badges quedan fuera porque el alcance de Bronze es `users` + `post` (dos tablas, como pide el video).

### 3.4 Reglas de negocio y agregaciones
| Regla | Detalle |
|---|---|
| Usuario sin posts también cuenta | `LEFT JOIN`: queda en `user_engagement` con métricas en 0 y fechas de publicación `NULL` |
| Pregunta / respuesta | por `post_type_id` (1 = Question, 2 = Answer), ya normalizado en Silver |
| Respuesta aceptada | `has_accepted_answer` (derivada en Silver de `AcceptedAnswerId`) |
| Etiquetas | `explode(tag_list)` sobre las preguntas (PostTypeId = 1) |
| Ranking estable | `tag_rank` por `num_questions` descendente, desempate alfabético por `tag` |
| Mes | `date_trunc('month', creation_date)` |
| Contadores nulos | `COALESCE(SUM(...), 0)` para usuarios sin posts |

### 3.5 Clave utilizada para el MERGE y cómo funciona
Cada tabla usa su **clave de negocio**: `user_id` (engagement y post_counts), `tag` (top_tags), `mes` (monthly). Tras el agrupado la clave es única; el script verifica `COUNT(*) = COUNT(DISTINCT clave)` al terminar.

```sql
MERGE INTO nessie.gold.user_engagement AS t
USING _gold_src AS s
ON t.user_id = s.user_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *   -- cambió la métrica: se actualiza (y su fecha_cargue)
WHEN NOT MATCHED THEN INSERT *                                -- clave nueva: se inserta
```
- **No es overwrite:** las métricas que no cambiaron conservan su `fecha_cargue`; las que cambiaron se actualizan.
- **Idempotente:** re-ejecutar con los mismos datos da `filas_nuevas = 0` y no modifica filas sin cambios (verificado en la prueba E2E).
- **Histórico:** cada MERGE queda como *commit* en Nessie (rama `main`).

### 3.6 Ejecutar Gold
```bash
# 4 tablas (lo normal; así queda listo para Dremio)
docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/gold_agg.py

# solo la tabla del pipeline (lo mínimo que ejecuta el DAG)
docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/gold_agg.py --tables user_engagement
```
Desde Python/Airflow: `from gold_agg import run_gold; run_gold()` (todas) o `run_gold(["user_engagement"])`.
**Notebook de respaldo:** `notebooks/gold_agg.ipynb` (JupyterLab → *Run All Cells*; modo local `local[2]`).

### 3.7 Cómo verificarla
- **Nessie UI:** `http://localhost:19130` → rama `main` → namespace `gold` → las 4 tablas (cada MERGE es un *commit*).
- **Spark/Trino:** `SHOW TABLES IN nessie.gold`; `SELECT * FROM iceberg.gold.user_engagement LIMIT 10` (Trino, mismo catálogo REST).
- **MinIO:** bucket `bucket-iceberg` → `gold/…/data` y `metadata`.
- **Réplica desde Silver (rúbrica):** `sql/replicate_gold_from_silver.sql` §5 — cada tabla Gold se reproduce **solo con SQL sobre Silver** (`docker exec -i trino_proyecto trino < sql/replicate_gold_from_silver.sql`).

### 3.8 Dremio: conexión y consulta de Gold (parte de Persona 3)
La rúbrica pide específicamente que **Gold sea consultable desde Dremio** (10%). **La configuración
es automática**: el servicio `dremio-init` del `docker-compose.yml` (mismo patrón que `minio-init`)
crea el usuario y el source Nessie cuando se hace `docker compose up` — no hay que correr nada aparte:

```bash
docker compose up -d            # (o el subset: … trino dremio dremio-init)
docker logs dremio_init         # → "source 'nessie_source' creado … validación OK: gold.user_engagement = 489 filas"
```
- Corre **una vez** y termina (`exited 0`), como `minio_init`. Es **idempotente**: re-ejecutar `up` no
  duplica nada ("ya existe y funciona … se conserva") — evidencia automática en `evidencias/dremio_init.log`.
- Usuario: `admin` / `proyecto2fyhbd` (variables `DREMIO_USER`/`DREMIO_PASS`); UI: `http://localhost:9047`.
- La lógica vive en `scripts/dremio_init.py` (lee la API de Dremio; no hace falta el sh).

**Configuración manual (si se prefiere a mano o para entender qué hace el init):**
primer uso → crear usuario → **Sources → Add Source → Nessie**: Nessie endpoint
`http://nessie:19120/api/v2`, auth None, AWS root path `/bucket-iceberg`, credenciales MinIO
(`admin`/`password`), y en *Advanced Options → Property* agregar:

| Propiedad | Valor | Por qué |
|---|---|---|
| `fs.s3a.endpoint` | `minio:9000` | **sin esquema** — el SDK v1 no parsea `http://` dentro del endpoint y el job queda colgado en `METADATA_RETRIEVAL` |
| `fs.protocol` | `http` | el esquema va aparte (MinIO sin TLS) |
| `fs.s3a.path.style.access` | `true` | rutas por bucket, no por subdominio |
| `dremio.s3.compat` | `true` | salta la verificación contra STS de AWS (MinIO no tiene STS); sin esto: "Credential Verification failed" |

Notas de la API: primer usuario con `PUT /apiv2/bootstrap/firstuser`; el secreto se llama
`awsAccessSecret`; resultados de SQL en `GET /api/v3/job/{id}/results`.

**Consultar Gold desde Dremio:** `SELECT * FROM nessie_source.gold.user_engagement LIMIT 10` (desde el editor SQL de la UI o **DBeaver** conectado por JDBC — el profesor sugiere DBeaver para Trino y Dremio en la presentación).

### 3.9 Consultas de validación (réplica Trino ↔ Gold Dremio)
Lo que el profesor revisa en la presentación: ejecutar la **réplica desde Trino sobre Silver** y compararla **registro a registro** con la **Gold leída desde Dremio**.

```sql
-- TRINO (sobre Silver) — la réplica:
SELECT u.id AS user_id, u.display_name, u.reputation,
       COUNT_IF(p.post_type_id = 1) AS num_questions, COUNT_IF(p.post_type_id = 2) AS num_answers,
       COUNT(p.id) AS num_posts, COALESCE(SUM(p.score),0) AS total_score,
       COUNT_IF(p.has_accepted_answer) AS questions_with_accepted_answer
FROM iceberg.silver.users_hist u LEFT JOIN iceberg.silver.post_hist p ON p.owner_user_id = u.id
GROUP BY u.id, u.display_name, u.reputation;

-- DREMIO (sobre Gold) — lo que debe coincidir:
SELECT user_id, display_name, reputation,
       num_questions, num_answers, num_posts, total_score, questions_with_accepted_answer
FROM nessie_source.gold.user_engagement;

-- Verificación de volumen (ambos deben dar igual):
--   Trino:  SELECT COUNT(*) FROM (réplica de arriba)
--   Dremio: SELECT COUNT(*) FROM nessie_source.gold.user_engagement
```
En la prueba E2E local quedó verificada **tres veces**: réplica Trino = `gold.user_engagement` en Spark = **Gold leída desde Dremio** → 489 filas idénticas (top usuarios, 56 sin posts, y volúmenes 489/433/10/24 iguales por las tres vías). Evidencia: `evidencias/gold_dremio.txt` y `evidencias/gold_trino.txt`. Las réplicas completas de las 4 tablas están en `sql/replicate_gold_from_silver.sql` §5.

### 3.10 Evidencias de Gold + Dremio
Guardar en `evidencias/` (capturas) para la entrega:

- [ ] `docker compose ps` con `spark_master`/`spark_worker`/`dremio_proyecto` arriba.
- [ ] Salida de `spark-submit … gold_agg.py` (líneas `[OK]` con filas antes/después por tabla).
- [ ] Segunda ejecución: `filas_nuevas = 0`, `claves_duplicadas = 0` (MERGE idempotente).
- [ ] Nessie UI: namespace `gold` con las 4 tablas y commits.
- [ ] Dremio: `docker logs dremio_init` (source creado automáticamente por el compose) + `SELECT` sobre `gold.user_engagement` (evidencias: `evidencias/dremio_init.log` y `evidencias/gold_dremio.txt`).
- [ ] Comparación en vivo: réplica Trino == Gold Dremio (registro a registro, §3.9).
- [ ] `evidencias/gold_agg.log` (resumen JSON generado automáticamente).

### 3.11 Notas para el equipo
- **Airflow (tarea Gold):** el contrato es el comando de §3.6 / `run_gold(["user_engagement"])` (o todas). Igual que Silver: el contenedor de Airflow no trae Spark — usar `spark-submit` contra `spark://spark-master:7077` o `DockerOperator` sobre `fyhbd/spark-proyecto2:1.0`.
- **Dependencia de datos:** Gold necesita `silver.post_hist` Y `silver.users_hist`. Para el run en clase, `post_hist` se consolida manualmente ANTES de disparar el DAG (ver doc de Persona 2 §2.7): el DAG hace users_2021 → users_hist → user_engagement.
- **Cambios al trabajo de Persona 2:** ninguno (solo se importa `get_spark`).
