# Proyecto 2 FYHBD — Lakehouse StackOverflow

Arquitectura Lakehouse basada en **MinIO + Apache Iceberg + Nessie + Spark + Airflow + Trino + Dremio**, implementando el patrón **Medallion Bronze → Silver → Gold** sobre datos públicos de StackOverflow.

El proyecto permite ejecutar el flujo principal desde Airflow con un solo Play, mantener ejecución manual de respaldo desde Jupyter y consultar los resultados desde Trino y Dremio.

---

## 1. Arquitectura

![Arquitectura del proyecto](docs/arquitectura_lakehouse.png)

Flujo principal:

```text
StackOverflow
    │
    ▼
DLT
    │
    ▼
MinIO - Bronze (Parquet)
    │
    ▼
PySpark + Iceberg
    │
    ▼
Nessie - Silver
    │
    ▼
PySpark + Iceberg
    │
    ▼
Nessie - Gold
   / \
  ▼   ▼
Trino Dremio
```

Airflow orquesta el flujo:

```text
bronze_ingest → silver_transform → gold_agg
```

---

## 2. Servicios

| Servicio | Función | Acceso local |
|---|---|---|
| MinIO | Object Storage Bronze + warehouse Iceberg | API `http://localhost:9020` / Consola `http://localhost:9021` |
| Nessie | Catálogo de tablas Iceberg | `http://localhost:19130` |
| Spark Master | Procesamiento PySpark | `http://localhost:8082` |
| Spark Worker | Ejecución distribuida | `http://localhost:8083` |
| JupyterLab | Ejecución manual de respaldo | `http://localhost:8890` |
| Airflow | Orquestación | `http://localhost:8080` |
| Trino | Consulta SQL sobre Silver | `http://localhost:8081` |
| Dremio | Consulta y visualización de Gold | `http://localhost:9047` |
| PostgreSQL | Base de metadatos de Airflow | interno |

Credenciales por defecto:

- Airflow: `airflow / airflow` si no se sobrescriben en `.env`.
- Dremio: `admin / proyecto2fyhbd` si no se sobrescriben en `.env`.
- MinIO: usar `MINIO_ROOT_USER` y `MINIO_ROOT_PASSWORD` definidos en `.env`.

---

## 3. Estructura principal

```text
proyecto2/
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   └── lakehouse_pipeline_dag.py
├── docs/
│   ├── arquitectura_lakehouse.png
│   ├── 01_infraestructura_bronze.md
│   ├── 02_silver_spark_trino.md
│   └── 03_gold_dremio.md
├── evidencias/
├── notebooks/
│   ├── bronze_ingest.ipynb
│   ├── silver_transform.ipynb
│   └── gold_agg.ipynb
├── scripts/
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_agg.py
│   └── dremio_init.py
├── sql/
│   └── replicate_gold_from_silver.sql
├── spark/
├── trino/
│   └── catalog/
├── .env.example
├── docker-compose.yml
└── README.md
```

---

## 4. Requisitos

- Docker Desktop con Docker Compose.
- Aproximadamente 8 GB de RAM disponibles como mínimo para el stack.
- Espacio suficiente para los Parquet de Bronze y las tablas Iceberg.
- Puerto 8080, 8081, 8082, 8083, 8890, 9020, 9021, 9047 y 19130 disponibles.

---

## 5. Configuración inicial

Desde la raíz del proyecto, crear `.env` a partir del ejemplo.

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

Revisar las variables del archivo `.env`, especialmente las credenciales de MinIO y PostgreSQL.

---

## 6. Levantar el stack completo

```powershell
docker compose up -d --build
```

Verificar estado:

```powershell
docker compose ps -a
```

Los servicios persistentes deben aparecer como `Up` o `healthy`.

Es normal que los siguientes servicios terminen como `Exited (0)` porque son tareas de inicialización de una sola ejecución:

```text
minio_init
airflow_init
dremio_init
```

`dremio-init` espera automáticamente a que Dremio esté disponible, crea o reutiliza el source `nessie_source` y valida la tabla Gold principal.

---

## 7. Capa Bronze

Bronze almacena datos crudos en formato **Parquet** dentro del bucket `bronze` de MinIO.

Datasets utilizados:

| Dataset | Año | Ejecución |
|---|---:|---|
| post | 2020 | manual |
| post | 2021 | manual |
| users | 2020 | manual |
| users | 2021 | Airflow |

Rutas:

```text
s3://bronze/post/2020/post_2020.parquet
s3://bronze/post/2021/post_2021.parquet
s3://bronze/users/2020/users_2020.parquet
s3://bronze/users/2021/users_2021.parquet
```

La ingesta utiliza DLT y `write_disposition="replace"`, por lo que Bronze puede sobrescribirse sin acumular duplicados por reejecución.

### Carga manual previa requerida

Antes de ejecutar el DAG por primera vez, cargar `post 2020`, `post 2021` y `users 2020`:

```powershell
docker exec -w /workspace jupyter_proyecto python scripts/bronze_ingest.py --manual
```

Verificar objetos Bronze:

```powershell
docker exec -w /workspace jupyter_proyecto python scripts/bronze_ingest.py --list
```

El dataset `users 2021` se carga durante el DAG de Airflow.

---

## 8. Capa Silver

Silver utiliza **Apache Iceberg** y Nessie como catálogo.

Tablas:

```text
nessie.silver.users_hist
nessie.silver.post_hist
```

La transformación se realiza con PySpark, limpieza, normalización, deduplicación y escritura mediante `MERGE`.

Ambas tablas contienen datos históricos de 2020 y 2021 y columna `fecha_cargue`.

### Construcción manual de `post_hist`

Por alcance del proyecto, `post_hist` se prepara manualmente antes de ejecutar el DAG.

En el entorno usado para la entrega se limitó el volumen a 500.000 registros por año para ajustarse a los recursos locales:

```powershell
docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/silver_transform.py --table post_hist --years 2020,2021 --limit 500000
```

Resultado validado:

```text
post_hist = 1,000,000 registros
```

`users_hist` se genera dentro del pipeline de Airflow a partir de `users 2020 + users 2021`.

Resultado validado:

```text
users_hist = 4,977,567 registros
```

---

## 9. Capa Gold

Gold utiliza Iceberg y `MERGE` para generar tablas analíticas, métricas y agregaciones.

El script soporta:

```text
user_engagement
post_counts_by_user
top_tags
post_activity_monthly
```

La tabla utilizada en el DAG es:

```text
nessie.gold.user_engagement
```

`user_engagement` combina `users_hist` con `post_hist` y conserva una fila por usuario.

Resultado validado:

```text
user_engagement = 4,977,567 registros
```

---

## 10. Orquestación con Airflow

DAG:

```text
lakehouse_stackoverflow
```

Tareas:

```text
bronze_ingest
    ↓
silver_transform
    ↓
gold_agg
```

La ejecución realiza:

1. **Bronze:** ingesta de `users 2021` con DLT.
2. **Silver:** consolidación de `users_hist` con PySpark + Iceberg + MERGE.
3. **Gold:** construcción de `user_engagement` con PySpark + Iceberg + MERGE.

### Ejecutar desde la interfaz

Abrir:

```text
http://localhost:8080
```

Iniciar sesión, habilitar `lakehouse_stackoverflow` y pulsar **Trigger DAG / Play**.

El DAG fue validado de extremo a extremo con las tres tareas finalizando en estado `Success`.

### Prueba de una tarea individual

Ejemplo:

```powershell
docker exec airflow_scheduler airflow tasks test lakehouse_stackoverflow bronze_ingest 2026-10-13
```

---

## 11. Ejecución manual desde Jupyter

La rúbrica contempla una ruta manual en caso de fallo de Airflow.

Abrir:

```text
http://localhost:8890
```

Notebooks disponibles:

```text
notebooks/bronze_ingest.ipynb
notebooks/silver_transform.ipynb
notebooks/gold_agg.ipynb
```

Cada notebook utiliza la misma lógica de los scripts principales y sirve como respaldo de su respectiva capa.

---

## 12. Consulta con Trino

Trino está configurado para consultar las tablas Iceberg administradas por Nessie.

### Ver tablas Silver

```powershell
docker exec trino_proyecto trino --catalog iceberg --schema silver --execute "SHOW TABLES"
```

Resultado esperado:

```text
post_hist
users_hist
```

### Conteos validados

```powershell
docker exec trino_proyecto trino --catalog iceberg --schema silver --execute "SELECT COUNT(*) AS filas FROM users_hist"
```

Resultado:

```text
4977567
```

```powershell
docker exec trino_proyecto trino --catalog iceberg --schema silver --execute "SELECT COUNT(*) AS filas FROM post_hist"
```

Resultado:

```text
1000000
```

### Replicar lógica Gold desde Silver

El archivo:

```text
sql/replicate_gold_from_silver.sql
```

contiene las consultas SQL que reproducen métricas Gold usando únicamente tablas Silver.

En PowerShell:

```powershell
Get-Content .\sql\replicate_gold_from_silver.sql -Raw | docker exec -i trino_proyecto trino --catalog iceberg --schema silver
```

> Algunas consultas devuelven millones de filas. Para evidencias se recomienda usar `COUNT(*)` o `LIMIT` para evitar imprimir resultados completos en la terminal.

### Validar Gold desde Trino

```powershell
docker exec trino_proyecto trino --catalog iceberg --schema gold --execute "SELECT COUNT(*) AS filas FROM user_engagement"
```

Resultado validado:

```text
4977567
```

El aviso `Unable to create a system terminal, creating a dumb terminal` de la CLI no afecta la ejecución de las consultas.

---

## 13. Consulta con Dremio

Abrir:

```text
http://localhost:9047
```

El servicio `dremio-init` configura automáticamente el source:

```text
nessie_source
```

Consulta principal:

```sql
SELECT COUNT(*) AS total_registros
FROM nessie_source.gold.user_engagement;
```

Resultado validado:

```text
4,977,567
```

También puede consultarse una muestra:

```sql
SELECT *
FROM nessie_source.gold.user_engagement
LIMIT 100;
```

---

## 14. Reproducibilidad e idempotencia

El stack fue probado deteniendo y recreando los contenedores sin eliminar los volúmenes:

```powershell
docker compose down
docker compose up -d --no-build
```

Luego:

```powershell
docker compose ps -a
```

se verificó que los servicios persistentes regresaran a estado `Up/healthy` y que `minio_init`, `airflow_init` y `dremio_init` terminaran correctamente con código `0`.

El proyecto es idempotente en sus capas principales:

- Bronze usa reemplazo controlado.
- Silver utiliza `MERGE` sobre Iceberg.
- Gold utiliza `MERGE` sobre Iceberg.
- Los servicios de inicialización conservan recursos que ya existen.

### Detener sin eliminar datos

```powershell
docker compose down
```

### Volver a levantar

```powershell
docker compose up -d --no-build
```

### Reconstruir imágenes después de cambios en Dockerfiles

```powershell
docker compose up -d --build
```

> No usar `docker compose down -v` durante una prueba normal, porque elimina los volúmenes persistentes.

---

## 15. Evidencias recomendadas

Guardar en `evidencias/` capturas o salidas de:

- `docker compose ps -a` con servicios correctamente levantados.
- MinIO mostrando los Parquet de Bronze.
- Spark ejecutando Silver/Gold.
- Airflow Graph con las tres tareas en verde.
- Airflow Event Log del DAG completo.
- Trino mostrando `users_hist` y `post_hist` y sus conteos.
- Dremio consultando `gold.user_engagement`.
- Logs de `dremio-init` terminando con `Exited (0)`.

---

## 16. Validaciones realizadas

| Validación | Resultado |
|---|---|
| Stack Docker completo | OK |
| MinIO Bronze | OK |
| Nessie + Iceberg | OK |
| Spark Master / Worker | OK |
| Airflow Webserver / Scheduler | OK |
| DAG Bronze → Silver → Gold | OK |
| `post_hist` | 1,000,000 filas |
| `users_hist` | 4,977,567 filas |
| `user_engagement` | 4,977,567 filas |
| Trino sobre Silver | OK |
| Trino sobre Gold | OK |
| Dremio sobre Gold | OK |
| Reinicio completo del stack | OK |
| `dremio-init` con espera y reutilización del source | OK |

---

## 17. Entregables

El proyecto incluye:

```text
docker-compose.yml
README.md
dags/lakehouse_pipeline_dag.py
notebooks/bronze_ingest.ipynb
notebooks/silver_transform.ipynb
notebooks/gold_agg.ipynb
sql/replicate_gold_from_silver.sql
docs/arquitectura_lakehouse.png
```

Con estos archivos se cubren infraestructura, pipeline Medallion, orquestación, ejecución manual de respaldo, consultas SQL y diagrama de arquitectura.
