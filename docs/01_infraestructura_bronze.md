## 1. Infraestructura y capa Bronze

**Responsable:** Persona 1 · **Entregables:** `docker-compose.yml` (base), `scripts/bronze_ingest.py`, `notebooks/bronze_ingest.ipynb`, este documento.

### 1.1 Arquitectura Docker base

Todos los servicios viven en una única red Docker, `red_proyecto_fyhbd` (*attachable*), para que el resto del equipo (Spark, Airflow, Trino, Dremio) se conecte **por nombre de servicio** (`minio`, `nessie`, ...).

```
                      red_proyecto_fyhbd
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │   Jupyter ──────────► MinIO  ◄────────── Nessie          │
  │  (notebooks +        (data lake S3)     (catalogo        │
  │   scripts)            ├─ bronze           Iceberg)       │
  │                       └─ bucket-iceberg ◄──┘             │
  │                            ▲                             │
  │              minio-init ───┘ (crea los buckets, 1 vez)   │
  └──────────────────────────────────────────────────────────┘
        Spark / Airflow / Trino / Dremio  → los agregan otras personas a la misma red
```

| Servicio | Contenedor | Puerto host | Acceso |
|---|---|---|---|
| MinIO (API S3) | `minio_proyecto` | 9020 | `http://localhost:9020` |
| MinIO (consola) | `minio_proyecto` | 9021 | `http://localhost:9021` (usuario/clave del `.env`) |
| Nessie | `nessie_proyecto` | 19130 | `http://localhost:19130` |
| JupyterLab | `jupyter_proyecto` | 8890 | `http://localhost:8890` (sin token) |

### 1.2 MinIO
Object storage compatible con S3: es el **data lake**. Guarda los Parquet de Bronze y, más adelante, los archivos de las tablas Iceberg (Silver/Gold). Dentro de la red se accede en `http://minio:9000` (path-style).

### 1.3 Nessie
Catálogo de metadatos de Iceberg (con *versionado tipo Git*). Silver y Gold se registran aquí. Expone el catálogo REST de Iceberg en `http://nessie:19120/iceberg` y usa como *warehouse* `s3://bucket-iceberg/`. **Bronze no pasa por Nessie** (son Parquet crudos, no tablas Iceberg).

### 1.4 Jupyter
JupyterLab con `dlt`, `pyarrow`, `s3fs` y `pyiceberg` instalados (`jupyter/Dockerfile`). Monta `./notebooks`, `./scripts`, `./docs` y `./evidencias`. Permite ejecutar cada capa a mano si falla Airflow.

### 1.5 Buckets creados
El servicio `minio-init` los crea de forma idempotente al levantar el stack:

| Bucket | Uso |
|---|---|
| `bronze` | Parquet crudos (esta capa) |
| `bucket-iceberg` | Warehouse de Nessie: tablas Iceberg de Silver y Gold |

### 1.6 Dataset utilizado
**StackOverflow (ClickHouse)** — Parquet públicos en `datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/`. Para Bronze se usan dos tablas, dos años cada una:

| Tabla | Fuente | Años |
|---|---|---|
| `post` (Posts) | `posts/<año>.parquet` (un archivo por año) | 2020, 2021 |
| `users` (Users) | `users.parquet` (**un solo archivo**; se filtra por año de `CreationDate`) | 2020, 2021 |

Esto cumple el requisito de **más de un año** de información en Bronze. Se conservan los **nombres de columna originales** (`PostTypeId`, `OwnerUserId`, ...), sin pasarlos a snake_case.

### 1.7 Cómo funciona la ingesta con dlt

```
Fuente (HTTP/S3)  →  dlt (recurso + PyArrow por lotes)  →  Parquet  →  MinIO / bronze
```

1. Un **recurso dlt** abre el Parquet remoto con `fsspec` y lo lee por **lotes** (`iter_batches`, 100 000 filas) para no cargar millones de filas con `Body` pesado en la RAM del contenedor.
2. Para `users` (archivo único), cada lote se **filtra por año** de `CreationDate`.
3. dlt escribe el Parquet en MinIO con `write_disposition="replace"`: **Bronze permite sobrescritura**; re-ejecutar reemplaza el archivo y no duplica filas.
4. Al terminar, el script **relee los metadatos desde MinIO** y verifica que las filas del archivo coincidan con las cargadas; deja un resumen JSON en `evidencias/bronze_ingest.log`.

### 1.8 Dónde queda el Parquet (contrato para Silver/Gold)

```
s3://bronze/post/2020/post_2020.parquet       (manual)
s3://bronze/post/2021/post_2021.parquet       (manual)
s3://bronze/users/2020/users_2020.parquet     (manual)
s3://bronze/users/2021/users_2021.parquet     (pipeline Airflow)
```

> Junto a cada Parquet dlt deja carpetas de metadatos (`_dlt_loads/`, `_dlt_pipeline_state/`, `_dlt_version/`) y un archivo `init`. **Silver debe leer el archivo exacto** (o filtrar `*.parquet`), no la carpeta completa con un comodín que incluya `.jsonl`.

### 1.9 Cómo ejecutar `bronze_ingest.py`

```bash
# 0) Levantar la base de infraestructura (copiar antes .env.example -> .env)
docker compose up -d --build minio minio-init nessie jupyter
docker compose ps                      # minio, nessie, jupyter "running"; minio-init "exited (0)"

# El compose define también Spark, Airflow, Trino y Dremio, pero su arranque queda
# pendiente de las carpetas que aportan otras personas (airflow/, dags/, trino/catalog/, ...).
# La base de esta capa no depende de ellas y se levanta con el comando de arriba.

# 1) Carga manual previa a la clase: post 2020, post 2021 y users 2020
docker exec -w /workspace jupyter_proyecto python scripts/bronze_ingest.py --manual

# 2) Lo que ejecuta el pipeline de Airflow: users 2021
docker exec -w /workspace jupyter_proyecto python scripts/bronze_ingest.py --dataset users --year 2021

# 3) Ver qué hay en Bronze
docker exec -w /workspace jupyter_proyecto python scripts/bronze_ingest.py --list
```

Desde Airflow (tarea Bronze) basta con:

```python
from bronze_ingest import run_bronze
run_bronze("users", 2021)
```
(el script debe estar montado en el contenedor de Airflow, p. ej. `./scripts:/opt/airflow/scripts`, y las variables `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` definidas).

### 1.10 Cómo ejecutar `bronze_ingest.ipynb`
1. Abrir `http://localhost:8890` → `notebooks/bronze_ingest.ipynb`.
2. En la celda de **Parámetros** dejar `DATASET="users"`, `YEAR=2021` (o cambiar). Poner `CARGAR_MANUAL=True` para además cargar post 2020/2021 y users 2020.
3. **Run → Run All Cells.** Es autónomo y usa la misma lógica del script.

### 1.11 Evidencias de Bronze
Guardar en `evidencias/` (capturas) para la entrega:

- [ ] `docker compose ps` con los servicios arriba.
- [ ] Consola MinIO (`localhost:9021`) mostrando los buckets `bronze` y `bucket-iceberg`.
- [ ] Consola MinIO con las 4 rutas Parquet de la sección 1.8.
- [ ] Salida de `bronze_ingest.py --list` y del notebook (filas/columnas por archivo).
- [ ] `evidencias/bronze_ingest.log` (resumen JSON generado automáticamente por cada ejecución).
- [ ] Prueba de sobrescritura: ejecutar dos veces `users 2021` y mostrar que las filas no cambian.
