-- ============================================================================
-- Proyecto 2 FYHBD | Persona 2 | Trino -> Silver (Iceberg)
-- Replica las tablas GOLD usando SOLO consultas sobre las tablas Silver.
--
-- Ejecutar:  docker exec -it trino_proyecto trino --catalog iceberg --schema silver
--            (o pegar cada consulta en el cliente que prefieran; UI: http://localhost:8081)
-- AJUSTE: los nombres/columnas de Gold son de Persona 3; compare el resultado de
--         la consulta 3.1 con su tabla gold y ajuste alias/filtros si difieren.
-- ============================================================================

-- ---------------------------------------------------------------- 0. Descubrir Silver
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
DESCRIBE iceberg.silver.users_hist;
DESCRIBE iceberg.silver.post_hist;

-- ---------------------------------------------------------------- 1. Volumen y fecha de cargue
SELECT 'users_hist' AS tabla, COUNT(*) AS filas, COUNT(DISTINCT id) AS ids_unicos,
       MIN(creation_date) AS desde, MAX(creation_date) AS hasta, MAX(fecha_cargue) AS ultimo_cargue
FROM iceberg.silver.users_hist
UNION ALL
SELECT 'post_hist', COUNT(*), COUNT(DISTINCT id), MIN(creation_date), MAX(creation_date), MAX(fecha_cargue)
FROM iceberg.silver.post_hist;

-- mas de un anio en cada tabla
SELECT creation_year, COUNT(*) AS filas FROM iceberg.silver.users_hist GROUP BY 1 ORDER BY 1;
SELECT creation_year, post_type, COUNT(*) AS filas FROM iceberg.silver.post_hist GROUP BY 1, 2 ORDER BY 1, 2;

-- ---------------------------------------------------------------- 2. Metadatos Iceberg (snapshot vigente; el historial de commits de cada MERGE se ve en Nessie)
SELECT snapshot_id, committed_at, operation FROM iceberg.silver."users_hist$snapshots" ORDER BY committed_at;
SELECT * FROM iceberg.silver."users_hist$history" ORDER BY made_current_at;

-- ---------------------------------------------------------------- 3. Replica de GOLD desde Silver
-- 3.1 Metricas por usuario: cruza users_hist con post_hist (tabla de metricas del pipeline)
SELECT
    u.id                                              AS user_id,
    u.display_name,
    u.location,
    u.reputation,
    COUNT_IF(p.post_type_id = 1)                      AS num_questions,
    COUNT_IF(p.post_type_id = 2)                      AS num_answers,
    COUNT(p.id)                                       AS num_posts,
    COALESCE(SUM(p.score), 0)                         AS total_score,
    COUNT_IF(p.has_accepted_answer)                   AS questions_with_accepted_answer,
    COALESCE(SUM(p.view_count), 0)                    AS total_views,
    MIN(p.creation_date)                              AS first_post_date,
    MAX(p.creation_date)                              AS last_post_date
FROM iceberg.silver.users_hist u
LEFT JOIN iceberg.silver.post_hist p ON p.owner_user_id = u.id
GROUP BY u.id, u.display_name, u.location, u.reputation
ORDER BY num_posts DESC, total_score DESC
LIMIT 50;

-- 3.2 post_counts_by_user (preguntas y respuestas por usuario)
SELECT owner_user_id AS user_id,
       COUNT_IF(post_type_id = 1) AS num_questions,
       COUNT_IF(post_type_id = 2) AS num_answers
FROM iceberg.silver.post_hist
WHERE owner_user_id IS NOT NULL
GROUP BY owner_user_id
ORDER BY num_questions + num_answers DESC
LIMIT 50;

-- 3.3 top_tags: ranking de etiquetas por numero de preguntas
SELECT tag, COUNT(*) AS num_questions, ROUND(AVG(score), 2) AS avg_score
FROM iceberg.silver.post_hist
CROSS JOIN UNNEST(tag_list) AS t(tag)
WHERE post_type_id = 1
GROUP BY tag
ORDER BY num_questions DESC
LIMIT 20;

-- 3.4 actividad por mes
SELECT date_trunc('month', creation_date) AS mes,
       COUNT_IF(post_type_id = 1) AS preguntas,
       COUNT_IF(post_type_id = 2) AS respuestas
FROM iceberg.silver.post_hist
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------- 4. Calidad: no debe haber duplicados por id
SELECT 'users_hist' AS tabla, COUNT(*) - COUNT(DISTINCT id) AS ids_duplicados FROM iceberg.silver.users_hist
UNION ALL
SELECT 'post_hist', COUNT(*) - COUNT(DISTINCT id) FROM iceberg.silver.post_hist;

-- ---------------------------------------------------------------- 5. Réplicas EXACTAS de las tablas Gold (definidas por Persona 3)
-- Cada tabla Gold se reproduce SOLO con SQL sobre Silver (entregable de la rúbrica).
-- row_hash y fecha_cargue son columnas técnicas del MERGE: no forman parte de la réplica.

-- 5.1 gold.user_engagement (tabla del pipeline: cruza las dos históricas; réplica completa)
SELECT
    u.id                                              AS user_id,
    u.display_name,
    u.location,
    u.reputation,
    COUNT_IF(p.post_type_id = 1)                      AS num_questions,
    COUNT_IF(p.post_type_id = 2)                      AS num_answers,
    COUNT(p.id)                                       AS num_posts,
    COALESCE(SUM(p.score), 0)                         AS total_score,
    COUNT_IF(p.has_accepted_answer)                   AS questions_with_accepted_answer,
    COALESCE(SUM(p.view_count), 0)                    AS total_views,
    MIN(p.creation_date)                              AS first_post_date,
    MAX(p.creation_date)                              AS last_post_date
FROM iceberg.silver.users_hist u
LEFT JOIN iceberg.silver.post_hist p ON p.owner_user_id = u.id
GROUP BY u.id, u.display_name, u.location, u.reputation;

-- 5.2 gold.post_counts_by_user
SELECT owner_user_id                                   AS user_id,
       COUNT_IF(post_type_id = 1)                      AS num_questions,
       COUNT_IF(post_type_id = 2)                      AS num_answers
FROM iceberg.silver.post_hist
WHERE owner_user_id IS NOT NULL
GROUP BY owner_user_id;

-- 5.3 gold.top_tags (ranking de etiquetas por número de preguntas)
SELECT tag,
       num_questions,
       avg_score,
       RANK() OVER (ORDER BY num_questions DESC, tag) AS tag_rank
FROM (
    SELECT tag, COUNT(*) AS num_questions, ROUND(AVG(score), 2) AS avg_score
    FROM iceberg.silver.post_hist
    CROSS JOIN UNNEST(tag_list) AS t(tag)
    WHERE post_type_id = 1
    GROUP BY tag
);

-- 5.4 gold.post_activity_monthly
SELECT date_trunc('month', creation_date)              AS mes,
       COUNT_IF(post_type_id = 1)                      AS preguntas,
       COUNT_IF(post_type_id = 2)                      AS respuestas
FROM iceberg.silver.post_hist
GROUP BY 1
ORDER BY 1;

-- 5.5 Verificación: la réplica 5.1 debe tener las mismas filas que la tabla Gold
SELECT COUNT(*) AS filas_replica FROM (
    SELECT u.id FROM iceberg.silver.users_hist u
    LEFT JOIN iceberg.silver.post_hist p ON p.owner_user_id = u.id
    GROUP BY u.id, u.display_name, u.location, u.reputation
);
SELECT COUNT(*) AS filas_gold FROM iceberg.gold.user_engagement;
