\set top 10

\pset fieldsep '\t'
\pset tuples_only on
\pset format unaligned
\pset null '\\N'
SELECT to_char(now(), 'YYYYmmdd_HH24MISS') AS ts
\gset

-- Get full report on all system catalog tables in yellowbrick database in machine-readable format
\c yellowbrick
\o global_catalog_:ts.mr.out.txt
\i get_db_catalog_table_sizes.sql
\o

CREATE TEMP TABLE z__yellowbrick_table_sizes (
	global_scope BOOLEAN,
	table_id     BIGINT,
	table_db     VARCHAR(128),
	table_schema VARCHAR(128),
	table_name   VARCHAR(128),
	total_size   BIGINT,
	index_size   BIGINT,
	toast_size   BIGINT,
	total_size_pretty VARCHAR(32)
);

-- Load the report into the temp table for further processing
\set copy '\\copy z__yellowbrick_table_sizes from global_catalog_' :ts '.mr.out.txt'
:copy

\pset tuples_only off
\pset format aligned
\pset null ''

\qecho == "Global" (yellowbrick/shared) system catalog usage summary
WITH a AS (
	SELECT table_schema
		, table_name
		, round(total_size/1024^2,2) AS total_size_mb
		, round(index_size/1024^2,2) AS index_size_mb
		, round(toast_size/1024^2,2) AS toast_size_mb
		, row_number() OVER (ORDER BY total_size DESC) AS rn
	FROM z__yellowbrick_table_sizes
)
SELECT rn, table_schema, table_name, total_size_mb, index_size_mb, toast_size_mb FROM a WHERE rn <= :top
UNION ALL
SELECT :top+1, '-- remaining '||max(rn)-:top, '', sum(total_size_mb), sum(index_size_mb), sum(toast_size_mb) FROM a WHERE rn > :top
UNION ALL
SELECT :top+2, '-- overall', '', sum(total_size_mb), sum(index_size_mb), sum(toast_size_mb) FROM a
ORDER BY rn;

\o global_catalog_:ts.hr.out.txt
SELECT global_scope AS shared, table_id, table_db, table_schema, table_name
	, round(total_size/1024^2,2) AS total_size_mb, round(index_size/1024^2,2) AS index_size_mb
FROM z__yellowbrick_table_sizes
ORDER BY total_size DESC;
\o

\i get_user_database_sizes_summary.sql
\o user_catalog_:ts.hr.out.txt
\i get_user_database_sizes.sql
\o

-- NOTE: The limit is hardcoded as there's no way to get PGDATA XFS quota size for on-prem or /mnt/ybdata mount size for CN from inside PG
SELECT 200 AS pg_catalog_quota_gb
	, (SELECT round(sum(total_size)/1024^3,2) FROM z__yellowbrick_table_sizes) AS global_usage_gb
	, (SELECT round(sum(database_size)/1024^3,2) FROM z__tmp_user_db_sizes) AS base_usage_gb
	, global_usage_gb + base_usage_gb AS catalog_total_usage_gb
	, round(catalog_total_usage_gb/pg_catalog_quota_gb*100,2) AS percent_used
;
