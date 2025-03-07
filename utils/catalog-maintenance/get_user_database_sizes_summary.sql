CREATE TEMP TABLE z__tmp_user_db_sizes (
	database_id   BIGINT,
	database_name VARCHAR(128),
	database_size BIGINT
);
SELECT to_char(now(), 'YYYYmmdd_HH24MISS') AS ts
\gset
\set copy_to '\\copy (SELECT oid, datname, pg_database_size(oid) FROM pg_database WHERE oid > 16000) to user_catalog_' :ts '.mr.out.txt'
:copy_to
\set copy_from '\\copy z__tmp_user_db_sizes from user_catalog_' :ts '.mr.out.txt'
:copy_from
\set top 10
\qecho == "Base" (user) system catalog usage summary
WITH a AS (
	SELECT ROW_NUMBER() OVER (ORDER BY database_size DESC) AS rn, database_id, database_name, database_size
	FROM z__tmp_user_db_sizes
)
SELECT rn    , database_name                , round(database_size/1024^2,2) AS size_mb FROM a WHERE rn <= :top
UNION ALL
SELECT :top+1, '-- remaining '||max(rn)-:top, round(sum(database_size)/1024^2,2)       FROM a WHERE rn > :top
UNION ALL
SELECT :top+2, '-- overall'                 , round(sum(database_size)/1024^2,2)       FROM a
ORDER BY rn;
