DROP PROCEDURE IF EXISTS yb_chunk_optimal_rows_p(VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE yb_chunk_optimal_rows_p(
    a_table_name          VARCHAR
    , a_schema_name       VARCHAR
    , a_db_name           VARCHAR
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_blades INTEGER;
    v_chunks INTEGER;
    v_chunk_row_size BIGINT;
    v_sql_chunk_row_size TEXT := REPLACE(REPLACE(REPLACE($STR$
WITH
const AS (
    SELECT
        <blades> AS blades
        , 1024 ^ 3 AS shard_size
        , 10 AS shards_per_blade --this is the key number in deciding how to calculate the chunk size
        , '<schema_name>' AS schema_name
        , '<table_name>' AS table_name
)
, chunks AS (
    SELECT
        COUNT(DISTINCT table_id)
        , TRUNC( SUM( s.row_count ) )                                  AS rows
        , SUM( s.size_uncomp_bytes )                                   AS uncomp_bytes
        , TRUNC(uncomp_bytes / shard_size / blades / shards_per_blade) AS chunks
        , DECODE(chunks, 0, 1, chunks)                                 AS chunks_min_1
        , TRUNC(rows / chunks_min_1 / 10000000) * 10000000             AS chunk_row_size
        , DECODE(chunk_row_size, 0, 10000000, chunk_row_size)          AS chunk_row_size_min_10000000
    FROM
        sys.shardstore AS s
        JOIN <db_name>.pg_catalog.pg_class  AS c
            ON s.table_id = c.oid::BIGINT
        JOIN <db_name>.pg_catalog.pg_namespace AS n
            ON c.relnamespace = n.oid
        CROSS JOIN const
    WHERE
        TRIM( n.nspname ) = schema_name
        AND TRIM( c.relname ) = table_name
    GROUP BY shard_size, blades, shards_per_blade
)
SELECT chunks_min_1 AS chunks, chunk_row_size_min_10000000 AS chunk_row_size FROM chunks$STR$
        , '<table_name>', a_table_name)
        , '<schema_name>', a_schema_name)
        , '<db_name>', a_db_name);
    --
    _fn_name   VARCHAR(256) := 'yb_chunk_optimal_rows_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    EXECUTE 'SELECT COUNT(DISTINCT worker) AS blades FROM sys.shardstore' INTO v_blades;
    v_sql_chunk_row_size := REPLACE(v_sql_chunk_row_size, '<blades>', v_blades::VARCHAR);
    EXECUTE v_sql_chunk_row_size INTO v_chunks, v_chunk_row_size;
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN v_chunk_row_size;
END$$;