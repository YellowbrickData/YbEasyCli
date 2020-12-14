CREATE OR REPLACE PROCEDURE yb_analyze_columns_p(
    a_dbname VARCHAR
    , a_tablename VARCHAR
    , a_filter_clause VARCHAR
    , a_level INTEGER DEFAULT 1
    , a_delimited_output BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_rec_tables RECORD;
    v_rec_aggs RECORD;
    v_rec_grps RECORD;
    v_ct BIGINT;
    v_group_ct BIGINT := 10;
    v_rec_tables_row INTEGER := 0;
    v_str TEXT;
    v_pad_len1 INTEGER;
    v_pad_len2 INTEGER;
    --
    v_has_stats BOOLEAN := FALSE;
    v_has_counts BOOLEAN := FALSE;
    --
    v_query VARCHAR(4000) := REPLACE(REPLACE($STR$
WITH
o AS (
    SELECT
        a.attname AS columnname
        , a.attnum AS column_num
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
        , UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) AS data_type
        , NOT(nullable) AS is_not_nullable
        , NVL2(distribution_key, TRUE, FALSE) AS is_distribution_key
        , NVL2(sort_key, TRUE, FALSE) AS is_sort_key
        , NVL2(cluster_key, TRUE, FALSE) AS is_cluster_key
        , NVL2(partition_key, TRUE, FALSE) AS is_partition_key
        , CASE
            WHEN a.attlen = -1
                THEN
                    CASE
                        WHEN SUBSTR(data_type, 1, 7) = 'NUMERIC'
                            THEN (CASE WHEN a.atttypmod > 1245207 THEN 16 ELSE 8 END)
                            ELSE a.atttypmod
                    END
                ELSE a.attlen
        END AS max_bytes
        , s.est_null_pct::NUMERIC(10,3)
        , s.est_byte_width
        , s.est_count_distinct
        , s.est_rows
        , s.est_total_bytes
    FROM
        <dbname>.pg_catalog.pg_class AS c
        LEFT JOIN <dbname>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <dbname>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
        LEFT JOIN <dbname>.sys.column AS col
            ON c.oid = col.table_id
            AND a.attnum = col.column_id
        LEFT JOIN (<stats_query>) AS s
            ON a.attrelid   = s.starelid
            AND a.attnum = s.staattnum
    WHERE
        c.relkind IN ('r')
        AND column_num > 0
        AND tablename LIKE '<tablename>'
        AND <filter_clause>
)
SELECT
    data_type
    , columnname
    , tablename
    , schemaname
    , tableowner
    , column_num
    , DECODE(TRUE, data_type LIKE '%(%', RTRIM(SPLIT_PART(data_type,'(',2),')')) AS dimensions
    , SPLIT_PART(dimensions,',',1)::INT AS precision
    , SPLIT_PART(dimensions,',',2) AS scale_char
    , DECODE(scale_char, NULL, NULL::INT, '', NULL::INT, scale_char::INT) AS scale
    , is_not_nullable
    , is_distribution_key
    , is_sort_key
    , is_cluster_key
    , is_partition_key
    , max_bytes
    , est_null_pct
    , est_byte_width
    , est_count_distinct
    , est_rows
    , est_total_bytes
FROM
    o
ORDER BY
    schemaname, tablename, column_num$STR$
        , '<tablename>', a_tablename)
        , '<filter_clause>', a_filter_clause);
    --
    v_query_stats VARCHAR(4000) := $STR$
SELECT
    starelid
    , staattnum
    , MAX(stanullfrac)::VARCHAR AS est_null_pct
    , MAX(stawidth)::BIGINT::VARCHAR AS est_byte_width
    , MAX(stadistinct)::BIGINT::VARCHAR AS est_count_distinct
    , SUM(rows_columnstore)::BIGINT::VARCHAR AS est_rows
    , (MAX(stawidth) * est_rows::BIGINT)::BIGINT::VARCHAR AS est_total_bytes
FROM
    <dbname>.pg_catalog.pg_statistic AS stats
    LEFT JOIN <dbname>.sys.table_storage AS strg
        ON starelid = strg.table_id
GROUP BY 1, 2$STR$;
    --
    v_query_stats_dummy VARCHAR(4000) := $STR$
SELECT
    NULL::BIGINT AS starelid
    , NULL::BIGINT AS staattnum
    , NULL::VARCHAR AS est_null_pct
    , NULL::VARCHAR AS est_byte_width
    , NULL::VARCHAR AS est_count_distinct
    , NULL::VARCHAR AS est_rows
    , NULL::VARCHAR AS est_total_bytes$STR$;
    --
    _fn_name   VARCHAR(256) := 'yb_analyze_columns_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    IF a_level = 3 THEN -- forces expanded output
        a_delimited_output := FALSE;
    END IF;
    --RETURN TRUE;
    BEGIN --check if user has privilege to query pg_statistic table
        SELECT 'foo' INTO v_str FROM pg_catalog.pg_statistic LIMIT 1;
        v_has_stats := TRUE;
        v_query := REPLACE(v_query, '<stats_query>', v_query_stats);
    EXCEPTION
        WHEN OTHERS THEN
            v_has_stats := FALSE;
            v_query := REPLACE(v_query, '<stats_query>', v_query_stats_dummy);
    END;
    v_query := REPLACE(v_query, '<dbname>', a_dbname);
    --
    --RAISE INFO '%', v_query; --DEBUG
    --
    IF a_delimited_output THEN -- header for delimited output
        v_str := 'database|column'
            || '|table_order|data_type'
            || '|is_1null_2dist_3sort_4clust_5part'
            || '|bytes_max';
        IF v_has_stats THEN
            v_str := v_str || '|est_null_pct|est_byte_width|est_count_distinct|est_rows|est_total_bytes';
        END IF;
        IF a_level >= 2 THEN
            v_str := v_str
                || '|count_rows|count_nulls|count_distinct'
                || '|char_bytes_min|char_bytes_max|char_bytes_avg|char_bytes_total'
                || '|max_len_int|max_len_frac'
                || '|is_uniq';
                IF v_has_stats THEN
                    v_str := v_str || '|bytes_total';
                END IF;
        END IF;
        RAISE INFO '%', v_str;
    END IF;
    --
    FOR v_rec_tables IN EXECUTE v_query
    LOOP
        v_rec_tables_row := v_rec_tables_row + 1;
        --
        BEGIN
            IF a_level >= 2 THEN
                v_query := REPLACE('
    SELECT
        COUNT(*) AS count
        , SUM(CASE WHEN <columnname> IS NULL THEN 1 ELSE 0 END) AS count_null
        , COUNT(DISTINCT(<columnname>)) AS count_distinct
        , count_distinct + CASE WHEN count_null = 0 THEN 0 ELSE 1 END AS group_count
                    ','<columnname>',v_rec_tables.columnname);
                IF v_rec_tables.data_type NOT IN ('BOOLEAN', 'UUID') THEN
                    v_query := v_query || REPLACE('
        , MIN(<columnname>)::VARCHAR AS min_value
        , MAX(<columnname>)::VARCHAR AS max_value
                    ','<columnname>',v_rec_tables.columnname);
                ELSE
                    v_query := v_query || '
        , NULL::VARCHAR AS min_value
        , NULL::VARCHAR AS max_value
                    ';
                END IF;
                IF v_rec_tables.data_type LIKE '%CHAR%' THEN
                    v_query := v_query || REPLACE('
        , MIN(LENGTH(<columnname>::VARCHAR))::BIGINT AS min_length
        , MAX(LENGTH(<columnname>::VARCHAR))::BIGINT AS max_length
        , ROUND(AVG(LENGTH(<columnname>::VARCHAR) * 1.0), 2)::BIGINT AS avg_length
        , SUM(LENGTH(<columnname>::VARCHAR)) AS total_char_bytes
                        ','<columnname>',v_rec_tables.columnname);
                ELSE
                    v_query := v_query || '
        , NULL::BIGINT AS min_length
        , NULL::BIGINT AS max_length
        , NULL::BIGINT AS avg_length
        , NULL::BIGINT AS total_char_bytes
                        ';
                END IF;
                IF v_rec_tables.data_type LIKE 'NUMERIC%' THEN
                    v_query := v_query || '
        , LENGTH(LTRIM(SPLIT_PART(max_value::VARCHAR, ''.'', 1), ''0''))::BIGINT AS max_len_integer
                    ';
                ELSE
                    v_query := v_query || '
        , NULL::BIGINT AS max_len_integer
                    ';
                END IF;
                IF v_rec_tables.data_type LIKE 'NUMERIC%' AND v_rec_tables.scale > 0 THEN
                    v_query := v_query || REPLACE('
        , MAX(LENGTH(RTRIM(SPLIT_PART(<columnname>::VARCHAR, ''.'', 2), ''0'')))::BIGINT AS max_len_fraction
                    ','<columnname>',v_rec_tables.columnname);
                ELSE
                    v_query := v_query || '
        , NULL::BIGINT AS max_len_fraction
                    ';
                END IF;
                v_query := v_query || '
    FROM
        ' || a_dbname || '.' || v_rec_tables.schemaname || '.' || v_rec_tables.tablename || '
                ';
                --
                --RAISE INFO '%', v_query; --DEBUG
                --
                BEGIN --check if user has privilege to query pg_statistic table
                    EXECUTE v_query INTO v_rec_aggs;
                    v_has_counts := TRUE;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_has_counts := FALSE;
                END;
            END IF;
            --
            IF a_delimited_output THEN -- header for delimited output
                v_str := a_dbname || '|' || v_rec_tables.schemaname || '.' || v_rec_tables.tablename || '.' || v_rec_tables.columnname
                    || '|' || v_rec_tables.column_num || '|' || v_rec_tables.data_type
                    || '|' || DECODE(TRUE,NOT(v_rec_tables.is_not_nullable),'X','-') || DECODE(TRUE,v_rec_tables.is_distribution_key,'X','-') || DECODE(TRUE,v_rec_tables.is_sort_key,'X','-')
                    || DECODE(TRUE,v_rec_tables.is_cluster_key,'X','-') || DECODE(TRUE,v_rec_tables.is_partition_key,'X','-')
                    || '|' || NVL(v_rec_tables.max_bytes::VARCHAR,'');
                IF v_has_stats THEN
                    v_str := v_str
                        || '|' || v_rec_tables.est_null_pct
                        || '|' || v_rec_tables.est_byte_width
                        || '|' || v_rec_tables.est_count_distinct
                        || '|' || v_rec_tables.est_rows
                        || '|' || v_rec_tables.est_total_bytes;
                END IF;
                IF a_level >= 2 THEN
                    v_str := v_str
                        || '|' || v_rec_aggs.count || '|' || v_rec_aggs.count_null || '|' || v_rec_aggs.count_distinct
                        || '|' || NVL(v_rec_aggs.min_length::VARCHAR,'') || '|' || NVL(v_rec_aggs.max_length::VARCHAR,'') || '|' || NVL(v_rec_aggs.avg_length::VARCHAR,'') || '|' || NVL(v_rec_aggs.total_char_bytes::VARCHAR,'')
                        || '|' || NVL(v_rec_aggs.max_len_integer::VARCHAR,'') || '|' || NVL(v_rec_aggs.max_len_fraction::VARCHAR,'')
                        || '|' || DECODE(TRUE,(v_rec_aggs.count = v_rec_aggs.count_distinct),'X','-');
                    IF v_has_stats THEN
                        v_str := v_str || '|' || (v_rec_tables.est_byte_width::REAL * v_rec_aggs.count)::BIGINT;
                    END IF;
                END IF;
                RAISE INFO '%', v_str;
            ELSE
                IF v_rec_tables_row > 1 THEN
                    RAISE INFO '';
                    RAISE INFO '';
                END IF;
                SELECT 'ANALYSIS OF: ' || a_dbname || '.' || v_rec_tables.schemaname || '.' || v_rec_tables.tablename || '.' || v_rec_tables.columnname INTO v_str;
                RAISE INFO '%', v_str;
                RAISE INFO '%', SUBSTR('------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------', 1, LENGTH(v_str));
                RAISE INFO 'Column is: %', DECODE(TRUE, v_rec_tables.is_not_nullable, 'NOT NULLABLE', 'NULLABLE')
                    || DECODE(TRUE, v_rec_tables.is_distribution_key, ', DISTRIBUTION KEY', '')
                    || DECODE(TRUE, v_rec_tables.is_sort_key, ', SORT KEY', '')
                    || DECODE(TRUE, v_rec_tables.is_cluster_key, ', CLUSTER KEY', '')
                    || DECODE(TRUE, v_rec_tables.is_partition_key, ', PARTITION KEY', '');
--                    || DECODE(TRUE, v_rec_aggs.count = v_rec_aggs.count_distinct, ', UNIQUE', '')
                RAISE INFO 'Column Position Ordinal: %', v_rec_tables.column_num;
                RAISE INFO 'Data Type              : %', v_rec_tables.data_type;
                IF v_has_stats THEN
                    RAISE INFO 'Estimated Byte Width   : %', v_rec_tables.est_byte_width;
                    RAISE INFO 'Estimated Distinct Cnt : %', v_rec_tables.est_count_distinct;
                    RAISE INFO 'Estimated Rows         : %', v_rec_tables.est_rows;
                    RAISE INFO 'Estimated Total Bytes  : %', v_rec_tables.est_total_bytes;
                    RAISE INFO 'Estimated Null Percent : %', v_rec_tables.est_null_pct;
                    RAISE INFO 'Max Bytes              : %', v_rec_tables.max_bytes;
                END IF;
                IF a_level >= 2 THEN
                    RAISE INFO 'Row Count              : %', v_rec_aggs.count;
                    RAISE INFO 'Row Count with NULLS   : %', v_rec_aggs.count_null;
                    RAISE INFO 'Row Distinct Count     : %', v_rec_aggs.count_distinct;
                    IF v_rec_tables.data_type NOT IN ('BOOLEAN', 'UUID') THEN
                        RAISE INFO 'Min Value              : %', v_rec_aggs.min_value;
                        RAISE INFO 'Max Value              : %', v_rec_aggs.max_value;
                    END IF;
                    IF v_rec_tables.data_type LIKE '%CHAR%' THEN
                        RAISE INFO 'Min Length             : %', v_rec_aggs.min_length;
                        RAISE INFO 'Max Length             : %', v_rec_aggs.max_length;
                        RAISE INFO 'Average Length         : %', v_rec_aggs.avg_length;
                        RAISE INFO 'Total Character Bytes  : %', v_rec_aggs.total_char_bytes;
                    END IF;
                    IF v_rec_tables.data_type LIKE 'NUMERIC%' THEN
                        RAISE INFO 'Max Digits Integer     : %', v_rec_aggs.max_len_integer;
                    END IF;
                    IF v_rec_tables.data_type LIKE 'NUMERIC%' AND v_rec_tables.scale > 0 THEN
                        RAISE INFO 'Max Digits Fraction    : %', v_rec_aggs.max_len_fraction;
                    END IF;
                    RAISE INFO 'Is Unique              : %', DECODE(TRUE, v_rec_aggs.count = v_rec_aggs.count_distinct, 'TRUE', 'FALSE');
                    IF v_has_stats THEN
                        RAISE INFO 'Total Bytes            : %', (v_rec_tables.est_byte_width::REAL * v_rec_aggs.count)::BIGINT;
                    END IF;
                END IF;
                -- this doube IF is required because AND clauses are not being evaluated left to right in plpgsql
                IF (a_level = 3) THEN IF (
                    v_rec_aggs.count <> v_rec_aggs.count_distinct
                    AND v_rec_tables.data_type != 'BOOLEAN'
                    AND v_rec_aggs.min_value <> v_rec_aggs.max_value)
                    THEN
                    v_ct := 1;
                    v_query := REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('
WITH
t AS (
    SELECT
        COUNT(*) AS ct
        , TO_CHAR(ROUND(ct / (<count> / 100.0), 4), ''90.9999'') AS pct
        , <columnname>::VARCHAR AS col_value
    FROM
        <dbname>.<schemaname>.<tablename>
    GROUP BY 3
    ORDER BY 1 DESC, 3 DESC
    LIMIT <group_ct>
)
, b AS (
    SELECT
        COUNT(*) AS ct
        , TO_CHAR(ROUND(ct / (<count> / 100.0), 4), ''90.9999'') AS pct
        , <columnname>::VARCHAR AS col_value
    FROM
        <dbname>.<schemaname>.<tablename>
    GROUP BY 3
    ORDER BY 1, 3 ASC
    LIMIT <group_ct>
)
SELECT ct, pct, col_value FROM t
UNION SELECT ct, pct, col_value FROM b
ORDER BY 1 DESC, 3'
                    ,'<columnname>',v_rec_tables.columnname)
                    ,'<tablename>',v_rec_tables.tablename)
                    ,'<schemaname>',v_rec_tables.schemaname)
                    ,'<dbname>',a_dbname)
                    ,'<count>', v_rec_aggs.count::VARCHAR)
                    ,'<group_ct>',v_group_ct::VARCHAR);
                    --
                    --RAISE INFO '%', v_query; --DEBUG
                    --
                    FOR v_rec_grps IN EXECUTE v_query
                    LOOP
                        IF v_ct = 1 THEN
                            v_pad_len2 := LENGTH(v_rec_grps.ct::VARCHAR);
                        END IF;
                        IF v_ct = v_group_ct + 1 AND v_rec_aggs.group_count > v_group_ct * 2 THEN
                            RAISE INFO '...';
                            v_ct := v_rec_aggs.group_count - (v_group_ct - 1);
                        END IF;
                        -- 
                        v_pad_len1 :=  LENGTH(v_rec_aggs.group_count::VARCHAR);
                        RAISE INFO 'Group: %,% Row Count: %,% %% of Total Rows: %, Value: %'
                            , v_ct
                            , SUBSTR('                    ', 1, v_pad_len1 - LENGTH(v_ct::VARCHAR))
                            , v_rec_grps.ct
                            , SUBSTR('                    ', 1, v_pad_len2 - LENGTH(v_rec_grps.ct::VARCHAR))
                            , v_rec_grps.pct
                            , v_rec_grps.col_value;
                        v_ct := v_ct + 1;
                    END LOOP;
                END IF; END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE INFO 'ERROR: % %', SQLSTATE, SQLERRM;
        END;
    END LOOP;
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN TRUE;
END$$;