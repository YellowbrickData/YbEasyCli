CREATE PROCEDURE yb_analyze_columns_p(a_dbname VARCHAR, a_tablename VARCHAR, a_filter_clause VARCHAR)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_rec_tables RECORD;
    v_rec_aggs RECORD;
    v_rec_grps RECORD;
    v_ct BIGINT;
    v_group_ct BIGINT := 10;
    v_rec_tables_row INTEGER := 0;
    v_str VARCHAR;
    v_pad_len1 INTEGER;
    v_pad_len2 INTEGER;
    v_query VARCHAR(2000) := REPLACE(REPLACE(REPLACE('
WITH
o AS (
    SELECT
        a.attname AS columnname
        , a.attnum AS column_num
        , c.relname AS tablename
        , n.nspname AS schemaname
        , pg_get_userbyid(c.relowner) AS tableowner
        , UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as data_type
    FROM
        <dbname>.pg_catalog.pg_class AS c
        LEFT JOIN <dbname>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <dbname>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind IN (''r'')
        AND column_num > 0
        AND tablename LIKE ''<tablename>''
        AND <filter_clause>
)
SELECT
    data_type
    , columnname
    , tablename
    , schemaname
    , tableowner
    , column_num
    , DECODE(TRUE, data_type LIKE ''%(%'', RTRIM(SPLIT_PART(data_type,''('',2),'')'')) AS dimensions
    , SPLIT_PART(dimensions,'','',1)::INT AS precision
    , SPLIT_PART(dimensions,'','',2) AS scale_char
    , DECODE(scale_char, NULL, NULL::INT, '''', NULL::INT, scale_char::INT) AS scale
FROM
    o
ORDER BY
    schemaname, tablename, column_num'
        ,'<dbname>',a_dbname)
        ,'<tablename>',a_tablename)
        ,'<filter_clause>',a_filter_clause);
    --
    _fn_name   VARCHAR(256) := 'yb_analyze_columns_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    --
    --RAISE INFO '%', v_query; --DEBUG
    --
    FOR v_rec_tables IN EXECUTE v_query
    LOOP
        v_rec_tables_row := v_rec_tables_row + 1;
        IF v_rec_tables_row > 1 THEN
            RAISE INFO '';
            RAISE INFO '';
        END IF;
        --
        BEGIN
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
            END IF;
            IF v_rec_tables.data_type LIKE '%CHAR%' THEN
                v_query := v_query || REPLACE('
    , MIN(LENGTH(<columnname>::VARCHAR)) AS min_length
    , MAX(LENGTH(<columnname>::VARCHAR)) AS max_length
    , ROUND(AVG(LENGTH(<columnname>::VARCHAR) * 1.0), 2) AS avg_length
    , SUM(LENGTH(<columnname>::VARCHAR)) AS total_bytes
                    ','<columnname>',v_rec_tables.columnname);
            END IF;
            IF v_rec_tables.data_type LIKE 'NUMERIC%' THEN
                v_query := v_query || '
    , LENGTH(LTRIM(SPLIT_PART(max_value::VARCHAR, ''.'', 1), ''0'')) AS max_len_integer
                ';
            END IF;
            IF v_rec_tables.data_type LIKE 'NUMERIC%' AND v_rec_tables.scale > 0 THEN
                v_query := v_query || REPLACE('
    , MAX(LENGTH(RTRIM(SPLIT_PART(<columnname>::VARCHAR, ''.'', 2), ''0''))) AS max_len_fraction
                ','<columnname>',v_rec_tables.columnname);
            END IF;
            v_query := v_query || '
FROM
    ' || a_dbname || '.' || v_rec_tables.schemaname || '.' || v_rec_tables.tablename || '
            ';
            --
            --RAISE INFO '%', v_query; --DEBUG
            --
            EXECUTE v_query INTO v_rec_aggs;
            --
            SELECT 'ANALYSIS OF: ' || a_dbname || '.' || v_rec_tables.schemaname || '.' || v_rec_tables.tablename || '.' || v_rec_tables.columnname INTO v_str;
            RAISE INFO '%', v_str;
            RAISE INFO '%', SUBSTR('------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------', 1, LENGTH(v_str));
            RAISE INFO 'Column Position Ordinal: %', v_rec_tables.column_num;
            RAISE INFO 'Data Type              : %', v_rec_tables.data_type;
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
                RAISE INFO 'Total Bytes            : %', v_rec_aggs.total_bytes;
            END IF;
            IF v_rec_tables.data_type LIKE 'NUMERIC%' THEN
                RAISE INFO 'Max Digits Integer     : %', v_rec_aggs.max_len_integer;
            END IF;
            IF v_rec_tables.data_type LIKE 'NUMERIC%' AND v_rec_tables.scale > 0 THEN
                RAISE INFO 'Max Digits Fraction    : %', v_rec_aggs.max_len_fraction;
            END IF;
            --
            IF v_rec_aggs.count = v_rec_aggs.count_distinct THEN
                RAISE INFO 'Unique                 : TRUE';
            ELSE
                IF v_rec_tables.data_type != 'BOOLEAN'
                    AND v_rec_aggs.min_value <> v_rec_aggs.max_value THEN
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
                END IF;
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