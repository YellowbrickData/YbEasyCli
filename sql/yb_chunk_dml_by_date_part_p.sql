CREATE PROCEDURE yb_chunk_dml_by_date_part_p(
    a_table_name          VARCHAR
    , a_ts_column_name    VARCHAR
    , a_date_part         VARCHAR
    , a_dml               VARCHAR
    , a_min_chunk_size    BIGINT
    , a_verbose           BOOLEAN DEFAULT TRUE
    , a_add_null_chunk    BOOLEAN DEFAULT FALSE
    , a_execute_chunk_dml BOOLEAN DEFAULT FALSE
    , a_print_chunk_dml   BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_rc REFCURSOR;
    v_rec RECORD;
    v_chunk_first_val    TIMESTAMP;
    v_total_size         BIGINT;
    v_null_count         BIGINT;
    v_running_total_size BIGINT := 0;
    v_chunk              BIGINT := 1;
    v_chunk_size         BIGINT := 0;
    v_chunk_max_size     BIGINT := 0;
    v_exec_dml TEXT;
    v_sql_where_clause  TEXT := REPLACE(
'/* chunk_clause(chunk: <chunk>, size: <chunk_size>) >>>*/ TO_TIMESTAMP(''<chunk_first_val>'',''YYYY-MM-DD HH24:MI:SS'') <= <ts_column_name> AND <ts_column_name> < TO_TIMESTAMP(''<chunk_last_val>'',''YYYY-MM-DD HH24:MI:SS'') /*<<< chunk_clause */'
        , '<ts_column_name>', a_ts_column_name);
    v_sql_select_total_size TEXT := REPLACE(
'SELECT COUNT(*) AS total_size FROM <table_name>'
        , '<table_name>', a_table_name);
    v_sql_select_null_count TEXT := REPLACE(REPLACE(
'SELECT COUNT(*) AS null_count FROM <table_name> WHERE <ts_column_name> IS NULL'
        , '<table_name>', a_table_name)
        , '<ts_column_name>', a_ts_column_name);
    v_sql_create_tmp_group_table TEXT := REPLACE(REPLACE(REPLACE($STR$
CREATE TEMPORARY TABLE chunked_groups AS
SELECT
    DATE_TRUNC('<date_part>', <ts_column_name>) AS start_val
    , COUNT(*) AS cnt
FROM <table_name>
GROUP BY 1
DISTRIBUTE ON (start_val)$STR$
        , '<date_part>', a_date_part)
        , '<table_name>', a_table_name)
        , '<ts_column_name>', a_ts_column_name);
    v_sql_select_groups TEXT := REPLACE($STR$
WITH
group_w_lead AS (
    SELECT
        start_val
        , LEAD(start_val, 1) OVER (ORDER BY start_val) AS next_val
        , cnt
    FROM chunked_groups
)
SELECT
    start_val
    , NVL(next_val, start_val + INTERVAL '1 <date_part>') AS next_val
    , NVL2(next_val, FALSE, TRUE) AS is_last_rec
    , cnt
FROM group_w_lead
ORDER BY 1$STR$
        , '<date_part>', a_date_part);
    v_start_ts     TIMESTAMP := CLOCK_TIMESTAMP();
    v_dml_start_ts TIMESTAMP;
    v_dml_total_duration INTERVAL := INTERVAL '0 DAYS';
    --
    _fn_name   VARCHAR(256) := 'yb_chunk_dml_by_date_part_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    IF a_verbose = TRUE THEN
        RAISE INFO '--%: Starting Date Part Chunking, first calculating % group counts', CLOCK_TIMESTAMP(), a_date_part;
    END IF;
    --
    EXECUTE v_sql_create_tmp_group_table;
    EXECUTE v_sql_select_total_size INTO v_total_size;
    EXECUTE v_sql_select_null_count INTO v_null_count;
    --
    OPEN v_rc FOR EXECUTE v_sql_select_groups;
    --RAISE INFO '--%: SQL :% ', CLOCK_TIMESTAMP(), v_sql_select_groups; --DEBUG
    FETCH NEXT FROM v_rc INTO v_rec;
    v_chunk_first_val := v_rec.start_val;
    --
    IF a_verbose = TRUE THEN
        RAISE INFO '--%: Build Chunk DMLs', CLOCK_TIMESTAMP();
    END IF;
    --
    LOOP
        v_chunk_size := v_chunk_size + v_rec.cnt;
        IF v_chunk_size >= a_min_chunk_size OR v_rec.is_last_rec THEN
            IF v_chunk_size > v_chunk_max_size THEN
                v_chunk_max_size := v_chunk_size;
            END IF;
            --
            IF a_verbose = TRUE THEN
                RAISE INFO '--%: Chunk: %, Rows: %, Range % <= % < %', CLOCK_TIMESTAMP(), v_chunk, v_chunk_size, v_chunk_first_val, a_ts_column_name, v_rec.next_val;
            END IF;
            --
            v_exec_dml := REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(a_dml,'<chunk_where_clause>', v_sql_where_clause), '<chunk_first_val>', TO_CHAR(v_chunk_first_val,'YYYY-MM-DD HH24:MI:SS')), '<chunk_last_val>',  TO_CHAR(v_rec.next_val,'YYYY-MM-DD HH24:MI:SS')), '<chunk_size>', v_chunk_size::VARCHAR), '<chunk>', v_chunk::VARCHAR);
            --
            IF a_print_chunk_dml = TRUE THEN
                RAISE INFO '%;', v_exec_dml;
            END IF;
            --
            IF a_execute_chunk_dml = TRUE THEN
                v_dml_start_ts := CLOCK_TIMESTAMP();
                EXECUTE v_exec_dml;
                v_dml_total_duration := v_dml_total_duration + (CLOCK_TIMESTAMP() - v_dml_start_ts);
            END IF;
            --
            v_running_total_size := v_running_total_size + v_chunk_size;
            EXIT WHEN v_rec.is_last_rec;
            --
            v_chunk := v_chunk + 1;
            v_chunk_first_val := v_rec.next_val;
            v_chunk_size := 0;
        END IF;
        --RAISE INFO '%', v_rec;
        FETCH NEXT FROM v_rc INTO v_rec;
    END LOOP;
    CLOSE v_rc;
    --
    IF a_add_null_chunk = TRUE THEN
        v_chunk := v_chunk + 1;
        --
        IF a_verbose = TRUE THEN
            RAISE INFO '--%: Chunk: %, Rows: %, % IS NULL', CLOCK_TIMESTAMP(), v_chunk, v_null_count, a_ts_column_name;
        END IF;
        --
        v_exec_dml := REPLACE(a_dml,'<chunk_where_clause>', a_ts_column_name || ' IS NULL');
        --
        IF a_print_chunk_dml = TRUE THEN
            RAISE INFO '%;', v_exec_dml;
        END IF;
        --
        IF a_execute_chunk_dml = TRUE THEN
            v_dml_start_ts := CLOCK_TIMESTAMP();
            EXECUTE v_exec_dml;
            v_dml_total_duration := v_dml_total_duration + (CLOCK_TIMESTAMP() - v_dml_start_ts);
        END IF;
        --
        v_running_total_size := v_running_total_size + v_null_count;
    END IF;
    --
    IF a_verbose = TRUE THEN
        RAISE INFO '--%: Completed Date Part Chunked DML', CLOCK_TIMESTAMP();
        IF a_add_null_chunk = FALSE AND v_null_count <> 0 THEN
            RAISE INFO '--******WARNING******: There are records with NULL vales and you have not requested for a NULL chunk!';
        END IF;
        RAISE INFO '--Total Rows         : %', v_total_size;
        RAISE INFO '--IS NULL Rows       : %', v_null_count;
        RAISE INFO '--Running total check: %', DECODE(TRUE, (DECODE(TRUE, a_add_null_chunk, v_total_size, v_total_size - v_null_count) = v_running_total_size), 'PASSED', 'FAILED');
        RAISE INFO '--Duration           : %', CLOCK_TIMESTAMP() - v_start_ts;
        RAISE INFO '--Overhead duration  : %', (CLOCK_TIMESTAMP() - v_start_ts) - v_dml_total_duration;
        RAISE INFO '--Total Chunks       : %', v_chunk;
        RAISE INFO '--Min chunk size     : %', a_min_chunk_size;
        RAISE INFO '--Largest chunk size : %', v_chunk_max_size;
        RAISE INFO '--Average chunk size : %', v_running_total_size / v_chunk;
    END IF;
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN (v_total_size = v_running_total_size);
END $$;