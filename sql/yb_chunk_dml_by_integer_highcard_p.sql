CREATE OR REPLACE PROCEDURE yb_chunk_dml_by_integer_highcard_p(
    a_table_name            VARCHAR
    , a_integer_column_name VARCHAR
    , a_dml                 VARCHAR
    , a_min_chunk_size      BIGINT
    , a_table_where_clause  VARCHAR DEFAULT 'TRUE'
    , a_verbose             BOOLEAN DEFAULT TRUE
    , a_add_null_chunk      BOOLEAN DEFAULT TRUE
    , a_print_chunk_dml     BOOLEAN DEFAULT FALSE
    , a_execute_chunk_dml   BOOLEAN DEFAULT FALSE
) RETURNS BOOLEAN
-- chunks data where a_interger_column_name column contains higher cardinality data
--     where a column value may have 1000 or less rows or the column contains unique values
LANGUAGE plpgsql AS $$
DECLARE
    v_rc REFCURSOR;
    v_rec RECORD;
    v_chunk_first_val    BIGINT;
    v_total_size         BIGINT;
    v_null_count         BIGINT;
    v_running_total_size BIGINT := 0;
    v_chunk              BIGINT := 1;
    v_chunk_size         BIGINT := 0;
    v_chunk_max_size     BIGINT := 0;
    v_exec_dml           TEXT;
    v_sql_rowcount       BIGINT;
    --
    v_sql_where_clause   TEXT := REPLACE(
'/* chunk_clause(chunk: <chunk>, size: <chunk_size>) >>>*/ <chunk_first_val> <= <integer_column_name> AND <integer_column_name> < <chunk_last_val> /*<<< chunk_clause */'
        , '<integer_column_name>', a_integer_column_name);
    --
    v_sql_select_total_size TEXT := REPLACE(REPLACE(
'SELECT COUNT(*) AS total_size FROM <table_name> WHERE <table_where_clause>'
        , '<table_name>', a_table_name)
        , '<table_where_clause>', a_table_where_clause);
    --
    v_sql_select_null_count TEXT := REPLACE(REPLACE(REPLACE(
'SELECT COUNT(*) AS null_count FROM <table_name> WHERE <integer_column_name> IS NULL AND <table_where_clause> AND <table_where_clause>'
        , '<table_name>', a_table_name)
        , '<integer_column_name>', a_integer_column_name)
        , '<table_where_clause>', a_table_where_clause);
    --
    v_bit_shift INT := 0;
    v_sql_bit_shift TEXT := REPLACE(
'SELECT
    <bit_shift>::BIGINT AS bit_shift
    , 2^10::BIGINT AS magnitude_down
    , <a_min_chunk_size>::BIGINT AS group_size
    , (2::BIGINT ^ bit_shift) AS reduced_group_size
    , DECODE(TRUE, (group_size - (reduced_group_size * magnitude_down)) < 0, TRUE, FALSE) AS found'
        , '<a_min_chunk_size>', a_min_chunk_size::VARCHAR);
    --
    v_sql_create_tmp_group_table TEXT := REPLACE(REPLACE(REPLACE(
$STR$DROP TABLE IF EXISTS chunked_groups;
CREATE TEMPORARY TABLE chunked_groups AS
WITH
shift_group_id AS (
    SELECT
        <integer_column_name> AS val
        , val >> <bit_shift> AS shift_group_id
    FROM
        <table_name>
    WHERE
        <table_where_clause>
)
, shift_group AS (
    SELECT
        MIN(val) AS start_val
        , MAX(val) AS last_val
        , COUNT(*) AS cnt
    FROM shift_group_id
    GROUP BY shift_group_id
)
SELECT
    start_val
    , last_val
    , LEAD(start_val, 1) OVER (ORDER BY start_val) AS next_val
    , cnt
FROM shift_group
DISTRIBUTE ON (start_val)$STR$
        , '<table_name>', a_table_name)
        , '<integer_column_name>', a_integer_column_name)
        , '<table_where_clause>', a_table_where_clause);
    v_sql_select_groups TEXT := 
'SELECT
    start_val
    , NVL(next_val, last_val + 1) AS next_val  
    , NVL2(next_val, FALSE, TRUE) AS is_last_rec
    , cnt
FROM chunked_groups
ORDER BY 1
';
    v_start_ts     TIMESTAMP := CLOCK_TIMESTAMP();
    v_dml_start_ts TIMESTAMP;
    v_dml_total_duration INTERVAL := INTERVAL '0 DAYS';
    --
    _fn_name   VARCHAR(256) := 'yb_chunk_dml_by_integer_highcard_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _tags);
    IF a_verbose = TRUE THEN RAISE INFO '--%: Starting Integer Chunking, first calculating group counts', CLOCK_TIMESTAMP(); END IF;
    --
    EXECUTE v_sql_select_total_size INTO v_total_size;
    EXECUTE v_sql_select_null_count INTO v_null_count;
    --
    --
    -- first pass on chunk grouping, using bit shift on integer to perform crude groupings
    IF a_verbose = TRUE THEN RAISE INFO '--%: Build Chunk Groupings, first pass', CLOCK_TIMESTAMP(); END IF;
    --
    EXECUTE REPLACE(v_sql_bit_shift, '<bit_shift>', v_bit_shift::VARCHAR) INTO v_rec;
    LOOP
        EXIT WHEN v_rec.found;
        v_bit_shift := v_bit_shift + 1;
        EXECUTE REPLACE(v_sql_bit_shift, '<bit_shift>', v_bit_shift::VARCHAR) INTO v_rec;
    END LOOP;
    --
    EXECUTE REPLACE(v_sql_create_tmp_group_table, '<bit_shift>', v_bit_shift::VARCHAR);
    --
    OPEN v_rc FOR EXECUTE v_sql_select_groups;
    --RAISE INFO '--%: SQL :% ', CLOCK_TIMESTAMP(), v_sql_select_groups; --DEBUG
    FETCH NEXT FROM v_rc INTO v_rec;
    v_chunk_first_val := v_rec.start_val;
    --
    --
    IF a_verbose = TRUE THEN RAISE INFO '--%: Build Chunk DMLs', CLOCK_TIMESTAMP(); END IF;
    --
    LOOP
        v_chunk_size := v_chunk_size + v_rec.cnt;
        IF v_chunk_size >= a_min_chunk_size OR v_rec.is_last_rec THEN
            IF v_chunk_size > v_chunk_max_size THEN
                v_chunk_max_size := v_chunk_size;
            END IF;
            --
            IF a_verbose = TRUE THEN
                RAISE INFO '--%: Chunk: %, Rows: %, Range % <= % < %', CLOCK_TIMESTAMP(), v_chunk, v_chunk_size, v_chunk_first_val, a_integer_column_name, v_rec.next_val;
            END IF;
            --
            v_exec_dml := REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(a_dml,'<chunk_where_clause>', v_sql_where_clause), '<chunk_first_val>', v_chunk_first_val::VARCHAR), '<chunk_last_val>',  v_rec.next_val::VARCHAR), '<chunk_size>', v_chunk_size::VARCHAR), '<chunk>', v_chunk::VARCHAR);
            --
            IF a_print_chunk_dml = TRUE THEN RAISE INFO '%;', v_exec_dml; END IF;
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
        IF a_verbose = TRUE THEN RAISE INFO '--%: Chunk: %, Rows: %, % IS NULL', CLOCK_TIMESTAMP(), v_chunk, v_null_count, a_integer_column_name; END IF;
        --
        v_exec_dml := REPLACE(a_dml, '<chunk_where_clause>', a_integer_column_name || ' IS NULL');
        --
        IF a_print_chunk_dml = TRUE THEN RAISE INFO '%;', v_exec_dml; END IF;
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
        RAISE INFO '--%: Completed Integer Chunked DML', CLOCK_TIMESTAMP();
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
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN (v_total_size = v_running_total_size);
END$$;