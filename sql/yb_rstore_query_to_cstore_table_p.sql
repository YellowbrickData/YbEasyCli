CREATE OR REPLACE PROCEDURE yb_rstore_query_to_cstore_table_p(
    a_query VARCHAR(60000)
    , a_tablename VARCHAR(200)
    , a_create_temp_table BOOLEAN DEFAULT FALSE
    , a_drop_table BOOLEAN DEFAULT FALSE
    , a_max_varchar_size INTEGER DEFAULT 10000)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS $$
--description:
--    Materialize a rowstore table or in memory query to a columnstore table.
--arguments:
--    a_query: the query to materialize
--    a_tablename:
--       the destination table name
--    a_create_temp_table:
--       create destination table as temporary table, defaults ito false.
--    a_drop_table: 
--       first drop the destination(a_tablename) if it exists, defaults to false.
--    a_max_varchar_size:
--       truncate size of all VARCHAR columns in the destination table, defaults to 10000
--todo: handle objects with column names in multi-case like; sys.vt_worker_storage
DECLARE
    v_rec RECORD;
    v_rec_ct INTEGER := 0;
    v_rec_json RECORD;
    v_is_first_rec BOOLEAN := TRUE;
    v_tmp_table TEXT := 'yb_columns_' || TO_CHAR(NOW(), 'YYYYMMDDHH24MISS') || '_' || (RANDOM() * 1000)::INT::VARCHAR;
    v_do_query TEXT := REPLACE('CREATE TEMP TABLE <table> AS ', '<table>', v_tmp_table);
    v_do_declare TEXT := '';
    v_do_set TEXT := '';
    v_do_values TEXT := '';
    v_query_first_rec TEXT := REPLACE('SELECT * FROM (<query>) AS foo WHERE FALSE LIMIT 1', '<query>', a_query);
    v_query_is_column_store TEXT := REPLACE('SELECT one FROM (<query>) AS foo CROSS JOIN sys.const WHERE FALSE LIMIT 1', '<query>', a_query);
    v_query TEXT;
    v_sql_name TEXT;
    --
BEGIN
    --check if input query is coming from column store
    --  if yes exit with warning
    BEGIN
        EXECUTE v_query_is_column_store INTO v_rec;
        RAISE EXCEPTION 'table/s from column store databese are NOT permitted in the input query...';
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            RAISE EXCEPTION '%', SQLERRM;
        WHEN OTHERS THEN
            NULL;
    END;
    --
    --
    -- query the first record of the input catalog table/view query(a_query)
    --   use this record to build dynamic pg/plsql and SQL
    EXECUTE v_query_first_rec INTO v_rec;
    FOR v_rec_json IN SELECT * FROM json_each(row_to_json(v_rec))
    LOOP
        IF NOT v_is_first_rec THEN
            v_do_query := v_do_query || CHR(13) || CHR(10) || 'UNION ALL ';
        END IF;
        v_rec_ct := v_rec_ct + 1;
        v_do_declare := v_do_declare || REPLACE('v_<ct> TEXT; ', '<ct>', v_rec_ct::VARCHAR);
        v_do_set := v_do_set || REPLACE(REPLACE('v_<ct> = pg_typeof(v_rec.<name>); ', '<ct>', v_rec_ct::VARCHAR), '<name>', v_rec_json.key);
        v_do_query := v_do_query || REPLACE(REPLACE(
            $STR$SELECT <ct> AS ordinal, '<name>' AS name, v_<ct> AS data_type $STR$
            , '<ct>', v_rec_ct::VARCHAR)
            , '<name>', v_rec_json.key);
        v_is_first_rec := FALSE;
    END LOOP;
    --RAISE INFO '%', v_do_declare; --DEBUG
    --RAISE INFO '%', v_do_set; --DEBUG
    --RAISE INFO '%', v_do_query; --DEBUG
    --
    --
    -- create a temp table like yb_columns_YYYYMMDDHHMMSS_XXX(ordinal INT, name VARCHAR, data_type VARCHAR)
    --   that contains the structure of the data returned by the input catalog table/view query(a_query)
    v_query := REPLACE(REPLACE(REPLACE(REPLACE($STR$
DO $MAIN$
DECLARE
    v_rec RECORD;
    v_query_first_rec TEXT := <query_first_rec>;
    <declare>
BEGIN
    EXECUTE v_query_first_rec INTO v_rec;
    <set>
    <create_table>;
END; $MAIN$
$STR$
        , '<create_table>', v_do_query)
        , '<declare>', v_do_declare)
        , '<set>', v_do_set)
        , '<query_first_rec>', '$STR$' || v_query_first_rec || '$STR$');
    --RAISE INFO '%', v_query; --DEBUG
    EXECUTE v_query;
    --
    --
    -- create a column store table that can store the data from the input catalog table/view query(a_query)
    v_is_first_rec := TRUE;
    --EXECUTE 'DROP TABLE IF EXISTS ' || a_tablename ||;
    IF a_create_temp_table THEN
        v_query := 'CREATE TEMP TABLE ';
    ELSE
        v_query := 'CREATE TABLE ';
    END IF;
    v_query := v_query || a_tablename || ' (';
    FOR v_rec IN EXECUTE 'SELECT ordinal, LOWER(name) AS name, UPPER(data_type) AS data_type FROM ' || v_tmp_table || ' ORDER BY 1'
    LOOP
        --RAISE INFO '%', v_rec; --DEBUG
        --some key word column names need to be doouble quoted
        IF v_rec.name IN ('collation', 'user', 'order') THEN
            v_sql_name := '"' || v_rec.name || '"';
        ELSIF v_rec.name IN ('xmin') THEN
            v_sql_name := '"xmin2"';
        ELSE 
            v_sql_name := v_rec.name;
        END IF;
        v_query := v_query || CHR(13) || CHR(10) || '    ';
        IF NOT v_is_first_rec THEN
            v_query := v_query || ', ';
            v_do_values := v_do_values || ', ';
        END IF;
        IF v_rec.data_type IN ('BIGINT', 'BOOLEAN', 'DATE', 'DOUBLE PRECISION', 'INTEGER', 'IPV4', 'IPV6', 'MACADDR', 'MACADDR8', 'NUMERIC'
            , 'REAL', 'SMALLINT', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE', 'TIME WITHOUT TIME ZONE', 'UUID') THEN
            v_query := v_query || v_sql_name || ' ' || v_rec.data_type;
            v_do_values := v_do_values || REPLACE('v_rec.<name>', '<name>', v_rec.name);
        ELSIF v_rec.data_type IN ('OID', 'INFORMATION_SCHEMA.CARDINAL_NUMBER') THEN
            v_query := v_query || v_sql_name || ' BIGINT';
            v_do_values := v_do_values || REPLACE('v_rec.<name>', '<name>', v_rec.name);
        --TODO: possible improvemts in data type mappings like INTERVAL
        ELSIF v_rec.data_type IN ('BYTEA', '"CHAR"', 'CHARACTER', 'CHARACTER VARYING', 'INTERVAL', 'NAME', 'PG_LSN', 'PG_NODE_TREE', 'REGPROC', 'TEXT', 'XID'
                                 , 'INFORMATION_SCHEMA.CHARACTER_DATA', 'INFORMATION_SCHEMA.SQL_IDENTIFIER') THEN 
            v_query := v_query || v_sql_name || ' VARCHAR(' || a_max_varchar_size || ')';
            v_do_values := v_do_values || REPLACE('SUBSTR(v_rec.<name>::TEXT, 1, ' || a_max_varchar_size || ')', '<name>', v_rec.name);
        ELSIF v_rec.data_type IN ('ACLITEM[]', 'ANYARRAY', '"CHAR"[]', 'INET', 'INT2VECTOR', 'NAME[]', 'OID[]', 'OIDVECTOR', 'REAL[]', 'REGTYPE[]', 'SMALLINT[]', 'TEXT', 'TEXT[]') THEN
            v_query := v_query || v_sql_name || ' VARCHAR(' || a_max_varchar_size || ')';
            v_do_values := v_do_values || REPLACE('SUBSTR(v_rec.<name>::TEXT, 1, ' || a_max_varchar_size || ')', '<name>', v_rec.name);
        ELSIF v_rec.data_type IN ('RECORD') THEN
            v_query := v_query || v_sql_name || ' VARCHAR(' || a_max_varchar_size || ')';
            v_do_values := v_do_values || REPLACE('SUBSTR(ROW_TO_JSON(v_rec.<name>)::TEXT, 1, ' || a_max_varchar_size || ')', '<name>', v_rec.name);
        ELSIF v_rec.data_type IN ('INFORMATION_SCHEMA.YES_OR_NO') THEN
            v_query := v_query || v_sql_name || ' BOOLEAN';
            v_do_values := v_do_values || REPLACE('DECODE(v_rec.<name>::TEXT, ''YES'', TRUE, ''NO'', FALSE, NULL::BOOLEAN)', '<name>', v_rec.name);
        ELSIF v_rec.data_type IN ('ABSTIME', 'INFORMATION_SCHEMA.TIME_STAMP') THEN
            v_query := v_query || v_sql_name || ' TIMESTAMP';
            v_do_values := v_do_values || REPLACE('v_rec.<name>', '<name>', v_rec.name);
        ELSE
            v_do_values := v_do_values || REPLACE('v_rec.<name>', '<name>', v_rec.name);
            v_query := v_query || v_rec.name || ' unhandled data type >!>!>!>!> ' || v_rec.data_type || ' <!<!<!<!<'; 
        END IF;
        v_is_first_rec := FALSE;
        --RAISE INFO '%', v_query; --DEBUG
    END LOOP;
    v_query := v_query || CHR(13) || CHR(10) || ') DISTRIBUTE RANDOM'; 
    --RAISE INFO '%', v_query; --DEBUG
    IF a_drop_table THEN
        EXECUTE 'DROP TABLE IF EXISTS ' || a_tablename;
    END IF;
    EXECUTE v_query;
    EXECUTE 'DROP TABLE ' || v_tmp_table;
    --
    --
    -- execute the input catalog table/view query(a_query) and move record by record
    --   to the new column store table
    v_query := REPLACE(REPLACE(REPLACE($STR$
DO $MAIN$
DECLARE
    v_rec RECORD;
    v_query TEXT := <query>;
BEGIN
    FOR v_rec IN EXECUTE v_query
    LOOP
        INSERT INTO <table> VALUES (<values>);
    END LOOP;
END; $MAIN$
$STR$
        , '<table>', a_tablename)
        , '<values>', v_do_values)
        , '<query>', '$STR$' || a_query || '$STR$');
    --RAISE INFO '%', v_query; --DEBUG
    EXECUTE v_query;
    --
    RETURN TRUE;
END$$;