CREATE OR REPLACE PROCEDURE yb_query_to_stored_proc_p(
    a_query VARCHAR(60000)
    , a_stored_proc_name VARCHAR(200)
    , a_stored_proc_template VARCHAR(60000)
    , a_limit_default BIGINT
    , a_max_varchar_size INTEGER DEFAULT 10000
    , a_grant_execute_to VARCHAR(10000) DEFAULT 'public')
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS $proc$
--description:
--    Create a stored procedure that runs the input a_query with the privileges of the user
--       difining/creating the stored procedure.
--arguments:
--    a_query: the query to build as a stored procedure
--    a_stored_proc_name:
--       the destination stored procedure name
--    a_max_varchar_size:
--       truncate size of all VARCHAR columns in the destination table, defaults to 10000
--notes:
--    In addition to the stored procedure an empty table named a_stored_proc_name with the
--    suffix '_t' is created to support the SETOF rtrn clause from the stored procedure.
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
    v_select_clause TEXT := '';
    v_query_first_rec TEXT := REPLACE('SELECT * FROM (<query>) AS foo WHERE FALSE LIMIT 1', '<query>', a_query);
    v_query TEXT;
    v_sql_name TEXT;
BEGIN
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
    --EXECUTE 'DROP TABLE IF EXISTS ' || a_stored_proc_name || '_t';
    v_query := '';
    FOR v_rec IN EXECUTE 'SELECT ordinal, LOWER(name) AS name, UPPER(data_type) AS data_type FROM ' || v_tmp_table || ' ORDER BY 1'
    LOOP
        --RAISE INFO '%', v_rec; --DEBUG
        --some key word column names need to be double quoted
        IF v_rec.name IN ('collation', 'user', 'order') THEN
            v_sql_name := '"' || v_rec.name || '"';
        ELSIF v_rec.name IN ('xmin') THEN
            v_sql_name := '"xmin2"';
        ELSE 
            v_sql_name := v_rec.name;
        END IF;
        v_query := v_query || CHR(13) || CHR(10) || '    ';
        v_select_clause := v_select_clause || CHR(13) || CHR(10) || '    ';
        IF NOT v_is_first_rec THEN
            v_query := v_query || ', ';
            v_select_clause := v_select_clause || ', ';
        END IF;
        IF v_rec.data_type IN ('BIGINT', 'BOOLEAN', 'DATE', 'DOUBLE PRECISION', 'INTEGER', 'IPV4', 'IPV6', 'MACADDR', 'MACADDR8', 'NUMERIC'
            , 'REAL', 'SMALLINT', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE', 'TIME WITHOUT TIME ZONE', 'UUID') THEN
            v_query := v_query || v_sql_name || ' ' || v_rec.data_type;
            v_select_clause := v_select_clause || v_sql_name;
        ELSIF v_rec.data_type IN ('OID', 'INFORMATION_SCHEMA.CARDINAL_NUMBER') THEN
            v_query := v_query || v_sql_name || ' BIGINT';
            v_select_clause := v_select_clause || v_sql_name;
        --TODO: possible improvements in data type mappings like INTERVAL
        ELSIF v_rec.data_type IN ('BYTEA', '"CHAR"', 'CHARACTER', 'CHARACTER VARYING', 'INTERVAL', 'NAME', 'PG_LSN', 'PG_NODE_TREE', 'REGPROC', 'TEXT', 'XID'
                                 , 'INFORMATION_SCHEMA.CHARACTER_DATA', 'INFORMATION_SCHEMA.SQL_IDENTIFIER'
                                 , 'ACLITEM[]', 'ANYARRAY', '"CHAR"[]', 'INET', 'INT2VECTOR', 'NAME[]', 'OID[]', 'OIDVECTOR', 'REAL[]', 'REGTYPE[]', 'SMALLINT[]', 'TEXT', 'TEXT[]'
                                 , 'RECORD') THEN
            v_query := v_query || v_sql_name || ' VARCHAR(' || a_max_varchar_size || ')';
            v_select_clause := v_select_clause || v_sql_name || '::VARCHAR(' || a_max_varchar_size || ') AS ' || v_sql_name;
        ELSIF v_rec.data_type IN ('INFORMATION_SCHEMA.YES_OR_NO') THEN
            v_query := v_query || v_sql_name || ' BOOLEAN';
            v_select_clause := v_select_clause || v_sql_name;
        ELSIF v_rec.data_type IN ('ABSTIME', 'INFORMATION_SCHEMA.TIME_STAMP') THEN
            v_query := v_query || v_sql_name || ' TIMESTAMP';
            v_select_clause := v_select_clause || v_sql_name;
        ELSE
            v_query := v_query || v_rec.name || ' unhandled data type >!>!>!>!> ' || v_rec.data_type || ' <!<!<!<!<'; 
            v_select_clause := v_select_clause || v_sql_name;
        END IF;
        v_is_first_rec := FALSE;
        --RAISE INFO '%', v_query; --DEBUG
    END LOOP;
    v_query := 'CREATE TABLE ' || a_stored_proc_name || '_t ('
        || v_query || CHR(13) || CHR(10) || ') DISTRIBUTE RANDOM'; 
    --RAISE INFO '%', v_query; --DEBUG
    EXECUTE 'DROP TABLE IF EXISTS ' || a_stored_proc_name || '_t CASCADE';
    EXECUTE v_query;
    EXECUTE 'DROP TABLE ' || v_tmp_table;
    --
    --
    -- create the stored procedure
    --RAISE INFO '%', v_select_clause; --DEBUG
    v_query := REPLACE(REPLACE(REPLACE(REPLACE(a_stored_proc_template
        , '<stored_proc_name>', a_stored_proc_name)
        , '<query>', a_query)
        , '<select_clause>', v_select_clause)
        , '<limit_default>', a_limit_default::VARCHAR);
    --RAISE INFO '%', v_query; --DEBUG
    EXECUTE v_query;
    EXECUTE REPLACE(REPLACE('GRANT EXECUTE ON PROCEDURE <stored_proc_name>(_limit BIGINT) TO <roles>'
        , '<stored_proc_name>', a_stored_proc_name)
        , '<roles>', a_grant_execute_to);
    --
    RETURN TRUE;
END$proc$;