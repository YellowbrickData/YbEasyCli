CREATE OR REPLACE PROCEDURE create_log_query_history_p(
    a_table_name VARCHAR(256) DEFAULT 'log_query_history'
    , a_where_clause VARCHAR(10000) DEFAULT 'TRUE')
    RETURNS BOOLEAN
    LANGUAGE 'plpgsql' 
    VOLATILE
    SECURITY DEFINER
AS $PROC$
DECLARE
    v_table_text_name TEXT;
    v_tmp_table_name TEXT;
    v_tmp_table_text_name TEXT;
    ts TEXT := TO_CHAR(CURRENT_TIMESTAMP, '_YYYYMMDDHH24MISS');
    v_cnt BIGINT;
    v_rec RECORD;
BEGIN
    SELECT REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, ('\1_text\2')::VARCHAR) INTO v_table_text_name;
    SELECT REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, ('\1' || ts || '\2')::VARCHAR) INTO v_tmp_table_name;
    SELECT REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, ('\1' || ts || '_text\2')::VARCHAR) INTO v_tmp_table_text_name;
    --
    BEGIN
        --check if the history objects have been created
        EXECUTE 'SELECT TRUE AS ret FROM ' || a_table_name || ' WHERE FALSE' INTO v_rec;
    EXCEPTION
        WHEN SQLSTATE '42P01' THEN
            SELECT materialize_sys_log_query_p(a_table_name, CURRENT_USER, 'FALSE', TRUE) INTO v_rec;
            RAISE INFO '--created % table, % table and % view', a_table_name, v_table_text_name, REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, ('\1_v\2')::VARCHAR);
        WHEN OTHERS THEN
            NULL;
    END;
    --
    SELECT materialize_sys_log_query_p(v_tmp_table_name, CURRENT_USER, a_where_clause) INTO v_rec;
    --
    EXECUTE 'INSERT INTO ' || a_table_name      || ' SELECT * FROM ' || v_tmp_table_name      || ' WHERE query_id NOT IN (SELECT query_id FROM ' || a_table_name      || ')';
    GET DIAGNOSTICS v_cnt = row_count;
    EXECUTE 'INSERT INTO ' || v_table_text_name || ' SELECT * FROM ' || v_tmp_table_text_name || ' WHERE query_id NOT IN (SELECT query_id FROM ' || v_table_text_name || ')';
    --
    EXECUTE 'DROP TABLE ' || v_tmp_table_name;
    EXECUTE 'DROP TABLE ' || v_tmp_table_text_name;
    --
    RAISE INFO '--inserted % queries into % and % tables', v_cnt, a_table_name, v_table_text_name;
    RETURN TRUE;
END; $PROC$