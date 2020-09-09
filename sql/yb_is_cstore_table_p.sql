CREATE OR REPLACE PROCEDURE yb_is_cstore_table_p(
    a_tablename VARCHAR(200))
RETURNS BOOLEAN
--description:
--    Check if table resides in the column store.
--arguments:
--    a_tablename: the table being checked
LANGUAGE plpgsql AS $$
DECLARE
    v_query_is_column_store TEXT := REPLACE('SELECT 1 FROM <tablename> AS foo CROSS JOIN sys.const WHERE FALSE LIMIT 1', '<tablename>', a_tablename);
    v_ret_value BOOLEAN := FALSE;
    v_rec RECORD;
    --v_state TEXT; --DEBUG
    --v_msg TEXT;
    --v_detail TEXT; --DEBUG
    --v_hint TEXT; --DEBUG
    --v_context TEXT; --DEBUG
    --
    _fn_name   VARCHAR(256) := 'yb_is_cstore_table_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _tags || ':query_is_column_store');
    --check if input query is coming from column store
    --  if yes exit with warning
    BEGIN
        --RAISE INFO 'v_query_is_column_store: %', v_query_is_column_store; --DEBUG
        EXECUTE v_query_is_column_store INTO v_rec;
        v_ret_value := TRUE;
    EXCEPTION
        WHEN SQLSTATE '42P01' THEN
            --GET STACKED DIAGNOSTICS --DEBUG
            --    v_state   = returned_sqlstate, --DEBUG
            --    v_msg     = message_text, --DEBUG
            --    v_detail  = pg_exception_detail, --DEBUG
            --    v_hint    = pg_exception_hint, --DEBUG
            --    v_context = pg_exception_context; --DEBUG
            --RAISE INFO 'ERROR ------> % %',SQLERRM, SQLSTATE; --DEBUG
            --RAISE INFO 'v_state ----> %', v_state; --DEBUG
            --RAISE INFO 'v_msg ------> %', v_msg; --DEBUG
            --RAISE INFO 'v_detail ---> %', v_detail; --DEBUG
            --RAISE INFO 'v_hint -----> %', v_hint; --DEBUG
            --RAISE INFO 'v_context --> %', v_context; --DEBUG
            RAISE EXCEPTION '%', SQLERRM;
        WHEN OTHERS THEN
            NULL;
    END;

    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN v_ret_value;
END$$;