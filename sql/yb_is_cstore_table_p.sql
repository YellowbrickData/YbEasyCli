CREATE OR REPLACE PROCEDURE yb_is_cstore_table_p(
    a_table VARCHAR(200))
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS $proc$
--description:
--    Check if table resides in the column store.
--arguments:
--    a_table: the table being checked
DECLARE
    v_query_is_column_store TEXT := REPLACE('SELECT 1 FROM <table> AS foo CROSS JOIN sys.const WHERE FALSE LIMIT 1', '<table>', a_table);
    v_ret_value BOOLEAN := FALSE;
    v_rec RECORD;
BEGIN
    --check if input query is coming from column store
    --  if yes exit with warning
    BEGIN
        --RAISE INFO 'v_query_is_column_store: %', v_query_is_column_store; --DEBUG
        EXECUTE v_query_is_column_store INTO v_rec;
        v_ret_value := TRUE;
    EXCEPTION
        WHEN SQLSTATE '42P01' THEN
            RAISE EXCEPTION '%', SQLERRM;
        WHEN OTHERS THEN
            NULL;
    END;
    RETURN v_ret_value;
END$proc$;