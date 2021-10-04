CREATE OR REPLACE PROCEDURE materialize_sys_log_query_p(
    a_table_name VARCHAR(256)
    , a_object_owner VARCHAR(256)
    , a_where_clause VARCHAR(10000) DEFAULT 'TRUE'
    , a_create_view BOOLEAN DEFAULT FALSE)
    RETURNS BOOLEAN
    LANGUAGE 'plpgsql' 
    VOLATILE
    SECURITY DEFINER
AS $proc$
DECLARE
    v_column_list TEXT := '';
    v_sql TEXT := $SQL$
        SELECT
            DECODE(ordinal_position, 1, '', ', ') || column_name AS column
        FROM
            information_schema.columns
        WHERE
            table_schema = 'sys'
            AND table_name = 'log_query'
            AND column_name != 'query_text'
        ORDER BY ordinal_position $SQL$;
    v_rec RECORD;
    v_table_text_name TEXT;
    v_view_name TEXT;
BEGIN
    SELECT REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, '\1_text\2'::VARCHAR) INTO v_table_text_name;
    SELECT REGEXP_REPLACE(a_table_name, '(.*)("$)|($)'::VARCHAR, '\1_v\2'::VARCHAR) INTO v_view_name;
    --
    FOR v_rec IN EXECUTE v_sql
    lOOP
        v_column_list := v_column_list || v_rec.column;
    END LOOP;
    --RAISE INFO '%', v_column_list; --DEBUG 
    --
    EXECUTE '
        CREATE TABLE ' || a_table_name || ' AS
        SELECT ' || v_column_list || '
        FROM sys.log_query
        WHERE ' || a_where_clause || '
        DISTRIBUTE ON (query_id)';
    EXECUTE 'ALTER TABLE ' || a_table_name || ' OWNER TO '      || a_object_owner;
    --
    EXECUTE '
        CREATE TABLE ' || v_table_text_name || ' AS
        SELECT
            DISTINCT query_id, query_text
        FROM sys.log_query
        WHERE ' || a_where_clause || '
        DISTRIBUTE ON (query_id)';
    EXECUTE 'ALTER TABLE ' || v_table_text_name || ' OWNER TO ' || a_object_owner;
    --
    IF a_create_view THEN
        EXECUTE '
            CREATE VIEW ' || v_view_name || ' AS
            SELECT
                q.*
                , qt.query_text
            FROM
                ' || a_table_name || ' AS q
                JOIN ' || v_table_text_name || ' AS qt
                    USING (query_id)';
        EXECUTE 'ALTER VIEW '  || v_view_name || ' OWNER TO '    || a_object_owner;
    END IF;
    --
    RETURN TRUE;
END; $proc$