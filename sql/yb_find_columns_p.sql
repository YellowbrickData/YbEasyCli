CREATE OR REPLACE PROCEDURE yb_find_columns(
    a_column_filter_clause VARCHAR(60000) DEFAULT 'TRUE')
RETURNS BOOLEAN
-- Description: Find all columns that match the filter clause
-- Arguments:
--    a_column_filter_clause: an SQL where clause which filters the columns to be
--        updated, the filter can use the following object names; tableowner,
--        schemaname, tablename, columnname, datatype
/*
-- Examples usage:
--
-- find all columns that are of the data type CHARACTER
SELECT yb_find_columns(
    $$
tableowner LIKE '%'
AND schemaname LIKE '%'
AND tablename LIKE '%'
AND columnname LIKE '%'
AND datatype LIKE '%CHARACTER%'
$$
);
 */
LANGUAGE plpgsql AS $$
DECLARE
    v_query_cols TEXT := REPLACE(REPLACE($STR1$
SELECT
    UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as datatype
    , a.attname AS columnname
    , a.attnum AS columnordinal
    , c.relname AS tablename
    , n.nspname AS schemaname
    , '<database>' AS databasename
    , databasename
    || '|' || schemaname
    || '|' || tablename AS tablepath
    , pg_get_userbyid(c.relowner) AS tableowner
FROM <database>.pg_catalog.pg_class AS c
    LEFT JOIN <database>.pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
    JOIN <database>.pg_catalog.pg_attribute AS a
        ON a.attrelid = c.oid
WHERE
    c.relkind = 'r'::CHAR
    AND columnordinal > 0
    AND schemaname NOT IN ('information_schema', 'pg_catalog', 'sys')
    AND <filter_clause>
ORDER BY UPPER(databasename), UPPER(schemaname), UPPER(tablename), columnordinal
$STR1$
        , '<database>', CURRENT_DATABASE())
        , '<filter_clause>', a_column_filter_clause);
    v_rec_cols RECORD;
    v_col_ct INTEGER := 0;
    --
    _fn_name   VARCHAR(256) := 'yb_find_columns';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'ybutils:' || _fn_name;
BEGIN
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _tags);
    --RAISE INFO 'v_query_cols: %', v_query_cols; --DEBUG
    --
    FOR v_rec_cols IN EXECUTE v_query_cols
    lOOP
        RAISE INFO '%|%|%|%|%'
            , v_rec_cols.columnordinal, v_rec_cols.tablepath, v_rec_cols.columnname, v_rec_cols.datatype, v_rec_cols.tableowner;
        v_col_ct := v_col_ct + 1;
    END LOOP;
    --
    -- Reset ybd_query_tags back to its previous value
    EXECUTE REPLACE($STR1$ SET ybd_query_tags TO '<tags>' $STR1$, '<tags>', _prev_tags);
    RETURN TRUE;
END $$;
