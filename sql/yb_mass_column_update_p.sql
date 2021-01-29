CREATE OR REPLACE PROCEDURE yb_mass_column_update_p(
    a_update_where_clause VARCHAR(60000)
    , a_set_clause VARCHAR(60000)
    , a_column_filter_clause VARCHAR(60000) DEFAULT 'TRUE'
    , a_exec_updates BOOLEAN DEFAULT FALSE)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS $$
-- Description: Update the value of multiple columns
-- Arguments:
--    a_update_where_clause: an SQL clause which rtrns a BOOLEAN value to determine which
--        rows are updated
--    a_set_clause: an SQL clause used to update the column value when
--        a_update_where_clause evaluates to TRUE
--    a_column_filter_clause: an SQL where clause which filters the columns to be
--        updated, the filter can use the following object names; tableowner,
--        schemaname, tablename, columnname, datatype
--    a_exec_updates: execute the updates, default is FALSE
-- Note:
--    the special string '<column>' can be used in the arguments a_update_where_clause
--        and a_set_clause to aid in building dynamic SQL clauses
/*
-- Examples usage:
--
-- for all columns which of the datatype CHARACTER and contain the string
--     '\NULL update the values to NULL  
SELECT yb_mass_column_update(
    $$"<column>" = '\NULL'$$
    , $$NULL$$
    , $$
tableowner LIKE '%'
AND schemaname LIKE '%'
AND tablename LIKE '%'
AND columnname LIKE '%'
AND datatype LIKE '%CHARACTER%'
$$
);
--
-- for all columns which are of the datatype CHARACTER and contain trailing
--     spaces update the values removing the trailing spaces
SELECT yb_mass_column_update(
    $$LENGTH("<column>") <> LENGTH(RTRIM("<column>"))$$
    , $$RTRIM("<column>")$$
    , $$
tableowner LIKE '%'
AND schemaname LIKE '%'
AND tablename LIKE '%'
AND columnname LIKE '%'
AND datatype LIKE '%CHARACTER%'
$$
);
 */
DECLARE
    v_query_cols TEXT := REPLACE(REPLACE(REPLACE($STR1$
WITH
table_cols AS (
    SELECT
        UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) as datatype
        , a.attname AS columnname
        , a.attnum AS columnordinal
        , c.relname AS tablename
        , n.nspname AS schemaname
        , CURRENT_DATABASE() AS databasename
        , '"' || databasename || '"' 
        || '."' || schemaname || '"'
        || '."' || tablename || '"' AS tablepath
        , pg_get_userbyid(c.relowner) AS tableowner
    FROM <database>.pg_catalog.pg_class AS c
        LEFT JOIN <database>.pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        JOIN <database>.pg_catalog.pg_attribute AS a
            ON a.attrelid = c.oid
    WHERE
        c.relkind = 'r'::CHAR
        AND schemaname NOT IN (
            'information_schema'
            , 'pg_catalog'
            , 'sys'
        )
        AND <filter_clause>
)
, table_info AS (SELECT tablepath, COUNT(*) AS rows FROM table_cols GROUP BY tablepath)
, enriched_table_cols AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY tablepath ORDER BY columnordinal) AS tablerow
        , rows
        , DECODE(tablerow, 1, TRUE, FALSE) AS is_first_row_in_table
        , DECODE(tablerow, rows, TRUE, FALSE) AS is_last_row_in_table
        , table_cols.*
    FROM
        table_cols JOIN table_info USING (tablepath)
    ORDER BY UPPER(databasename), UPPER(schemaname), UPPER(tablename), columnordinal
)
SELECT
    '    ' AS i1, i1 || i1 AS i2
    , DECODE(TRUE, is_first_row_in_table, 'SELECT' || CHR(13) || CHR(10) || i2, i2 || ', ')
    || REPLACE(
        $STR2$SUM(DECODE(TRUE, <update_where_clause>, 1, 0)) AS "update_<column>_ct"$STR2$
        , '<column>', columnname) || CHR(13) || CHR(10)
    || DECODE(TRUE, is_last_row_in_table, i1 || 'FROM' || CHR(13) || CHR(10) || i2 || tablepath , '')
    AS check_query
    , DECODE(TRUE, is_first_row_in_table, '', i1 || 'UNION ALL ') || 'SELECT '
    || REPLACE(REPLACE(REPLACE(
        $STR2$<columnordinal> AS columnordinal, '<column>' AS columnname, '<tablepath>' AS tablepath, "update_<column>_ct" AS ct $STR2$
        , '<column>', columnname)
        , '<columnordinal>', columnordinal::VARCHAR)
        , '<tablepath>', tablepath)
    || 'FROM check_cte' || DECODE(TRUE, is_last_row_in_table, '', CHR(13) || CHR(10))
    AS pivot_query
    , *
FROM enriched_table_cols
ORDER BY UPPER(databasename), UPPER(schemaname), UPPER(tablename), columnordinal
$STR1$
        , '<database>', CURRENT_DATABASE())
        , '<update_where_clause>', a_update_where_clause)
        , '<filter_clause>', a_column_filter_clause);
    v_query_check_template TEXT := $STR1$
WITH
check_cte AS (
    <check_cte>
)
, pivot_cte AS (
    <pivot_cte>
)
SELECT columnordinal, columnname, tablepath, ct
FROM pivot_cte
WHERE ct > 0
ORDER BY columnordinal
$STR1$;
    v_rec_cols RECORD;
    v_rec_check RECORD;
    v_query_check TEXT;
    v_query_pivot TEXT;
    v_query_update TEXT;
    v_update_ct INTEGER := 0;
BEGIN
    --RAISE INFO 'v_query_cols: %', v_query_cols; --DEBUG
    RAISE INFO '-- Running: yb_mass_column_update';
    --
    FOR v_rec_cols IN EXECUTE v_query_cols
    lOOP
        --RAISE INFO '--- %', v_rec_cols; --DEBUG
        IF v_rec_cols.is_first_row_in_table THEN
            v_query_check := '';
            v_query_pivot := '';
        END IF;
        v_query_check := v_query_check || v_rec_cols.check_query;
        v_query_pivot := v_query_pivot || v_rec_cols.pivot_query;
        IF v_rec_cols.is_last_row_in_table THEN
            v_query_check := REPLACE(REPLACE(
                v_query_check_template
                , '<check_cte>', v_query_check)
                , '<pivot_cte>', v_query_pivot);
            --RAISE INFO 'v_query_check: %', v_query_check; --DEBUG
            FOR v_rec_check IN EXECUTE v_query_check
            lOOP
                v_query_update := REPLACE(REPLACE(REPLACE(REPLACE(
                    $STR1$UPDATE <tablepath> SET "<column>" = <set_clause> WHERE <update_where_clause>$STR1$
                    , '<update_where_clause>', a_update_where_clause)
                    , '<set_clause>', a_set_clause)
                    , '<tablepath>', v_rec_check.tablepath)
                    , '<column>', v_rec_check.columnname);
                IF a_exec_updates THEN
                    v_query_update := REPLACE(
                        '/* updating <ct> row/s */ ' || v_query_update
                        , '<ct>', v_rec_check.ct::VARCHAR);
                    RAISE INFO '%', v_query_update;
                    EXECUTE v_query_update;
                ELSE
                    v_query_update := REPLACE(
                        '/* dryrun, this query will update <ct> row/s */ ' || v_query_update
                        , '<ct>', v_rec_check.ct::VARCHAR);
                    RAISE INFO '%', v_query_update;
                END IF;
                v_update_ct = v_update_ct + 1;
            END LOOP;
        END IF;
    END LOOP;
    IF a_exec_updates THEN
        RAISE INFO '-- % column/s have been updated', v_update_ct;
    ELSE
        RAISE INFO '-- % column/s need to be updated', v_update_ct;
    END IF;
    --
    RETURN TRUE;
END $$;
