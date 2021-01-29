CREATE OR REPLACE PROCEDURE yb_check_db_views_p(a_filter VARCHAR DEFAULT 'TRUE')
    RETURNS BOOLEAN
-- Note: sys.view did not return all broken views, using pg_class
    LANGUAGE plpgsql
AS $$
DECLARE
    v_rec record;
    v_query_views TEXT := REPLACE($STR1$
SELECT
    c.relname AS viewname
    , n.nspname AS schemaname
    , pg_get_userbyid(c.relowner) AS ownername
    , '"' || CURRENT_DATABASE() || '"."' || schemaname || '"."' || viewname || '"' AS view_path_quoted
    , CURRENT_DATABASE() || '.' || schemaname || '.' || viewname AS view_path
FROM
    pg_catalog.pg_class AS c
    LEFT JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
WHERE
    c.relkind IN ('v')
    AND <filter>
    AND schemaname NOT IN ('information_schema', 'pg_catalog', 'sys')
ORDER BY LOWER(schemaname), LOWER(viewname)
$STR1$
        , '<filter>', a_filter);
BEGIN
    --SELECT SPLIT_PART(SPLIT_PART(VERSION(), ' ', 4), '.', 1)::INT >= 4 INTO v_is_yb_4_or_higher;
    --RAISE INFO 'View Query: %', v_query_views; --DEBUG
    --
    FOR v_rec IN EXECUTE v_query_views
    LOOP
        BEGIN
            EXECUTE 'SELECT 1 FROM ' || v_rec.view_path_quoted || ' WHERE FALSE';
            --RAISE INFO 'GOOD VIEW: %', v_rec.viewname; --DEBUG
        EXCEPTION
            WHEN OTHERS THEN
                --RAISE INFO 'ERROR --> % %', SQLERRM, SQLSTATE; --DEBUG
                IF STRPOS(SQLERRM, 'does not exist') > 0 THEN
                    RAISE INFO '%', v_rec.view_path;
                END IF;
        END;
    END LOOP;
    --
    RETURN TRUE;
END $$;