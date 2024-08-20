/*
SELECT parse_objects_from_sql_p(
    'object_hold_t'
    , $$
SELECT query_id, query_text
FROM sys.log_query
WHERE
    (
        submit_time::DATE = '2024/07/29'
        AND EXTRACT(HOUR   FROM submit_time) BETWEEN 9 AND 9
        AND EXTRACT(MINUTE FROM submit_time) BETWEEN 0 AND 60
    )
    AND (type IN ('ctas', 'delete', 'insert', 'select', 'update'))
ORDER BY query_id
$$
);
*/

CREATE OR REPLACE PROCEDURE parse_objects_from_sql_p(
    i_new_table        VARCHAR(128)
    , i_sql            VARCHAR(10000)
    , i_tmp_work_table BOOLEAN DEFAULT TRUE
)
RETURNS VARCHAR(1000) AS $$
DECLARE
    RESULT TEXT;
    ch CHAR;
    i INT;
    len INT;
    in_single_line_comment BOOLEAN;
    in_multi_line_comment  BOOLEAN;
    in_single_quote_string BOOLEAN;
    in_dollar_quote_string BOOLEAN;
    dollar_quote_tag TEXT;
    c_sq VARCHAR(1) := CHR(39); -- "'"
    c_ds VARCHAR(1) := CHR(36); -- '$'
    ---
    sql_text      TEXT;
    index         INTEGER;
    temp_text     TEXT;
    match         TEXT;
    paren_matches TEXT[];
    clause        TEXT;
    rec           RECORD;
    query_rec     RECORD;
BEGIN
    --
    DROP TABLE IF EXISTS pg_temp.parse_tmp_t;
    DROP TABLE IF EXISTS pg_temp.query_w_hash_t;
    --
    CREATE TEMP TABLE parse_tmp_t (
        string_id BIGINT         NOT NULL
        , type    VARCHAR(256)   NOT NULL
        , step    VARCHAR(256)   NOT NULL
        , id      INTEGER        NOT NULL
        , string  VARCHAR(60000)
    ) 
    ON COMMIT DROP
    DISTRIBUTE ON (string_id);
    --
    EXECUTE REPLACE($STR$
CREATE TEMP TABLE query_w_hash_t AS SELECT query_id, query_text, HASH(query_text) AS query_hash
FROM (<i_sql>) AS foo
DISTRIBUTE ON (query_id)
    $STR$, '<i_sql>', i_sql);
    --
    FOR query_rec IN
        SELECT query_id, query_text
        FROM query_w_hash_t
        WHERE query_id IN (
            SELECT MIN(query_id)
            FROM query_w_hash_t
            GROUP BY query_hash
        )
        ORDER BY query_id
    LOOP
        sql_text    := query_rec.query_text;
        index       := 1;
        --
        -- Insert the initial original string
        INSERT INTO parse_tmp_t (string_id, type, step, id, string)
        VALUES (query_rec.query_id, 'paren_parse', 'original', 0, sql_text);
        ---
        ---
        ---
        result := '';
        i := 1;
        len := LENGTH(sql_text);
        in_single_line_comment := FALSE;
        in_multi_line_comment  := FALSE;
        in_single_quote_string := FALSE;
        in_dollar_quote_string := FALSE;
        dollar_quote_tag := '';
        WHILE i <= len LOOP
            ch := SUBSTRING(sql_text FROM i FOR 1);
            --
            IF NOT in_single_quote_string AND NOT in_dollar_quote_string THEN
                -- Handle single-line comments
                IF NOT in_multi_line_comment AND ch = '-' AND SUBSTRING(sql_text FROM i FOR 2) = '--' THEN
                    in_single_line_comment := TRUE;
                ELSIF in_single_line_comment AND ch = E'\n' THEN
                    in_single_line_comment := FALSE;
                    ch := '';
                -- Handle multi-line comments
                ELSIF NOT in_single_line_comment AND ch = '/' AND SUBSTRING(sql_text FROM i FOR 2) = '/*' THEN
                    in_multi_line_comment := TRUE;
                ELSIF in_multi_line_comment AND ch = '*' AND SUBSTRING(sql_text FROM i FOR 2) = '*/' THEN
                    in_multi_line_comment := FALSE;
                    ch := '';
                    i := i + 1;
                END IF;
            END IF;
            --
            -- Handle single-quoted strings
            IF NOT in_single_line_comment AND NOT in_multi_line_comment AND NOT in_dollar_quote_string THEN
                IF ch = c_sq THEN
                    in_single_quote_string := NOT in_single_quote_string;
                END IF;
            END IF;
            --
            -- Handle dollar-quoted strings
            IF NOT in_single_line_comment AND NOT in_multi_line_comment AND NOT in_single_quote_string THEN
                IF ch = c_ds THEN
                    IF in_dollar_quote_string THEN
                        IF SUBSTRING(sql_text FROM i FOR LENGTH(dollar_quote_tag)) = dollar_quote_tag THEN
                            in_dollar_quote_string := FALSE;
                            dollar_quote_tag := '';
                            i := i + LENGTH(dollar_quote_tag) - 1;
                        END IF;
                    ELSE
                        dollar_quote_tag := SUBSTRING(sql_text FROM i FOR POSITION(c_ds IN SUBSTRING(sql_text FROM i + 1)) + 1);
                        in_dollar_quote_string := TRUE;
                        i := i + LENGTH(dollar_quote_tag) - 1;
                    END IF;
                END IF;
            END IF;
            --
            -- Append character to result if not in a comment
            RAISE INFO '%', ch;
            IF NOT in_single_line_comment AND NOT in_multi_line_comment THEN
                result := result || ch;
            END IF;
            --
            i := i + 1;
        END LOOP;
        sql_text := RESULT;
        --
        -- 
        --
        LOOP
            -- Extract the last inner parentheses content
            match := REGEXP_REPLACE(sql_text, '.*\(([^()]*)\).*', '\1', 'i');
            --
            -- Exit the loop if no more inner parentheses are found
            IF match IS NULL OR match = sql_text THEN
                EXIT;
            END IF;
            --
            -- Insert the match into the temporary table
            INSERT INTO parse_tmp_t (string_id, type, step, id, string)
            VALUES (query_rec.query_id, 'paren_parse', 'match', index, match);
            --
            -- Replace the last inner parentheses content with [pX]
            sql_text := REGEXP_REPLACE(sql_text, '\(([^()]*)\)(?!.*\(([^()]*)\))', '[p' || index || ']', 'i');
            index := index + 1;
        END LOOP;
        --
        -- Insert the final replaced string
        INSERT INTO parse_tmp_t (string_id, type, step, id, string)
        VALUES (query_rec.query_id, 'paren_parse', 'final', index, sql_text);
        --
        --
        --
    END LOOP;
    --
    --
    --
    -- parse WITH clause from paren matches
    i := 1;
    FOR rec IN
        SELECT string_id, id, string
        FROM parse_tmp_t
        WHERE
            type = 'paren_parse'
            AND string ~* 'WITH\s+[^\s]+\s+AS'
            AND id > 0
        ORDER BY string_id
    LOOP
        -- Extract the match (the WITH clause name)
        --
        -- There seems to be a bug with REGEX_REPLACE, it always prints past the end of the string
        --     adding the trailing unmatched string, this required the SPLIT_PART
        match := SPLIT_PART(REGEXP_REPLACE(rec.string, '.*(WITH\s+[^\s]+\s+AS\s*\[p\d+\]\s*(\s*,\s*[^\s]+\s+AS\s*\[p\d+\])*)', '\1|=*<>*=|', 'i'), '|=*<>*=|', 1);
        --
        INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
        VALUES (rec.string_id, 'with_clause', 'none', i, match);
        i := i + 1;
        --
        index := 1;
        LOOP
            clause := REGEXP_REPLACE(match, '(.*)(\s*,\s*([^\s]+)\s+AS\s*\[p\d+\].*)', '\1|=*<>*=|\3', 'i');
            --
            -- Insert the extracted FROM clause into the temporary table
            IF clause LIKE '%|=*<>*=|%'
            THEN
                match := SPLIT_PART(clause, '|=*<>*=|', 1);
                INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
                VALUES (rec.string_id, 'cte_name', 'none', index, SPLIT_PART(clause, '|=*<>*=|', 2));
            ELSE
                EXIT;
            END IF;
            index := index + 1;
        END LOOP;
        --
        match := REGEXP_REPLACE(match, '(.*)(WITH\s*([^\s]+)\s+AS\s*\[p\d+\].*)', '\3', 'i');
        RAISE INFO '----5----: %', match;
        INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
        VALUES (rec.string_id, 'cte_name', 'none', index, match);
        --
    END LOOP;
    --
    --
    --
    FOR rec IN
        SELECT string_id, id, string
        FROM parse_tmp_t
        WHERE
            type = 'paren_parse'
            AND id > 0
        ORDER BY string_id
    LOOP
        clause := REGEXP_REPLACE(rec.string, '.*\sFROM\s*([a-zA-Z\_\"].*?)(\s(WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|$).*)?', '\1', 'i');
        --
        -- Insert the extracted FROM clause into the temporary table
        IF clause IS NOT NULL AND clause != rec.string
        THEN
            INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
            VALUES (rec.string_id, 'from_clause', 'none', rec.id, clause);
        END IF;
    END LOOP;
    --
    --
    --
    FOR rec IN
        SELECT string_id, id, string
        FROM parse_tmp_t
        WHERE type = 'from_clause'
        ORDER BY string_id
    LOOP
        RAISE INFO '-------------2--------------';
        clause := rec.string;
        RAISE INFO 'clause(%)', clause;
        --
        -- Loop to extract tables/views after each 'JOIN'
        LOOP
            RAISE INFO '-------------3--------------';
            match := REGEXP_REPLACE(clause, '.*\s+JOIN\s+([^\s]+).*', '\1', 'i');
            RAISE INFO 'match(%)', match;
            --
            -- Exit the loop if no more 'JOIN' keywords are found
            --IF NOT FOUND THEN
            IF match IS NULL OR match = clause THEN
                EXIT;
            END IF;
            --
            -- Insert the extracted table/view into the temporary table
            INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
            VALUES (rec.string_id, 'table_view', 'join', rec.id, match);
            --
            -- Remove the processed 'JOIN' table/view from from_clause
            clause := REGEXP_REPLACE(clause, '(.*)(\sJOIN\s+[^\s]+.*)', '\1', 'i');
            RAISE INFO 'clause(%)', clause;
        END LOOP;
        --
        -- Extract tables/views in the classic comma-separated format
        LOOP
            RAISE INFO '-------------4--------------';
            RAISE INFO 'clause(%)', clause;
            match := REGEXP_REPLACE(clause, '(.*,)?\s*([^\s]+).*', '\2', 'i');
            --SELECT REGEXP_REPLACE(c, '(.*,)?\s*([^\s]+).*', '\2', 'i')
            --INTO match
            --FROM (SELECT clause AS c) AS t
            --WHERE c ~* '\s*([^\s]+)';
            RAISE INFO 'match(%)', match;
            --
            -- Exit the loop if no more tables/views are found
            --IF NOT FOUND THEN
            --IF match IS NULL OR match = clause THEN
            IF match IS NULL OR TRIM(MATCH) = '' THEN
                EXIT;
            END IF;
            --
            -- Insert the extracted table/view into the temporary table
            INSERT INTO parse_tmp_t (string_id, type, step, id, string) 
            VALUES (rec.string_id, 'table_view', 'comma', rec.id, match);
            --
            -- Remove the processed table/view from from_clause
            clause := RTRIM(REGEXP_REPLACE(clause, '(.*,)?\s*([^,\s]+).*', '\1', 'i'), ',');
        END LOOP;
        match := NULL;
    END LOOP;
    --
    --
    --
    -- collect ctas, insert, update, delete table names
    INSERT INTO parse_tmp_t (string_id, type, step, id, string)
    WITH
    ctas_tbls AS (
        SELECT
            string_id, type, step, id, string
            , SPLIT_PART(TRIM(REGEXP_REPLACE(
                string
                , '(CREATE\s+TABLE\s+([^\s]+)\s+AS[\s\(])'
                , '\2|=*<>*=|\1', 'i'
            )), '|=*<>*=|', 1) AS table_name
            , 'ctas' AS statement_type
        FROM parse_tmp_t
        WHERE
            string ~* '(CREATE\s+TABLE\s+([^\s]+)\s+AS[\s\(])'
            AND type = 'paren_parse'
    )
    , modify_tbls AS (
        SELECT
            string_id, type, step, id, string
            , REGEXP_REPLACE(
                string
                , '.*?(INSERT\s+INTO\s+|UPDATE\s+|DELETE\s+FROM\s+)([^\s\(]+)'
                , '\2|=*<>*=|\1|=*<>*=|', 'i'
            ) AS parse
            , TRIM(SPLIT_PART(parse, '|=*<>*=|', 1)) AS table_name
            , SPLIT_PART(parse, '|=*<>*=|', 2) AS clause2
            , LOWER(SPLIT_PART(REGEXP_REPLACE(
                clause2
                , '([^\s\(]+)'
                , '\1|=*<>*=|', 'i'
            ), '|=*<>*=|', 1)) AS statement_type
        FROM parse_tmp_t
        WHERE
            string ~* '(INSERT\s+INTO\s+|UPDATE\s+|DELETE\s+FROM\s+)([^\s\(]+)'
            AND type = 'paren_parse' AND step = 'final'
    )
    , tbls AS (
        (
            SELECT string_id, type, step, id, string, table_name, statement_type FROM ctas_tbls
        )
        UNION ALL
        (
            SELECT string_id, type, step, id, string, table_name, statement_type FROM modify_tbls
        )
    )
    SELECT string_id, 'table', statement_type, id, table_name FROM tbls;
    --
    --
    --
    IF NOT i_tmp_work_table
    THEN
        temp_text := TO_CHAR(NOW(), 'parse_YYYYMMDDHH24MISS_t');
        EXECUTE REPLACE(
            'CREATE TABLE <table_name> AS SELECT * FROM parse_tmp_t DISTRIBUTE ON (string_id)'
            , '<table_name>', temp_text
        );
    ELSE
        temp_text := NULL;
    END IF;
    --
    EXECUTE REPLACE($sql$
CREATE TABLE <table_name> AS
SELECT DISTINCT
    p.string_id AS query_id
    , p.string::VARCHAR(256) AS object
    , p.type
    , h.query_hash
FROM
    parse_tmp_t AS p
    JOIN query_w_hash_t AS h
        ON p.string_id = h.query_id
WHERE
    type IN ('table_view', 'table', 'cte_name')
    AND object NOT LIKE '[%]'
DISTRIBUTE ON (query_id)$sql$
        , '<table_name>', i_new_table
    );
    --
    EXECUTE REPLACE($sql$
INSERT INTO <table_name>
SELECT
    h.query_id
    , o.object
    , o.type
    , o.query_hash
FROM
    <table_name> AS o
    JOIN query_w_hash_t AS h
        ON o.query_hash = h.query_hash
WHERE
    h.query_id NOT IN (SELECT DISTINCT query_id FROM <table_name>)$sql$
        , '<table_name>', i_new_table
    );
    --
    DROP TABLE IF EXISTS pg_temp.parse_tmp_t;
    DROP TABLE IF EXISTS pg_temp.query_w_hash_t;
    --
    RETURN ('Created object table: ' || i_new_table
        || DECODE(i_tmp_work_table, FALSE, ', Created parse table: ' || temp_text, ''))::VARCHAR(1000);
    --RETURN ('Created object table: ')::VARCHAR(1000);
END;
$$ LANGUAGE plpgsql;
