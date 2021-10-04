CREATE OR REPLACE PROCEDURE <stored_proc_name>(_limit BIGINT DEFAULT <limit_default>)
    RETURNS SETOF <stored_proc_name>_t
    LANGUAGE 'plpgsql' 
    VOLATILE
    SECURITY DEFINER
AS $CODE$
DECLARE
    --
    _sql          TEXT := '';
    _limit_clause TEXT := DECODE(0, _limit, '', ' LIMIT ' || _limit::VARCHAR);
    _rec        <stored_proc_name>_t%rowtype;
    --
    _fn_name   VARCHAR(256) := '<stored_proc_name>';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags      VARCHAR(256) := DECODE(_prev_tags, '', '', _prev_tags || ':') || _fn_name;
BEGIN
    --
    /* Txn read_only to protect against potential SQL injection attack overwrites
    */
    --SET TRANSACTION READ ONLY;
    _sql := REPLACE($STR$ SET ybd_query_tags  TO '<tags>' $STR$, '<tags>', _tags);
    EXECUTE _sql ;
    --
    _sql := REPLACE($STR$WITH
foo AS (
<query>
<limit>
)
SELECT<select_clause>
FROM
    foo$STR$
    , '<limit>', _limit_clause);
    --
    -- RAISE INFO '_sql is: %', _sql ; --DEBUG
    FOR _rec IN EXECUTE( _sql ) 
    LOOP
        -- RAISE INFO '_rec: %', _rec ; --DEBUG
        RETURN NEXT _rec;
    END LOOP;
    --
    /* Reset ybd_query_tags back to its previous value
    */
    _sql := REPLACE($STR$ SET ybd_query_tags  TO '<tags>' $STR$, '<tags>', _prev_tags);
    EXECUTE _sql ;
    --
END;$CODE$;