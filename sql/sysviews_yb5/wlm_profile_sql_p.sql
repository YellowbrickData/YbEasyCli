/* ****************************************************************************
** wlm_profile_sql_p()
**
** Returns SQL(PLPGSQL script) to create a WLM profile.
**
** Usage:
**   See COMMENT ON FUNCTION text further below.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2023.03.19 - Created.
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS wlm_profile_sql_t CASCADE
;

CREATE TABLE wlm_profile_sql_t
   (
      code VARCHAR (61000)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE wlm_profile_sql_p( _profile_name VARCHAR DEFAULT '' )
RETURNS SETOF wlm_profile_sql_t 
   LANGUAGE 'plpgsql'
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql          TEXT    := '';
   _pools_detail TEXT    := '';
   _pools        TEXT    := '';
   _pools_js     TEXT    := 'profile = [];';
   _rec          RECORD;
   _code_line    INT     := 1;
   
   _fn_name   VARCHAR(256) := 'wlm_profile_sql_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN
   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   DROP TABLE IF EXISTS profile_code;
   CREATE TEMP TABLE profile_code (code VARCHAR (61000));

   ---------------------------------------------------------
   -- BUILDING beginning of PLPGSQL script
   ---------------------------------------------------------
   INSERT INTO profile_code VALUES (REPLACE(REPLACE('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || $PLPGSQL$
DO $code$
DECLARE
    ---------------------------------------------------------------------------------
    -- profile   : {the_profile_name}
    -- created at: {now}
    -- notes:      - this SQL script creates the WLM profile
    --             - modify the following 3 arguments as needed
    ---------------------------------------------------------------------------------
    -- name of profile to create
    v_profile_name               VARCHAR := $str${the_profile_name}$str$;
    --
    -- flag when set that drops the old named profile before creating the new profile
    v_drop_old_profile_if_exists BOOLEAN := FALSE;
    --
    -- flag when set that will activate the newly created profile
    v_activate_new_profile       BOOLEAN := FALSE;
    ---------------------------------------------------------------------------------
    --
    v_wlm_rec           RECORD;
    v_snippet_rec       RECORD;
    v_wlm_entry         INTEGER := 0;
    v_code              VARCHAR(61000);
    v_pools_js          VARCHAR(61000);
	v_is_active_profile BOOLEAN;
BEGIN
    DROP TABLE IF EXISTS wlm_code;
    CREATE TEMP TABLE wlm_code (wlm_entry INTEGER, code VARCHAR(61000));

    DROP TABLE IF EXISTS wlm_snippet;
    CREATE TEMP TABLE wlm_snippet (alias VARCHAR(128), sub_order INTEGER, code VARCHAR(61000));

    SELECT name = v_profile_name INTO v_is_active_profile FROM sys.wlm_active_profile WHERE active;

    INSERT INTO wlm_snippet (alias, sub_order, code)
    VALUES
        ('{profile_name}'        , 100, v_profile_name)
        , ('{my_include_example}', 1, $JS$example = 'example string';$JS$)
    ;
$PLPGSQL$
        , '{the_profile_name}', _profile_name)
        , '{now}'             , NOW()::VARCHAR));

   ---------------------------------------------------------
   -- profile query
   ---------------------------------------------------------
   _sql := REPLACE($str$
WITH
p AS (
   SELECT
       name, default_pool, updated, NVL(activated, '1900-01-01'::DATE) AS activated
   FROM sys.wlm_profile
)
, m AS (
   SELECT name, updated, MAX(activated) AS activated
   FROM
       (SELECT name, MAX(updated) AS updated FROM p GROUP BY 1) AS max_u
       JOIN p USING (name, updated)
   GROUP BY 1, 2
)
SELECT
    name AS profile_name
    , default_pool
FROM p JOIN m USING (name, updated, activated)
WHERE
    name = '{profile_name}'
$str$, '{profile_name}', _profile_name);

   --RAISE INFO '_sql: %', _sql; --DEGUG
   EXECUTE _sql INTO _rec;

   ---------------------------------------------------------
   -- BUILDING profile into PLPGSQL script
   ---------------------------------------------------------
   _code_line := _code_line + 1;
   INSERT INTO profile_code VALUES (REPLACE(REPLACE('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || $PLPGSQL$
----------------------- Start Profile ------------------------------
-- Profile: {profile_name} 
--------------------------------------------------------------------
    IF v_drop_old_profile_if_exists
    THEN
        v_wlm_entry := v_wlm_entry + 1;
        INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_PROFILE$
            DROP WLM PROFILE IF EXISTS "{profile_name}";
$WLM_PROFILE$);
    END IF;

    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_PROFILE$
    CREATE WLM PROFILE "{profile_name}" ( DEFAULT_POOL "{default_pool}" );
$WLM_PROFILE$);
----------------------- End Profile   ------------------------------
$PLPGSQL$
         , '{default_pool}', _rec.default_pool)
         , _profile_name, '{profile_name}'));

   ---------------------------------------------------------
   -- pools query
   ---------------------------------------------------------
   _sql := REPLACE($str$
WITH
rp AS (
   SELECT
       name, updated, NVL(activated, '1900-01-01'::DATE) AS activated
       , maximum_wait_limit, maximum_row_limit, maximum_exec_time_limit
       , next_memory_queue, next_exec_time_limit_queue
   FROM sys.wlm_resource_pool
)
, m AS (
   SELECT name, updated, MAX(activated) AS activated
   FROM
       (SELECT name, MAX(updated) AS updated FROM rp GROUP BY 1) AS max_u
       JOIN rp USING (name, updated)
   GROUP BY 1, 2
)
, data AS (
    SELECT
        name
        , profile_name
        , DECODE(p.memory_requested, 'remainder', 'NULL', '''' || RTRIM(p.memory_requested, '%') || '''') AS requested_memory
        , DECODE(p.temp_space_requested, 'remainder', 'NULL', '''' || RTRIM(p.temp_space_requested, '%') || '''') AS max_spill_pct
        , p.max_concurrency
        , p.min_concurrency
        , p.queue_size
        , NVL(rp.maximum_wait_limit::VARCHAR, 'NULL')         AS maximum_wait_limit
        , NVL(rp.maximum_row_limit::VARCHAR, 'NULL')          AS maximum_row_limit
        , NVL(rp.maximum_exec_time_limit::VARCHAR, 'NULL')    AS maximum_exec_time
        , NVL(rp.next_memory_queue::VARCHAR, 'NULL')          AS next_memory_queue
        , NVL(rp.next_exec_time_limit_queue::VARCHAR, 'NULL') AS next_exec_time_limit_queue
        , p.max_concurrency AS max_slots, p.min_concurrency AS min_slots, p.queue_size
        , memory_per_query_bytes / (1000.0 ^ 3) AS mem_max_slots_query_GB
        , mem_max_slots_query_GB * max_slots AS mem_worker_GB
        , (mem_max_slots_query_GB * max_slots) / min_slots AS mem_min_slots_query_GB
        , temp_space_per_query_bytes / (1000.0 ^ 3) AS temp_max_slots_query_GB
        , temp_max_slots_query_GB * max_slots AS temp_worker_GB
        , (temp_max_slots_query_GB * max_slots) / min_slots AS temp_min_slots_query_GB
    FROM
        rp
        JOIN m USING (name, updated, activated)
        JOIN sys.wlm_pending_pool AS p USING (name)
    WHERE
        profile_name = '{profile_name}'
)
, data_sum AS (
    SELECT
        SUM(mem_worker_GB) AS total_mem_worker_GB
        , SUM(temp_worker_GB) AS total_temp_worker_GB
        , MAX(LENGTH(name)) AS max_len_name
    FROM data
)
SELECT
    *
    -- Pools JS
    , RPAD(REPLACE($$profile['{name}']$$, '{name}', name), max_len_name + 12, ' ')
    || '= { slots: '  || LPAD(FORMAT(FLOOR(max_slots), 0), 3, ' ') || ', '
    || 'slotsMin: '   || LPAD(FORMAT(FLOOR(min_slots), 0), 3, ' ') || ', '
    || 'memMB: '      || LPAD(FORMAT(FLOOR(mem_worker_GB  * 1000), 0), 8, ' ') || ', '
    || 'tempMB: '     || LPAD(FORMAT(FLOOR(temp_worker_GB * 1000), 0), 8, ' ') || ', '
    || 'memSlotMB: '  || LPAD(FORMAT(FLOOR((mem_worker_GB  * 1000) / max_slots), 0), 8, ' ') || ', '
    || 'tempSlotMB: ' || LPAD(FORMAT(FLOOR((temp_worker_GB * 1000) / max_slots), 0), 8, ' ') || ' };' AS pools_js
    -- Pools Description
    , CHR(10) || '-- ' || RPAD(name, (max_len_name + 5), ' ')
    || LPAD(FORMAT(max_slots, '0'), 3, ' ') || ' / '|| RPAD(FORMAT(min_slots, '0'), 3, ' ')
    || LPAD(FORMAT(ROUND(mem_worker_GB, 1), '0.0'), 7, ' ') || '   '
    || LPAD(FORMAT(ROUND(mem_max_slots_query_GB, 1), '0.0'), 6, ' ')
    || DECODE(mem_max_slots_query_GB, mem_min_slots_query_GB
        , '        '
        , '->' || RPAD(FORMAT(ROUND(mem_min_slots_query_GB, 1), '0.0'), 6, ' '))
    || LPAD(ROUND((100 * mem_worker_GB) / total_mem_worker_GB), 4) || '   '
    || LPAD(FORMAT(ROUND(temp_worker_GB, 1), '0.0'), 7, ' ') || '   '
    || LPAD(FORMAT(ROUND(temp_max_slots_query_GB, 1), '0.0'), 6, ' ')
    || DECODE(temp_max_slots_query_GB, temp_min_slots_query_GB
        , '        '
        , '->' || RPAD(FORMAT(ROUND(temp_min_slots_query_GB, 1), '0.0'), 6, ' '))
    || LPAD(ROUND((100 * temp_worker_GB) / total_temp_worker_GB), 4) AS pool_details
FROM 
    DATA CROSS JOIN data_sum
ORDER BY profile_name, name
$str$, '{profile_name}', _profile_name);

   ---------------------------------------------------------
   -- BUILDING pools into PLPGSQL script
   ---------------------------------------------------------
   --RAISE INFO '_sql: %', _sql; --DEGUG
   FOR _rec IN EXECUTE( _sql )
   LOOP
      _pools_detail := _pools_detail || _rec.pool_details;

      IF _rec.name != 'system'
      THEN
         _pools_js := _pools_js || CHR(10) || _rec.pools_js;
         --
         _pools := _pools || CHR(10) ||
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($PLPGSQL$
----------------------- Start Pool ---------------------------------
-- Pool: {name} 
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_POOL$
    DROP WLM RESOURCE POOL IF EXISTS "{name}";
    CREATE WLM RESOURCE POOL "{name}"
    (
        PROFILE                      "{profile_name}"
        , MAX_CONCURRENCY            {max_concurrency}
        , MIN_CONCURRENCY            {min_concurrency}
        , QUEUE_SIZE                 {queue_size}
        , MAX_SPILL_PCT              {max_spill_pct}
        , REQUESTED_MEMORY           {requested_memory}
        , MAXIMUM_WAIT_LIMIT         {maximum_wait_limit}
        , MAXIMUM_ROW_LIMIT          {maximum_row_limit}
        , MAXIMUM_EXEC_TIME          {maximum_exec_time}
        , NEXT_MEMORY_QUEUE          {next_memory_queue}
        , NEXT_EXEC_TIME_LIMIT_QUEUE {next_exec_time_limit_queue}
    );
$WLM_POOL$);
----------------------- End Pool   ---------------------------------
$PLPGSQL$
            , '{name}', _rec.name), '{max_concurrency}', _rec.max_concurrency)
            , '{min_concurrency}', _rec.min_concurrency), '{queue_size}', _rec.queue_size)
            , '{maximum_wait_limit}', _rec.maximum_wait_limit), '{maximum_row_limit}', _rec.maximum_row_limit)
            , '{maximum_exec_time}', _rec.maximum_exec_time), '{max_spill_pct}', _rec.max_spill_pct)
            , '{requested_memory}', _rec.requested_memory), '{next_memory_queue}', _rec.next_memory_queue)
            , '{next_exec_time_limit_queue}', _rec.next_exec_time_limit_queue)
            , _profile_name, '{profile_name}');
      END IF;
   END LOOP;

   _pools_detail := REPLACE(
          CHR(10) || '-- ' || RPAD('POOL', _rec.max_len_name, ' ') || '       SLOTS   MEMORY_GB                      SPILL_SPACE_GB'
       || CHR(10) || '-- ' || RPAD('    ', _rec.max_len_name, ' ') || '               WORKER    QUERY     PERCENT    WORKER    QUERY     PERCENT'
       || CHR(10) || '-- ' || RPAD('----', _rec.max_len_name, '-') || '-------------------------------------------------------------------------'
       || _pools_detail
       || CHR(10) || '-- ' || RPAD('----', _rec.max_len_name, '-') || '-------------------------------------------------------------------------'
       || CHR(10) || '-- ' || RPAD('    ', _rec.max_len_name, ' ') || LPAD(FORMAT(ROUND(_rec.total_mem_worker_GB, 1), '0.0'), 21) || LPAD(FORMAT(ROUND(_rec.total_temp_worker_GB, 1), '0.0'), 31)
       , _profile_name, '{profile_name}');

   _pools_js := REPLACE(CHR(10) || CHR(10) || '    v_pools_js := $JS$' || _pools_js || '$JS$;' || $PGPSQL$
    INSERT INTO wlm_snippet (alias, sub_order, code)
    VALUES ('{pools_js}', 1, v_pools_js);$PGPSQL$
       , _profile_name, '{profile_name}');

   _code_line := _code_line + 1;
   INSERT INTO profile_code VALUES ('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || _pools_detail);
   _code_line := _code_line + 1;
   INSERT INTO profile_code VALUES ('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || _pools_js);
   _code_line := _code_line + 1;
   INSERT INTO profile_code VALUES ('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || _pools);

   ---------------------------------------------------------
   -- rules query
   ---------------------------------------------------------
   _sql := REPLACE($str$
WITH
rule AS (
   SELECT
      enabled
      , wpr.superuser
      , wpr.rule_type
      , wpr."order"
      , wpr.expression
      , wpr.profile_name
      , wpr.rule_name
      , wpr.enabled
      , DECODE(wpr.rule_type, 'submit', 10, 'assemble', 20, 'compile', 30, 'run', 40, 'runtime', 40, 'restart_for_error', 50, 'restart_for_user', 60, 'completion', 70
         , 80) AS rule_type_order
   FROM
      sys.wlm_pending_rule AS wpr
   WHERE
      wpr.profile_name = '{profile_name}'
)
SELECT
   TRIM(TO_CHAR(ROW_NUMBER() OVER(ORDER BY superuser, rule_type_order, "order", profile_name), '000')) AS rule_ct
   , *
FROM rule
ORDER BY rule_ct
$str$, '{profile_name}', _profile_name);

   ---------------------------------------------------------
   -- BUILDING rules into PLPGSQL script
   ---------------------------------------------------------
   --RAISE INFO '_sql: %', _sql; --DEGUG
   FOR _rec IN EXECUTE( _sql )
   LOOP
      _code_line := _code_line + 1;
      INSERT INTO profile_code VALUES (REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || $PLPGSQL$
----------------------- Start Rule {rule_ct} -----------------------
-- Superuser: {superuser}, Type: {rule_type}, Order: {order} 
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_RULE$
    DROP WLM RULE IF EXISTS "{rule_name}";
    CREATE WLM RULE "{rule_name}"
    (
        PROFILE      "{profile_name}"
        , TYPE       {rule_type}
        , RULE_ORDER {order}
        , ENABLED    {enabled}
        , SUPERUSER  {superuser}
        , JAVASCRIPT
/* start JS ----------------------------------------------- */
$JS${expression}$JS$
/* end JS ------------------------------------------------- */
    );
$WLM_RULE$);
----------------------- End Rule {rule_ct}   -----------------------
$PLPGSQL$
         , '{rule_name}', _rec.rule_name), '{rule_type}', _rec.rule_type)
         , '{order}', _rec."order"), '{enabled}', _rec.enabled), '{superuser}', _rec.superuser)
         , '{expression}', _rec.expression), '{rule_ct}', _rec.rule_ct)
         , _profile_name, '{profile_name}'));
   END LOOP;

   ---------------------------------------------------------
   -- BUILDING end of PLPGSQL script
   ---------------------------------------------------------
   _code_line := _code_line + 1;
   INSERT INTO profile_code VALUES ('--' || LPAD(CAST(_code_line AS TEXT), 5, '0') || $PLPGSQL$
    ---------------------------------------------------------
    -- BUILDING profile
    ---------------------------------------------------------
    FOR v_wlm_rec IN SELECT code FROM wlm_code ORDER BY wlm_entry
    LOOP
        v_code := v_wlm_rec.code;

	    FOR v_snippet_rec IN SELECT alias, code FROM wlm_snippet ORDER BY sub_order
	    LOOP
		    v_code := REPLACE(v_code, v_snippet_rec.alias, v_snippet_rec.code);
	    END LOOP;

        --RAISE INFO '%', v_code; --DEBUG
	    EXECUTE v_code;
    END LOOP;

    COMMIT;

    IF v_activate_new_profile OR v_is_active_profile
    THEN
        EXECUTE FORMAT('ALTER WLM PROFILE "%s" ACTIVATE 10 WITHOUT CANCEL', v_profile_name);
    END IF;

END $code$;
$PLPGSQL$);


   RETURN QUERY EXECUTE $$SELECT code FROM profile_code ORDER BY code$$;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

END;   
$proc$ 
;


COMMENT ON FUNCTION wlm_profile_sql_p( _profile_name VARCHAR ) IS 
$str$Description:
Returns SQL(PLPGSQL script) to create a WLM profile.
  
Examples:
  SELECT * FROM wlm_profile_sql_p(); 
  SELECT * FROM wlm_profile_sql_p( 'my_profile' );  

Arguments: 
. _profile_name - VARCHAR - profile to report on

Version:
. 2023.03.19 - Yellowbrick Technical Support 
$str$
;