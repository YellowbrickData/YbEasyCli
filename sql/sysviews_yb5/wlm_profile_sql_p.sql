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
      code VARCHAR (60000)
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

   _sql         TEXT    := '';
   _rec         RECORD;
   
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
   CREATE TEMP TABLE profile_code (code VARCHAR (60000));

   ---------------------------------------------------------
   -- BUILDING beginning of PLPGSQL script
   ---------------------------------------------------------
   INSERT INTO profile_code VALUES (REPLACE($PLPGSQL$
---------------- WLM Profile SQL Creation Script -------------------
-- Profile: {the_profile_name}
--------------------------------------------------------------------
-- **** Running this script overwrites the existing WLM Profile ****
--------------------------------------------------------------------
DO $code$
DECLARE
    v_wlm_rec RECORD;
    v_js_rec RECORD;
    v_wlm_entry INTEGER := 0;
    v_code VARCHAR(60000);
BEGIN
    DROP TABLE IF EXISTS wlm_code;
    CREATE TEMP TABLE wlm_code (wlm_entry INTEGER, code VARCHAR(60000));

    DROP TABLE IF EXISTS wlm_snippet;
    CREATE TEMP TABLE wlm_snippet (alias VARCHAR(128), sub_order INTEGER, code VARCHAR(60000));

    INSERT INTO wlm_snippet (alias, sub_order, code)
    VALUES
        ('{profile_name}', 1, $str${the_profile_name}$str$)
        , ('{my_include_example}', 1, $JS$example = 'example string';$JS$)
    ;
$PLPGSQL$
        , '{the_profile_name}', _profile_name));

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
      UPDATE profile_code SET code = code || 
         REPLACE(REPLACE($PLPGSQL$
----------------------- Start Profile ------------------------------
-- Profile: {profile_name} 
--------------------------------------------------------------------
    v_wlm_entry := v_wlm_entry + 1;
    INSERT INTO wlm_code VALUES (v_wlm_entry, $WLM_PROFILE$
    DROP WLM PROFILE IF EXISTS "{profile_name}";
    CREATE WLM PROFILE "{profile_name}" ( DEFAULT_POOL "{default_pool}" );
$WLM_PROFILE$);
----------------------- End Profile   ------------------------------
$PLPGSQL$
         , '{default_pool}', _rec.default_pool)
         , _profile_name, '{profile_name}');

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
SELECT
    name
    , profile_name
    , DECODE(p.memory_requested, 'remainder', 'NULL', '''' || RTRIM(p.memory_requested, '%') || '''') AS requested_memory
    --, memory_per_query_bytes INT
    , DECODE(p.temp_space_requested, 'remainder', 'NULL', '''' || RTRIM(p.temp_space_requested, '%') || '''') AS max_spill_pct
    --, temp_space_per_query_bytes INT
    , p.max_concurrency
    , p.min_concurrency
    , p.queue_size
    , NVL(rp.maximum_wait_limit::VARCHAR, 'NULL')         AS maximum_wait_limit
    , NVL(rp.maximum_row_limit::VARCHAR, 'NULL')          AS maximum_row_limit
    , NVL(rp.maximum_exec_time_limit::VARCHAR, 'NULL')    AS maximum_exec_time
    , NVL(rp.next_memory_queue::VARCHAR, 'NULL')          AS next_memory_queue
    , NVL(rp.next_exec_time_limit_queue::VARCHAR, 'NULL') AS next_exec_time_limit_queue
FROM 
    rp
    JOIN m USING (name, updated, activated)
    JOIN sys.wlm_pending_pool AS p USING (name)
WHERE
    profile_name = '{profile_name}'
    AND name != 'system'
ORDER BY 2, 1
$str$, '{profile_name}', _profile_name);

   ---------------------------------------------------------
   -- BUILDING pools into PLPGSQL script
   ---------------------------------------------------------
   --RAISE INFO '_sql: %', _sql; --DEGUG
   FOR _rec IN EXECUTE( _sql )
   LOOP
      UPDATE profile_code SET code = code || 
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
   END LOOP;

   ---------------------------------------------------------
   -- rules query
   ---------------------------------------------------------
   _sql := REPLACE($str$
WITH
rule AS (
   SELECT
      enabled
      , war.superuser
      , war.rule_type
      , war."order"
      , war.expression
      , war.profile_name
      , war.rule_name
      , war.enabled
      , DECODE(war.rule_type, 'submit', 10, 'assemble', 20, 'compile', 30, 'run', 40, 'runtime', 40, 'restart_for_error', 50, 'restart_for_user', 60, 'completion', 70
         , 80) AS rule_type_order
   FROM
      sys.wlm_active_rule AS war
      LEFT JOIN sys.wlm_active_profile AS wap
         ON (wap.name = war.profile_name)
   WHERE
      war.profile_name = '{profile_name}'
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
      UPDATE profile_code SET code = code ||
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE($PLPGSQL$
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
         , _profile_name, '{profile_name}');
   END LOOP;

   ---------------------------------------------------------
   -- BUILDING end of PLPGSQL script
   ---------------------------------------------------------
   UPDATE profile_code SET code = code || $PLPGSQL$
   FOR v_wlm_rec IN SELECT code FROM wlm_code ORDER BY wlm_entry
   LOOP
       v_code := v_wlm_rec.code;

	   FOR v_js_rec IN SELECT alias, code FROM wlm_snippet ORDER BY sub_order
	   LOOP
		   v_code := REPLACE(v_code, v_js_rec.alias, v_js_rec.code);
	   END LOOP;

	   EXECUTE v_code;
   END LOOP;

END $code$;
$PLPGSQL$;


   RETURN QUERY EXECUTE $$SELECT code AS a FROM profile_code$$;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;   

END;   
$proc$ 
;


COMMENT ON FUNCTION wlm_profile_rule_p( _profile_name VARCHAR ) IS 
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