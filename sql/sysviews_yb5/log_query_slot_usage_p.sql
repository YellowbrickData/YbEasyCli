/* log_query_slot_usage_p.sql
**
** Create a WLM slot usage report by analyzing sys.log_query data.
**
** . This procedure is designed to be run by a superuser in order that users can
**  see the statements run by all users.
**
** Usage: See COMMENT ON FUNCTION below after CREATE PROCEDURE for usage and notes.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2022.10.07 - ybCliUtils inclusion.
*/

/* ****************************************************************************
** Example result:
**
** pool_id       |slots|secs   |secs_pct|min_sec_ts             |max_sec_ts             |
** --------------+-----+-------+--------+-----------------------+-----------------------+
** PROD admin    |    0|2579587|   99.52|20XX-09-07 00:00:17.000|20XX-10-06 23:55:08.000|
** PROD admin    |    1|  12354|    0.48|20XX-09-07 00:00:17.000|20XX-10-06 23:55:08.000|
** PROD admin    |    2|     59|    0.00|20XX-09-07 10:00:28.000|20XX-10-06 22:55:51.000|
** PROD large    |    0|2047268|   78.98|20XX-09-06 23:58:21.000|20XX-10-06 23:51:42.000|
** PROD large    |    1| 397095|   15.32|20XX-09-06 23:58:21.000|20XX-10-06 23:51:42.000|
** PROD large    |    2| 104183|    4.02|20XX-09-06 23:59:27.000|20XX-10-06 23:02:06.000|
** PROD large    |    3|  43454|    1.68|20XX-09-07 01:09:56.000|20XX-10-06 22:55:55.000|
** PROD med      |    0|2042807|   78.83|20XX-09-06 23:59:12.000|20XX-10-06 23:50:16.000|
** PROD med      |    1| 483255|   18.64|20XX-09-06 23:59:12.000|20XX-10-06 23:50:16.000|
** PROD med      |    2|  45159|    1.74|20XX-09-07 00:08:24.000|20XX-10-06 22:55:14.000|
** PROD med      |    3|   8421|    0.32|20XX-09-07 00:26:58.000|20XX-10-06 22:54:51.000|
** PROD med      |    4|   4723|    0.18|20XX-09-07 01:19:02.000|20XX-10-06 21:38:20.000|
** PROD med      |    5|   3377|    0.13|20XX-09-07 09:19:25.000|20XX-10-06 14:57:30.000|
** PROD med      |    6|   4258|    0.16|20XX-09-08 00:03:41.000|20XX-09-08 21:06:54.000|
** PROD small    |    0|2369978|   91.42|20XX-09-06 23:59:32.000|20XX-10-06 23:55:06.000|
** PROD small    |    1| 117392|    4.53|20XX-09-06 23:59:32.000|20XX-10-06 23:55:06.000|
** PROD small    |    2|  19083|    0.74|20XX-09-07 05:36:17.000|20XX-10-06 22:57:31.000|
** PROD small    |    3|  15193|    0.59|20XX-09-07 05:36:32.000|20XX-10-06 22:54:13.000|
** PROD small    |    4|  57434|    2.22|20XX-09-07 05:36:48.000|20XX-10-06 21:55:26.000|
** PROD small    |    5|   8708|    0.34|20XX-09-07 05:36:34.000|20XX-10-06 21:54:42.000|
** PROD small    |    6|   1282|    0.05|20XX-09-07 05:36:45.000|20XX-10-06 21:52:40.000|
** PROD small    |    7|   1006|    0.04|20XX-09-07 05:36:36.000|20XX-10-05 12:52:53.000|
** PROD small    |    8|   1924|    0.07|20XX-09-07 05:36:38.000|20XX-10-05 12:52:20.000|
** system        |    0|2560195|   98.77|20XX-09-06 23:58:02.000|20XX-10-06 23:51:54.000|
** system        |    1|  31615|    1.22|20XX-09-06 23:58:02.000|20XX-10-06 23:51:54.000|
** system        |    2|    190|    0.01|20XX-09-07 00:38:32.000|20XX-10-06 22:28:19.000|
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_slot_usage_t CASCADE;
CREATE TABLE log_query_slot_usage_t (
    pool_id CHARACTER VARYING(128)
    , slots      BIGINT
    , secs       BIGINT
    , secs_pct   NUMERIC(38,2)
    , min_sec_ts TIMESTAMP WITHOUT TIME ZONE
    , max_sec_ts TIMESTAMP WITHOUT TIME ZONE
);

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_slot_usage_p(
    _non_su      VARCHAR
    , _days      INT DEFAULT 30 
    , _from_date DATE DEFAULT (CURRENT_DATE - _days + 1))
    RETURNS SETOF log_query_slot_usage_t
    LANGUAGE 'plpgsql' 
    VOLATILE
AS
$proc$
DECLARE
    _end_date DATE := _from_date + _days;
    _total_secs INT := 60 * 60 * 24 * _days;
    _rec RECORD;
    --
    _sql TEXT;
    _fn_name   VARCHAR(256) := 'column_stats_p';
    _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
    _tags VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      
BEGIN
    --RAISE INFO '_from_date: %' , _from_date;
    --RAISE INFO '_end_date: %' , _end_date;
    --RAISE INFO '_total_secs: %' , _total_secs;
    _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
    EXECUTE _sql ;    
    --
    EXECUTE 'SET SESSION AUTHORIZATION ' || _non_su;
    --
    DROP TABLE IF EXISTS cnt;
    --
    CREATE TEMP TABLE cnt AS
    WITH
    min_worker_lid AS (
        SELECT
            MIN(worker_lid) AS min_worker_lid
        FROM sys.rowgenerator
        WHERE range BETWEEN 0 and 0
    )
    SELECT
        r.row_number + 1 AS cnt
    FROM
        sys.rowgenerator AS r
    WHERE
        range BETWEEN 1 AND _total_secs
        AND worker_lid = (SELECT min_worker_lid FROM min_worker_lid)
    ORDER BY 1
    DISTRIBUTE REPLICATE
    SORT ON (cnt);
    --
    DROP TABLE IF EXISTS secs;
    --
    CREATE TEMP TABLE secs AS
    WITH
    start_end_dates AS (
        SELECT
            -- I don't know why but if I change the following 2 lines to
            -- _end_date AS end_date, _from_date AS start_date
            -- performance in the fallowing FOR LOOP gets very bad
            DATE_TRUNC('SECONDS', _from_date) AS start_date
            , DATE_TRUNC('SECONDS', _end_date) AS end_date
    )
    , secs AS (
        SELECT
            start_date + (INTERVAL '1' SECOND * (cnt -1)) AS sec_ts
            , sec_ts::DATE AS sec_date
            , EXTRACT('HOUR' FROM sec_ts) AS sec_hr
            , EXTRACT('MINUTE' FROM sec_ts) AS sec_mi
        FROM
            cnt
            CROSS JOIN start_end_dates
        WHERE
            start_date <= sec_ts AND sec_ts <= end_date
    )
    SELECT sec_ts, sec_date, sec_hr, sec_mi FROM secs
    ORDER BY 1
    DISTRIBUTE REPLICATE
    SORT ON (sec_ts);
    --
    DROP TABLE IF EXISTS pool_per_sec;
    --
    CREATE TEMP TABLE pool_per_sec AS
    SELECT
        pool_id
        , NULL::TIMESTAMP AS sec_ts
        , NULL::BIGINT AS slots
    FROM sys.log_query
    WHERE FALSE
    DISTRIBUTE RANDOM;
    --
    FOR _rec IN SELECT sec_ts::DATE AS dt FROM secs GROUP BY 1 ORDER BY 1
    LOOP
        --RAISE INFO 'dt ====> %', _rec.dt;
        --
        DROP TABLE IF EXISTS q;
        --
        RESET SESSION AUTHORIZATION;
        --
        CREATE TEMP TABLE q AS
        SELECT
            pool_id, query_id
            , submit_time, done_time
            --, DECODE(TRUE, num_restart > 0, restart_time
            --   , (submit_time + ((INTERVAL '1 USECONDS')
            --   * ((NVL(parse_ms, 0.0) /*+ wait_parse_ms + wait_lock_ms*/ + NVL(plan_ms, 0.0) + /*wait_plan_ms +*/ NVL(assemble_ms, 0.0) + /*wait_assemble_ms +*/ NVL(compile_ms, 0.0) + /*wait_compile_ms +*/ NVL(acquire_resources_ms, 0.0)) * 1000)
            --   ) ) ) AS adj_submit_time
            , (done_time - ((INTERVAL '1 USECONDS')
               * ((NVL(client_ms, 0.0) /* + wait_client_ms*/) * 1000)
               ) ) AS adj_done_time
            , (adj_done_time - ((INTERVAL '1 USECONDS')
               * (NVL(run_ms, 0.0) * 1000)
               ) ) AS adj_submit_time
        FROM
            sys.log_query
        WHERE
            pool_id IS NOT NULL
            AND adj_submit_time < _rec.dt + 1 AND adj_done_time >= _rec.dt
        DISTRIBUTE RANDOM;
        --
        EXECUTE 'ALTER TABLE q OWNER TO ' || _non_su; 
        --
        EXECUTE 'SET SESSION AUTHORIZATION ' || _non_su;
        --
        INSERT INTO pool_per_sec
        WITH
        s AS (
            SELECT * FROM secs
            WHERE sec_ts BETWEEN _rec.dt AND _rec.dt + 1
        )
        SELECT
            pool_id, sec_ts, COUNT(*) AS slots 
        FROM
            s
            JOIN q
                ON adj_submit_time < sec_ts AND adj_done_time >= sec_ts
        GROUP BY 1, 2;
        --
    END LOOP;
    --
    DROP TABLE IF EXISTS wlm_acts;
    --
    FOR _rec IN
        SELECT
            name AS pool_id
            , activated AS act_start
            , NVL(deactivated, (CURRENT_DATE + 10)::TIMESTAMP) AS act_end
            , max_concurrency AS slots  
        FROM sys.wlm_resource_pool
        WHERE activated IS NOT NULL
        ORDER BY 1, activated
    LOOP
        CREATE TEMP TABLE IF NOT EXISTS wlm_acts AS SELECT _rec.pool_id::VARCHAR, _rec.act_start, _rec.act_end, _rec.slots WHERE FALSE DISTRIBUTE REPLICATE;
        INSERT INTO wlm_acts VALUES (_rec.pool_id::VARCHAR, _rec.act_start, _rec.act_end, _rec.slots);
    END LOOP;
    --
    DELETE 
    FROM pool_per_sec AS ps
    USING wlm_acts AS act
    WHERE ps.pool_id = act.pool_id
        AND ps.sec_ts BETWEEN act.act_start AND act.act_end
        AND ps.slots > act.slots;
    --
    DROP TABLE IF EXISTS log_query_slot_usage;
    --
    CREATE TEMP TABLE log_query_slot_usage AS
    WITH
    sm AS (
        SELECT
            pool_id, slots
            , COUNT(*) AS secs
            , ROUND((COUNT(*) / (_total_secs * 1.0)) * 100.0, 2) AS secs_pct
            , MIN(sec_ts) AS min_sec_ts
            , MAX(sec_ts) AS max_sec_ts
        FROM pool_per_sec
        GROUP BY 1, 2
    )
        SELECT
            pool_id
            , 0 AS slots
            , _total_secs - SUM(secs) AS secs
            , ROUND(100.0 - SUM(secs_pct), 2) AS secs_pct
            , MIN(min_sec_ts) AS min_sec_ts
            , MAX(max_sec_ts) AS max_sec_ts
        FROM
            sm
        GROUP BY pool_id
    UNION all
        SELECT * FROM sm;
    --
    RESET SESSION AUTHORIZATION;
    --    
    RETURN QUERY EXECUTE 'SELECT * FROM log_query_slot_usage ORDER BY 1, 2';
    --
    /* Reset ybd_query_tags back to its previous value
    */
    _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
    EXECUTE _sql;
END $proc$;

COMMENT ON FUNCTION log_query_slot_usage_p( VARCHAR, DATE, INT ) IS 
$$Description:
Create a WLM slot usage report by analyzing sys.log_query data. 

Examples:
  SELECT * FROM log_query_slot_usage_p('dze');

Arguments:
. _non_su    - the utility must be run by a super user and requires a non-super user to execute
                    without running into out of memory or spill issues
. _from_date - the date to start analyzing data on, defaults to 30 days before NOW
. _days      - the number of days of data to analyze, defaults to 30 days

Revision:
$$
;