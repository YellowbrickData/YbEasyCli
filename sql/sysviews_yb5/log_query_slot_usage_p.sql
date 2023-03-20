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
** . 2023.01.07 - Fix to COMMENT ON.
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
DROP   TABLE IF EXISTS log_query_slot_usage_t CASCADE;
CREATE TABLE           log_query_slot_usage_t (
   pool_id    VARCHAR(128)
 , slots      BIGINT
 , secs       BIGINT
 , secs_pct   NUMERIC(38,2)
 , min_sec_ts TIMESTAMP
 , max_sec_ts TIMESTAMP
);

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_slot_usage2_p(
    _non_su         VARCHAR 
    , _from_date    DATE    DEFAULT NULL
    , _days         INT     DEFAULT 30
    , _days_of_week VARCHAR DEFAULT NULL
    , _hours_of_day VARCHAR DEFAULT NULL)
   RETURNS SETOF log_query_slot_usage_t
   LANGUAGE 'plpgsql' 
   VOLATILE
AS
$proc$
DECLARE
    _start_date DATE := NVL(_from_date, (CURRENT_DATE - _days + 1));
    _end_date   DATE := _start_date + _days;
    _total_secs INT := 60 * 60 * 24 * _days;
    _rec        RECORD;
    _ts         VARCHAR(15);
    --
    _sql TEXT;
    _fn_name   VARCHAR(256) := 'column_stats_p';
    _prev_tags VARCHAR(256) := CURRENT_SETTING('ybd_query_tags');
    _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      
BEGIN
    --RAISE INFO '_start_date: %' , _start_date; --DEBUG
    --RAISE INFO '_end_date: %' , _end_date; --DEBUG
    --RAISE INFO '_total_secs: %' , _total_secs; --DEBUG
    _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
    EXECUTE _sql ;
    --
    SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS') INTO _ts;
    --RAISE INFO '_ts: %' , _ts; --DEBUG
    --
    _sql := REPLACE($$CREATE TEMP TABLE sys_log_query_{ts} AS
        SELECT
            pool_id, query_id
            , submit_time, done_time
            , client_ms, run_ms
            , (done_time - ((INTERVAL '1 USECONDS')
               * ((NVL(client_ms, 0.0) /* + wait_client_ms*/) * 1000)
               ) ) AS adj_done_time
            , (adj_done_time - ((INTERVAL '1 USECONDS')
               * (NVL(run_ms, 0.0) * 1000)
               ) ) AS adj_submit_time
            , (EXTRACT('EPOCH' FROM adj_done_time)*1000000)::BIGINT AS adj_done_epoch
            , (EXTRACT('EPOCH' FROM adj_submit_time)*1000000)::BIGINT AS adj_submit_epoch
        FROM
            sys.log_query
        WHERE
            pool_id IS NOT NULL AND TRIM(pool_id) <> ''
        DISTRIBUTE RANDOM
        SORT ON (submit_time) $$, '{ts}', _ts);
    EXECUTE _sql;
    --
    EXECUTE 'ALTER TABLE sys_log_query_' || _ts || ' OWNER TO ' || _non_su;
    --
    EXECUTE 'SET SESSION AUTHORIZATION ' || _non_su;
    --
    _sql := REPLACE($$CREATE TEMP TABLE cnt_{ts} AS
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
            range BETWEEN 1 AND $1
            AND worker_lid = (SELECT min_worker_lid FROM min_worker_lid)
        ORDER BY 1
        DISTRIBUTE REPLICATE
        SORT ON (cnt)$$, '{ts}', _ts);
    EXECUTE _sql USING _total_secs;
    --
    _sql := '';
    IF _days_of_week IS NOT NULL AND _days_of_week != ''THEN
        _sql := _sql || $$ AND EXTRACT('DOW' FROM sec_ts) IN ($$ || _days_of_week || ')';
    END IF;
    IF _hours_of_day IS NOT NULL AND _hours_of_day != '' THEN
        _sql := _sql || $$ AND EXTRACT('HOUR' FROM sec_ts) IN ($$ || _hours_of_day || ')';
    END IF;
    --
    _sql := REPLACE(REPLACE($$CREATE TEMP TABLE secs_{ts} AS
        WITH
        secs AS (
            SELECT
                $1 + (INTERVAL '1' SECOND * (cnt -1)) AS sec_ts
                , (EXTRACT('EPOCH' FROM sec_ts)*1000000)::BIGINT AS sec_epoch
                , sec_ts::DATE AS sec_date
                , (EXTRACT('EPOCH' FROM sec_date)*1000000)::BIGINT AS sec_epoch_date
                , (EXTRACT('EPOCH' FROM DATE_TRUNC('HOUR', sec_ts))*1000000)::BIGINT AS sec_epoch_hr
                --, sec_ts::DATE AS sec_epoch_hr
                --, sec_ts::DATE AS sec_date
                , EXTRACT('HOUR' FROM sec_ts) AS sec_hr
                , EXTRACT('MINUTE' FROM sec_ts) AS sec_mi
            FROM
                cnt_{ts}
        )
        SELECT sec_ts, sec_epoch, sec_epoch_date, sec_epoch_hr, sec_date, sec_hr, sec_mi
        FROM secs
        WHERE TRUE{where_clause}
        ORDER BY 1
        DISTRIBUTE ON (sec_epoch_hr)
        SORT ON (sec_ts)$$, '{ts}', _ts), '{where_clause}', _sql);
    EXECUTE _sql USING _start_date;
    --
    EXECUTE REPLACE('SELECT COUNT(*) FROM secs_{ts}', '{ts}', _ts) INTO _total_secs;
    --
    _sql := REPLACE($$CREATE TEMP TABLE q_hr_{ts} AS 
        WITH
        epoch_hr AS (
            SELECT
                sec_epoch_hr AS epoch_hr_start
                , (epoch_hr_start + 3599999999)::BIGINT AS epoch_hr_end
            FROM secs_{ts} GROUP BY 1,2 ORDER BY 1
        )
        , q AS (
        SELECT
            q.*
            , epoch_hr.epoch_hr_start AS epoch_hr
        FROM
            sys_log_query_{ts} AS q
            JOIN epoch_hr
                ON adj_submit_epoch <= epoch_hr_end AND adj_done_epoch >= epoch_hr_start
        )
        SELECT * FROM q
        DISTRIBUTE ON (epoch_hr)
        SORT ON (adj_submit_time)$$, '{ts}', _ts);
    EXECUTE _sql;
    --
    _sql := REPLACE($$CREATE TEMP TABLE q_sec_{ts} AS
        SELECT
            pool_id, query_id, sec_ts, adj_submit_time, adj_done_time, epoch_hr
        FROM
            q_hr_{ts} AS q_hr
            JOIN secs_{ts} AS s
                ON s.sec_epoch_hr = q_hr.epoch_hr
                AND s.sec_epoch BETWEEN q_hr.adj_submit_epoch AND q_hr.adj_done_epoch 
        DISTRIBUTE ON (epoch_hr)
        SORT ON (sec_ts)$$, '{ts}', _ts);
    EXECUTE _sql;
    --
    _sql := REPLACE($$CREATE TEMP TABLE pool_per_sec_{ts} AS
    SELECT
        pool_id
        , sec_ts
        , COUNT(*) AS slots
    FROM q_sec_{ts}
    GROUP BY 1, 2
    DISTRIBUTE REPLICATE
    SORT ON (sec_ts) $$, '{ts}', _ts);
    EXECUTE _sql;
    --
    EXECUTE REPLACE($$CREATE TEMP TABLE wlm_acts_{ts}
    AS SELECT NULL::VARCHAR AS pool_id, NULL::TIMESTAMP AS act_start, NULL::TIMESTAMP AS act_end, NULL::BIGINT AS slots
    WHERE FALSE DISTRIBUTE REPLICATE$$, '{ts}', _ts);
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
        _sql := REPLACE($$INSERT INTO wlm_acts_{ts}
        VALUES ($1, $2, $3, $4)$$, '{ts}', _ts);
        EXECUTE _sql USING _rec.pool_id::VARCHAR, _rec.act_start, _rec.act_end, _rec.slots;
    END LOOP;
    --
    _sql := REPLACE($$DELETE
    FROM pool_per_sec_{ts} AS ps
    USING wlm_acts_{ts} AS act
    WHERE ps.pool_id = act.pool_id
        AND ps.sec_ts BETWEEN act.act_start AND act.act_end
        AND ps.slots > act.slots$$, '{ts}', _ts);
    EXECUTE _sql;
    --
    _sql := REPLACE($$CREATE TEMP TABLE log_query_slot_usage_{ts} AS
    WITH
    sm AS (
        SELECT
            pool_id, slots
            , COUNT(*) AS secs
            , ROUND((COUNT(*) / ($1 * 1.0)) * 100.0, 2) AS secs_pct
            , MIN(sec_ts) AS min_sec_ts
            , MAX(sec_ts) AS max_sec_ts
        FROM pool_per_sec_{ts}
        GROUP BY 1, 2
    )
        SELECT
            pool_id
            , 0 AS slots
            , $1 - SUM(secs) AS secs
            , ROUND(100.0 - SUM(secs_pct), 2) AS secs_pct
            , NULL::TIMESTAMP AS min_sec_ts
            , NULL::TIMESTAMP AS max_sec_ts
        FROM
            sm
        GROUP BY pool_id
    UNION all
        SELECT * FROM sm$$, '{ts}', _ts);
    EXECUTE _sql USING _total_secs;
    --
    RESET SESSION AUTHORIZATION;
    --
    RETURN QUERY EXECUTE 'SELECT * FROM log_query_slot_usage_' || _ts || ' ORDER BY 1, 2';
   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );
   
END 
$proc$;

COMMENT ON FUNCTION log_query_slot_usage_p( VARCHAR, DATE, INT, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
Create a WLM slot usage by pool report by analyzing sys.log_query data. 

Examples:
  SELECT * FROM log_query_slot_usage_p('dze');

Arguments:
. _non_su       (reqd) - the utility must be run by a super user and requires a non-super user to execute
                         without running into out of memory or spill issues
. _from_date    (optl) - the date to start analyzing data on, defaults to 30 days before NOW
. _days         (optl) - the number of days of data to analyze, defaults to 30 days
. _days_of_week (optl) - days of the week to report on as a string of 0 to 6 numbers comma seperated, like
                         '1,2,3,4,5', where 0 is Sunday, 1 is Monday, ..., 6 is Sunday, defaults to all days
. _hours_of_day (optl) - hours of the day to report on as a string of 0 to 23 numbers comma seperated, like
                        '9,10,11', defaults to all hours
Revision:
. 2023-01-20 - Yellowbrick Technical Support 
$cmnt$
;