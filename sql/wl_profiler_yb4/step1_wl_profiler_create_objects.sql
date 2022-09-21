SET SESSION AUTHORIZATION :owner;

CREATE TABLE wl_profiler_hrs AS
WITH
decimals AS (
    SELECT 01 AS ct
    UNION ALL SELECT 02
    UNION ALL SELECT 03
    UNION ALL SELECT 04
    UNION ALL SELECT 05
    UNION ALL SELECT 06
    UNION ALL SELECT 07
    UNION ALL SELECT 08
    UNION ALL SELECT 09
    UNION ALL SELECT 10
)
, hrs AS (
    SELECT
        d1.ct + (d2.ct - 1) * 10 + (d3.ct - 1) * 100 + (d4.ct - 1) * 1000 + (d5.ct - 1) * 10000 AS cnt
        , DATE_TRUNC('HOUR', ('2020-01-01'::DATE)) + (INTERVAL '1' HOUR * (cnt -1)) AS start_hr
        , DATE_TRUNC('HOUR', ('2020-01-01'::DATE)) + (INTERVAL '1' HOUR * cnt) AS finish_hr
    FROM
        decimals d1
        CROSS JOIN decimals d2
        CROSS JOIN decimals d3
        CROSS JOIN decimals d4
        CROSS JOIN decimals d5
    WHERE
        start_hr < '2031-01-01'::DATE
)
SELECT * FROM hrs
DISTRIBUTE REPLICATE;

CREATE TABLE wl_profiler_sys_log_query AS
    SELECT
        submit_time AS ts01_submit
        , ts01_submit + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wait_admission_ms,0)/1000.0) AS ts02_end_wait_concur_users
        , ts02_end_wait_concur_users + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(planning_ms,0)/1000.0) AS ts03_end_plan
        , start_time AS ts04_start
        , ts04_start + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(queue_ms,0)/1000.0) AS ts05_end_queue
        , ts05_end_queue + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(prepare_ms,0)/1000.0) AS ts06_end_prepare
        , execution_time AS ts07_execution
        , ts07_execution + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wait_concurrency_ms,0)/1000.0) AS ts08_end_throttle
        , ts08_end_throttle + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(lock_ms,0)/1000.0) AS ts09_end_lock
        , ts09_end_lock + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(runtime_ms,0)/1000.0) AS ts10_end_run
        , end_time AS ts11_end
        , ts11_end + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(spool_ms,0)/1000.0) AS ts12_end_spool
        , DECODE(start_time, NULL, TRUE, FALSE) AS has_no_start_ts
        , query_id
        , parent_id
        , prior_id
        , original_id
        , transaction_id
        , plan_id
        , status
        , requeue_status
        , user_name
        , application_name
        , database_name
        , session_id
        , tags
        , type
        , submit_time
        , start_time
        , execution_time
        , end_time
        , planning_ms
        , lock_ms
        , wait_admission_ms
        , wait_concurrency_ms
        , queue_ms
        , prepare_ms
        , runtime_ms
        , runtime_execution_ms
        , spool_ms
        , total_ms
        , io_read_bytes
        , io_write_bytes
        , io_spill_read_bytes
        , io_spill_write_bytes
        , io_spill_space_bytes
        , io_network_bytes
        , avg_cpu_percent
        , rows_inserted
        , rows_deleted
        , rows_returned
        , memory_bytes
        , memory_total_bytes
        , memory_estimated_bytes
        , memory_required_bytes
        , memory_granted_bytes
        , memory_estimate_confidence
        , cost
        , priority
        , query_text
        , pool_id
        , slot
    FROM
        sys.log_query
    WHERE FALSE
    DISTRIBUTE ON (query_id)
;


CREATE VIEW wl_profiler_sum_log_query_v AS
WITH
hr AS (
    SELECT
        TO_CHAR(start_hr, 'YYYY-MM-DD HH24:MI') AS hr
        , start_hr AS shr
        , finish_hr AS fhr
    FROM
        wl_profiler_hrs
)
, q AS (
    SELECT
        DECODE(TRUE, user_name IS NULL, '^null^', TRIM(user_name) = '', '^null^', TRIM(user_name)) AS usr
        , DECODE(TRUE,  application_name IS NULL, '^null^', TRIM(application_name) = '', '^null^', application_name LIKE 'DBeaver%', 'DBeaver', TRIM(application_name)) AS app
        , DECODE(TRUE, pool_id IS NULL, '^null^', TRIM(pool_id) = '', '^null^', TRIM(pool_id)) AS pool
        , *
    FROM
        wl_profiler_sys_log_query
)
, t_user_wait AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts02_end_wait_concur_users > fhr, fhr, ts02_end_wait_concur_users))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts01_submit < shr, shr, ts01_submit)))/60.0)::NUMERIC(10,2) AS user_wait_mnts
    FROM
        hr JOIN q ON ts01_submit < fhr AND ts02_end_wait_concur_users >= shr
    GROUP BY 1,2,3,4
)
, t_plan AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts03_end_plan > fhr, fhr, ts03_end_plan))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts02_end_wait_concur_users < shr, shr, ts02_end_wait_concur_users)))/60.0)::NUMERIC(10,2) AS plan_mnts
    FROM
        hr JOIN q ON ts02_end_wait_concur_users < fhr AND ts03_end_plan >= shr
    GROUP BY 1,2,3,4
)
, t_queue AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts05_end_queue > fhr, fhr, ts05_end_queue))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts04_start < shr, shr, ts04_start)))/60.0)::NUMERIC(10,2) AS queue_mnts
    FROM
        hr JOIN q ON ts04_start < fhr AND ts05_end_queue >= shr
    GROUP BY 1,2,3,4
)
, t_prepare AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts06_end_prepare > fhr, fhr, ts06_end_prepare))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts05_end_queue < shr, shr, ts05_end_queue)))/60.0)::NUMERIC(10,2) AS prepare_mnts
    FROM
        hr JOIN q ON ts05_end_queue < fhr AND ts06_end_prepare >= shr
    GROUP BY 1,2,3,4
)
, t_throttle AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts08_end_throttle > fhr, fhr, ts08_end_throttle))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts07_execution < shr, shr, ts07_execution)))/60.0)::NUMERIC(10,2) AS throttle_mnts
    FROM
        hr JOIN q ON ts07_execution < fhr AND ts08_end_throttle >= shr
    GROUP BY 1,2,3,4
)
, t_lock AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts09_end_lock > fhr, fhr, ts09_end_lock))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts08_end_throttle < shr, shr, ts08_end_throttle)))/60.0)::NUMERIC(10,2) AS lock_mnts
    FROM
        hr JOIN q ON ts08_end_throttle < fhr AND ts09_end_lock >= shr
    GROUP BY 1,2,3,4
)
, t_run AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts10_end_run > fhr, fhr, ts10_end_run))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts09_end_lock < shr, shr, ts09_end_lock)))/60.0)::NUMERIC(10,2) AS run_mnts
    FROM
        hr JOIN q ON ts09_end_lock < fhr AND ts10_end_run >= shr
    GROUP BY 1,2,3,4
)
, d_exec AS (
    SELECT
        hr, usr, app, pool
        , (EXTRACT(EPOCH FROM ts10_end_run) - EXTRACT(EPOCH FROM ts09_end_lock)) AS run_secs
        , (EXTRACT(EPOCH FROM DECODE(TRUE, ts10_end_run > fhr, fhr, ts10_end_run))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts09_end_lock < shr, shr, ts09_end_lock))) AS in_hr_run_secs
        , DECODE(run_secs, 0, 0, in_hr_run_secs / run_secs) AS in_hr_run_pct
        , runtime_execution_ms
        , (runtime_execution_ms * in_hr_run_pct) AS in_hr_runtime_execution_ms
        , avg_cpu_percent
    FROM
        hr JOIN q ON ts09_end_lock < fhr AND ts10_end_run >= shr
)
, t_exec AS (
    SELECT
        hr, usr, app, pool
        , (SUM(in_hr_runtime_execution_ms) / 1000.0 / 60.0)::NUMERIC(10,2) exec_mnts
    FROM
        d_exec
    GROUP BY 1,2,3,4
)
, t_spool AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts12_end_spool > fhr, fhr, ts12_end_spool))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts11_end < shr, shr, ts11_end)))/60.0)::NUMERIC(10,2) AS spool_mnts
    FROM
        hr JOIN q ON ts11_end < fhr AND ts12_end_spool >= shr
    GROUP BY 1,2,3,4
)
, t_other AS (
    SELECT
        hr, usr, app, pool
        , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts11_end > fhr, fhr, ts11_end))
        - EXTRACT(EPOCH FROM DECODE(TRUE, ts01_submit < shr, shr, ts01_submit)))/60.0)::NUMERIC(10,2) AS other_mnts
    FROM
        hr JOIN q ON ts01_submit < fhr AND ts11_end >= shr
    WHERE
        has_no_start_ts
    GROUP BY 1,2,3,4
)
SELECT
    hr, usr, app, pool
    , COALESCE(user_wait_mnts, 0) AS user_wait_mnts
    , COALESCE(plan_mnts, 0) AS plan_mnts
    , COALESCE(queue_mnts, 0) AS queue_mnts
    , COALESCE(prepare_mnts, 0) AS prepare_mnts
    , COALESCE(throttle_mnts, 0) AS throttle_mnts
    , COALESCE(lock_mnts, 0) AS lock_mnts
    , COALESCE(run_mnts, 0) AS run_mnts
    , COALESCE(exec_mnts, 0) AS exec_mnts
    , COALESCE(spool_mnts, 0) AS spool_mnts
    , COALESCE(other_mnts, 0) AS other_mnts
FROM
    t_user_wait
    FULL OUTER JOIN t_plan USING (hr, usr, app, pool)
    FULL OUTER JOIN t_queue USING (hr, usr, app, pool)
    FULL OUTER JOIN t_prepare USING (hr, usr, app, pool)
    FULL OUTER JOIN t_throttle USING (hr, usr, app, pool)
    FULL OUTER JOIN t_lock USING (hr, usr, app, pool)
    FULL OUTER JOIN t_run USING (hr, usr, app, pool)
    FULL OUTER JOIN t_exec USING (hr, usr, app, pool)
    FULL OUTER JOIN t_spool USING (hr, usr, app, pool)
    FULL OUTER JOIN t_other USING (hr, usr, app, pool)
ORDER BY 1,2,3,4 DESC
;

CREATE TABLE wl_profiler_sum_log_query AS SELECT * FROM wl_profiler_sum_log_query_v WHERE FALSE DISTRIBUTE ON (hr);

CREATE VIEW wl_profiler_user_sum_v AS
WITH
s AS (
    SELECT
        usr, LOWER(usr) AS lusr
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        wl_profiler_sum_log_query
    GROUP BY 1
)
, a AS (
    SELECT
        '^all^' AS usr, '^all^' AS lusr
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        s
    GROUP BY 1
)
, u AS (
    SELECT * FROM a
    UNION ALL SELECT * FROM s
)
SELECT usr, user_wait_mnts, plan_mnts, queue_mnts, prepare_mnts, throttle_mnts, lock_mnts, run_mnts, exec_mnts, spool_mnts, other_mnts FROM u ORDER BY lusr
;

CREATE VIEW wl_profiler_app_sum_v AS
WITH
s AS (
    SELECT
        app, LOWER(app) AS lapp
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        wl_profiler_sum_log_query
    GROUP BY 1
)
, a AS (
    SELECT
        '^all^' AS app, '^all^' AS lapp
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        s
    GROUP BY 1
)
, u AS (
    SELECT * FROM a
    UNION ALL SELECT * FROM s
)
SELECT app, user_wait_mnts, plan_mnts, queue_mnts, prepare_mnts, throttle_mnts, lock_mnts, run_mnts, exec_mnts, spool_mnts, other_mnts FROM u ORDER BY lapp
;

CREATE VIEW wl_profiler_pool_sum_v AS
WITH
s AS (
    SELECT
        pool, LOWER(pool) AS lpool
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        wl_profiler_sum_log_query
    GROUP BY 1
)
, a AS (
    SELECT
        '^all^' AS pool, '^all^' AS lpool
        , SUM(user_wait_mnts) AS user_wait_mnts
        , SUM(plan_mnts) AS plan_mnts
        , SUM(queue_mnts) AS queue_mnts
        , SUM(prepare_mnts) AS prepare_mnts
        , SUM(throttle_mnts) AS throttle_mnts
        , SUM(lock_mnts) AS lock_mnts
        , SUM(run_mnts) AS run_mnts
        , SUM(exec_mnts) AS exec_mnts
        , SUM(spool_mnts) AS spool_mnts
        , SUM(other_mnts) AS other_mnts
    FROM
        s
    GROUP BY 1
)
, u AS (
    SELECT * FROM a
    UNION ALL SELECT * FROM s
)
SELECT pool, user_wait_mnts, plan_mnts, queue_mnts, prepare_mnts, throttle_mnts, lock_mnts, run_mnts, exec_mnts, spool_mnts, other_mnts FROM u ORDER BY lpool
;

CREATE VIEW wl_profiler_step_sum_v AS
WITH
s AS (
    SELECT 1 AS ord, SUM(user_wait_mnts) AS s, 'Wait' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 2 AS ord, SUM(plan_mnts) AS s, 'Planner' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 3 AS ord, SUM(queue_mnts) AS s, 'Queue' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 4 AS ord, SUM(prepare_mnts) AS s, 'Prepare' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 5 AS ord, SUM(throttle_mnts) AS s, 'Throttle' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 6 AS ord, SUM(lock_mnts) AS s, 'Lock' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 7 AS ord, SUM(run_mnts) AS s, 'Run' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 7 AS ord, SUM(exec_mnts) AS s, 'Exec' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 8 AS ord, SUM(spool_mnts) AS s, 'Spool' AS n FROM wl_profiler_sum_log_query
    UNION ALL SELECT 9 AS ord, SUM(other_mnts) AS s, 'Other' AS n FROM wl_profiler_sum_log_query
)
SELECT n, s FROM s ORDER BY ord;
