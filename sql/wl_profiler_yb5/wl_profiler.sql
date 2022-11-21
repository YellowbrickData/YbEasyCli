CREATE TEMP TABLE wl_profiler_sys_log_query_ms AS
    SELECT
        query_id
        , session_id
        , transaction_id
        , plan_id
        , state
        , username
        , application_name
        , database_name
        , type
        , tags
        , error_code
        , error_message
        --, query_text
        , pool_id
        , priority
        , slot
        , num_workers
        , longest_worker_id
        , submit_time
        , done_time
        , state_time
        , restart_time
        , parse_ms             --prepare timing
        , wait_parse_ms        --wait_prep timing
        , wait_lock_ms         --wait_lock timing
        , plan_ms              --prepare timing
        , wait_plan_ms         --wait_prep timing
        , assemble_ms          --prepare timing
        , wait_assemble_ms     --wait_prep timing
        , compile_ms           --prepare timing
        , wait_compile_ms      --wait_prep timing
        , acquire_resources_ms --queue timing (assuming this is throttle and queue)
        , run_ms               --run timing
        , wait_run_cpu_ms      --overlap timing
        , wait_run_io_ms       --overlap timing
        , wait_run_spool_ms    --overlap timing
        , client_ms            --client timing
        , wait_client_ms       --client timing
        , total_ms             --overlap timing
        , cancel_ms            --overlap timing
        , restart_ms           --overlap timing
        , wlm_runtime_ms       --overlap timing
        , spool_ms             --overlap timing
        , (SELECT MAX(num_workers) AS max_num_workers from sys.log_query) AS max_num_workers
        --, SUBSTR(query_text, 1, 1000) AS query_text_1000
    FROM
        sys.log_query
    DISTRIBUTE ON (query_id);

ALTER TABLE wl_profiler_sys_log_query_ms OWNER TO :owner;

SET SESSION AUTHORIZATION :owner;

CREATE TEMP TABLE wl_profiler_hrs AS
    WITH
    start_end_dates AS (
        SELECT
            CURRENT_DATE  AS end_date
            , end_date - 35 AS start_date
    )
    , decimals AS (
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
            CROSS JOIN start_end_dates        
        WHERE
            start_date <= start_hr AND start_hr < (end_date + 1)
    )
    SELECT * FROM hrs
    ORDER BY 1
    DISTRIBUTE REPLICATE
    SORT ON (start_hr);

CREATE TEMP TABLE wl_profiler_sys_log_query AS
    WITH
    q AS (
        SELECT
            query_id
            , session_id
            , transaction_id
            , plan_id
            , state
            , username
            , application_name
            , database_name
            , type
            , tags
            , error_code
            , error_message
            --, query_text
            , pool_id
            , priority
            , slot
            , num_workers
            , longest_worker_id
            , submit_time
            , done_time
            , state_time
            , restart_time
            , NVL(wait_lock_ms, 0.0) AS wl_wait_lock_ms
            , NVL(wait_parse_ms, 0.0) + NVL(wait_plan_ms, 0.0) + NVL(wait_assemble_ms, 0.0) + NVL(wait_compile_ms, 0.0) AS wl_wait_prep_ms
            , NVL(parse_ms, 0.0) + NVL(plan_ms, 0.0) + NVL(assemble_ms, 0.0) + NVL(compile_ms, 0.0) AS wl_prep_ms
            , NVL(acquire_resources_ms, 0.0) AS wl_queue_ms
            --, NVL(wait_run_cpu_ms, 0.0) + NVL(wait_run_io_ms, 0.0) + NVL(wait_run_spool_ms, 0.0) + NVL(run_ms, 0.0) AS wl_run_ms
            -- wait_run_*_ms are included in run_ms
            --, NVL(run_ms, 0.0) AS wl_run_ms
            , NVL(run_ms, 0.0) - (NVL(wait_run_cpu_ms, 0.0) + NVL(wait_run_io_ms, 0.0)) AS wl_run_ms
            , NVL(wait_run_io_ms, 0.0) AS wl_io_ms
            , NVL(client_ms, 0.0) + NVL(wait_client_ms, 0.0) AS wl_client_ms
            , NVL(restart_time, submit_time) AS ts01_start
            , ts01_start         + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_wait_lock_ms,0)/1000.0) AS ts02_end_wait_lock
            , ts02_end_wait_lock + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_wait_prep_ms,0)/1000.0) AS ts03_end_wait_prep
            , ts03_end_wait_prep + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_prep_ms,0)/1000.0)      AS ts04_end_prep
            , ts04_end_prep      + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_queue_ms,0)/1000.0)     AS ts05_end_queue
            , DECODE(num_workers, max_num_workers
                , ts05_end_queue     + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_run_ms,0)/1000.0)
                , ts05_end_queue)                                                              AS ts06_end_run
            , DECODE(num_workers, max_num_workers
                , ts06_end_run       + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_io_ms,0)/1000.0)
                , ts06_end_run)                                                                AS ts07_end_io
            , ts07_end_io        + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_client_ms,0)/1000.0)    AS ts08_end_client
            , parse_ms             --prepare timing
            , wait_parse_ms        --wait_prep timing
            , wait_lock_ms         --wait_lock timing
            , plan_ms              --prepare timing
            , wait_plan_ms         --wait_prep timing
            , assemble_ms          --prepare timing
            , wait_assemble_ms     --wait_prep timing
            , compile_ms           --prepare timing
            , wait_compile_ms      --wait_prep timing
            , acquire_resources_ms --queue timing (assuming this is throttle and queue)
            , run_ms               --run timing
            , wait_run_cpu_ms      --overlap timing
            , wait_run_io_ms       --overlap timing
            , wait_run_spool_ms    --overlap timing
            , client_ms            --client timing
            , wait_client_ms       --client timing
            , total_ms             --overlap timing
            , cancel_ms            --overlap timing
            , restart_ms           --overlap timing
            , wlm_runtime_ms       --overlap timing
            , spool_ms             --overlap timing
            --, query_text_1000
        FROM
            wl_profiler_sys_log_query_ms
    )
    SELECT
        query_id
        , session_id
        , transaction_id
        , plan_id
        , state
        , username
        , application_name
        , database_name
        , type
        , tags
        , error_code
        , error_message
        , pool_id
        , priority
        , slot
        , num_workers
        , submit_time
        , done_time
        , state_time
        , restart_time
        , longest_worker_id
        , wl_wait_lock_ms
        , wl_wait_prep_ms
        , wl_prep_ms
        , wl_queue_ms
        , wl_run_ms
        , wl_client_ms
        , ts01_start
        , ts02_end_wait_lock
        , ts03_end_wait_prep
        , ts04_end_prep
        , ts05_end_queue
        , ts06_end_run
        , ts07_end_io
        , ts08_end_client
        , parse_ms             --prepare timing
        , wait_parse_ms        --wait_prep timing
        , wait_lock_ms         --wait_lock timing
        , plan_ms              --prepare timing
        , wait_plan_ms         --wait_prep timing
        , assemble_ms          --prepare timing
        , wait_assemble_ms     --wait_prep timing
        , compile_ms           --prepare timing
        , wait_compile_ms      --wait_prep timing
        , acquire_resources_ms --queue timing (assuming this is throttle and queue)
        , run_ms               --run timing
        , wait_run_cpu_ms      --overlap timing
        , wait_run_io_ms       --overlap timing
        , wait_run_spool_ms    --overlap timing
        , client_ms            --client timing
        , wait_client_ms       --client timing
        , total_ms             --overlap timing
        , cancel_ms            --overlap timing
        , restart_ms           --overlap timing
        , wlm_runtime_ms       --overlap timing
        , spool_ms             --overlap timing
        --, query_text_1000
    FROM
        q
        JOIN wl_profiler_hrs
            ON ts01_start > start_hr AND ts01_start < finish_hr
    DISTRIBUTE ON (query_id);

CREATE TEMP VIEW wl_profiler_sum_log_query_v AS
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
            DECODE(TRUE, username IS NULL, '^null^', TRIM(username) = '', '^null^', TRIM(username)) AS usr
            , DECODE(TRUE,  application_name IS NULL, '^null^', TRIM(application_name) = '', '^null^', application_name LIKE 'DBeaver%', 'DBeaver', TRIM(application_name)) AS app
            , DECODE(TRUE, pool_id IS NULL, '^null^', TRIM(pool_id) = '', '^null^', TRIM(pool_id)) AS pool
            , *
        FROM
            wl_profiler_sys_log_query
    )
    , t_wait_lock AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts02_end_wait_lock > fhr, fhr, ts02_end_wait_lock))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts01_start < shr, shr, ts01_start)))/60.0)::NUMERIC(10,2) AS wait_lock_mnts
        FROM
            hr JOIN q ON ts01_start < fhr AND ts02_end_wait_lock >= shr
        GROUP BY 1,2,3,4
    )
    , t_wait_prep AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts03_end_wait_prep > fhr, fhr, ts03_end_wait_prep))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts02_end_wait_lock < shr, shr, ts02_end_wait_lock)))/60.0)::NUMERIC(10,2) AS wait_prep_mnts
        FROM
            hr JOIN q ON ts02_end_wait_lock < fhr AND ts03_end_wait_prep >= shr
        GROUP BY 1,2,3,4
    )
    , t_prep AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts04_end_prep > fhr, fhr, ts04_end_prep))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts03_end_wait_prep < shr, shr, ts03_end_wait_prep)))/60.0)::NUMERIC(10,2) AS prep_mnts
        FROM
            hr JOIN q ON ts03_end_wait_prep < fhr AND ts04_end_prep >= shr
        GROUP BY 1,2,3,4
    )
    , t_queue AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts05_end_queue > fhr, fhr, ts05_end_queue))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts04_end_prep < shr, shr, ts04_end_prep)))/60.0)::NUMERIC(10,2) AS queue_mnts
        FROM
            hr JOIN q ON ts04_end_prep < fhr AND ts05_end_queue >= shr
        GROUP BY 1,2,3,4
    )
    , t_run AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts06_end_run > fhr, fhr, ts06_end_run))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts05_end_queue < shr, shr, ts05_end_queue)))/60.0)::NUMERIC(10,2) AS run_mnts
        FROM
            hr JOIN q ON ts05_end_queue < fhr AND ts06_end_run >= shr
        GROUP BY 1,2,3,4
    )
    , t_io AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts07_end_io > fhr, fhr, ts07_end_io))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts06_end_run < shr, shr, ts06_end_run)))/60.0)::NUMERIC(10,2) AS io_mnts
        FROM
            hr JOIN q ON ts06_end_run < fhr AND ts07_end_io >= shr
        GROUP BY 1,2,3,4
    )
    , t_client AS (
        SELECT
            hr, usr, app, pool
            , SUM((EXTRACT(EPOCH FROM DECODE(TRUE, ts08_end_client > fhr, fhr, ts08_end_client))
            - EXTRACT(EPOCH FROM DECODE(TRUE, ts07_end_io < shr, shr, ts07_end_io)))/60.0)::NUMERIC(10,2) AS client_mnts
        FROM
            hr JOIN q ON ts07_end_io < fhr AND ts08_end_client >= shr
        GROUP BY 1,2,3,4
    )
    SELECT
        hr, usr, app, pool
        , COALESCE(wait_lock_mnts, 0) AS wait_lock_mnts
        , COALESCE(wait_prep_mnts, 0) AS wait_prep_mnts
        , COALESCE(prep_mnts, 0)      AS prep_mnts
        , COALESCE(queue_mnts, 0)     AS queue_mnts
        , COALESCE(run_mnts, 0)       AS run_mnts
        , COALESCE(io_mnts, 0)        AS io_mnts
        , run_mnts + io_mnts          AS run_plus_io_mnts
        , COALESCE(client_mnts, 0)    AS client_mnts
    FROM
        t_wait_lock
        FULL OUTER JOIN t_wait_prep USING (hr, usr, app, pool)
        FULL OUTER JOIN t_prep      USING (hr, usr, app, pool)
        FULL OUTER JOIN t_queue     USING (hr, usr, app, pool)
        FULL OUTER JOIN t_run       USING (hr, usr, app, pool)
        FULL OUTER JOIN t_io        USING (hr, usr, app, pool)
        FULL OUTER JOIN t_client    USING (hr, usr, app, pool)
    ORDER BY 1,2,3,4 DESC;

CREATE TEMP TABLE wl_profiler_sum_log_query AS SELECT * FROM wl_profiler_sum_log_query_v DISTRIBUTE ON (hr);

CREATE TEMP VIEW wl_profiler_user_sum_v AS
    WITH
    s AS (
        SELECT
            usr, LOWER(usr)         AS lusr
            , SUM(wait_lock_mnts)   AS wait_lock_mnts
            , SUM(wait_prep_mnts)   AS wait_prep_mnts
            , SUM(prep_mnts)        AS prep_mnts
            , SUM(queue_mnts)       AS queue_mnts
            , SUM(run_mnts)         AS run_mnts
            , SUM(io_mnts)          AS io_mnts
            , SUM(run_plus_io_mnts) AS run_plus_io_mnts
            , SUM(client_mnts)      AS client_mnts
        FROM
            wl_profiler_sum_log_query
        GROUP BY 1
    )
    , a AS (
        SELECT
            '^all^' AS usr, '^all^' AS lusr
            , SUM(wait_lock_mnts)   AS wait_lock_mnts
            , SUM(wait_prep_mnts)   AS wait_prep_mnts
            , SUM(prep_mnts)        AS prep_mnts
            , SUM(queue_mnts)       AS queue_mnts
            , SUM(run_mnts)         AS run_mnts
            , SUM(io_mnts)          AS io_mnts
            , SUM(run_plus_io_mnts) AS run_plus_io_mnts
            , SUM(client_mnts)      AS client_mnts
        FROM
            s
        GROUP BY 1
    )
    , u AS (
        SELECT * FROM a
        UNION ALL SELECT * FROM s
    )
    SELECT usr, wait_lock_mnts, wait_prep_mnts, prep_mnts, queue_mnts, run_mnts, io_mnts, run_plus_io_mnts, client_mnts FROM u ORDER BY lusr;

CREATE TEMP VIEW wl_profiler_app_sum_v AS
    WITH
    s AS (
        SELECT
            app, LOWER(app)         AS lapp
            , SUM(wait_lock_mnts)   AS wait_lock_mnts
            , SUM(wait_prep_mnts)   AS wait_prep_mnts
            , SUM(prep_mnts)        AS prep_mnts
            , SUM(queue_mnts)       AS queue_mnts
            , SUM(run_mnts)         AS run_mnts
            , SUM(io_mnts)          AS io_mnts
            , SUM(run_plus_io_mnts) AS run_plus_io_mnts
            , SUM(client_mnts)      AS client_mnts
        FROM
            wl_profiler_sum_log_query
        GROUP BY 1
    )
    , a AS (
        SELECT
            '^all^' AS app, '^all^' AS lapp
            , SUM(wait_lock_mnts)   AS wait_lock_mnts
            , SUM(wait_prep_mnts)   AS wait_prep_mnts
            , SUM(prep_mnts)        AS prep_mnts
            , SUM(queue_mnts)       AS queue_mnts
            , SUM(run_mnts)         AS run_mnts
            , SUM(io_mnts)          AS io_mnts
            , SUM(run_plus_io_mnts) AS run_plus_io_mnts
            , SUM(client_mnts)      AS client_mnts
        FROM
            s
        GROUP BY 1
    )
    , u AS (
        SELECT * FROM a
        UNION ALL SELECT * FROM s
    )
    SELECT app, wait_lock_mnts, wait_prep_mnts, prep_mnts, queue_mnts, run_mnts, io_mnts, run_plus_io_mnts, client_mnts FROM u ORDER BY lapp;

CREATE TEMP VIEW wl_profiler_pool_sum_v AS
    WITH
    s AS (
        SELECT
            pool, LOWER(pool)       AS lpool
            , SUM(wait_lock_mnts)   AS wait_lock_mnts
            , SUM(wait_prep_mnts)   AS wait_prep_mnts
            , SUM(prep_mnts)        AS prep_mnts
            , SUM(queue_mnts)       AS queue_mnts
            , SUM(run_mnts)         AS run_mnts
            , SUM(io_mnts)          AS io_mnts
            , SUM(run_plus_io_mnts) AS run_plus_io_mnts
            , SUM(client_mnts)      AS client_mnts
        FROM
            wl_profiler_sum_log_query
        GROUP BY 1
    )
    , a AS (
        SELECT
            '^all^' AS pool, '^all^' AS lpool
            , SUM(wait_lock_mnts)    AS wait_lock_mnts
            , SUM(wait_prep_mnts)    AS wait_prep_mnts
            , SUM(prep_mnts)         AS prep_mnts
            , SUM(queue_mnts)        AS queue_mnts
            , SUM(run_mnts)          AS run_mnts
            , SUM(io_mnts)           AS io_mnts
            , SUM(run_plus_io_mnts)  AS run_plus_io_mnts
            , SUM(client_mnts)       AS client_mnts
        FROM
            s
        GROUP BY 1
    )
    , u AS (
        SELECT * FROM a
        UNION ALL SELECT * FROM s
    )
    SELECT pool, wait_lock_mnts, wait_prep_mnts, prep_mnts, queue_mnts, run_mnts, io_mnts, run_plus_io_mnts, client_mnts FROM u ORDER BY lpool;

CREATE TEMP VIEW wl_profiler_step_sum_v AS
    WITH
    s AS (
        SELECT 1 AS ord, SUM(wait_lock_mnts)                    AS s, 'Wait_Lock' AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 2 AS ord, SUM(wait_prep_mnts)          AS s, 'Wait_Prep' AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 3 AS ord, SUM(prep_mnts)               AS s, 'Prepare'   AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 4 AS ord, SUM(queue_mnts)              AS s, 'Queue'     AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 6 AS ord, SUM(run_mnts)                AS s, 'Run'       AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 7 AS ord, SUM(io_mnts)                 AS s, 'IO'        AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 7 AS ord, SUM(run_mnts) + SUM(io_mnts) AS s, 'Run+IO'    AS n FROM wl_profiler_sum_log_query
        UNION ALL SELECT 8 AS ord, SUM(client_mnts)             AS s, 'Client'    AS n FROM wl_profiler_sum_log_query
    )
    SELECT n, s FROM s ORDER BY ord;

\COPY (SELECT * FROM wl_profiler_sum_log_query ORDER BY 1,2,3,4) TO 'wl_profiler_data.csv' WITH DELIMITER ','

\COPY (SELECT * FROM wl_profiler_user_sum_v) to 'wl_profiler_user.csv' WITH DELIMITER ','
    --
\COPY (SELECT * FROM wl_profiler_app_sum_v) to 'wl_profiler_app.csv' WITH DELIMITER ','
    --
\COPY (SELECT * FROM wl_profiler_pool_sum_v) to 'wl_profiler_pool.csv' WITH DELIMITER ','
    --
\COPY (SELECT * FROM wl_profiler_step_sum_v) to 'wl_profiler_step.csv' WITH DELIMITER ','