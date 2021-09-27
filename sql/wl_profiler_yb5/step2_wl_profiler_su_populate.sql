INSERT INTO wl_profiler_sys_log_query
    --Queries for non-system users(NOT LIKE 'sys_ybd_%')
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
        , ts07_end_client
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
        , wait_run_cpu_ms      --run timing
        , wait_run_io_ms       --run timing
        , wait_run_spool_ms    --run timing
        , client_ms            --client timing
        , wait_client_ms       --client timing
        , total_ms             --overlap timing
        , cancel_ms            --overlap timing
        , restart_ms           --overlap timing
        , wlm_runtime_ms       --overlap timing
        , spool_ms             --overlap timing
        , query_text_1000
    FROM
        wl_profiler_sys_log_query_v
        JOIN wl_profiler_hrs
            ON ts01_start > start_hr AND ts01_start < finish_hr
/*
    WHERE
        username NOT LIKE 'sys_ybd_%'    
UNION ALL
    --Aggregating for system users(LIKE 'sys_ybd_%') queries, otherwise to many detail records are produced to import into excel
    (WITH
    sys_user_query_sum AS (
        SELECT
            start_hr
            , NULL::BIGINT AS query_id
            , NULL::BIGINT AS session_id
            , NULL::BIGINT AS transaction_id
            , NULL::VARCHAR(64) AS plan_id
            , state
            , username
            , application_name
            , '<masked for wl_profiler optimization>'::VARCHAR(128) AS database_name
            , type
            , tags
            , error_code
            , error_message
            , pool_id
            , priority
            , slot
            , num_workers
            , NULL::VARCHAR(38) AS longest_worker_id
            , COUNT(*) AS query_ct
            , MIN(ts01_start) AS ts01_start
            , SUM(wl_wait_lock_ms) AS wl_wait_lock_ms
            , SUM(wl_wait_prep_ms) AS wl_wait_prep_ms
            , SUM(wl_prep_ms) AS wl_prep_ms
            , SUM(wl_queue_ms) AS wl_queue_ms
            , SUM(wl_run_ms) AS wl_run_ms
            , SUM(wl_client_ms) AS wl_client_ms
            , SUM(NVL(parse_ms, 0.0)) AS parse_ms                         --prepare timing
            , SUM(NVL(wait_parse_ms, 0.0)) AS wait_parse_ms               --wait_prep timing
            , SUM(NVL(wait_lock_ms, 0.0)) AS wait_lock_ms                 --wait_lock timing
            , SUM(NVL(plan_ms, 0.0)) AS plan_ms                           --prepare timing
            , SUM(NVL(wait_plan_ms, 0.0)) AS wait_plan_ms                 --wait_prep timing
            , SUM(NVL(assemble_ms, 0.0)) AS assemble_ms                   --prepare timing
            , SUM(NVL(wait_assemble_ms, 0.0)) AS wait_assemble_ms         --wait_prep timing
            , SUM(NVL(compile_ms, 0.0)) AS compile_ms                     --prepare timing
            , SUM(NVL(wait_compile_ms, 0.0)) AS wait_compile_ms           --wait_prep timing
            , SUM(NVL(acquire_resources_ms, 0.0)) AS acquire_resources_ms --queue timing (assuming this is throttle and queue)
            , SUM(NVL(run_ms, 0.0)) AS run_ms                             --run timing
            , SUM(NVL(wait_run_cpu_ms, 0.0)) AS wait_run_cpu_ms           --run timing
            , SUM(NVL(wait_run_io_ms, 0.0)) AS wait_run_io_ms             --run timing
            , SUM(NVL(wait_run_spool_ms, 0.0)) AS wait_run_spool_ms       --run timing
            , SUM(NVL(client_ms, 0.0)) AS client_ms                       --client timing
            , SUM(NVL(wait_client_ms, 0.0)) AS wait_client_ms             --client timing
            , SUM(NVL(total_ms, 0.0)) AS total_ms                         --overlap timing
            , SUM(NVL(cancel_ms, 0.0)) AS cancel_ms                       --overlap timing
            , SUM(NVL(restart_ms, 0.0)) AS restart_ms                     --overlap timing
            , SUM(NVL(wlm_runtime_ms, 0.0)) AS wlm_runtime_ms             --overlap timing
            , SUM(NVL(spool_ms, 0.0)) AS spool_ms                         --overlap timing
        FROM
            wl_profiler_sys_log_query_v
            JOIN wl_profiler_hrs
                ON ts01_start > start_hr AND ts01_start < finish_hr
        WHERE
            username LIKE 'sys_ybd_%'
        GROUP BY
            start_hr
            , state
            , username
            , application_name
            --, database_name
            , type
            , tags
            , error_code
            , error_message
            , pool_id
            , priority
            , slot
            , num_workers
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
        , longest_worker_id
        , wl_wait_lock_ms
        , wl_wait_prep_ms
        , wl_prep_ms
        , wl_queue_ms
        , wl_run_ms
        , wl_client_ms
        , ts01_start
        , ts01_start         + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_wait_lock_ms,0)/1000.0) AS ts02_end_wait_lock
        , ts02_end_wait_lock + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_wait_prep_ms,0)/1000.0) AS ts03_end_wait_prep
        , ts03_end_wait_prep + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_prep_ms,0)/1000.0)      AS ts04_end_prep
        , ts04_end_prep      + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_queue_ms,0)/1000.0)     AS ts05_end_queue
        , ts05_end_queue     + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_run_ms,0)/1000.0)       AS ts06_end_run
        , ts06_end_run       + MAKE_INTERVAL(0,0,0,0,0,0,COALESCE(wl_client_ms,0)/1000.0)    AS ts07_end_client
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
        , wait_run_cpu_ms      --run timing
        , wait_run_io_ms       --run timing
        , wait_run_spool_ms    --run timing
        , client_ms            --client timing
        , wait_client_ms       --client timing
        , total_ms             --overlap timing
        , cancel_ms            --overlap timing
        , restart_ms           --overlap timing
        , wlm_runtime_ms       --overlap timing
        , spool_ms             --overlap timing
        , '--Aggregate of ' || query_ct || ' queries run during the hour of ' || TO_CHAR(start_hr, 'YYYY-MM-DD HH24:MI')  AS query_text_1000
    FROM
        sys_user_query_sum)
*/
;
