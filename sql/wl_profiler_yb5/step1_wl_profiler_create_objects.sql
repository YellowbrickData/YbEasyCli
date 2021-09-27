CREATE TABLE wl_profiler_hrs AS
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

CREATE VIEW wl_profiler_sys_log_query_v AS
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
    , query_text
    , pool_id
    , priority
    , slot
    , num_workers
    , longest_worker_id
    , NVL(wait_lock_ms, 0.0) AS wl_wait_lock_ms
    , NVL(wait_parse_ms, 0.0) + NVL(wait_plan_ms, 0.0) + NVL(wait_assemble_ms, 0.0) + NVL(wait_compile_ms, 0.0) AS wl_wait_prep_ms
    , NVL(parse_ms, 0.0) + NVL(plan_ms, 0.0) + NVL(assemble_ms, 0.0) + NVL(compile_ms, 0.0) AS wl_prep_ms
    , NVL(acquire_resources_ms, 0.0) AS wl_queue_ms
    , NVL(wait_run_cpu_ms, 0.0) + NVL(wait_run_io_ms, 0.0) + NVL(wait_run_spool_ms, 0.0) + NVL(run_ms, 0.0) AS wl_run_ms
    , NVL(client_ms, 0.0) + NVL(wait_client_ms, 0.0) AS wl_client_ms
    , NVL(restart_time, submit_time) AS ts01_start
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
    , SUBSTR(query_text, 1, 1000) AS query_text_1000
FROM
    sys.log_query;

CREATE TABLE wl_profiler_sys_log_query AS
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
DISTRIBUTE RANDOM;