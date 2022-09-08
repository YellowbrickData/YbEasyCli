CREATE TABLE wl_profiler_sys_log_query_ms AS
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
    sys.log_query
DISTRIBUTE ON (query_id);

ALTER TABLE wl_profiler_sys_log_query_ms OWNER TO :owner;
