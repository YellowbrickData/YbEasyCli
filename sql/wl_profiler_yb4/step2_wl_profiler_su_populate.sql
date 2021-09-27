INSERT INTO wl_profiler_sys_log_query
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
;
