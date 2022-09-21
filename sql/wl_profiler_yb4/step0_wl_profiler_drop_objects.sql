SET SESSION AUTHORIZATION :owner;

DROP TABLE IF EXISTS wl_profiler_hrs CASCADE;

DROP TABLE IF EXISTS wl_profiler_sys_log_query CASCADE;

DROP VIEW IF EXISTS wl_profiler_sum_log_query_v CASCADE;

DROP TABLE IF EXISTS wl_profiler_sum_log_query CASCADE;

DROP VIEW IF EXISTS wl_profiler_user_sum_v;

DROP VIEW IF EXISTS wl_profiler_app_sum_v;

DROP VIEW IF EXISTS wl_profiler_pool_sum_v;

DROP VIEW IF EXISTS wl_profiler_step_sum_v;