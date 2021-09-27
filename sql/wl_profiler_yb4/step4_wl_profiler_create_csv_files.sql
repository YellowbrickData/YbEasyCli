\copy (SELECT * FROM wl_profiler_sum_log_query ORDER BY 1,2,3,4) TO 'wl_profiler_data.csv' WITH DELIMITER ','

\copy (SELECT * FROM wl_profiler_user_sum_v) to 'wl_profiler_user.csv' WITH DELIMITER ',';

\copy (SELECT * FROM wl_profiler_app_sum_v) to 'wl_profiler_app.csv' WITH DELIMITER ',';

\copy (SELECT * FROM wl_profiler_pool_sum_v) to 'wl_profiler_pool.csv' WITH DELIMITER ',';

\copy (SELECT * FROM wl_profiler_step_sum_v) to 'wl_profiler_step.csv' WITH DELIMITER ',';
