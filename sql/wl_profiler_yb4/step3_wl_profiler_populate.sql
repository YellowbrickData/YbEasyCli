--This update is to deal with planner timing issues that cause reporting issues
--   TEMP TABLE DROPs incorrectly report planner time as time when the session ends
--   ANALYZE reports planner end as the time when the time CTAS completes.
UPDATE
    wl_profiler_sys_log_query
SET ts03_end_plan = ts02_end_wait_concur_users
WHERE
    UPPER(SUBSTR(query_text, 1, 15)) LIKE 'DROP TABLE ID%'
    OR UPPER(SUBSTR(query_text, 1, 15)) LIKE 'ANALYZE HLL%'
;

--There are certain cases where a lane is showing run time of more than 1 query at the same time.
--In these cases the following CTAS and UPDATE set the run time end to the time the next query starts.
CREATE TEMP TABLE wl_profiler_fixed_end_run AS
WITH s AS (
   SELECT
       query_id, pool_id, slot, ts09_end_lock, ts10_end_run
       , ROW_NUMBER() OVER (PARTITION BY pool_id, slot ORDER BY ts09_end_lock) AS rn
   FROM
       wl_profiler_sys_log_query
   WHERE
       pool_id IS NOT NULL
   ORDER BY
       1,2,3
)
SELECT
    s1.query_id
    , s2.ts09_end_lock AS ts10_end_run
FROM
    s AS s1
    LEFT JOIN s AS s2
        ON s1.pool_id = s2.pool_id
        AND s1.slot = s2.slot
        AND s1.rn = s2.rn - 1
WHERE s1.ts10_end_run > s2.ts09_end_lock
DISTRIBUTE ON (query_id)
;

UPDATE wl_profiler_sys_log_query AS slq
    SET ts10_end_run = fix.ts10_end_run
FROM wl_profiler_fixed_end_run AS fix
WHERE
    slq.query_id = fix.query_id
;

INSERT INTO wl_profiler_sum_log_query SELECT * FROM wl_profiler_sum_log_query_v;
