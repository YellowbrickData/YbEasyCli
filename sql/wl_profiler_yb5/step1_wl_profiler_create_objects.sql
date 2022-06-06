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
SORT ON (start_hr)
;