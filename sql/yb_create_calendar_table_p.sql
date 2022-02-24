CREATE OR REPLACE PROCEDURE yb_create_calendar_table_p(
    a_table VARCHAR(200)
    , a_start_date DATE
    , a_end_date DATE
    , a_absolute_start_date DATE)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS $proc$
--description:
--    Create a calendar dimension table.
--arguments:
--    a_table:               name of calendar table to be created, defaults to 'calendar'
--    a_start_date:          the first date in the calendar
--    a_end_date:            the last date in the calendar
--    a_absolute_start_date: the anchor date for the entire calendar table for
--        which several of the calendar columns are measured from
--    all dates are entered in the form 'YYYY-MM-DD'
DECLARE
    v_query_create_calendar_table TEXT := REPLACE(REPLACE(REPLACE(REPLACE($DDL$
CREATE TABLE <table> AS
WITH
input_cte AS (
    SELECT
        TO_DATE('<start_date>', 'YYYY-MM-DD')            AS first_date
        , TO_DATE('<end_date>', 'YYYY-MM-DD')            AS last_date
        , TO_DATE('<absolute_start_date>', 'YYYY-MM-DD') AS absolute_start_date
        , ((last_date - first_date) + 1)::BIGINT         AS total_days
)
, wrkrs_cte AS (
    /* Generate sequential values even if you have failed blades. */
    SELECT
        worker_lid AS use_lid
    FROM sys.rowgenerator
    WHERE range BETWEEN 0 and 0
)
, seq_cte AS (
    SELECT
        r.row_number +( w.use_lid * 3652059::BIGINT ) AS num
    FROM
        sys.rowgenerator AS r
        JOIN wrkrs_cte AS w
            ON r.worker_lid = w.use_lid
    WHERE
        --3652059 = TO_DATE('9999-12-31', 'YYYY-MM-DD') - TO_DATE('0000-01-01', 'YYYY-MM-DD') + 1
        range BETWEEN 1 AND 3652059
)
, cal_cte AS (
    SELECT
        first_date, last_date, absolute_start_date
        , EXTRACT( YEAR FROM absolute_start_date )   AS cal_first_year
        , EXTRACT( YEAR FROM first_date )            AS first_year
        , DATEADD(DAY, num::INT, first_date )::DATE  AS calendar_date
        , DATE_PART('WEEK', calendar_date)::INT      AS datepart_week
        , DATE_PART('MONTH', calendar_date)::INT     AS datepart_month
        , DATE_PART('YEAR', calendar_date)::INT      AS datepart_year
        , calendar_date - absolute_start_date + 1    AS day_of_calendar
        , EXTRACT(DOW FROM DATE_TRUNC('MONTH', calendar_date)) + 1 AS dow_of_day1_in_month
        , EXTRACT(DOW FROM DATE_TRUNC('YEAR', calendar_date)) + 1  AS dow_of_day1_in_year
        , EXTRACT(DOW FROM absolute_start_date) + 1                AS dow_of_day1_in_cal
    FROM seq_cte CROSS JOIN input_cte
    WHERE num < total_days
)
, enriched_cal_cte AS (
    SELECT
        calendar_date                                             AS calendar_date
        , EXTRACT( DOW FROM calendar_date )::INT + 1              AS day_of_week
        , DAY( calendar_date )::INT                               AS day_of_month
        , EXTRACT( DOY FROM calendar_date )::INT                  AS day_of_year
        , day_of_calendar                                         AS day_of_calendar
        , CEIL( EXTRACT( DAY FROM calendar_date ) / 7 )::INT      AS weekday_of_month
        , weekday_of_month -
            CASE
                WHEN dow_of_day1_in_month = 1            THEN 0
                WHEN day_of_week >= dow_of_day1_in_month THEN 1
                ELSE 0
            END                                                   AS week_of_month
        , (day_of_year-1)/7 +
            CASE
                WHEN dow_of_day1_in_year = 1            THEN 1
                WHEN day_of_week >= dow_of_day1_in_year THEN 0
                ELSE 1
            END                                                   AS week_of_year
        , CASE
            WHEN calendar_date >= absolute_start_date THEN
                (calendar_date - absolute_start_date)::INT/7 +
                CASE
                    WHEN dow_of_day1_in_cal = 1           THEN 1
                    WHEN day_of_week < dow_of_day1_in_cal THEN 1
                    ELSE 0
                END
            ELSE (calendar_date - absolute_start_date)::INT/7 +
                DECODE(TRUE, dow_of_day1_in_cal = 1, 0, -1) +
                CASE
                    WHEN day_of_week <= dow_of_day1_in_cal  THEN 1
                    WHEN dow_of_day1_in_cal = 1             THEN 0
                    ELSE 0
                END
        END AS week_of_calendar
        , CASE
            WHEN EXTRACT( MONTH FROM calendar_date )::INT IN( 1, 4, 7, 10 ) THEN 1
            WHEN EXTRACT( MONTH FROM calendar_date )::INT IN( 2, 5, 8, 11 ) THEN 2
            ELSE 3
        END                                                       AS month_of_quarter
        , datepart_month                                          AS month_of_year
        , ((( datepart_year - cal_first_year ) * 12 ) + datepart_month)::INT AS month_of_calendar
        , CASE
            WHEN EXTRACT( MONTH FROM calendar_date )::INT < 4 THEN 1
            WHEN EXTRACT( MONTH FROM calendar_date )::INT BETWEEN 4 AND 6 THEN 2
            WHEN EXTRACT( MONTH FROM calendar_date )::INT BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END                                                      AS quarter_of_year
        , ((( datepart_year - cal_first_year ) * 4 ) + quarter_of_year)::INT AS quarter_of_calendar
        , EXTRACT( YEAR FROM calendar_date )::INT                            AS year_of_calendar
        , EXTRACT( ISODOW FROM calendar_date )::INT                 AS iso_day_of_week
        , EXTRACT( WEEK FROM calendar_date )::INT                   AS iso_week
        , EXTRACT( ISOYEAR FROM calendar_date )::INT                AS iso_year
        , (CASE
            WHEN iso_year < 10   THEN '000'
            WHEN iso_year < 100  THEN '00'
            WHEN iso_year < 1000 THEN '0'
            ELSE ''
        END || iso_year::VARCHAR
        || '-W'
        || CASE
            WHEN iso_week < 10   THEN '0'
            ELSE ''
        END || iso_week::VARCHAR
        || '-' || iso_day_of_week)::VARCHAR(10)                     AS iso_date_str
        , iso_year * 1000 + iso_week * 10 + iso_day_of_week         AS iso_date_int
        , year_of_calendar * 10000 + month_of_year * 100 + day_of_month AS calendar_date_int
        , EXTRACT( EPOCH FROM calendar_date )::BIGINT                   AS calendar_date_epoch
        , EXTRACT( DOW FROM calendar_date )::INT                        AS yb_day_of_week
    FROM cal_cte
)
SELECT * FROM enriched_cal_cte ORDER BY calendar_date
DISTRIBUTE REPLICATE
SORT ON (calendar_date)
$DDL$
    , '<table>'              , a_table)
    , '<start_date>'         , TO_CHAR(a_start_date, 'YYYY-MM-DD'))
    , '<end_date>'           , TO_CHAR(a_end_date, 'YYYY-MM-DD'))
    , '<absolute_start_date>', TO_CHAR(a_absolute_start_date, 'YYYY-MM-DD'));
    --
    v_ret_value BOOLEAN := FALSE;
BEGIN
    EXECUTE v_query_create_calendar_table;
    RETURN TRUE;
END$proc$;