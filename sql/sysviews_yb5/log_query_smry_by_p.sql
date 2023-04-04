/* log_query_smry_by_p.sql
**
** Return an aggregated subset of the sys.log_query columns with an optional argument
** for begining date for WLM effectiveness evaluation.
**
** Usage: See COMMENT ON FUNCTION below after CREATE PROCEDURE for usage and notes.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
**
** Revision History:
** . 2023.03.20 - Added _to_ts and period.
** . 2022.07.08 - Fixed AVG spill; now avg of only stmts that did spill.
**                exe secs now includes io wait.
**                Fixed av_ru_sec column order problem.
** . 2022.04.11 - Fixed av_exe_sec & mx_exe_sec.
** . 2021.12.23 - Added cols: sys_ybd, rstrts, errs, av_exe_sec, mx_exe_sec.
** . 2021.12.09 - Integrated with YbEasyCli
** . 2021.05.05 - Yellowbrick Technical Support  (for version 5.x)
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
      begining       |  by  |    pool     | stmts | sys_ybd | rstrts | errs | qued | que_pct | spld | spld_pct 
---------------------+------+-------------+-------+---------+--------+------+------+---------+------+----------
 2023-03-01 00:00:00 | hour | front_end   |  5795 |    5795 |      0 |    0 |    0 |     0.0 |    0 |      0.0 
 2023-03-01 00:00:00 | hour | prod: large |     1 |       0 |      0 |    0 |    0 |     0.0 |    0 |      0.0 
 2023-03-01 00:00:00 | hour | system      |  1287 |    1287 |      0 |    0 |    0 |     0.0 |    0 |      0.0 
 2023-03-01 01:00:00 | hour | front_end   |  5801 |    5801 |      0 |    0 |    0 |     0.0 |    0 |      0.0 
 2023-03-01 01:00:00 | hour | prod: large |     1 |       0 |      0 |    0 |    1 |   100.0 |    0 |      0.0 
...| av_que_sec | mx_que_sec | av_exe_sec | mx_exe_sec | av_run_sec | mx_run_sec | av_mb | mx_mb | av_spl_mb | mx_spl_mb
...+------------+------------+------------+------------+------------+------------+-------+-------+-----------+-----------
...|            |            |            |            |        0.0 |        0.0 |     0 |     0 |           |         0
...|        0.0 |        0.0 |        0.0 |        0.0 |        0.1 |        0.1 |   499 |   499 |           |         0
...|        0.0 |        0.0 |        0.1 |        2.6 |        0.1 |        2.6 |  1275 |  1762 |           |         0
...|            |            |            |            |        0.0 |        0.0 |     0 |     0 |           |         0
...|        0.1 |        0.1 |        0.0 |        0.0 |        0.1 |        0.1 |   499 |   499 |           |         0
** ... 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_smry_by_t CASCADE ;
CREATE TABLE log_query_smry_by_t
   (
      begining   TIMESTAMP
    , "by"       VARCHAR (16) 
    , pool       VARCHAR (128)
    , stmts      BIGINT
    , sys_ybd    BIGINT
    , rstrts     BIGINT
    , errs       BIGINT
    , qued       BIGINT
    , que_pct    NUMERIC (5, 1)
    , spld       BIGINT
    , spld_pct   NUMERIC (5, 1)
    , av_que_sec NUMERIC (15, 1)
    , mx_que_sec NUMERIC (15, 1)
    , av_exe_sec NUMERIC (15, 1)
    , mx_exe_sec NUMERIC (15, 1)                        
    , av_run_sec NUMERIC (15, 1)
    , mx_run_sec NUMERIC (15, 1)
    , av_mb      NUMERIC (15, 0)
    , mx_mb      NUMERIC (15, 0)
    , av_spl_mb  NUMERIC (15, 0)
    , mx_spl_mb  NUMERIC (15, 0)          
   )
;
 

/* ****************************************************************************
** Create the procedure.
*/

CREATE OR REPLACE PROCEDURE log_query_smry_by_p( 
        _from_ts   TIMESTAMP DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7)::TIMESTAMP 
      , _to_ts     TIMESTAMP DEFAULT  CURRENT_TIMESTAMP
      , _date_part VARCHAR   DEFAULT  'week'
      ) 
   RETURNS SETOF log_query_smry_by_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT         := '';

   _fn_name   VARCHAR(256) := 'log_query_smry_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
    
BEGIN

   -- Append sysviews proc to query tags
   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';  
  
  _sql := 'SELECT
   DATE_TRUNC(' || quote_literal(_date_part) || ', submit_time )::TIMESTAMP              AS begining
 , ' || quote_literal(_date_part) || '::VARCHAR(16)                                      AS by   
 , NVL( pool_id, ''front_end'' )                                                         AS pool
 , COUNT(*)                                                                              AS stmts
 , SUM ( CASE 
            WHEN username  LIKE ''sys_ybd%'' THEN 1
            ELSE                                  0
         END )                                                                           AS sys_ybd
 , SUM ( CASE 
            WHEN num_restart > 0 THEN 1 
            ELSE 0 
         END )                                                                           AS rstrts
 , SUM ( CASE 
            WHEN error_code IS NOT NULL AND error_code != ''00000'' THEN 1 
            ELSE 0 
         END )                                                                           AS errs         
 , SUM( CASE
         WHEN acquire_resources_ms > 50 THEN 1
         ELSE 0
      END )                                                                              AS qued
 , ROUND(( SUM(CASE
                  WHEN acquire_resources_ms > 50 THEN 1.000
                  ELSE 0.0
               END ) / COUNT(*) * 100 ), 1 )::NUMERIC(5,1)                               AS que_pct
 , SUM( CASE
         WHEN io_spill_write_bytes = 0 THEN 0
         ELSE 1
      END )                                                                              AS spld
 , ROUND(( SUM(CASE
                  WHEN io_spill_write_bytes = 0 THEN 0.000
                  ELSE 1.0000
               END ) / COUNT(*) * 100 ), 1 )::NUMERIC(5,1)                               AS spld_pct
 , ROUND( AVG( acquire_resources_ms )  / 1000, 1 )::NUMERIC(15, 1)                       AS av_que_sec
 , ROUND( MAX( acquire_resources_ms )  / 1000, 1 )::NUMERIC(15, 1)                       AS mx_que_sec
 , ROUND( AVG( run_ms - wait_run_cpu_ms   )/ 1000.0, 1 )::NUMERIC(15, 1)                 AS av_exe_sec
 , ROUND( MAX( run_ms - wait_run_cpu_ms   )/ 1000.0, 1 )::NUMERIC(15, 1)                 AS mx_exe_sec
 , ROUND( AVG( run_ms )                / 1000, 1 )::NUMERIC(15, 1)                       AS av_run_sec
 , ROUND( MAX( run_ms )                / 1000, 1 )::NUMERIC(15, 1)                       AS mx_run_sec
 , ROUND( AVG( memory_required_bytes ) /( 1024.0^2 ), 0 )::NUMERIC(15, 0)                AS av_mb
 , ROUND( MAX( memory_required_bytes ) /( 1024.0^2 ), 0 )::NUMERIC(15, 0)                AS mx_mb
 , ROUND( AVG( nullif(io_spill_write_bytes,0 ))  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)      AS av_spl_mb
 , ROUND( MAX( io_spill_write_bytes )  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)                AS mx_spl_mb
FROM
   sys.log_query
WHERE
       submit_time      >= ' || quote_literal( _from_ts ) || '
   AND submit_time      <  ' || quote_literal( _to_ts ) || ' 
-- AND application_name NOT LIKE ''yb-%''
-- AND pool             IS NOT NULL
GROUP BY
   begining, pool, by
ORDER BY
   begining, pool, by
  ';
    
   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;
$proc$
;


COMMENT ON FUNCTION log_query_smry_by_p( TIMESTAMP, TIMESTAMP, VARCHAR ) IS 
$cmnt$Description:
Aggregated sys.log_query data for a given time range and aggregation period.

Typically used in evaluation WLM rule effectiveness.

. Has optional submit date/timestamp arg for WLM effectiveness evaluation.
. If no _from_ts is specified, the default start timestamp will be the begining
  of the previous week.

Examples:
. SELECT * FROM log_query_smry_p( '2023-03-01' ,  '2023-03-02' , 'hour'  ) ;
. SELECT * FROM log_query_smry_p( DATE_TRUNC('week', CURRENT_DATE - 7), DATE_TRUNC('week', CURRENT_DATE), 'day' )
. SELECT * FROM log_query_smry_p( '2023-01-01' ,  CURRENT_TIMESTAMP, 'month' ) ;
   
Arguments:
. _from_ts TIMESTAMP - (optl) TIMESTAMP for the minimum submit_time to use.
                       Default: midnight of the first day of the previous week.
. _to_ts   TIMESTAMP - (optl) TIMESTAMP for the minimum submit_time to use.
                       Default: TIMESTAMP for the maximum submit_time to use.
. _date_part VARCHAR - (Optl) i.e. yr, mon, week, hr. See:
                       https://docs.yellowbrick.com/5.2/ybd_sqlref/dateparts_supported.html  


NOTE:
. To do a pivot table analysis of the historical queries use log_query_pivot_p.
. DROP TABLE and ANALYZE times can be are misleading. Their start time may be the start
  time of the preceeding CTAS, DROP, etc...

Version:
. 2023.03.20 - Yellowbrick Technical Support    
$cmnt$
;

