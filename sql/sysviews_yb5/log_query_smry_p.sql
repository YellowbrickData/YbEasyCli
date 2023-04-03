/* log_query_smry_p.sql
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
** . 2023.03.29 - Change _submit_ts to _from_dt to avoid problems with overloaded
**                version of function (log_query_smry_by_p.sql)
**                Added _to_dt.
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
**  week_begin | pool  | stmts | qued | que_pct | spld | spl_pct 
** ------------+-------+-------+------+---------+------+---------
**  2020-02-03 | admin |   302 |    0 |     0.0 |    0 |     0.0 
**  2020-02-03 | small |     6 |    0 |     0.0 |    0 |     0.0 
** ...
** ...| av_que_sec | mx_que_sec | av_exe_sec | mx_exe_sec | av_run_sec | mx_run_sec | av_mb | mx_mb | av_spl_mb | mx_spl_mb 
** ...+------------+------------+------------+------------+------------+------------+-------+-------+-----------+----------- 
** ...|        0.0 |        0.0 |        0.0 |        0.1 |        0.1 |        5.0 |   188 |  2265 |         0 |         0 
** ...|        0.0 |        0.0 |        0.0 |        0.0 |        0.0 |        0.0 |    89 |    89 |         0 |         0 
** ... 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_smry_t CASCADE ;
CREATE TABLE log_query_smry_t
   (
      week_begin DATE
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

        _from_ts   TIMESTAMP DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7) 
      , _to_ts     TIMESTAMP DEFAULT  CURRENT_TIMESTAMP
      , _date_part VARCHAR   DEFAULT  'week'

CREATE OR REPLACE PROCEDURE log_query_smry_p( 
      _from_dt DATE  DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7) 
    , _to_dt   DATE  DEFAULT  CURRENT_DATE + 1
   ) 
   RETURNS SETOF log_query_smry_t 
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
   DATE_TRUNC(''WEEK'', submit_time )::date                                              AS week_begin
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
 , ROUND( AVG( NULLIF(io_spill_write_bytes,0 ))  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)      AS av_spl_mb
 , ROUND( MAX( io_spill_write_bytes )  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)                AS mx_spl_mb
FROM
   sys.log_query
WHERE
       submit_time      > ' || quote_literal( _from_dt ) || '
-- AND application_name NOT LIKE ''yb-%''
-- AND pool             IS NOT NULL
GROUP BY
   week_begin, pool
ORDER BY
   week_begin, pool 
  ';
    
   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;
$proc$
;


COMMENT ON FUNCTION log_query_smry_p( DATE ) IS 
$cmnt$Description:
Aggregated subset of the sys.log_query data.

. Has optional submit date/timestamp arg for WLM effectiveness evaluation.
. If no _from_dt is specified, the default start timestamp will be the begining
  of the previous week.

Examples:
. SELECT * FROM log_query_smry_p( );
. SELECT * FROM log_query_smry_p( '2020-01-01' );
   
Arguments:
. _from_dt (optional) - A DATE for the minimum submit_time to use.
  Default: midnight of the first day of the previous week.

NOTE:
. The first day of the week is Sunday (, not Saturday).
. To do a pivot table analysis of the historical queries use log_query_pivot_p.
. DROP TABLE and ANALYZE times are misleading. Their start time is not the start
  of the action but instead the time of the preceeding CTAS, DROP, etc...

Version:
. 2023.03.29 - Yellowbrick Technical Support    
$cmnt$
;