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
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
** . 2021.11.18 - Integrated with YbEasyCli
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
** ...| av_que_sec | mx_que_sec | 
 | mx_exe_sec | av_run_sec | mx_run_sec | av_mb | mx_mb | av_spl_mb | mx_spl_mb 
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

CREATE OR REPLACE PROCEDURE log_query_smry_p( _submit_ts TIMESTAMP DEFAULT NULL::TIMESTAMP ) 
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

   -- SET TRANSACTION READ ONLY;

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 
   
   IF ( _submit_ts IS NULL ) THEN 
      _submit_ts := DATE_TRUNC('WEEK', CURRENT_TIMESTAMP::TIMESTAMP );  
   END IF;
  
  _sql := 'SELECT
   DATE_TRUNC(''WEEK'', submit_time )::date                  AS week_begin
 , pool_id                                                 AS pool
/* 
 , split_part( application_name, '' '', 1 )                  AS app
 , user_name                                               AS "user"
 , trim(tags)                                              AS tags
 , priority                                                AS pri
 , CASE
      WHEN type LIKE ''alter%'' OR type LIKE ''create%'' OR type LIKE ''drop%'' THEN ''frontend''
      WHEN type LIKE ''grant%'' OR type LIKE ''revoke%''                      THEN ''frontend''
      WHEN type IN (''backup''  , ''restore'', ''ybackup'', ''ybrestore'' )       THEN ''bar''
      WHEN type IN (''copy''    , ''ctas'', ''delete'' , ''load'', ''insert'' 
                  , ''truncate table'' , ''update'' )                         THEN ''write''
      WHEN type IN (''analyze'' , ''flush'' , ''maintenance'', ''system'' )       THEN ''system''
      WHEN type IN (''explain'', ''fetch''   , ''select'' , ''unload'' )          THEN ''select''
      WHEN type IN ( ''create'' , ''call'' , ''deallocate'', ''describe'', ''fetch''
                   , ''show'', ''prepare'' , ''statement'', ''session'')          THEN ''frontend''
      WHEN type IN (''unknown'')                                            THEN ''unknown''
      ELSE                                                                     ''other''
   END                                                     AS type_grp
*/ 
 , COUNT(*)                                                AS stmts
 , SUM( CASE
         WHEN queue_ms > 50 THEN 1
         ELSE 0
      END )                                                AS qued
 , ROUND(( SUM(CASE
                  WHEN queue_ms > 50 THEN 1.000
                  ELSE 0.0
               END ) / COUNT(*) * 100 ), 1 )::NUMERIC(5,1) AS que_pct
 , SUM( CASE
         WHEN io_spill_write_bytes = 0 THEN 0
         ELSE 1
      END )                                                AS spld
 , ROUND(( SUM(CASE
                  WHEN io_spill_write_bytes = 0 THEN 0.000
                  ELSE 1.0000
               END ) / COUNT(*) * 100 ), 1 )::NUMERIC(5,1)               AS spl_pct
 , ROUND( AVG( queue_ms )              / 1000, 1 )::NUMERIC(15, 1)         AS av_que_sec
 , ROUND( MAX( queue_ms )              / 1000, 1 )::NUMERIC(15, 1)         AS mx_que_sec
 , ROUND( AVG( runtime_execution_ms )  / 1000, 1 )::NUMERIC(15, 1)         AS av_exe_sec
 , ROUND( MAX( runtime_execution_ms )  / 1000, 1 )::NUMERIC(15, 1)         AS mx_exe_sec
 , ROUND( AVG( runtime_ms )            / 1000, 1 )::NUMERIC(15, 1)         AS av_run_sec
 , ROUND( MAX( runtime_ms )            / 1000, 1 )::NUMERIC(15, 1)         AS mx_run_sec
 , ROUND( AVG( memory_required_bytes ) /( 1024.0^2 ), 0 )::NUMERIC(15, 0)  AS av_mb
 , ROUND( MAX( memory_required_bytes ) /( 1024.0^2 ), 0 )::NUMERIC(15, 0)  AS mx_mb
 , ROUND( AVG( io_spill_write_bytes )  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)  AS av_spl_mb
 , ROUND( MAX( io_spill_write_bytes )  /( 1024.0^2 ), 0 )::NUMERIC(15, 0)  AS mx_spl_mb
FROM
   sys.log_query
WHERE
       submit_time      > ' || quote_literal( _submit_ts ) || '
   AND application_name NOT LIKE ''yb-%''
   AND pool             IS NOT NULL
GROUP BY
   week_begin, pool
ORDER BY
   week_begin, pool 
  ';

  -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;
  
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 

END;
$proc$
;

-- ALTER FUNCTION log_query_smry_p( TIMESTAMP )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION log_query_smry_p( TIMESTAMP ) IS 
'Description:
Aggregated subset of the sys.log_query data.

. Has optional submit date/timestamp arg for WLM effectiveness evaluation.
. If no _submit_ts is specified, the default start timestamp will be the begining
  of the current week.

Examples:
. SELECT * FROM log_query_smry_p( );
. SELECT * FROM log_query_smry_p( ''2020-01-01'' );
. SELECT * FROM log_query_smry_p( ''2020-01-01 00:00:00'' ) ;
   
Arguments:
. _submit_ts (optional) - A DATE or TIMESTAMP for the minimum submit_time to use.
  Default: midnight of the first day of the current week.

NOTE:
. The first day of the week is Sunday (, not Saturday).
. To do a pivot table analysis of the historical queries use log_query_pivot_p.
. DROP TABLE and ANALYZE times are misleading. Their start time is not the start
  of the action but instead the time of the preceeding CTAS, DROP, etc...

Version:
. 2020.06.15 - Yellowbrick Technical Support    
. 2021.11.18 - Integrated with YbEasyCli    
'
;