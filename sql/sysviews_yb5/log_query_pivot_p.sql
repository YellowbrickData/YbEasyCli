/* ****************************************************************************
** log_query_pivot_p()
**
** Aggregated Yellowbrick version 5.x query history for use in a Pivot Table.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2024.07.10 - Updated to create data for 2 Pivot tables WLM_PivotTable and Performance_PivotTable
**                added rstrt and sngl_wrkr colunmns
**                changed, net, read, write, and spool gb to MB
**                used with Pivot rev 8.0
** . 2023.03.13 - Integrated into YbEasyCli
** . 2022.11.18 - Added _src_table option.
** . 2022.08.07 - Changed type: 'declare cursor' as 'select'
**                Added _to_ts arg.
**                Added grnt_gb_grp column. Requires use of a new pivot.xls.
** . 2022.04.11 - Fix exe_secs.   
** . 2022.02.23 - Yellowbrick Technical Support    
** . 2022.02.10 - Yellowbrick Technical Support                                                 
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.10.31 - Yellowbrick Technical Support 
** . 2020.07.31 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.05 - Yellowbrick Technical Support 
**
*/

/* ****************************************************************************
**  Example results:
**
yyyy|m|mon|week_begin|date      |dow|day|hour |pool     |slots|status      |username   |app_name   |tags|stmt_grp  |stmt_type         |gb_grp|grnt_gb_grp|confidence|est_gb_grp|spill|stmts|err|rstrt|qued|spilled|sngl_wrkr|mx_rows|tot_rows|mx_wt_prep_sec|tot_wt_prep_sec|mx_prep_sec|tot_prep_sec|mx_cmpl_sec|tot_cmpl_sec|mx_q_sec|tot_q_sec|mx_cpu_sec|tot_cpu_sec|mx_io_wt_sec|tot_io_wt_sec|mx_exe_sec|tot_exe_sec|mx_spool_sec|tot_spool_sec|mx_clnt_sec|tot_clnt_sec|mx_run_sec|tot_run_sec|mx_mb|tot_mb|mx_spl_mb|tot_spl_mb|net_mb|read_mb|write_mb|spool_mb|
----+-+---+----------+----------+---+---+-----+---------+-----+------------+-----------+-----------+----+----------+------------------+------+-----------+----------+----------+-----+-----+---+-----+----+-------+---------+-------+--------+--------------+---------------+-----------+------------+-----------+------------+--------+---------+----------+-----------+------------+-------------+----------+-----------+------------+-------------+-----------+------------+----------+-----------+-----+------+---------+----------+------+-------+--------+--------+
2024|7|Jul|2024-07-01|2024-07-01|  1|Mon|0:00 |front_end|    1|done        |sys_ybd*   |replication|    |ddl       |alter backup chain|     1|           |          |         1|n    |   24|  0|    0|   0|      0|        0|      0|       0|              |               |           |            |           |            |        |         |      0.00|       0.00|            |             |      0.00|       0.00|            |             |       0.00|        0.00|      0.00|       0.00|    0|     0|        0|         0|     0|      0|       0|       0|
2024|7|Jul|2024-07-01|2024-07-01|  1|Mon|0:00 |front_end|    1|done        |sys_ybd*   |replication|    |other     |session           |     1|           |          |         1|n    |   24|  0|    0|   0|      0|        0|      0|       0|              |               |           |            |           |            |        |         |      0.00|       0.00|            |             |      0.00|       0.00|            |             |       0.00|        0.00|      0.00|       0.00|    0|     0|        0|         0|     0|      0|       0|       0|
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS log_query_pivot_t CASCADE
;

CREATE TABLE log_query_pivot_t
(
   yyyy        INTEGER
 , m           INTEGER
 , mon         VARCHAR (16)
 , week_begin  DATE
 , "date"      DATE
 , dow         INTEGER
 , day         VARCHAR (16)
 , hour        VARCHAR (23)
 , pool        VARCHAR (128)
 , slots       INTEGER
 , status      VARCHAR (255)
 , username    VARCHAR (255)
 , app_name    VARCHAR (255)
 , tags        VARCHAR (255)
 , stmt_grp    VARCHAR (255)
 , stmt_type   VARCHAR (255)
 , gb_grp      INTEGER
 , grnt_gb_grp INTEGER
 , confidence  VARCHAR (16)
 , est_gb_grp  INTEGER
 , spill       VARCHAR (16)
 , stmts       BIGINT
 , err         BIGINT
 , rstrt       BIGINT
 , qued        BIGINT
 , spilled     BIGINT
 , sngl_wrkr   BIGINT
 , mx_rows         NUMERIC (28, 0)
 , tot_rows        NUMERIC (28, 0)
 , mx_wt_prep_sec  NUMERIC (16, 2)
 , tot_wt_prep_sec NUMERIC (16, 2)
 , mx_prep_sec     NUMERIC (16, 2)
 , tot_prep_sec    NUMERIC (16, 2)
 , mx_cmpl_sec     NUMERIC (16, 2)
 , tot_cmpl_sec    NUMERIC (16, 2)
 , mx_q_sec        NUMERIC (16, 2)
 , tot_q_sec       NUMERIC (16, 2)
 , mx_cpu_sec      NUMERIC (16, 2)
 , tot_cpu_sec     NUMERIC (16, 2)
 , mx_io_wt_sec    NUMERIC (16, 2)
 , tot_io_wt_sec   NUMERIC (16, 2)
 , mx_exe_sec      NUMERIC (16, 2)
 , tot_exe_sec     NUMERIC (16, 2)
 , mx_spool_sec    NUMERIC (16, 2)
 , tot_spool_sec   NUMERIC (16, 2)
 , mx_clnt_sec     NUMERIC (16, 2)
 , tot_clnt_sec    NUMERIC (16, 2)
 , mx_run_sec      NUMERIC (16, 2)
 , tot_run_sec     NUMERIC (16, 2)
 , mx_mb           NUMERIC (19, 0)
 , tot_mb          NUMERIC (19, 0)
 , mx_spl_mb       NUMERIC (19, 0)
 , tot_spl_mb      NUMERIC (19, 0)
 , net_mb          numeric (19, 0)
 , read_mb         numeric (19, 0)
 , write_mb        numeric (19, 0)
 , spool_mb        numeric (19, 0)
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_pivot_p(
        _from_ts   TIMESTAMP DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7) 
      , _to_ts     TIMESTAMP DEFAULT  CURRENT_TIMESTAMP
      , _src_table VARCHAR   DEFAULT 'sys.log_query'
   )
   RETURNS SETOF log_query_pivot_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT         := '';
   _fn_name   VARCHAR(256) := 'log_query_pivot_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _new_tags  VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  
   
   -- Append sysviews proc to query tags
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _new_tags );

   _sql := $SQL$SELECT
   DATE_PART ('years', DATE_TRUNC ('week', submit_time)::DATE)::INTEGER                  AS yyyy
 , DATE_PART ('months', DATE_TRUNC ('week', submit_time)::DATE)::INTEGER                 AS m
 , TO_CHAR (DATE_TRUNC ('week', submit_time::DATE), 'Mon')::VARCHAR(16)                  AS mon
 , DATE_TRUNC ('week', submit_time)::DATE                                                  AS week_begin
 , DATE_TRUNC ('day', submit_time)::DATE                                                   AS date
 , DATE_PART ('dow', submit_time::DATE)::INTEGER                                           AS dow
 , TO_CHAR (submit_time::DATE, 'Dy')::VARCHAR(16)                                          AS day
 , (DATE_PART ('hour', submit_time) || ':00')::VARCHAR(23)                               AS hour
 , NVL(pool_id, 'front_end')::VARCHAR(128)                                                 AS pool
 , (COUNT( DISTINCT slot ) + 1)::INTEGER                                                     AS slots
 , (CASE
      WHEN LEFT(error_code, 2) = '01'                                      THEN 'warning'
      WHEN LEFT(error_code, 2) IN('26','34','3D','3F','42','P0') THEN 'syntax_error'
      ELSE                                                                        SPLIT_PART (state, ' ', 1)
   END)::VARCHAR(255)                                                                        AS status
 , CASE WHEN username LIKE 'sys_ybd%' THEN 'sys_ybd*'
        ELSE username
   END::VARCHAR(255)                                                                         AS username
 , REGEXP_EXTRACT(application_name, '\s*\w+')::VARCHAR(255)                                AS app_name
 , REGEXP_EXTRACT(tags, '\s*\w+')::VARCHAR(255)                                            AS tags
 , CASE
      WHEN type IN ('delete', 'ctas', 'insert', 'update'
                  , 'select', 'truncate table', 'load', 'create table as'
                  , 'unload', 'analyze', 'fetch'
                  , 'copy'  , 'gc'     , 'flush' , 'yflush', 'ycopy'
                  , 'ybload', 'ybunload')
                                                      THEN type
      WHEN type =     'declare cursor'              THEN 'select'
      WHEN username = 'sys_ybd_replicator'
           AND type = 'backup'                      THEN 'replicate'
      WHEN username = 'sys_ybd_replicator'
           AND type = 'restore'                     THEN 'replicated'
      WHEN type =     'backup'                      THEN 'backup'
      WHEN type =     'restor%'                     THEN 'restore'
      WHEN type ILIKE 'create%'                     THEN 'ddl'
      WHEN type ILIKE 'drop%'                       THEN 'ddl'
      WHEN type ILIKE 'alter%'                      THEN 'ddl'
      ELSE 'other'
   END::VARCHAR(255)                                                                         AS stmt_grp
 , type::VARCHAR(255)                                                                        AS stmt_type
 , (CASE
     WHEN memory_bytes_max < 0                            THEN 1073741824::INTEGER
     WHEN memory_bytes_max < 1073741824                   THEN 1::INTEGER
     ELSE 2^ (CEIL(log (2, (memory_bytes_max / (1024.0^3))::DECIMAL) ) )
    END
   )::INTEGER                                                                                AS gb_grp
 ,(CASE
     WHEN memory_granted_bytes < 0                    THEN 1073741824::INTEGER
     WHEN memory_granted_bytes < 1073741824           THEN 1::INTEGER
     ELSE 2^ (CEIL(log (2, (memory_granted_bytes / (1024.0^3))::DECIMAL) ) )
    END
   )::INTEGER                                                                                AS grnt_gb_grp
 , memory_estimate_confidence::VARCHAR(16)                                                   AS confidence
 , (CASE
     WHEN memory_estimated_bytes < 0                  THEN 1073741824::INTEGER
     WHEN memory_estimated_bytes < 1073741824         THEN 1::INTEGER
     ELSE 2^ (CEIL(log (2, (memory_estimated_bytes / (1024^3))::DECIMAL) ) )
    END
   )::INTEGER                                                                                AS est_gb_grp
 , CASE
      WHEN io_spill_space_bytes_max = 0               THEN 'n'
      ELSE 'y'
   END::VARCHAR(16)                                                                          AS spill
 , COUNT(*)::BIGINT                                                                          AS stmts
 , SUM (CASE
         WHEN status = 'error'                      THEN 1::BIGINT
         ELSE 0::BIGINT
      END
   )::BIGINT                                                                                 AS err
 , SUM( DECODE( num_restart, 0, 0, 1 ) )::BIGINT                                             AS rstrt 
 , SUM( CASE WHEN acquire_resources_ms > 50           THEN 1 ELSE 0 END )::BIGINT            AS qued
 , SUM( CASE
           WHEN io_spill_space_bytes_max IS NULL      THEN 0::BIGINT
           WHEN io_spill_space_bytes_max = 0          THEN 0::BIGINT
           ELSE 1::BIGINT
        END
      )::BIGINT                                                                              AS spilled
 , SUM( DECODE( num_workers, 1, 1, 0 ) )::BIGINT                                             AS sngl_wrkr
 , MAX( GREATEST( rows_inserted, rows_deleted, rows_returned) )::NUMERIC(28,0)               AS mx_rows
 , SUM( GREATEST( rows_inserted, rows_deleted, rows_returned) )::NUMERIC(28,0)               AS tot_rows

 , ROUND( MAX( wait_parse_ms + wait_lock_ms + wait_plan_ms + wait_assemble_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS mx_wt_prep_sec
 , ROUND( SUM( wait_parse_ms + wait_lock_ms + wait_plan_ms + wait_assemble_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS tot_wt_prep_sec
 , ROUND( MAX( parse_ms + plan_ms + assemble_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS mx_prep_sec
 , ROUND( SUM(  parse_ms + plan_ms + assemble_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS tot_prep_sec
 , ROUND( MAX( compile_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS mx_cmpl_sec
 , ROUND( SUM( compile_ms
             ) / (1000.0 * 60), 2 )::NUMERIC(16,2)                                           AS tot_cmpl_sec
 , ROUND( MAX( acquire_resources_ms )                     / 1000.0, 2 )::NUMERIC(16,2)       AS mx_q_sec
 , ROUND( SUM( acquire_resources_ms )                     / 1000.0, 2 )::NUMERIC(16,2)       AS tot_q_sec
 , ROUND( MAX( run_ms - NVL(wait_run_cpu_ms,0) - NVL(wait_run_io_ms,0) )/ 1000.0, 2 )::NUMERIC(16,2)
                                                                                             AS mx_cpu_sec
 , ROUND( SUM( run_ms - NVL(wait_run_cpu_ms,0) - NVL(wait_run_io_ms,0) )/ 1000.0, 2 )::NUMERIC(16,2)
                                                                                             AS tot_cpu_sec
 , ROUND( MAX( wait_run_io_ms )                           / 1000.0, 2 )::NUMERIC(16,2)       AS mx_io_wt_sec
 , ROUND( SUM( wait_run_io_ms )                           / 1000.0, 2 )::NUMERIC(16,2)       AS tot_io_wt_sec
 , ROUND( MAX( run_ms - NVL(wait_run_cpu_ms,0) )          / 1000.0, 2 )::NUMERIC(16,2)       AS mx_exe_sec
 , ROUND( SUM( run_ms - NVL(wait_run_cpu_ms,0) )          / 1000.0, 2 )::NUMERIC(16,2)       AS tot_exe_sec
 , ROUND( MAX( spool_ms )                                 / 1000.0, 2 )::NUMERIC(16,2)       AS mx_spool_sec
 , ROUND( SUM( spool_ms )                                 / 1000.0, 2 )::NUMERIC(16,2)       AS tot_spool_sec
 , ROUND( MAX( client_ms )                                / 1000.0, 2 )::NUMERIC(16,2)       AS mx_clnt_sec
 , ROUND( SUM( client_ms )                                / 1000.0, 2 )::NUMERIC(16,2)       AS tot_clnt_sec
 , ROUND( MAX( run_ms )                                   / 1000.0, 2 )::NUMERIC(16,2)       AS mx_run_sec
 , ROUND( SUM( run_ms )                                   / 1000.0, 2 )::NUMERIC(16,2)       AS tot_run_sec
 
 , CEIL( MAX( memory_bytes_max )                          /( 1024.0^2 ))::NUMERIC(19,0)      AS mx_mb
 , CEIL( SUM( memory_bytes_max )                          /( 1024.0^2 ))::NUMERIC(19,0)      AS tot_mb
 , CEIL( MAX( io_spill_space_bytes_max )                  /( 1024.0^2 ))::NUMERIC(19,0)      AS mx_spl_mb
 , CEIL( SUM( io_spill_space_bytes_max )                  /( 1024.0^2 ))::NUMERIC(19,0)      AS tot_spl_mb

 , CEIL( SUM(io_network_bytes     )                       /( 1024.0^2 ))::NUMERIC(19,0)      AS net_mb
 , CEIL( SUM(io_client_read_bytes )                       /( 1024.0^2 ))::NUMERIC(19,0)      AS read_mb
 , CEIL( SUM(io_client_write_bytes)                       /( 1024.0^2 ))::NUMERIC(19,0)      AS write_mb
 , CEIL( SUM(io_spool_write_bytes )                       /( 1024.0^2 ))::NUMERIC(19,0)      AS spool_mb

FROM
   $SQL$ || _src_table || $SQL$
WHERE
       (submit_time::DATE >= DATE_TRUNC('WEEK', $SQL$ || quote_literal( _from_ts ) || $SQL$::TIMESTAMP)::DATE)
   AND (submit_time::DATE <= $SQL$ || quote_literal( _to_ts   ) || $SQL$::TIMESTAMP::DATE)
GROUP BY
   1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
ORDER BY
   1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
$SQL$;

   --RAISE INFO '_sql = %', _sql;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
   
END;   
$proc$
;


COMMENT ON FUNCTION log_query_pivot_p( TIMESTAMP, TIMESTAMP, VARCHAR ) IS 
$cmnt$Description:
Satements for the last week aggregated by hour for use in WLM_PivotTable and Performance_PivotTable analysis.
See the Excel Pivot table worksheet that is included in the sysviews source.
Examples:
  SELECT * FROM log_query_pivot_p(); 
  SELECT * FROM log_query_pivot_p( '2022-08-08', '2022-08-12 16:00:00' ); 
  
Arguments:
. _from_ts (optional) - Starting timestamp of statements to analyze. Default: 
             begining of previous week (Sunday).
. _to_ts   (optional) - Ending timestamp of statements to analyze. Default: 
             now().
. _to_ts   (optional) - Ending timestamp of statements to analyze. Default: 
             now().

Notes:
. A number of fields have been modified so that the number of distinct values
  they return is limited  as makes sense for this kind of aggregated analysis.
  They are:
  . stmt_type: These are similar to many of the statement types in sys.log_query. 
               . CREATE (except for CTAS), ALTER, and DROP are rolled up into "ddl".
               . Statement types often taking little time are rolled into "other".    
                 This is types other than: analyze, copy, ctas, delete, insert, 
                 update, select, truncate table, load, create table as, gc, flush,
                 unload, yflush, ycopy , ybload, and ybunload.  
  . tags     : ybd_query_tags is split on the colon character ":" and only the
               first part is retained. i.e. "etl:daily:20200221" becomes "etl".
  . app_name : The application_name is split on the space character " " and only 
               the first part is retained.
  . status   : Only the status code is retained. Error messages are discarded.      
. This procedure will typically return a large number of rows and consume more 
  memory than the default admin pool provides. If the query runs out of memory
  or takes too long because it is spilling, add a WLM rule to move it to a 
  large pool.

Version:
. 2023.03.13 - Yellowbrick Technical Support
$cmnt$
;



