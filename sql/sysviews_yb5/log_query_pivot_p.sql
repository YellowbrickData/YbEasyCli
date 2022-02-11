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
 yyyy | m | mon | week_begin |    date    | dow | day | hour  |     pool     | slots | status |  username  | app_name | tags | stmt_type | grp_gb | confidence | est_grp_gb | spill | stmts | err | qued | spilled | mx_q_sec | tot_q_sec | mx_exe_sec | tot_exe_sec | mx_run_sec | tot_run_sec | mx_mb | tot_mb | mx_spl_mb | tot_spl_mb
------+---+-----+------------+------------+-----+-----+-------+--------------+-------+--------+-------------+----------+------+-----------+--------+------------+------------+-------+-------+-----+------+---------+----------+-----------+------------+-------------+------------+-------------+-------+--------+-----------+------------
 2020 | 2 | Feb | 2020-02-10 | 2020-02-10 |   1 | Mon | 12:00 | denav: admin |     2 | 00000  | kchou_test  | ybsql    |      | other     |      1 | High       |          1 | n     |     1 |   0 |    0 |       0 |      0.0 |       0.0 |        0.0 |         0.0 |        0.0 |         0.0 |    26 |     26 |         0 |          0
 2020 | 2 | Feb | 2020-02-10 | 2020-02-10 |   1 | Mon | 12:00 | denav: small |     2 | 00000  | yellowbrick | ybsql    |      | select    |      1 | High       |          1 | n     |     4 |   0 |    0 |       0 |      0.0 |       0.0 |        0.0 |         0.0 |        0.1 |         0.1 |   102 |    400 |         0 |          0
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
 , username   VARCHAR (255)
 , app_name    VARCHAR (255)
 , tags        VARCHAR (255)
 , stmt_type   VARCHAR (255)
 , gb_grp      INTEGER
 , confidence  VARCHAR (16)
 , est_gb_grp  INTEGER
 , spill       VARCHAR (16)
 , stmts       BIGINT
 , err         BIGINT
 , qued        BIGINT
 , spilled     BIGINT
 , mx_q_sec    NUMERIC (16, 1)
 , tot_q_sec   NUMERIC (16, 1)
 , mx_exe_sec  NUMERIC (16, 1)
 , tot_exe_sec NUMERIC (16, 1)
 , mx_run_sec  NUMERIC (16, 1)
 , tot_run_sec NUMERIC (16, 1)
 , mx_mb       NUMERIC (16, 0)
 , tot_mb      NUMERIC (16, 0)
 , mx_spl_mb   NUMERIC (16, 0)
 , tot_spl_mb  NUMERIC (16, 0)
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_query_pivot_p(
   _from_ts TIMESTAMP DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7) )
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
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  
   
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT
      DATE_PART (''years'', DATE_TRUNC (''week'', submit_time)::DATE)::INTEGER   AS yyyy
    , DATE_PART (''months'', DATE_TRUNC (''week'', submit_time)::DATE)::INTEGER  AS m  
    , TO_CHAR (DATE_TRUNC (''week'', submit_time::DATE), ''Mon'')::VARCHAR(16)   AS mon
    , DATE_TRUNC (''week'', submit_time)::DATE                                   AS week_begin
    , DATE_TRUNC (''day'', submit_time)::DATE                                    AS date
    , DATE_PART (''dow'', submit_time::DATE)::INTEGER                            AS dow 
    , TO_CHAR (submit_time::DATE, ''Dy'')::VARCHAR(16)                           AS day
    , (DATE_PART (''hour'', submit_time) || '':00'')::VARCHAR(23)                AS hour
    , NVL(pool_id, ''front_end'')::VARCHAR(128)                                  AS pool
    , COUNT( DISTINCT slot )::INTEGER                                            AS slots
    , split_part (state, '' '', 1)::VARCHAR(255)                                 AS state  
    , CASE WHEN username LIKE ''sys_ybd%'' THEN ''sys_ybd''
           ELSE username 
      END::VARCHAR(255)                                                          AS username
    , split_part (application_name, '' '', 1)::VARCHAR(255)                      AS app_name  
    , split_part (tags, '':'', 1)::VARCHAR(255)                                  AS tags  
  /*, type                                                                       AS type */ 
    , CASE
         WHEN type IN (''delete'', ''ctas'', ''insert'', ''update''
                     , ''select'', ''truncate table'', ''load'', ''create table as''
                     , ''unload'', ''analyze''
                     , ''copy''  , ''gc''     , ''flush'' , ''yflush'', ''ycopy''
                     , ''ybload'', ''ybunload'')          
                                                         THEN type
         WHEN type ILIKE ''%backup%'' 
          AND username =''sys_ybd_replicator''          THEN ''replicate''                     
         WHEN type ILIKE ''%restore%'' 
          AND username =''sys_ybd_replicator''          THEN ''replicated''                     
         WHEN type ILIKE ''%backup%''                    THEN ''backup''           
         WHEN type ILIKE ''%restore%''                   THEN ''restore'' 
         WHEN type ILIKE ''create%''                     THEN ''ddl''
         WHEN type ILIKE ''drop%''                       THEN ''ddl''      
         WHEN type ILIKE ''alter%''                      THEN ''ddl''                                                                
         ELSE ''other''
      END::VARCHAR(255)                                                     AS stmt_type
    , (CASE
        WHEN memory_bytes < 0                   THEN 1073741824::INTEGER
        WHEN memory_bytes < 1073741824          THEN 1::INTEGER
        ELSE 2^ (CEIL(log (2, (memory_bytes / (1024^3))::DECIMAL) ) ) 
       END
      )::INTEGER                                                            AS gb_grp   
    , memory_estimate_confidence::VARCHAR(16)                               AS confidence
    , (CASE
        WHEN memory_estimated_bytes < 0          THEN 1073741824::INTEGER
        WHEN memory_estimated_bytes < 1073741824 THEN 1::INTEGER
        ELSE 2^ (CEIL(log (2, (memory_estimated_bytes / (1024^3))::DECIMAL) ) ) 
       END
      )::INTEGER                                                            AS est_gb_grp   
    , CASE
         WHEN io_spill_write_bytes = 0            THEN ''n''
         ELSE                                       ''y''
      END::VARCHAR(16)                                                      AS spill   
    , COUNT(*)::BIGINT                                                      AS stmts
    , SUM (CASE
            WHEN error_code = ''00000''           THEN 0::BIGINT
            ELSE                                       1::BIGINT
         END
      )::BIGINT                                                             AS err
    , SUM( CASE WHEN acquire_resources_ms > 50 THEN 1 ELSE 0 END )::BIGINT  AS qued
    , SUM( CASE
              WHEN io_spill_write_bytes IS NULL   THEN 0::BIGINT       
              WHEN io_spill_write_bytes = 0       THEN 0::BIGINT
              ELSE                                     1::BIGINT
           END
         )::BIGINT                                                             AS spilled
    , ROUND( MAX( acquire_resources_ms )  / 1000.0, 1 )::NUMERIC(16, 1)     AS mx_q_sec
    , ROUND( SUM( acquire_resources_ms )  / 1000.0, 1 )::NUMERIC(16, 1)     AS tot_q_sec 
    , ROUND( MAX( run_ms )                / 1000.0, 1 )::NUMERIC(16, 1)     AS mx_exe_sec
    , ROUND( SUM( run_ms )                / 1000.0, 1 )::NUMERIC(16, 1)     AS tot_exe_sec
    , ROUND( MAX( run_ms )                / 1000.0, 1 )::NUMERIC(16, 1)     AS mx_run_sec
    , ROUND( SUM( run_ms )                / 1000.0, 1 )::NUMERIC(16, 1)     AS tot_run_sec    
    , CEIL( MAX( memory_bytes )          /( 1024.0^2 ))::NUMERIC(16, 0)     AS mx_mb
    , CEIL( SUM( memory_bytes )          /( 1024.0^2 ))::NUMERIC(16, 0)     AS tot_mb 
    , CEIL( MAX( io_spill_space_bytes )  /( 1024.0^2 ))::NUMERIC(16, 0)     AS mx_spl_mb
    , CEIL( SUM( io_spill_space_bytes )  /( 1024.0^2 ))::NUMERIC(16, 0)     AS tot_spl_mb 
   /*
    , CEIL( MAX( io_spill_write_bytes )  /( 1024.0^2 ))             AS mx_spl_wrt_mb 
    , CEIL( MAX( io_spill_write_bytes )  /( 1024.0^2 ))             AS mx_spl_wrt_mb 
    , add rows also
   */
   FROM
      sys.log_query
   WHERE
      submit_time    > ' || quote_literal( _from_ts ) || '::TIMESTAMP
      --AND pool_id    IS NOT NULL
      --AND username  NOT LIKE ''sys_ybd%''
      --AND type       NOT IN ( ''drop table'', ''analyze'' )
   GROUP BY
      1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14,15, 16, 17, 18, 19
   ORDER BY
      1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14,15, 16, 17, 18, 19
   ';

   --RAISE INFO '_sql = %', _sql;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
   
END;   
$proc$
;


COMMENT ON FUNCTION log_query_pivot_p( TIMESTAMP ) IS 
'Description:
Queries for the last week aggregated by hour for use in WLM pivot table analysis.
See the Excel Pivot table worksheet that is included in the sysviews source.
  
Examples:
  SELECT * FROM log_query_pivot_p() 
  
Arguments:
. _from_ts - (optional) Starting timestamp of statments to analyze. Default: 
             begining of previous week (Sunday).

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
. 2022.02.10 - Yellowbrick Technical Support
'
;