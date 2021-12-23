/* log_query_timing_p.sql
**
** Return a transformed subset of the sys.log_query columns with an optional argument
** for predicate to push down into the dynamically generated query.
**
** . Use care in using and granting permissions on this procedure as it is potentially
**  subject to malicious SQL attacks.
** . This procedure is designed to be created by a superuser in order that users can
**  see the statements run by all users.
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
** Revision History:
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.08 - Yellowbrick Technical Support 
** . 2021.04.20 - Yellowbrick Technical Support   
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id |     submit_time     | txn_id | session_id | pool_id | state | code  |      username      |  app_name   | tags |  type   
** ----------+---------------------+--------+------------+---------+-------+-------+--------------------+-------------+------+---------
**   3720709 | 2021-04-16 14:37:08 |  83490 |     151295 | large   | done  | 00000 | sys_ybd_replicator | replication |      | restore 
**   9251534 | 2021-04-19 12:15:41 | 384713 |     370080 | large   | done  | 00000 | sys_ybd_replicator | replication |      | restore 
**   9304023 | 2021-04-19 13:10:30 | 386807 |     371880 | large   | done  | 00000 | sys_ybd_replicator | replication |      | restore 
** ...
** ... |   rows    | prep_sec | cmpl_sec | que_sec | exec_sec | tot_sec | spool_sec | client_sec | restarts | mem_mb | spill_mb |            query_text
** ... +-----------+----------+----------+---------+----------+---------+-----------+------------+----------+--------+----------+----------------------------------
** ... | 120000120 |    116.0 |      2.1 |     0.8 |  32126.2 | 32245.6 |           |        2.1 |        0 |   1626 |        0 | YRESTORE WITH KEY 'BQvhRkvxasqTr
** ... | 120000120 |     25.2 |      1.3 |     0.9 |  31234.6 | 31262.4 |           |        2.0 |        0 |   1628 |        0 | YRESTORE WITH KEY 'Dq8VgN9jvm477
** ... | 120000120 |     27.1 |      1.5 |     0.8 |  31108.6 | 31138.0 |           |        2.1 |        0 |   1624 |        0 | YRESTORE WITH KEY 'MGiaVZIw2LzUM
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_timing_t CASCADE ;
CREATE TABLE         log_query_timing_t
(
   query_id                   BIGINT  
 , submit_time                TIMESTAMP     
 , txn_id                     BIGINT
 , session_id                 BIGINT
 , pool_id                    VARCHAR( 128 )
 , state                      VARCHAR(  50 )  
 , code                       VARCHAR(   5 )
 , username                   VARCHAR( 128 )
 , app_name                   VARCHAR( 128 )
 , tags                       VARCHAR( 255 )
 , type                       VARCHAR( 128 )
 , rows                       BIGINT
 , plnr_sec                   NUMERIC( 19, 1 ) 
 , cmpl_sec                   NUMERIC( 19, 1 ) 
 , que_sec                    NUMERIC( 19, 1 )
 , restart_sec                NUMERIC( 19, 1 ) 
 , run_sec                    NUMERIC( 19, 1 )
 , tot_sec                    NUMERIC( 19, 1 )
 , spool_sec                  NUMERIC( 19, 1 )
 , client_sec                 NUMERIC( 19, 1 )
 , restarts                   INTEGER
 , mem_mb                     NUMERIC( 19, 0 )
 , spill_mb                   NUMERIC( 19, 0 )
 , query_text                 VARCHAR( 60000 )
)
DISTRIBUTE ON ( txn_id )
;
 

/* ****************************************************************************
** Create the procedure.
*/

CREATE OR REPLACE PROCEDURE log_query_timing_p( _pred VARCHAR DEFAULT '' ) 
   RETURNS SETOF log_query_timing_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _dflt_pred  text    := ' type <> ''system'' AND username NOT LIKE ''sys_ybd%''  '; 
   _sql        text    := '';

   _fn_name   VARCHAR(256) := 'log_query_timing_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
  
BEGIN

   --SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;     
 
   _pred := TRIM ( _pred );
  
   IF ( _pred = '' ) THEN 
      _pred := 'WHERE  submit_time > dateadd (hours, -1, current_timestamp ) AND ' || _dflt_pred;  
   ELSEIF ( _pred  NOT ILIKE 'where%' ) THEN 
      _pred := 'WHERE  ' || _dflt_pred || ' ';
   ELSE 
      _pred := 'WHERE  ' || _dflt_pred || ' AND ' || SUBSTR( _pred, 6 );
   END IF;
  
  _sql := 'SELECT
     query_id                                                                    AS query_id
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                              AS submit_time     
   , transaction_id                                                              AS txn_id
   , session_id                                                                  AS session_id
   , pool_id::VARCHAR( 128 )                                                     AS pool_id
   , state::VARCHAR( 50 )                                                        AS state
   , error_code::VARCHAR( 5 )                                                    AS code
   , username::VARCHAR( 128 )                                                    AS username
   , SPLIT_PART( application_name, '' '', 1 )::VARCHAR( 128 )                    AS app_name
   , tags::VARCHAR( 255 )                                                        AS tags
   , type::VARCHAR( 128 )                                                        AS type
   , GREATEST( rows_deleted, rows_inserted, rows_returned )                      AS rows
   , ROUND( ( parse_ms   + wait_parse_ms + wait_lock_ms + plan_ms + wait_plan_ms + assemble_ms + wait_assemble_ms ) / 1000.0, 2 )::DECIMAL(19,1) 
                                                                                 AS plnr_sec   
   , ROUND( compile_ms /1000.0, 2)::DECIMAL(19,1)                                AS cmpl_sec
   , ROUND( acquire_resources_ms / 1000.0, 1 )::DECIMAL(19,1)                    AS que_sec
   , ROUND( restart_ms                      / 1000.0, 2 )::DECIMAL(19,1)         AS restart_sec    
   , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)         AS run_sec
   , ROUND( total_ms  / 1000.0, 1 )::DECIMAL(19,1)                               AS tot_sec
   , ROUND( spool_ms  / 1000.0, 1 )::DECIMAL(19,1)                               AS spool_sec   
   , ROUND( client_ms / 1000.0, 1 )::DECIMAL(19,1)                               AS client_sec      
   , num_restart                                                                 AS restarts
   , ROUND( memory_bytes         / 1024.0^2, 0 )::DECIMAL(19,0)                  AS mem_mb
   , ROUND( io_spill_space_bytes_max / 1024.0^2, 0 )::DECIMAL(19,0)              AS spill_mb   
   , TRANSLATE( SUBSTR( query_text, 1, 32 ), e''\n\t'', ''  '' )::VARCHAR(60000) AS query_text
  FROM
     sys.log_query 
  ' || _pred || ' 
ORDER BY query_id
  ';
    
   --RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;  
  
END;
$proc$
;


COMMENT ON FUNCTION log_query_timing_p( VARCHAR ) IS 
'Description:
Details on completed backend statements. 

A transformed subset of sys.log_query columns with an optional argument of a 
predicate to pushdown.

The predicate pushdown is important because without it, the generate result set 
can easily be 100s of GBs or more. For the same reason, a default predicate of
the last hour is inserted if the predicate argument is null.

Examples:
. SELECT * FROM log_query_timing_p( );
. SELECT * FROM log_query_timing_p( ''WHERE submit_time > dateadd (hour, -2, current_timestamp )'' );
. SELECT * FROM log_query_timing_p( $$WHERE submit_time > dateadd (hour, -2, current_timestamp ) 
                                      ORDER BY query_id DESC limit 10$$) ;
   
Arguments:
. _pred (optional) - A WHERE and/or ORDER BY and/or LIMIT clause. 
                    Default: predicate for only statements in the last hour.

Version:
. 2021.12.09 - Yellowbrick Technical Support 
'
;