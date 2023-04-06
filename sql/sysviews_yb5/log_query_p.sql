/* log_query_p.sql
**
** Return a transformed subset of the sys.log_query columns with an optional argument
** for predicate to push down into the dynamically generated query.
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
** NOTE:
** . Use care in using and granting permissions on this procedure as it is potentially
**   subject to malicious SQL attacks.
** . This procedure is designed to be created by a superuser in order that users can
**   see the statements run by all users.
**
** Revision History:
** . 2022.08.28 - db_name added as first column.
**                Added _date_part arg for timestamp truncation.
**                Changed args order.
**                Added restart number "n"
**                modified predicate handling
** . 2022.07.07 - line leading spaces removed
**                IO wait is aprt of exec time
**                Don't split application_name by default; now a proc option.
** . 2022.07.01 - prep_secs updated; should not include wait time.
** . 2022.06.06 - Condense plnr_sec & cmpl_sec into prep_secs.
** . 2022.05.24 - Add exe_secs.
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.07 - Yellowbrick Technical Support 
** . 2021.04.20 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support   
** . 2020.03.05 - Yellowbrick Technical Support   
** . 2020.02.16 - Yellowbrick Technical Support   
** . 2019.12.03 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id  | transaction_id | session_id | pool_id | state | code  | username | app_name | tags |  type   | rows 
** -----------+----------------+------------+---------+-------+-------+----------+----------+------+---------+------
**  176766458 |        2379156 |     767986 | admin   | done  | 00000 | kick     | ybsql    |      | ctas    |    1 
**  176766469 |        2379156 |     767986 | admin   | done  | 00000 | kick     | ybsql    |      | analyze |    0 
**  176772933 |        2379180 |     767986 | admin   | done  | 00000 | kick     | ybsql    |      | insert  |    1 
** ... 
** ... |     submit_time     | restart_sec | prep_sec | cmpl_sec | que_sec | exec_sec | tot_sec | spill_mb | max_mb | query_text
** ... +---------------------+-------------+----------+----------+---------+----------+---------+----------+--------+------------
** ... | 2021-05-05 21:14:54 |             |      0.0 |      0.8 |     0.0 |      0.0 |     0.9 |       28 |      0 | create tem
** ... | 2021-05-05 21:14:55 |             |      0.0 |      0.0 |     0.0 |      0.0 |     0.0 |       32 |      0 | ANALYZE HL  
** ... | 2021-05-05 21:15:16 |             |      0.0 |      0.9 |     0.0 |      0.0 |     0.9 |       72 |      0 | insert int
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
** Yellowbrick does not support user defined types or RETURNS TABLE. 
*/
DROP TABLE IF EXISTS log_query_t CASCADE ;
CREATE TABLE log_query_t
(
   db_name                    VARCHAR( 128 )
 , query_id                   BIGINT NOT NULL
 , n                          INTEGER
 , transaction_id             BIGINT
 , session_id                 BIGINT
 , pool_id                    VARCHAR( 128 )
 , state                      VARCHAR(  50 )  
 , code                       VARCHAR(   5 )
 , username                   VARCHAR( 128 )
 , app_name                   VARCHAR( 128 )
 , tags                       VARCHAR( 255 )
 , type                       VARCHAR( 128 )
 , rows                       BIGINT
 , submit_time                TIMESTAMP  
 , restart_sec                NUMERIC( 19, 1 ) 
 , prep_sec                   NUMERIC( 19, 1 )
 , que_sec                    NUMERIC( 19, 1 )
 , exe_sec                    NUMERIC( 19, 1 )
 , io_wt_sec                  NUMERIC( 19, 1 )
 , run_sec                    NUMERIC( 19, 1 )
 , tot_sec                    NUMERIC( 19, 1 )
 , max_mb                     NUMERIC( 19, 0 )
 , spill_mb                   NUMERIC( 19, 0 ) 
 , query_text                 VARCHAR( 60000 )
)
DISTRIBUTE ON ( transaction_id )
;
 

/* ****************************************************************************
** Create the procedure.
*/

CREATE OR REPLACE PROCEDURE log_query_p( _pred        VARCHAR DEFAULT ''
                                       , _show_all    BOOLEAN DEFAULT 't'
                                       , _query_chars INTEGER DEFAULT 32
                                       , _date_part   VARCHAR DEFAULT 'sec'
                                       , _split_name  BOOLEAN DEFAULT 'f'
                                       )
   RETURNS SETOF log_query_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sys_pred      text    := ' type <> ''system'' AND username NOT LIKE ''sys_ybd%'' ';
   _show_all_pred text    := CASE WHEN quote_literal( _show_all ) = 't' THEN ' ' ELSE _sys_pred END ;
   _dflt_pred     text    := 'submit_time > dateadd (hours, -1, current_timestamp )';
   _sql        text    := '';

   _fn_name   VARCHAR(256) := 'log_query_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
  
BEGIN

   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags );

   _pred :=  REGEXP_REPLACE( _pred, '\s*WHERE\s*', '', 'i' );

   IF ( _pred = '' ) 
   THEN
      _pred := _dflt_pred ;
   END IF;
  
   -- To prevent SQL injection attack.
   --PERFORM sql_inject_check_p('_yb_util_filter', _pred);

  _sql := 'SELECT
     database_name::VARCHAR( 128 )                                            AS database_name
   , query_id                                                                 AS query_id
   , num_restart                                                              AS n
   , transaction_id                                                           AS transaction_id
   , session_id                                                               AS session_id
   , pool_id::VARCHAR( 128 )                                                  AS pool_id
   , state::VARCHAR( 50 )                                                     AS state
   , error_code::VARCHAR( 5 )                                                 AS code   
   , username::VARCHAR( 128 )                                                 AS username 
   , CASE WHEN ' || quote_literal( _split_name ) || '
          THEN SPLIT_PART( application_name, '' '', 1 )::VARCHAR(128)
          ELSE application_name
     END                                                                      AS app_name
   , tags                                                                     AS tags
   , type                                                                     AS type
   , GREATEST( rows_deleted, rows_inserted, rows_returned )                   AS rows
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                           AS submit_time
   , ROUND( restart_ms                      / 1000.0, 2 )::DECIMAL(19,1)      AS restart_sec 
   , ROUND( ( parse_ms + plan_ms + assemble_ms + compile_ms ) / 1000.0, 2 )::DECIMAL(19,1)
                                                                              AS prep_sec
   , ROUND( acquire_resources_ms            / 1000.0, 2 )::DECIMAL(19,1)      AS que_sec   
   , ROUND( (run_ms - wait_run_cpu_ms     ) / 1000.0, 2 )::DECIMAL(19, 1)     AS exe_sec   
   , ROUND( (wait_run_io_ms               ) / 1000.0, 2 )::DECIMAL(19, 1)     AS io_wt_sec   
   , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)      AS run_sec
   , ROUND( total_ms                        / 1000.0, 2 )::DECIMAL(19,1)      AS tot_sec
   , CEIL( memory_bytes_max                / 1024.0^2, 2 )::DECIMAL(19,0)     AS max_mb
   , CEIL( io_spill_space_bytes_max        / 1024.0^2, 2 )::DECIMAL(19,0)     AS spill_mb   
   , REGEXP_REPLACE( SUBSTR( query_text, 1,' || _query_chars ||' ), ''(^\s+|\r\s*|\n\s*|\t\s*)'', '' '')::VARCHAR(60000)
                                                                              AS query_text
   FROM
     sys.log_query
   WHERE 
  ' || _show_all_pred || ' AND ' || _pred 
  ;
    
   -- RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );

  
END;
$proc$
;


COMMENT ON FUNCTION log_query_p( VARCHAR, BOOLEAN, INTEGER, VARCHAR, BOOLEAN ) IS
$cmnt$Description:
Details on completed statements.

A transformed subset of sys.log_query columns with an optional argument of a 
predicate to pushdown.

The predicate pushdown is important because without it, the generate result set 
can be 100s of GBs or more. For the same reason, a default predicate of the last
hour is inserted if the predicate argument is null.

Examples:
. SELECT * FROM log_query_p( '', 't', 60 );
. SELECT * FROM log_query_p( 'submit_time > dateadd (hour, -2, current_timestamp )' );
. SELECT * FROM log_query_p( $$WHERE submit_time > dateadd (hour, -2, current_timestamp ) 
                               ORDER BY query_id DESC LIMIT 10
                             $$) ;
   
Arguments:
. _pred (optional) - A WHERE and/or ORDER BY and/or LIMIT clause. 
                            Default: only statements in the last hour.
. _show_all    (optional) - Include type 'system' AND username LIKE 'sys_ybd%' statements.
                            DEFAULT 't'.
. _query_chars (optional) - First "n" Characters of query text to display. Default 32.
. _date_part   (optional) - Precision of timestamp columns. i.e. msec, etc...  DEFAULT 'sec'.
. _split_name  (optional) - Display only the characters in the app_name up to the
                            first blank space character. Default 'f'.

Notes:
. If no _pred predicate is provided, the results are limited to statements
  executed within the previous 1 hour.
. You can use $$ quoting for the outer quote char to not have to escape single quotes.

Version:
. 2023.04.04 - Yellowbrick Technical Support
$cmnt$
;
