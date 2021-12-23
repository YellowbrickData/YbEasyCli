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
   query_id                   BIGINT NOT NULL
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
 , plnr_sec                   NUMERIC( 19, 1 ) 
 , cmpl_sec                   NUMERIC( 19, 1 ) 
 , que_sec                    NUMERIC( 19, 1 )
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

CREATE OR REPLACE PROCEDURE log_query_p(
   _pred VARCHAR DEFAULT ''
   , _query_chars INTEGER DEFAULT 32 ) 
   RETURNS SETOF log_query_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS
$proc$
DECLARE

   _dflt_pred  text    := ' type <> ''system'' AND username NOT LIKE ''sys_ybd%''  '; 
   _sql        text    := '';

   _fn_name   VARCHAR(256) := 'log_query_p';
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
     query_id                                                                 AS query_id
   , transaction_id                                                           AS transaction_id
   , session_id                                                               AS session_id
   , pool_id::VARCHAR( 128 )                                                  AS pool_id
   , state::VARCHAR( 50 )                                                     AS state
   , error_code::VARCHAR( 5 )                                                 AS code   
   , username::VARCHAR( 128 )                                                 AS username 
   , SPLIT_PART( application_name, '' '', 1 )::VARCHAR(128)                   AS app_name
   , tags                                                                     AS tags
   , type                                                                     AS type
   , GREATEST( rows_deleted, rows_inserted, rows_returned )                   AS rows
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                           AS submit_time
   , ROUND( restart_ms                      / 1000.0, 2 )::DECIMAL(19,1)      AS restart_sec 
   , ROUND( ( parse_ms   + wait_parse_ms + wait_lock_ms + plan_ms + wait_plan_ms + assemble_ms + wait_assemble_ms ) / 1000.0, 2 )::DECIMAL(19,1) 
                                                                              AS plnr_sec
   , ROUND( (compile_ms + wait_compile_ms ) / 1000.0, 2 )::DECIMAL(19,1)      AS cmpl_sec                                                                             
   , ROUND( acquire_resources_ms            / 1000.0, 2 )::DECIMAL(19,1)      AS que_sec   
   , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)      AS run_sec
   , ROUND( total_ms                        / 1000.0, 2 )::DECIMAL(19,1)      AS tot_sec
   , ROUND( memory_bytes                    / 1024.0^2, 2 )::DECIMAL(19,0)    AS max_mb
   , ROUND( io_spill_space_bytes            / 1024.0^2, 2 )::DECIMAL(19,0)    AS spill_mb   
   /* add total spill written */
   , TRANSLATE( SUBSTR( query_text, 1,' || _query_chars ||' ), e''\n\t'', ''  '' )::VARCHAR(60000) AS query_text
   FROM
     sys.log_query
  ' || _pred || '      
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


COMMENT ON FUNCTION log_query_p( VARCHAR, INTEGER ) IS 
'Description:
Details on completed backend statements. 

A transformed subset of sys.log_query columns with an optional argument of a 
predicate to pushdown.

The predicate pushdown is important because without it, the generate result set 
can be 100s of GBs or more. For the same reason, a default predicate of the last
hour is inserted if the predicate argument is null.

Examples:
. SELECT * FROM log_query_p( );
. SELECT * FROM log_query_p( ''WHERE submit_time > dateadd (hour, -2, current_timestamp )'' );
. SELECT * FROM log_query_p( $$WHERE submit_time > dateadd (hour, -2, current_timestamp ) 
                                      ORDER BY query_id DESC limit 10$$) ;
   
Arguments:
. _pred (optional) - A WHERE and/or ORDER BY and/or LIMIT clause. 
                    Default: predicate for only statements in the last hour.

Version:
. 2021.12.09 - Yellowbrick Technical Support 
'
;