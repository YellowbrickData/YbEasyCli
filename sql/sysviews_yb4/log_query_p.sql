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
** Revision History:
** . 2020.06.15 - Yellowbrick Technical Support   
** . 2020.03.05 - Yellowbrick Technical Support   
** . 2020.02.16 - Yellowbrick Technical Support   
** . 2019.12.03 - Yellowbrick Technical Support
** . 2021.11.18 - Integrated with YbEasyCli
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id  | transaction_id | session_id | pool_id | status |  user_name  | app_name | tags |  type   | rows 
** -----------+----------------+------------+---------+--------+-------------+----------+------+---------+-----
**  377465377 |   133144426462 |     708340 | large   | 00000  | yellowbrick | ybsql    |      | analyze |  10 
**  377465065 |   133144426432 |     708340 | large   | 00000  | yellowbrick | ybsql    |      | analyze |  10 
**
** ... |    submit_time      | que_sec | plan_sec | exec_sec | lock_sec | tot_sec | spill_mb | max_mb  | query_text  
** ... +---------------------+---------+----------+----------+----------+---------+----------+---------+----------------
** ... | 2018-11-05 17:13:16 |    0.00 |     0.00 |     0.00 |     1.30 |    0.00 |   164.00 | 4060.11 | SELECT query_id
** ... | 2018-11-05 17:13:18 |    0.03 |     0.03 |     0.00 |     0.00 |    0.14 |     0.00 | 3202.65 | SELECT query_id
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
 , status                     VARCHAR( 255 ) NOT NULL
 , user_name                  VARCHAR( 255 )
 , app_name                   VARCHAR( 255 )
 , tags                       VARCHAR( 255 )
 , type                       VARCHAR( 255 )
 , rows                       BIGINT
 , submit_time                TIMESTAMP  
 , que_sec                    NUMERIC( 19, 1 )
 , plan_sec                   NUMERIC( 19, 1 )
 , exec_sec                   NUMERIC( 19, 1 )
 , lock_sec                   NUMERIC( 19, 1 )
 , tot_sec                    NUMERIC( 19, 1 )
 , max_mb                     NUMERIC( 19, 0 )
 , max_spill_mb               NUMERIC( 19, 0 )
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

   _dflt_pred  text    := ' type <> ''system'' AND user_name NOT LIKE ''sys_ybd%''  '; 
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
   , status::VARCHAR(255)                                                     AS status
   , user_name::VARCHAR(255)                                                  AS user_name
   , SPLIT_PART( application_name, '' '', 1 )::VARCHAR(255)                   AS app_name
   , tags::VARCHAR(255)                                                       AS tags
   , type::VARCHAR(255)                                                       AS type
   , GREATEST( rows_deleted, rows_inserted, rows_returned )                   AS rows
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                           AS submit_time
   , ROUND( queue_ms             / 1000.0, 2 )::DECIMAL(19,1)                 AS que_sec
   , ROUND( planning_ms          / 1000.0, 2 )::DECIMAL(19,1)                 AS plan_sec
   , ROUND( runtime_execution_ms / 1000.0, 2 )::DECIMAL(19,1)                 AS exec_sec
   , ROUND( lock_ms              / 1000.0, 2 )::DECIMAL(19,1)                 AS lock_sec
   , ROUND( total_ms             / 1000.0, 2 )::DECIMAL(19,1)                 AS tot_sec
   , ROUND( memory_bytes         / 1024.0^2, 2 )::DECIMAL(19,0)               AS max_mb
   , ROUND( io_spill_space_bytes / 1024.0^2, 2 )::DECIMAL(19,0)               AS mx_spill_mb   
   , TRANSLATE( SUBSTR( query_text, 1, ' || _query_chars ||' ), e''\n\t'', ''  '' )::VARCHAR(60000) AS query_text
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

-- ALTER FUNCTION log_query_p( VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION log_query_p( VARCHAR, INTEGER ) IS 
'Description:
Details on completed backend statements. 

A transformed subset of sys.log_query columns with an optional argument of a 
predicate to pushdown.

The predicate pushdown is important because without it, the generate result set 
can easily be 100s of GBs or more. For the same reason, a default predicate of
the last hour is inserted if the predicate argument is null.

Examples:
. SELECT * FROM log_query_p( );
. SELECT * FROM log_query_p( ''WHERE submit_time > dateadd (hour, -2, current_timestamp )'' );
. SELECT * FROM log_query_p( $$WHERE submit_time > dateadd (hour, -2, current_timestamp ) 
                                      ORDER BY query_id DESC limit 10$$) ;
   
Arguments:
. _pred (optional) - A WHERE and/or ORDER BY and/or LIMIT clause. 
                    Default: predicate for only statements in the last hour.

NOTE:
. Use care in using and granting permissions on this procedure as it is potentially
  subject to malicious SQL attacks.
. This procedure is designed to be created by a superuser in order that users can
  see the statements run by all users.

Version:
. 2020.10.11 - Yellowbrick Technical Support 
. 2021.11.18 - Integrated with YbEasyCli
'
;
