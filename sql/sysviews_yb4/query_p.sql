/* query_p.sql
**
** Returns a transformed subset of the sys.query columns for currently running statements. 
**
** NOTE:
**
** . This procedure needs to be created by a superuser for priveleged users to 
**   all running queries, not only their own.
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
** . 2020.04.25 - Yellowbrick Technical Support 
** . 2019.12.05 - Yellowbrick Technical Support
** . 2021.11.18 - Integrated with YbEasyCli 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id | transaction_id | session_id | pool_id | state  | user_name | app_name | tags |  type  | rows 
** ----------+----------------+------------+---------+--------+-----------+----------+------+--------+------
**           |              0 |   14138508 |         | active | veradigm  | ybsql    |      | select |      
** ...
** ...| submit_time | que_sec | plan_sec | exec_sec | lock_sec | tot_sec | spill_mb | max_mb |            query_text
** ...+-------------+---------+----------+----------+----------+---------+----------+--------+----------------------------------
** ...|             |         |          |          |          |         |          |        | SELECT * FROM public.query_p( );
** 
*/
DROP TABLE IF EXISTS public.query_t CASCADE ;
CREATE TABLE public.query_t
(
   query_id                   BIGINT
 , transaction_id             BIGINT
 , session_id                 BIGINT
 , pool_id                    VARCHAR( 128 )
 , state                      VARCHAR( 255 ) NOT NULL
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
 , spill_mb                   NUMERIC( 19, 0 )
 , max_mb                     NUMERIC( 19, 0 )
 , query_text                 VARCHAR( 60000 )
)
DISTRIBUTE ON ( transaction_id )
;
 

/* ****************************************************************************
** Create the procedure.
*/

CREATE OR REPLACE PROCEDURE public.query_p(
   _pred VARCHAR DEFAULT 'TRUE'
   , _query_chars INTEGER DEFAULT 32 )  
   RETURNS SETOF public.query_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql         TEXT    := '';

   _fn_name   VARCHAR(256) := 'query_steps_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   

BEGIN

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _sql := 'SELECT
     query_id                                                                 AS query_id
   , transaction_id                                                           AS transaction_id
   , session_id                                                               AS session_id
   , pool_id::VARCHAR( 128 )                                                  AS pool_id
   , state::VARCHAR( 255 )                                                    AS state
   , user_name::VARCHAR( 255 )                                                AS user_name
   , SPLIT_PART( application_name, '' '', 1 )::VARCHAR( 255 )                 AS app_name
   , tags::VARCHAR( 255 )                                                     AS tags
   , type::VARCHAR( 255 )                                                     AS type
   , GREATEST( rows_deleted, rows_inserted, rows_returned )::BIGINT           AS num_rows
   , date_trunc( ''secs'', submit_time )::TIMESTAMP                           AS submit_time
   , ROUND( queue_ms             / 1000.0, 2 )::DECIMAL(19,1)                 AS que_sec
   , ROUND( planning_ms          / 1000.0, 2 )::DECIMAL(19,1)                 AS plan_sec
   , ROUND( runtime_execution_ms / 1000.0, 2 )::DECIMAL(19,1)                 AS exec_sec
   , ROUND( lock_ms              / 1000.0, 2 )::DECIMAL(19,1)                 AS lock_sec
   , ROUND( total_ms             / 1000.0, 2 )::DECIMAL(19,1)                 AS tot_sec
   , ROUND( memory_bytes         / 1024.0^2, 2 )::DECIMAL(19,0)               AS max_mb
   , ROUND( io_spill_space_bytes / 1024.0^2, 2 )::DECIMAL(19,0)               AS mx_spill_mb   
   /* add total spill written */
   , TRANSLATE( SUBSTR( query_text, 1, ' || _query_chars ||' ), e''\n\t'', ''  '' )::VARCHAR(60000) AS query_text
   FROM
     sys.query
   WHERE type NOT IN (''system'', ''unknown'') AND user_name NOT LIKE ''sys_ybd%''  
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

-- ALTER FUNCTION query_p( VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION query_p( VARCHAR, INTEGER ) IS 
'Description:
Transformed subset of sys.query columns for currently running statements. 

Examples:
  SELECT * FROM query_p();

Arguments:
. None

Revision:
. 2020.06.15 - Yellowbrick Technical Support
. 2021.11.18 - Integrated with YbEasyCli
'
;
