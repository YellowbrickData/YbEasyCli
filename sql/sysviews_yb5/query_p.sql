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
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.05.07 - Yellowbrick Technical Support 
** . 2021.04.20 - Yellowbrick Technical Support 
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.04.25 - Yellowbrick Technical Support 
** . 2019.12.05 - Yellowbrick Technical Support 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
**
** Example result:
**
**  query_id  | transaction_id | session_id | pool_id | state | username  | app_name | tags |  type  | rows 
** -----------+----------------+------------+---------+-------+-----------+----------+------+--------+------
**  208514762 |              0 |     890238 |         | plan  | kick      | ybsql    |      | select |    0 
**  207624351 |              0 |     884757 |         | parse | redpoint2 | pgAdmin  |      | update |    0 
** ...
** ... |     submit_time     | restart_sec | prep_sec | cmpl_sec | que_sec | exec_sec | tot_sec | spill_mb | max_mb | query_text
** ... +---------------------+-------------+----------+----------+---------+----------+---------+----------+--------+----------------
** ... | 2021-05-07 16:57:02 |             |          |          |         |          |     0.0 |        0 |      0 | select * from q
** ... | 2021-05-07 14:51:31 |             |          |          |         |          |  7530.4 |        0 |      0 | UPDATE settings 
** 
*/
DROP TABLE IF EXISTS public.query_t CASCADE ;
CREATE TABLE public.query_t
(
   query_id                   BIGINT  
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
 , cmpl_sec                   NUMERIC( 19, 1 ) 
 , que_sec                    NUMERIC( 19, 1 )
 , exec_sec                   NUMERIC( 19, 1 )
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

   _fn_name     VARCHAR(256) := 'query_steps_p';
   _prev_tags   VARCHAR(256) := current_setting('ybd_query_tags');
   _tags        VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   

BEGIN

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

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
                                                                              AS prep_sec
   , ROUND( (compile_ms + wait_compile_ms ) / 1000.0, 2 )::DECIMAL(19,1)      AS cmpl_sec                                                                             
   , ROUND( acquire_resources_ms            / 1000.0, 2 )::DECIMAL(19,1)      AS que_sec   
   , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)      AS run_sec
   , ROUND( total_ms                        / 1000.0, 2 )::DECIMAL(19,1)      AS tot_sec
   , ROUND( memory_bytes                    / 1024.0^2, 2 )::DECIMAL(19,0)    AS max_mb
   , ROUND( io_spill_space_bytes            / 1024.0^2, 2 )::DECIMAL(19,0)    AS mx_spill_mb   
   /* add total spill written */
   , TRANSLATE( SUBSTR( query_text, 1,' || _query_chars ||' ), e''\n\t'', ''  '' )::VARCHAR(60000) AS query_text
   FROM
     sys.query
   WHERE type NOT IN (''system'', ''unknown'') 
     AND STRPOS(username, ''sys_ybd'') != 1
     AND ' || _pred;
    
   --RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;  
  
END;
$proc$
;


COMMENT ON FUNCTION query_p( VARCHAR, INTEGER ) IS 
'Description:
Transformed subset of sys.query columns for currently running statements. 

Examples:
  SELECT * FROM query_p();

Arguments:
. None

Revision:
. 2021.12.09 - Yellowbrick Technical Support 
'
;
