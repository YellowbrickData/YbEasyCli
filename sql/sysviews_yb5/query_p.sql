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
** . 2022.08.28 - db_name added as first column.
**                Added _date_part arg for timestamp truncation. 
**                Changed args order.
**                Added restart_number "n"
** . 2022.07.07 - Line leading spaces removed
**                IO wait is aprt of exec time
**                Condense plnr_sec & cmpl_sec into prep_secs.
** .              Add exe_secs.
**                Don't split application_name by default; now a proc option.
**                First arg of prediate replaced with boolean for showing system stmts.
** . 2022.07.01 - prep_secs updated; should not include wait time.
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
   db_name                    VARCHAR( 128 )
 , query_id                   BIGINT  
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

CREATE OR REPLACE PROCEDURE query_p( _show_all    BOOLEAN DEFAULT 't'
                                   , _query_chars INTEGER DEFAULT 32 
                                   , _date_part   VARCHAR DEFAULT 'sec'
                                   , _split_name  BOOLEAN DEFAULT 'f'
                                   )  
   RETURNS SETOF public.query_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _pred        TEXT    := ' type <> ''system'' AND username NOT LIKE ''sys_ybd%''  ';
   _sql         TEXT    := '';

   _fn_name     VARCHAR(256) := 'query_steps_p';
   _prev_tags   VARCHAR(256) := current_setting('ybd_query_tags');
   _tags        VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   

BEGIN

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';
   
   IF NOT _show_all 
   THEN 
      _pred  := ' type <> ''system'' AND username NOT LIKE ''sys_ybd%''  ';
   END IF;

   _sql := 'SELECT
     database_name ::VARCHAR( 128 )                                           AS db_name
   , query_id                                                                 AS query_id
   , num_restart                                                              AS n
   , session_id                                                               AS session_id
   , transaction_id                                                           AS transaction_id
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
   , DATE_TRUNC(' || quote_literal(_date_part) || ',submit_time)::TIMESTAMP   AS submit_time
   , ROUND( restart_ms                      / 1000.0, 2 )::DECIMAL(19,1)      AS restart_sec 
   , ROUND( ( parse_ms + plan_ms + assemble_ms + compile_ms ) / 1000.0, 2 )::DECIMAL(19,1)
                                                                              AS prep_sec
   , ROUND( acquire_resources_ms            / 1000.0, 2 )::DECIMAL(19,1)      AS que_sec   
   , ROUND( (run_ms - wait_run_cpu_ms                       ) / 1000.0, 2 )::DECIMAL(19, 1)
                                                                              AS exe_sec
   , ROUND( run_ms                          / 1000.0, 2 )::DECIMAL(19,1)      AS run_sec
   , ROUND( total_ms                        / 1000.0, 2 )::DECIMAL(19,1)      AS tot_sec
   , ROUND( memory_bytes                    / 1024.0^2, 2 )::DECIMAL(19,0)    AS max_mb
   , ROUND( io_spill_space_bytes             / 1024.0^2, 2 )::DECIMAL(19,0)   AS spill_mb                                                                             
   , REGEXP_REPLACE( SUBSTR( query_text, 1,' || _query_chars ||' ), ''(^\s+|\r\s*|\n\s*|\t\s*)'', '' '')::VARCHAR(60000)
                                                                              AS query_text 
   FROM
     sys.query
   WHERE type NOT IN (''system'', ''unknown'') 
     AND STRPOS(username, ''sys_ybd'') != 1
   ';
    
   --RAISE INFO '_sql is: %', _sql ;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
  
END;
$proc$
;


COMMENT ON FUNCTION query_p( BOOLEAN, INTEGER, VARCHAR, BOOLEAN ) IS 
$cmnt$Description:
Transformed subset of sys.query columns for currently running statements. 

Examples:
  SELECT * FROM query_p();
  SELECT * FROM query_p( 60, 't');

Arguments:
. _show_all    (optional) - Show (or hide) system and sys_ybd* statements. Default 't'.
. _query_chars (optional) - First "n" Characters of query text to display. Default 32.
. _date_part   (optional) - Precision of timestamp columns. i.e. msec, etc...  DEFAULT 'sec'.
. _split_name  (optional) - Display only the characters in the app_name up to the
                            first blank space character. Default 'f'.

Revision:
. 2022.08.28 - Yellowbrick Technical Support 
$cmnt$
;
