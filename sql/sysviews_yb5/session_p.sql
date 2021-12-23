/* ****************************************************************************
** session_p()
**
** Current session state details.
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
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**    db_name   | session_id | pid  |   usename   |  app   |     client     | state  | mins |
** -------------+------------+------+-------------+--------+----------------+--------+------+
**  sysviews    |  126288925 | 8432 | kick        | ybsql  | 172.16.10.5/32 | active |    0 |
**  yellowbrick |  126281816 |  896 | yellowbrick | yb-smc |                | idle   |    0 |
** ...
** ...    state_change     | locks | waits | query_id |           query_text
** ...---------------------+-------+-------+----------+--------------------------------
** ... 2020-02-17 00:46:30 |     9 |     0 |        0 | select * from session_p();
** ... 2020-02-17 00:45:46 |       |       |        0 | select oid from pg_catalog.pg_
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS session_t CASCADE
;

CREATE TABLE session_t
   (
      db_name      VARCHAR (128)
    , session_id   BIGINT
    , pid          INTEGER
    , usename      VARCHAR (128)
    , app          VARCHAR (128)
    , client       VARCHAR (128)
    , state        VARCHAR (128)
    , mins         INTEGER
    , state_change TIMESTAMP
    , locks        BIGINT
    , waits        BIGINT
    , query_id     BIGINT
    , query_text   VARCHAR (128)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE session_p()
   RETURNS SETOF session_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';   
   
   _fn_name   VARCHAR(256) := 'session_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   

BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ; 

   _sql := 'SELECT 
      TRIM (ps.datname)::VARCHAR(128)                                             AS db_name
    , ps.session_id                                                               AS session_id
    , ps.pid                                                                      AS pid
    , TRIM (ps.usename)::VARCHAR(128)                                             AS usename
    , split_part (application_name, '' '', 1)::VARCHAR(128)                       AS app
    , NVL (client_hostname::VARCHAR(128), client_addr::VARCHAR(128))              AS client
    , ps.state::VARCHAR(128)                                                      AS state
    , (extract (epoch FROM (CURRENT_TIMESTAMP - ps.state_change)) ::integer / 60) AS mins
    , date_trunc (''secs'', ps.state_change) ::timestamp                          AS state_change
    , pl.t_locks_t                                                                AS locks
    , pl.t_locks_f                                                                AS waits
    , ps.query_id                                                                 AS query_id
    , substring (query, 1, 30)::VARCHAR(128)                                      AS query_text
   FROM pg_stat_activity                                                 ps
      LEFT JOIN 
      (  SELECT pid
          , SUM (CASE WHEN granted = ''t'' THEN 1 ELSE 0 END) AS t_locks_t
          , SUM (CASE WHEN granted = ''f'' THEN 1 ELSE 0 END) AS t_locks_f
         FROM pg_locks
         GROUP BY pid
      )                                                                  pl ON ps.pid = pl.pid
   WHERE usename NOT LIKE ''sys_ybd%''
   ORDER BY query_id
   ';

   RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;    

END;   
$proc$ 
;


COMMENT ON FUNCTION session_p() IS 
'Description:
Current session state details.

Examples:
  SELECT * FROM session_p() ;
  SELECT * FROM session_p() WHERE state != ''idle'' ;
  
Arguments:
. none

Notes:
. Transformed session information similar to pg_stat_activity.

Version:
. 2020.12.09 - Yellowbrick Technical Support 
'
;