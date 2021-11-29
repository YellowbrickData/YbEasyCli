/* ****************************************************************************
** session_smry_p()
**
** Current seesions aggregated by db, user, state, app, ip, etc...
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
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.03.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**      currrent_ts       |   db_name   |  user_name  | app_name |  client_ip  | sessions | waiting | state  | min_secs | max_secs |    min_connected_at    | min_mins | max_mins
** -----------------------+-------------+-------------+----------+-------------+----------+---------+--------+----------+----------+------------------------+----------+----------
** 2019-06-21 17:49:49-07 | yellowbrick | yellowbrick | ybsql    | 172.16.10.6 |        1 |       0 | active |       -0 |       -0 | 2019-06-21 17:38:09-07 |       11 |       11
** 2019-06-21 17:49:49-07 | dbo         | yellowbrick | ybsql    | 172.16.60.3 |        1 |       0 | idle   |      743 |      743 | 2019-06-21 15:47:45-07 |      122 |      122
** 2019-06-21 17:49:49-07 | kick        | kick        | DBeaver  | 172.16.10.6 |        2 |       0 | idle   |       39 |       40 | 2019-06-21 17:49:08-07 |        0 |        0
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS session_smry_t CASCADE
;

CREATE TABLE session_smry_t
(
      currrent_ts       timestamp without time zone            
   ,  db_name           character varying(128)                 
   ,  user_name         character varying(128)                 
   ,  app_name          character varying(256)               
   ,  client_ip         character varying(256)                 
   ,  sessions          bigint                                 
   ,  waiting           bigint                                 
   ,  state             character varying(256)               
   ,  min_secs          double precision                       
   ,  max_secs          double precision                       
   ,  min_connected_at  timestamp without time zone            
   ,  min_mins          double precision                       
   ,  max_mins          double precision   
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE session_smry_p()
   RETURNS SETOF session_smry_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'session_smry_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT
      date_trunc( ''secs'', CURRENT_TIMESTAMP )::TIMESTAMP                               AS currrent_ts
    , datname::VARCHAR( 128 )                                                            AS db_name
    , usename::VARCHAR( 128 )                                                            AS user_name
    , split_part( application_name, '' '', 1 )::VARCHAR( 256 )                           AS app_name
    , client_addr::VARCHAR( 256 )                                                        AS client_ip
    , COUNT( session_id )                                                                AS sessions
    , SUM( CASE WHEN waiting = ''t'' THEN 1 ELSE 0 END )                                 AS waiting
    , state::VARCHAR( 256 )                                                              AS "state"  
    , MIN( TRUNC( ABS(extract( epoch FROM CURRENT_TIMESTAMP - state_change ) )) )        AS min_secs
    , MAX( CEIL( ABS(extract( epoch FROM CURRENT_TIMESTAMP - state_change ) )) )         AS max_secs
    , MIN( date_trunc( ''secs'', backend_start ) )::TIMESTAMP                            AS min_connected_at
    , MIN( TRUNC( ABS(extract( epoch FROM CURRENT_TIMESTAMP - backend_start ) / 60 )) )  AS min_mins
    , MAX( CEIL( ABS(extract( epoch FROM CURRENT_TIMESTAMP - backend_start ) / 60.0 )) ) AS max_mins
   FROM
      pg_stat_activity                                             
   WHERE
      user_name NOT LIKE ''sys_ybd%''
   GROUP BY
      1, 2, 3, 4, 5, 8
   ORDER BY
      8, 2, 3, 4
   ';

   RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
   
END;   
$proc$
;

-- ALTER FUNCTION session_smry_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION session_smry_p() IS 
'Description:
Current sessions aggregated by db, user, state, app, ip, etc...
  
Examples:
  SELECT * FROM session_smry_p() 
  
Arguments:
. none

Version:
. 2020.06.15 - Yellowbrick Technical Support 
'
;