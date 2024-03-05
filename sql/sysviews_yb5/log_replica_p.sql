/* ****************************************************************************
** log_replica_p.sql
**
** Currently running and historical replication statements.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2022.12.27 - Added _yb_util_filter
**                YbEasyCli inclusion
** . 2022.08.29 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  db_name | replica_name  |             snapshot_id              | state | error |     start_time      |      end_time       |   duration   | mb_sent | mb/sec | backup_session_key
** ---------+---------------+--------------------------------------+-------+-------+---------------------+---------------------+--------------+---------+--------+--------------------
**  denav   | denav_replica | odsReplication2021_22_08_22_19_36_38 | DONE  |       | 2022-08-22 19:36:38 | 2022-08-22 19:37:52 | 00:01:14.439 |     0.0 |      0 | ...XDfrcQY=
**  kchou   | kchou_replica | kchou_replica_22_08_22_19_36_37      | DONE  |       | 2022-08-22 19:36:37 | 2022-08-22 19:37:52 | 00:01:15.616 |     0.0 |      0 | ...bzfFgsur
**  denav   | denav_replica | odsReplication2021_22_08_22_19_35_38 | DONE  |       | 2022-08-22 19:35:38 | 2022-08-22 19:36:32 | 00:00:53.82  |     0.0 |      0 | ...E2gy2GfP
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS log_replica_t CASCADE
;

CREATE TABLE log_replica_t
(
   db_name            VARCHAR (128)
 , replica_name       VARCHAR (256)
 , snapshot_id        VARCHAR (256)
 , state              VARCHAR (256)
 , error              VARCHAR (256)
 , start_time         TIMESTAMP
 , end_time           TIMESTAMP
 , duration           TIME
 , mb_sent            NUMERIC (19, 1)
 , "mb/sec"           INTEGER
 , backup_session_key VARCHAR (256)
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE log_replica_p(
        _from_ts        TIMESTAMP DEFAULT  (DATE_TRUNC('week', CURRENT_DATE)::DATE - 7) 
      , _to_ts          TIMESTAMP DEFAULT  CURRENT_TIMESTAMP
      , _yb_util_filter VARCHAR   DEFAULT 'TRUE'       
   )
   RETURNS SETOF log_replica_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'log_replica_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- Append sysviews:log_replica to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _tags );
   
   -- To prevent SQL injection attack.
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);  

   _sql := $$WITH replica_status_union AS
   (  SELECT *
       , TRUE AS active
      FROM sys.replica_status
      UNION ALL
      SELECT *
       , FALSE AS active
      FROM sys.log_replica_status
   )
   SELECT d.name::VARCHAR(128)                                                       AS db_name
    , r.name::VARCHAR(256)                                                           AS replica_name
    , snapshot_id::VARCHAR(256)                                                      AS snapshot_id
    , state::VARCHAR(256)                                                            AS state
    , nvl2( error_string, SUBSTR( error_string, 1, 64 ), NULL )::VARCHAR(256)        AS error
    , date_trunc( 'sec', start_time )::TIMESTAMP                                     AS start_time
    , date_trunc( 'sec', end_time )::TIMESTAMP                                       AS end_time
    , (NVL( end_time, CURRENT_TIMESTAMP ) - start_time)::TIME                        AS duration
    , ROUND( sent_bytes / 1024.0^2, 1 )::NUMERIC (19, 1)                             AS mb_sent
    , ROUND( sent_bytes / extract( epoch FROM( duration ) ), 0 )::INTEGER            AS "mb/sec"
    , RIGHT( '...' || RIGHT( backup_session_key, 8 ), 64 )::VARCHAR (256)            AS backup_session_key
   FROM sys.replica             AS r
      JOIN sys.database         AS d
         USING( database_id )
      JOIN replica_status_union AS rs
         USING( replica_id )
   WHERE  start_time    >= $$ || quote_literal( _from_ts ) || $$::TIMESTAMP
      AND start_time    <= $$ || quote_literal( _to_ts   ) || $$::TIMESTAMP
      AND $$ || _yb_util_filter || $$
   ORDER BY active DESC, start_time DESC, db_name
   $$;

   -- RAISE INFO '_sql: %', _sql;
   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO '|| quote_literal( _prev_tags );
   
END;   
$proc$
;

   
COMMENT ON FUNCTION log_replica_p( TIMESTAMP, TIMESTAMP, VARCHAR ) IS 
$cmnt$Description:
Currently running and historical replication statements.

Similar to sys.replica_status and sys.log_replica_status.
  
Examples:
  SELECT * FROM log_replica_p(); 
  SELECT * FROM log_replica_p( '2022-08-08', '2022-08-12 16:00:00' ); 
  
Arguments:
. _from_ts TIMESTAMP (optl)  - Starting TIMESTAMP of replication generated statements. 
                               Default: begining of previous week (Sunday).
. _to_ts   TIMESTAMP (optl)  - Ending TIMESTAMP of statements to analyze. 
                               Default: now().  
. _yb_util_filter (internal) - For YbEasyCli.

Version:
. 2022.08.29 - Yellowbrick Technical Support
$cmnt$
;
