/* ****************************************************************************
** blocking_sessions_p()
**
** Return blocking sessions holding exclusive locks on tables.
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
** . 2023.06.19 - Yellowbrick Technical Support, version 1
*/

/* ****************************************************************************
**  Example results:
**
|table_id |database_name|schema_name|table_name|is_granted|lock_type          |sess_id            |sess_user  |sess_app|sess_ip        |sess_started                 |sess_state|blocked_by_sess_id|b_sess_user|b_sess_app|b_sess_ip      |b_sess_started               |b_sess_state       |
|---------|-------------|-----------|----------|----------|-------------------|-------------------|-----------|--------|---------------|-----------------------------|----------|------------------|-----------|----------|---------------|-----------------------------|-------------------|
|2,624,185|myybdb       |public     |foo       |false     |AccessExclusiveLock|719,342,622,475,495|yellowbrick|ybsql   |172.16.10.16/32|2023-06-19 13:56:31.071 -0400|active    |719342622474357   |batman     |ybsql     |172.16.10.16/32|2023-06-19 13:42:00.817 -0400|idle in transaction|

*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS blocking_sessions_t CASCADE
;

CREATE TABLE blocking_sessions_t
   (
table_id       BIGINT,
database_name  VARCHAR(256),
schema_name    VARCHAR(256),
table_name     VARCHAR(256),
is_granted     BOOLEAN,
lock_type      VARCHAR(256),
-- blocked session
sess_id        BIGINT,
sess_user      VARCHAR(256),
sess_app       VARCHAR(256),
sess_ip        VARCHAR(256),
sess_started   TIMESTAMP WITH TIME ZONE,
sess_state     VARCHAR(256),
-- blocking session
b_sess_id      BIGINT,
b_sess_user    VARCHAR(256),
b_sess_app     VARCHAR(256),
b_sess_ip      VARCHAR(256),
b_sess_started TIMESTAMP WITH TIME ZONE,
b_sess_state   VARCHAR(256)
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE blocking_sessions_p()
   RETURNS SETOF blocking_sessions_t
   LANGUAGE 'plpgsql'
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT         := '';

   _fn_name   VARCHAR(256) := 'blocking_sessions_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;

BEGIN

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;

   _sql := '-- Get all blocking sessions:
SELECT
    l.table_id
  , d.name::VARCHAR(256)                  AS database_name
  , s.name::VARCHAR(256)                  AS schema_name
  , t.name::VARCHAR(256)                  AS table_name
  , l.is_granted
  , l.lock_type::VARCHAR(256)             AS lock_type
-- blocked session info
  , l.session_id                          AS sess_id
  , u1.name::VARCHAR(256)                 AS sess_user
  , rs.application_name                   AS sess_app
  , rs.client_ip_address                  AS sess_ip
  , date_trunc(''second'', rs.start_time) AS sess_started
  , rs.state                              AS sess_state
-- blocking session info
  , l.blocked_by_session_id::BIGINT       AS b_sess_id -- why oh why is it TEXT originally?
  , u2.name::VARCHAR(256)                 AS b_sess_user
  , bs.application_name                   AS b_sess_app
  , bs.client_ip_address                  AS b_sess_ip
  , date_trunc(''second'', bs.start_time) AS b_sess_started
  , bs.state                              AS b_sess_state
FROM sys.lock AS l
  JOIN sys.session AS rs -- regular session
    JOIN sys.user AS u1 ON u1.user_id = rs.user_id
  USING (session_id)
  JOIN sys.session AS bs -- blocking session
    JOIN sys.user AS u2 ON u2.user_id = bs.user_id
  ON bs.session_id = l.blocked_by_session_id
  JOIN sys.table AS t
    JOIN sys.database AS d USING (database_id)
    JOIN sys.schema   AS s USING (schema_id, database_id)
  USING (table_id)
WHERE l.session_id != l.blocked_by_session_id
  AND l.object_type = ''TABLE''
ORDER BY l.session_id, l.table_id';

   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;

END;
$proc$
;


COMMENT ON FUNCTION blocking_sessions_p() IS
'Description:
Return blocking sessions holding exclusive locks on tables.

As of version 1 shows only table locks.

Examples:
  SELECT * FROM blocking_sessions_p();

Arguments:
. none

Version:
. 2023.06.19 - Yellowbrick Technical Support, version 1
'
;
