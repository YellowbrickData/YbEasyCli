/* ****************************************************************************
** schema_p()
**
** All relations in all databases in YBDW >= 4.0. Similar to ybsql "\d".
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
** . 2021.05.08 - Yellowbrick Technical Support
** . 2020.11.10 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example result:
** 
    db_name     | schema_id |      schema_name   |    owner_name     | owner_type
----------------+-----------+--------------------+-------------------+------------
 MikeMTest      |     10000 | information_schema | ybdadmin          | USER
 MikeMTest      |      2200 | public             | ybdadmin          | USER
 MikeMTest      |      3300 | sys                | ybdadmin          | USER
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS schema_t CASCADE
;

CREATE TABLE schema_t
   (  db_name     VARCHAR( 128 )
    , schema_id   BIGINT
    , schema_name VARCHAR( 128 ) 
    , owner_name  VARCHAR( 128 )   
    , owner_type  VARCHAR(   8 )
   )
;
  

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE schema_p(
   _db_ilike VARCHAR DEFAULT '%'
   , _schema_ilike VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF schema_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _pred         TEXT := '';
   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'schema_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
  
BEGIN  

   -- SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   _sql := '
   WITH owners AS
   (  SELECT 
       ''USER''       AS owner_type
       , user_id      AS owner_id
       , name         AS owner_name
      FROM sys.user
      UNION ALL
      SELECT 
       ''ROLE''       AS owner_type
       , role_id      AS owner_id
       , name         AS owner_name
      FROM sys.role
   )
   SELECT 
      d.name::VARCHAR( 128 )       AS db_name
    , s.schema_id::BIGINT          AS schema_id    
    , s.name::VARCHAR( 128 )       AS schema_name
    , o.owner_name::VARCHAR( 128 ) AS owner_name
    , o.owner_type::VARCHAR(   8 ) AS owner_type
   FROM sys.schema    s
   JOIN sys.database  d ON s.database_id = d.database_id
   LEFT JOIN owners   o ON s.owner_id    = o.owner_id
   WHERE  
        d.name ILIKE ' || quote_literal( _db_ilike )     || '
    AND s.name ILIKE ' || quote_literal( _schema_ilike ) || '
    AND ' || _yb_util_filter || '
   ORDER BY db_name, schema_name 
   ';
   
   --RAISE INFO '_sql=%', _sql;
   RETURN QUERY EXECUTE _sql ;
 
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 

END;   
$proc$ 
;


COMMENT ON FUNCTION schema_p( VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
All user schemas across all databases. 

Examples:
  SELECT * FROM schema_p( ''my%'' );
  SELECT * FROM schema_p( ''my_db'', ''s%'' );
  SELECT * FROM schema_p( ''%'', ''public'' );  
  
Arguments:
. _db_ilike       - (optional) An ILIKE pattern for the database name. i.e. ''yellowbrick''.
                    The default is ''%''
. _schema_ilike   - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                    The default is ''%''

Revision History:
. 2021.12.09 - Yellowbrick Technical Support 
'
;