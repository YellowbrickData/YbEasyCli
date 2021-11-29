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
** . 2020.11.18 - Integrated with YbEasyCli
*/

/* ****************************************************************************
**  Example result:
** 
**  rel_id | db_name | schema_name |    rel_name    | schema_type | owner_name
** --------+---------+-------------+----------------+----------+-------------
**   25667 | acr     | public      | a_tstz         | table    | acr
**   29486 | acr     | public      | foo            | table    | yellowbrick
**   25310 | acr     | public      | rows_generated | table    | acr
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

   _pred := 'WHERE  '
   || '     db_name ILIKE ' || quote_literal( _db_ilike ) 
   || ' AND schema_name   ILIKE ' || quote_literal( _schema_ilike )
   || ' AND ' || _yb_util_filter
   || CHR(10);


   _sql := '
   WITH relations AS
   (
      SELECT
        database_id                     AS database_id
      , schema_id                       AS schema_id
      , view_id                         AS rel_id       
      , name                            AS rel_name
      , ''view''                        AS schema_type
      , owner_id                        AS owner_id
      FROM sys.view
   )
   , owners AS
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
    , r.schema_id::BIGINT          AS schema_id    
    , s.name::VARCHAR( 128 )       AS schema_name
    , o.owner_name::VARCHAR( 128 ) AS owner_name
    , o.owner_type::VARCHAR(   8 ) AS owner_type
   FROM relations     r
   JOIN sys.database  d ON r.database_id = d.database_id
   JOIN sys.schema    s ON r.database_id = s.database_id AND r.schema_id = s.schema_id
   LEFT JOIN owners   o ON r.owner_id    = o.owner_id
   ' || _pred || '
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

-- ALTER FUNCTION schema_p( VARCHAR, VARCHAR )
--    SET search_path = pg_catalog,pg_temp;

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
. 2020.11.18 - Integrated with YbEasyCli
'
;