/* ****************************************************************************
** _view_validate_p() 
** 
** Generates ddls for user views(s) (as sequetial varchar rows) including owner
** and GRANTs for a given database.
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
** . 2020.10.30 - Yellowbrick Technical Support 
*/
 
 
/* ****************************************************************************
**  Example result:
** 
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS view_validate_t CASCADE
;

CREATE TABLE view_validate_t
   (
      rel_id      BIGINT
    , db_name     VARCHAR( 128 )
    , schema_name VARCHAR( 128 ) 
    , view_name   VARCHAR( 128 )    
    , owner_name  VARCHAR( 128 ) 
    , is_valid    BOOLEAN
   )
;  

/* ****************************************************************************
** Create the procedure.
*/
DROP PROCEDURE IF EXISTS    view_validate_p( VARCHAR, VARCHAR, VARCHAR )
;

CREATE OR REPLACE PROCEDURE view_validate_p( _db_ilike     VARCHAR DEFAULT '%'
                                           , _schema_ilike VARCHAR DEFAULT '%'
                                           , _view_ilike   VARCHAR DEFAULT '%' )
RETURNS SETOF view_validate_t
AS
$$
DECLARE

   _pred      TEXT;
   _db_id    BIGINT;
   _db_rec   RECORD;  
   _sql      TEXT;   

   _ret_rec   view_validate_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'view_ddls_t';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   /* Txn read_only to help protect against potential SQL injection attack writes
   */
   SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _pred := 'WHERE  '
   || '     database_name ILIKE ' || quote_literal( _db_ilike ) 
   || ' AND schema_name   ILIKE ' || quote_literal( _schema_ilike )
   || ' AND view_name     ILIKE ' || quote_literal( _view_ilike  ) 
   || ' ' || CHR(10);
   
   _sql :='
   WITH owners AS
   ( SELECT user_id AS owner_id
    , name          AS owner_name
   FROM sys.user
   UNION ALL
   SELECT role_id AS owner_id
    , name        AS owner_name
   FROM sys.role
   )
   , usr_views AS
   ( SELECT 
      v.database_id                           AS database_id
    , v.database_name                         AS database_name
    , v.view_id                               AS view_id
    , v.schema_id                             AS schema_id
    , s.name                                  AS schema_name
    , v.name                                  AS view_name
    , v.owner_id                              AS owner_id
    , o.owner_name                            AS owner_name
    , CASE WHEN v.definition ILIKE ''ERROR%''
           THEN ''f''::BOOLEAN
           ELSE ''t''::BOOLEAN
      END                                     AS is_valid
   FROM    sys.view   v
      JOIN sys.schema s ON v.schema_id = s.schema_id AND v.database_id = s.database_id
      JOIN owners     o ON v.owner_id  = o.owner_id
   ' || _pred || '
   )
   
   SELECT 
      view_id       AS view_id
    , database_name AS db_name
    , schema_name   AS schema_name
    , view_name     AS view_name
    , owner_name    AS owner_name
    , is_valid      AS is_valid
   FROM usr_views
   ORDER BY view_id ASC
   ';
   
   RAISE INFO '_sql=%', _sql;

   FOR _ret_rec IN EXECUTE _sql 
   LOOP

      --RAISE INFO '_rec.state=%s', _rec.state;
      RETURN NEXT _ret_rec ;
      
      /*
      _ret_rec.state := 'CREATE OR REPLACE VIEW ' || _rec.schema_name || '.' || _rec.view_name || E' AS\n'
            || _rec.definition || E'\n;\n';
      RETURN NEXT _ret_rec ;
      
      _ret_rec.state := 'ALTER VIEW IF EXISTS ' || _rec.schema_name || '.' || _rec.view_name || ' WITH OWNER ' || quote_ident( _rec.owner_name ) || E'\n;\n';
      RETURN NEXT _ret_rec ;
      */
         
   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
;

COMMENT ON PROCEDURE view_validate_p( VARCHAR ) IS 
'Description:
Generates DDLs for all user views(s) in a database including GRANT and ALTER OWNER 
as sequential varchar rows.

Examples:
  SELECT   FROM view_validate_p( );
  SELECT * FROM view_validate_p( ''my_db'');
  SELECT * FROM view_validate_p( ''my_db'', ''dev%'', ''%tmp%'' );  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the database name.  i.e. ''yellowbrick%''.
                  The default is ''%''
. _schema_ilike - (optional) An ILIKE pattern for the schema name.    i.e. ''%qtr%''.
                  The default is ''%''
. _view_ilike    - (optional) An ILIKE pattern for the relation name. i.e. ''fact%''.
                  The default is ''%''

Version:
. 2020.11.25 - Yellowbrick Technical Support 
';

SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 't';
SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 'f';
