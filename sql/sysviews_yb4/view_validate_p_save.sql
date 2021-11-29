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
** . 2020.12.04 - Yellowbrick Technical Support 
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

   _db_id     BIGINT;
   _db_name   VARCHAR( 128 );
   _db_rec    RECORD;  
   _pred      TEXT;
   _sql       TEXT;   

   _ret_rec   view_validate_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'view_validate_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   /* Txn read_only to help protect against potential SQL injection attack writes
   */
   SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _pred := 'WHERE  
        r.schema_name ILIKE ' || quote_literal( _schema_ilike    ) || '
   AND  r.rel_name    ILIKE ' || quote_literal( _view_ilike  ) || CHR(10)
   ;

   /* Query for the databases to iterate over
   */
   _sql = 'SELECT 
      database_id AS db_id
    , name        AS db_name 
   FROM sys.database 
   WHERE name ILIKE ' || quote_literal( _db_ilike ) || ' 
   ORDER BY name
   ' ;
      
   --RAISE info '_sql = %', _sql;

   /* Iterate over each db and get the relation metadata including schema 
   */
   FOR _db_rec IN EXECUTE _sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;
   
      _sql := 'WITH owners AS
         (  SELECT ''USER''::VARCHAR(8)                       AS owner_type
             , user_id                                        AS owner_id
             , name::VARCHAR(128)                             AS owner_name
            FROM ' || quote_ident(_db_name) || '.sys.user
            UNION ALL
            SELECT ''ROLE''::VARCHAR(8)                       AS owner_type
             , role_id                                        AS owner_id
             , name::VARCHAR(128)                             AS owner_name
            FROM  ' || quote_ident(_db_name) || '.sys.role
         )
         , rels AS
         (  SELECT
               r.oid::BIGINT                                     AS rel_id     
             , ' || quote_literal( _db_name ) || '::VARCHAR(128) AS db_name
             , s.name::VARCHAR(128)                              AS schema_name 
             , r.relname::VARCHAR(128)                           AS rel_name 
             , relowner                                          AS owner_id
            FROM ' || quote_ident(_db_name)  || '.pg_catalog.pg_class r          
            JOIN ' || quote_ident(_db_name)  || '.sys.schema          s 
               ON r.relnamespace::BIGINT = s.schema_id
            WHERE
                   r.oid           > 16384
               AND r.relkind       IN ( ''v'' )
         )
      
      SELECT
         r.rel_id                     AS rel_id   
       , r.db_name                    AS db_name
       , r.schema_name                AS schema_name
       , r.rel_name                   AS rel_name
       , o.owner_name                 AS owner_name
       , CASE
           WHEN v.definition is null
              THEN ''f''::BOOLEAN
           ELSE ''t''::BOOLEAN
        END                           AS is_valid
      FROM
         rels               r
         LEFT JOIN sys.view v on r.rel_id    = v.view_id
         LEFT JOIN owners   o ON r.owner_id  = o.owner_id
      ' || _pred || '
      ORDER BY 2, 3, 4
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

COMMENT ON PROCEDURE view_validate_p( VARCHAR, VARCHAR, VARCHAR ) IS 
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

SELECT * FROM view_validate_p( 'yellowbrick' )
/*
SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 't';
SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 'f';
*/
