/* ****************************************************************************
** view_validate_p() 
** 
** Iterates over all views to determine which ones have missing dependencies from
** late bound views.  
** 
** Note:
** . This procedure is specific to YBD version 5.x. If you are running this 
**   against YB 3.x or 4.x, you need a different version of this procedure. 
** . Does not include _yb_util_filter as python version works differently.
**
** Usage:
**   See COMMENT ON FUNCTION statement after CREATE PROCEDURE.
**
** (c) 2018 - 2022 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
**
** Revision History:
** . 2022.12.27 - Include in YBEasyCli 
** . 2021.09.02 - Yellowbrick Technical Support 
*/
 
 
/* ****************************************************************************
**  Example result:
** 
**  view_id |   db_name   | schema_name |  view_name   | owner_name  | is_valid
** ---------+-------------+-------------+--------------+-------------+----------
**    89025 | yellowbrick | public      | n_v          | kick        | t
**    89019 | yellowbrick | public      | t_v          | yellowbrick | f
**    91699 | yellowbrick | public      | test         | yellowbrick | t
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS view_validate_t CASCADE
;

CREATE TABLE view_validate_t
   (
      view_id     BIGINT
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

CREATE OR REPLACE PROCEDURE view_validate_p( _db_ilike       VARCHAR DEFAULT '%'
                                           , _schema_ilike   VARCHAR DEFAULT '%'
                                           , _view_ilike     VARCHAR DEFAULT '%'
                                           , _debug          INTEGER DEFAULT 0
                                           , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF view_validate_t
   LANGUAGE 'plpgsql'
   SECURITY DEFINER
   VOLATILE
AS
$proc$
DECLARE

   _db_id     BIGINT;
   _db_name   VARCHAR( 128 );   
   _db_rec    RECORD;  
   _dbs_sql   TEXT := '';
   _msg_text  TEXT := '';
   _view_pred TEXT;
   _rec       RECORD;
   _sql_state TEXT := '00000';
   _views_sql TEXT;  
   _view_sql  TEXT;      

   _ret_rec   view_validate_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'view_validate_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   -- Append sysviews proc to query tags
   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';  
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   CREATE TEMP TABLE view_validate_report
   (
        view_id     BIGINT
      , db_name     VARCHAR( 128 )
      , schema_name VARCHAR( 128 ) 
      , view_name   VARCHAR( 128 )    
      , owner_name  VARCHAR( 128 ) 
      , is_valid    BOOLEAN
   ) ON COMMIT DROP 
   ;

   -- Get the list of databases to iterate over
   _dbs_sql = 'SELECT 
      database_id AS db_id
    , name        AS db_name 
   FROM sys.database 
   WHERE name ILIKE ' || quote_literal( _db_ilike ) || ' 
   ORDER BY name
   ' 
   ;      
   IF ( _debug > 0 ) THEN RAISE INFO '_dbs_sql=%', _dbs_sql; END IF;
     

   -- Iterate over each db and get the relation metadata including schema 
   FOR _db_rec IN EXECUTE _dbs_sql 
   LOOP
   
      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      IF ( _debug > 0 ) THEN RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ; END IF;
      
      /* Filtering predicates applied to view names are the same in each queried DATABASE.
      **
      **  view_id |   db_name   | schema_name |  view_name   | owner_name  | is_valid
      ** ---------+-------------+-------------+--------------+-------------+----------
      **    89025 | yellowbrick | public      | n_v          | kick        | t   
      */
      
      _views_sql := 'WITH owners AS
         (  SELECT ''USER'' AS owner_type
             , user_id      AS owner_id
             , name         AS owner_name
            FROM ' || quote_ident(_db_name) || '.sys.user
            UNION ALL
            SELECT ''ROLE'' AS owner_type
             , role_id    AS owner_id
             , name       AS owner_name
            FROM  ' || quote_ident(_db_name) || '.sys.role
         )
         , rels AS
         (  SELECT oid   AS obj_id
          , relnamespace AS schema_id
          , relname      AS obj_name
          , relowner     AS owner_id
         FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_class
         WHERE oid > 16384
            AND(   relnamespace  = 2200
                OR relnamespace > 16384 )
            AND relkind IN( ''v'' )
            AND relname ILIKE ' || quote_literal( _view_ilike ) || ' 
         )
         , schemas AS
         ( SELECT oid               AS obj_id
          , oid                     AS schema_id
          , ''SCHEMA''::VARCHAR( 16 ) AS obj_type
          , nspname                 AS obj_name
          , nspowner                AS owner_id
          , nspacl                  AS acl
         FROM  ' || quote_ident(_db_name) || '.pg_catalog.pg_namespace
         WHERE( oid   = 2200
               OR oid > 16384 )
            AND nspname NOT LIKE ''pg_t%''
            AND nspname    ILIKE ' || quote_literal( _schema_ilike ) || '
         )
         
      SELECT 
         v.obj_id::BIGINT                  AS view_id
       , ' || quote_literal(_db_name) || ' AS db_name
       , s.obj_name                        AS schema_name
       , v.obj_name                        AS view_name
       , o.owner_name                      AS owner_name
       , ''f''::BOOLEAN                    AS is_valid
      FROM    rels    AS v  
         JOIN owners  AS o ON v.owner_id  = o.owner_id
         JOIN schemas AS s ON v.schema_id = s.schema_id
      WHERE ' || _yb_util_filter || '
      '
      ;   
      
      IF ( _debug > 1 ) THEN RAISE INFO '_views_sql=%', _views_sql; END IF;        

      FOR _ret_rec IN EXECUTE _views_sql 
      LOOP
         /* 0 = 1 is a provably false predicate so should be almost instantaneous to evaluate 
         ** but still trip a late bound view created errors.
         */
         _sql_state := '00000';
         _view_sql  := 'SELECT 1::int4 FROM '
              || _ret_rec.db_name || '.' || _ret_rec.schema_name || '.' || _ret_rec.view_name 
              || ' WHERE 0 = 1';

         IF ( _debug > 2 ) THEN RAISE INFO '_view_sql=%', _view_sql; END IF;        

         BEGIN
            EXECUTE _view_sql;
            
            EXCEPTION WHEN OTHERS THEN
               GET STACKED DIAGNOSTICS _msg_text  = MESSAGE_TEXT,
                                       _sql_state = RETURNED_SQLSTATE;
            INSERT INTO view_validate_report VALUES (_ret_rec.view_id, _ret_rec.db_name, _ret_rec.schema_name, _ret_rec.view_name, _ret_rec.owner_name, _ret_rec.is_valid); 
            --RETURN NEXT _ret_rec ;
         END;           
       
         IF ( _sql_state = '00000' ) 
         THEN
            _ret_rec.is_valid := 't'::BOOLEAN;
            INSERT INTO view_validate_report VALUES (_ret_rec.view_id, _ret_rec.db_name, _ret_rec.schema_name, _ret_rec.view_name, _ret_rec.owner_name, _ret_rec.is_valid); 
            --RETURN NEXT _ret_rec ;
         ELSE
            IF ( _debug > 0 ) THEN 
                  RAISE INFO 'Exception with _msg_text=%, _sql_state=%s',_msg_text, _sql_state; 
            END IF;            
         END IF;
         
      END LOOP;         
            
   END LOOP;

   _views_sql := 'SELECT * FROM view_validate_report ORDER BY db_name, schema_name, view_name';
   RETURN QUERY EXECUTE _views_sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$proc$
;


COMMENT ON PROCEDURE view_validate_p( VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR ) IS 
$cmnt$Iterate over views to determine which ones have missing dependencies due
to late bound views. 

Examples:
  SELECT   FROM view_validate_p( );
  SELECT * FROM view_validate_p( 'my_db') WHERE is_valid = 'f';
  SELECT * FROM view_validate_p( 'my_db', 'dev%', '%tmp%' );  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the database name. i.e. 'yellowbrick%'.
                  The default is '%'
. _schema_ilike - (optional) An ILIKE pattern for the schema name.   i.e. '%qtr%'.
                  The default is '%'
. _view_ilike   - (optional) An ILIKE pattern for the relation name. i.e. 'fact%'.
                  The default is '%'
                  
Example Result:
  view_id |   db_name   | schema_name |  view_name   | owner_name  | is_valid
 ---------+-------------+-------------+--------------+-------------+----------
    89025 | yellowbrick | public      | n_v          | user1       | t
    89019 | yellowbrick | public      | t_v          | yellowbrick | f

Version:
. 2022.12.27 - Yellowbrick Technical Support 
$cmnt$
;
