/* ****************************************************************************
** view_invalid_drop_p.sql
** 
** Generates drop statements for invalid views in version 4 that were created in
** version 3.x.
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
** . 2020.01.07 - Yellowbrick Technical Support 
** . 2020.12.23 - Yellowbrick Technical Support 
*/
 
 
/* ****************************************************************************
**  Example result:
** 
**  -- ----------------------------------------------------
**  -- yellowbrick
**  -- ----------------------------------------------------
**  DROP  VIEW public.v_w_msng_tbl ;
**  -- OK VIEW public.x_v
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS view_invalid_drop_t CASCADE
;

CREATE TABLE view_invalid_drop_t
(
   sql VARCHAR( 64000 ) 
)
;  

/* ****************************************************************************
** Create the procedure.
*/
DROP PROCEDURE IF EXISTS    view_invalid_drop_p( VARCHAR, VARCHAR, VARCHAR )
;

CREATE OR REPLACE PROCEDURE view_invalid_drop_p( _db_ilike     VARCHAR DEFAULT '%'
                                               , _schema_ilike VARCHAR DEFAULT '%'
                                               , _view_ilike   VARCHAR DEFAULT '%' 
                                               , _debug        INTEGER DEFAULT 0 )
RETURNS SETOF view_invalid_drop_t
AS
$$
DECLARE

   _db_id           BIGINT;
   _db_name         VARCHAR( 128 );
   _db_rec          RECORD;  
   _msg_text        TEXT           := '';   
   _ret_rec         view_invalid_drop_t%ROWTYPE;   
   _sql             TEXT;   
   _sql_state       TEXT           := '00000';
   _view_addl_pred  TEXT;
   _view_rec        RECORD;    
   
   _fn_name   VARCHAR(256) := 'view_invalid_drop_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   /* Txn read_only to help protect against potential SQL injection attack writes
   */
   --SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   

   _view_addl_pred := '
         AND s.nspname     ILIKE ' || quote_literal( _schema_ilike ) || '
         AND r.relname     ILIKE ' || quote_literal( _view_ilike   ) || CHR(10)
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
      
   IF ( _debug > 0 ) THEN RAISE INFO '_sql=%', _sql; END IF;

   /* Iterate over each db and get the relation metadata including schema 
   */
   FOR _db_rec IN EXECUTE _sql 
   LOOP

      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;
   
      _sql := 'SELECT
         r.oid::BIGINT                                     AS view_id     
       , ' || quote_literal( _db_name ) || '::VARCHAR(128) AS db_name
       , s.nspname::VARCHAR(128)                           AS schema_name 
       , r.relname::VARCHAR(128)                           AS view_name 
       , r.relcreated                                      AS created_ts
      FROM ' || quote_ident( _db_name )  || '.pg_catalog.pg_class     r          
      JOIN ' || quote_ident( _db_name )  || '.pg_catalog.pg_namespace s 
         ON r.relnamespace::BIGINT = s.oid
      WHERE
             r.oid         > 16384
         AND r.relkind     IN ( ''v'' )
         --AND r.relcreated  IS NULL
      ' || _view_addl_pred || '
      ORDER BY 2, 3, 4
      ';
   
      _ret_rec.sql := '-- ----------------------------------------------------' 
                || E'\n-- ' || _db_rec.db_name 
                || E'\n-- ----------------------------------------------------'
                || E'\n\\c ' || _db_rec.db_name; 
      RETURN NEXT _ret_rec ;    
   
      IF ( _debug > 0 ) THEN RAISE INFO '_sql=%', _sql; END IF;

      FOR _view_rec IN EXECUTE _sql 
      LOOP

         _sql_state := '00000';
         _sql := 'SELECT 1::int4 FROM '
              || _view_rec.db_name || '.' || _view_rec.schema_name || '.' || _view_rec.view_name 
              || ' WHERE 0 = 1';
              
         IF ( _debug > 0 ) THEN RAISE INFO '_sql=%', _sql; END IF;
         
         BEGIN
            EXECUTE _sql;
            
            EXCEPTION WHEN OTHERS THEN
               GET STACKED DIAGNOSTICS _msg_text  = MESSAGE_TEXT,
                                       _sql_state = RETURNED_SQLSTATE;
               IF ( _debug > 1 ) THEN 
                  RAISE INFO 'Exception with _msg_text=%, _sql_state=%s',_msg_text, _sql_state; 
               END IF;
         END;           
       
         IF ( _sql_state != '00000' ) 
         THEN
            _ret_rec.sql := 'DROP  VIEW ' 
            || _view_rec.schema_name || '.' || _view_rec.view_name || ' ;'
            || CASE WHEN _view_rec.created_ts IS NULL THEN ' /* Version 3.x view */' ELSE '' END
            ;
            RETURN NEXT _ret_rec ;
         ELSE
            _ret_rec.sql := '-- OK VIEW ' 
            || _view_rec.schema_name || '.' || _view_rec.view_name;
            RETURN NEXT _ret_rec ;         
         END IF;
            
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

COMMENT ON PROCEDURE view_invalid_drop_p( VARCHAR, VARCHAR, VARCHAR, INTEGER ) IS 
'Description:
Generates DROP statements for invalid views in version 4.x that were created in version 3.x.

Views created in version 3.x that are still valid generate an "OK VIEW" comment line.


Examples:
  SELECT   FROM view_invalid_drop_p( );
  SELECT * FROM view_invalid_drop_p( ''my_db'');
  SELECT * FROM view_invalid_drop_p( ''my_db'', ''dev%'', ''%tmp%'' );  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the database name.  i.e. ''yellowbrick%''.
                  The default is ''%''
. _schema_ilike - (optional) An ILIKE pattern for the schema name.    i.e. ''%qtr%''.
                  The default is ''%''
. _view_ilike    - (optional) An ILIKE pattern for the relation name. i.e. ''fact%''.
                  The default is ''%''
. _debug         - (optional)                  

Version:
. 2020.01.07 - Yellowbrick Technical Support 
';

SELECT * FROM view_invalid_drop_p( 'drop%');
