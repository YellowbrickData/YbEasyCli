/* ****************************************************************************
** _view_validate_p() 
** 
** Iterates over all views to determine which ones have missing dependencies from
** late bound views.  
** 
** Note:
** . This procedure is specific to YBD version 4.0. If you are running this 
**   against a 3.x warehouse, you need a different procedure. 
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
** . 2020.12.18 - Yellowbrick Technical Support 
** . 2020.12.17 - Yellowbrick Technical Support 
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

CREATE OR REPLACE PROCEDURE view_validate_p( _db_ilike     VARCHAR DEFAULT '%'
                                           , _schema_ilike VARCHAR DEFAULT '%'
                                           , _view_ilike   VARCHAR DEFAULT '%'
                                           , _debug        INTEGER DEFAULT 0   )
RETURNS SETOF view_validate_t
AS
$$
DECLARE

   _db_id     BIGINT;
   _db_rec    RECORD;  
   _msg_text  TEXT := '';
   _pred      TEXT;
   _rec       RECORD;
   _sql       TEXT;   
   _sql_state TEXT := '00000';

   _ret_rec   view_validate_t%ROWTYPE;
   
   _fn_name   VARCHAR(256) := 'view_validate_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;      

BEGIN

   --SET TRANSACTION       READ ONLY;
   
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
    , ''f''::BOOLEAN                          AS is_valid
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
   ORDER BY db_name, schema_name, view_name
   ';
   
   IF ( _debug > 0 ) THEN 
      RAISE INFO '_sql=%', _sql;   
   END IF;

   FOR _ret_rec IN EXECUTE _sql 
   LOOP
      
      _sql_state := '00000';
      _sql := 'SELECT 1::int4 FROM '
           || _ret_rec.db_name || '.' || _ret_rec.schema_name || '.' || _ret_rec.view_name 
           || ' WHERE 0 = 1';
           
      IF ( _debug > 0 ) THEN 
         RAISE INFO '_sql=%', _sql;
      END IF;
      
      BEGIN
         EXECUTE _sql;
         
         EXCEPTION WHEN OTHERS THEN
           GET STACKED DIAGNOSTICS _msg_text  = MESSAGE_TEXT,
                                   _sql_state = RETURNED_SQLSTATE;
            IF ( _debug > 1 ) THEN 
               RAISE INFO 'Exception with _msg_text=%, _sql_state=%s',_msg_text, _sql_state; 
            END IF;

         RETURN NEXT _ret_rec ;
      END;           
    
      IF ( _sql_state = '00000' ) 
      THEN
         _ret_rec.is_valid := 't'::BOOLEAN;
         RETURN NEXT _ret_rec ;
      END IF;

            
   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';

END;
$$
LANGUAGE 'plpgsql'
SECURITY DEFINER
VOLATILE
CALLED ON NULL INPUT
;


COMMENT ON PROCEDURE view_validate_p( VARCHAR, VARCHAR, VARCHAR, INTEGER ) IS 
'Description:
Iterates over all views to determine which ones have missing dependencies from
late bound views. 

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
. 2020.12.18 - Yellowbrick Technical Support 
';

\qecho ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\qecho SELECT * FROM view_validate_p('subscription','public','v_subs_adjustment', 2);
\qecho
SELECT * FROM view_validate_p('subscription','public','v_subs_adjustment', 2);

\q

\qecho ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\qecho SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 't';
SELECT * FROM view_validate_p( 'yellowbrick' ) WHERE is_valid = 't';

\qecho ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
\qecho SELECT * FROM view_validate_p( 'd%' );
SELECT * FROM view_validate_p( 'd%' );

\qecho ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

\q