/* ****************************************************************************
** procedure_p()
**
** User created stored procedures.
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
** . 2021.04.26 - Yellowbrick Technical Support
** . 2020.10.30 - Yellowbrick Technical Support 
*/ 

/* ****************************************************************************
**  Example results:
**
**  proc_id | schema_name |       proc_name        |         arguments             |    owner    |     returns
** ---------+-------------+------------------------+-------------------------------+-------------+--------------------- 
**    82686 | public      | analyze_immed_user_p   |   _off_or_on VARCHAR          | yellowbrick | void
**    82687 | public      | analyze_immed_sess_p   |   _off_or_on VARCHAR          | yellowbrick | void
**    82692 | public      | column_dstr_p          |   _db_name VARCHAR           +| yellowbrick | SETOF column_dstr_t
**          |             |                        | , _schema_name VARCHAR       +|             |
**          |             |                        | , _table_name VARCHAR        +|             |
**          |             |                        | , _column_name VARCHAR       +|             |
**          |             |                        | , _log_n numeric DEFAULT 10   |             |
*/


/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS procedure_t CASCADE
;

CREATE TABLE procedure_t
(
     proc_id      BIGINT         
   , db_name      VARCHAR(128)     
   , schema_name  VARCHAR(128)   
   , proc_name    VARCHAR(128)   
   , arg_names    VARCHAR(4000)
   , arg_types    VARCHAR(4000)                   
   , "returns"    VARCHAR(300) 
   , owner        VARCHAR(128)      
   --, description  VARCHAR(60000)     
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE procedure_p(
   _db_ilike     VARCHAR DEFAULT '%'
   , _schema_ilike VARCHAR DEFAULT '%'
   , _rel_ilike    VARCHAR DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF procedure_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _db_id    BIGINT;
   _db_name  VARCHAR(128);   
   _db_rec   RECORD;  
   _ret_rec  RECORD;
   _sql      TEXT;        
   
   _fn_name   VARCHAR(256) := 'procedure_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   --SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* Query for the databases to iterate over
   */
   _sql = 'SELECT database_id AS db_id, name AS db_name 
      FROM sys.database 
      WHERE name ILIKE ' || quote_literal( _db_ilike ) || ' 
      ORDER BY name
   ';
      
   -- RAISE info '_sql = %', _sql;

   /* Iterate over each db and get the relation metadata including schema 
   */
   FOR _db_rec IN EXECUTE _sql 
   LOOP
   
      _db_id   := _db_rec.db_id ;
      _db_name := _db_rec.db_name ;

      --RAISE INFO '_db_id=%, _db_name=%',_db_id, _db_name ;
      
      _sql := FORMAT( 'WITH 
         owners AS
         (   SELECT 
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
       ,procs AS
         (  SELECT
               p.oid                                                AS oid
             , p.pronamespace                                       AS pronamespace
             , TRIM (p.proname)                                     AS proname
             , p.proretset                                          AS proretset
             , n.nspname||''.''||t.typname                          AS ret_type_name
             , p.pronargs                                           AS pronargs
             , p.proargnames                                        AS arg_names
             , (array (SELECT UNNEST (p.proargtypes)::REGTYPE))::VARCHAR(4000) 
                                                                    AS arg_type_names
             , p.proowner                                           AS proowner
             , p.prolang                                            AS prolang
             , p.prosp                                              AS prosp
            FROM
                 %I.pg_catalog.pg_proc      p
            JOIN %I.pg_catalog.pg_type      t on p.prorettype   = t.oid 
            JOIN %I.pg_catalog.pg_namespace n ON t.typnamespace = n.oid
         )
         SELECT
            p.oid::BIGINT                                                                      AS proc_id
          , %L::VARCHAR(128)                                                                   AS db_name
          , n.nspname::VARCHAR(128)                                                            AS schema_name
          , p.proname::VARCHAR(128)                                                            AS proc_name
          , REGEXP_REPLACE( REPLACE( REPLACE( REPLACE(
                                     p.arg_names , '',''                          , e''\n,''       )
                                                 , ''{''                          , '' ''          )
                                                 , ''}''                          , ''''           )
                                                 , ''(::[A-Z]+\S*)''              , '''', ''g''    )::VARCHAR(4000)
                                                                                              AS arg_names
          , REGEXP_REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(
                                p.arg_type_names , ''character varying''          , ''VARCHAR''    )
                                                 , ''timestamp without time zone'', ''TIMESTAMP''  )
                                                 , ''timestamp with time zone''   , ''TIMESTAMPTZ'')
                                                 , ''bigint''                     , ''INT8''       )
                                                 , ''integer''                    , ''INT4''       )
                                                 , '',''                          , e''\n,''       )
                                                 , ''{''                          , '' ''          )
                                                 , ''}''                          , ''''           )
                                                 , ''(::[A-Z]+\S*)''              , '''', ''g''    )::VARCHAR(4000)
                                                                                               AS args_type_names
          , (CASE WHEN p.proretset = ''t'' THEN ''SETOF '' ELSE '''' END
          || REGEXP_REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(
                                p.ret_type_name , ''character varying''          , ''VARCHAR''    )
                                                 , ''timestamp without time zone'', ''TIMESTAMP''  ) 
                                                 , ''timestamp with time zone''   , ''TIMESTAMPTZ'')                                            
                                                 , ''bigint''                     , ''INT8''     )
                                                 , ''integer''                    , ''INT4''       )
                                                 , '',''                          , e''\n,''       )  
                                                 , ''(::[A-Z]+\S*)''              , '''', ''g''      ))::VARCHAR(300)
                                                                                               AS "returns"
          , o.owner_name::VARCHAR(128)                                                         AS owner                                                                                                 
         --,NVL(trim(split_part( d.description, e''\n'', 2)), '''')::VARCHAR(60000)                    AS description
         FROM
                       procs                        p
            LEFT JOIN  %I.pg_catalog.pg_namespace   n ON p.pronamespace = n.oid 
            LEFT JOIN  %I.pg_catalog.pg_description d ON p.oid          = d.objoid
            LEFT JOIN  owners                       o ON p.proowner     = o.owner_id
            INNER JOIN pg_language                  l ON l.oid          = p.prolang           
         WHERE
            p.oid > 16384
            AND p.prosp    = ''t''
            AND n.nspname NOT IN (''pg_catalog'', ''information_schema'', ''sys'')    
            AND %s        
         ORDER BY
            1, 2, 3
      ', _db_rec.db_name, _db_rec.db_name, _db_rec.db_name, _db_rec.db_name, _db_rec.db_name, _db_rec.db_name, _yb_util_filter
      );

      --RAISE INFO '_sql is: %', _sql ;
      RETURN QUERY EXECUTE _sql;

   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;

COMMENT ON FUNCTION procedure_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
'Description:
User created stored procedures.
  
Examples:
  SELECT * FROM procedure_p() 
  SELECT * FROM procedure_p( ''yellowbrick'', ''p%'') ;
  SELECT * FROM procedure_p( ''%'', ''public'' ,''%p'');  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the schema name. i.e. ''%fin%''.
. _schema_ilike - (optional) An ILIKE pattern for the schema name. i.e. ''%qtr%''.
                  The default is ''%''
. _rel_ilike    - (optional) An ILIKE pattern for the table name.  i.e. ''fact%''.
                  The default is ''%''

Revision:
. 2021.12.09 - Yellowbrick Technical Support
'
;