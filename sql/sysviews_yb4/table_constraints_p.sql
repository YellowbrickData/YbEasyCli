/* ****************************************************************************
** table_constraints_p()
**
** OneSentenceDescriptionGoesHere.
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
** . 2020.10.22 - Yellowbrick Technical Support
** . 2021.11.21 - Integrated with YbEasyCli
*/

/* ****************************************************************************
**  Example results:
**
 constraint_catalog | constraint_schema |    constraint_name     | table_catalog | table_schema | table_name | constraint_type | is_deferrable | initially_deferred
--------------------+-------------------+------------------------+---------------+--------------+------------+-----------------+---------------+--------------------
 a_db               | q1310552977       | 65325_65333_1_not_null | a_db          | q1310552977  | analyze_a  | CHECK           | NO            | NO
 a_db               | q1310552977       | 65325_65333_2_not_null | a_db          | q1310552977  | analyze_b  | CHECK           | NO            | NO
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS table_constraints_t CASCADE
;

CREATE TABLE table_constraints_t
(
   constraint_catalog  VARCHAR(128)            
 , constraint_schema   VARCHAR(128)            
 , constraint_name     VARCHAR(1024)            
 , table_catalog       VARCHAR(128)            
 , table_schema        VARCHAR(128)            
 , table_name          VARCHAR(128)            
 , constraint_type     VARCHAR(16)             
 , is_deferrable       VARCHAR(3)              
 , initially_deferred  VARCHAR(3)              
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE table_constraints_p(
   _db_like VARCHAR(128) DEFAULT '%'
   , _yb_util_filter VARCHAR DEFAULT 'TRUE' )
   RETURNS SETOF table_constraints_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql      TEXT                         := '';
   _db_id    BIGINT;
   _db_name  VARCHAR(128);   
   _db_rec   RECORD;
   
   _fn_name   VARCHAR(256) := 'table_constraints_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);

   /* Query for the databases to iterate over
   */
   _sql = 'SELECT database_id AS db_id, name AS db_name 
      FROM sys.database 
      WHERE name LIKE ' || quote_literal( _db_like ) || ' 
         AND ' || _yb_util_filter || '
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
      
      _sql := 'SELECT 
        ' || quote_literal( _db_name ) || '::VARCHAR(128)                           AS constraint_catalog
      , nc.nspname::VARCHAR(128)                                   AS constraint_schema
      , c.conname::VARCHAR(1024)                                    AS constraint_name
      , ' || quote_literal( _db_name ) || '::VARCHAR(128)                            AS table_catalog
      , nr.nspname::VARCHAR(128)                                   AS table_schema
      , r.relname::VARCHAR(128)                                    AS table_name
      , (
            CASE c.contype
                WHEN ''c''::"char" THEN ''CHECK''::VARCHAR(16)
                WHEN ''f''::"char" THEN ''FOREIGN KEY''::VARCHAR(16)
                WHEN ''p''::"char" THEN ''PRIMARY KEY''::VARCHAR(16)
                WHEN ''u''::"char" THEN ''UNIQUE''::VARCHAR(16)
                ELSE NULL::text
            END)::VARCHAR(16)                                      AS constraint_type
      ,(
            CASE
                WHEN c.condeferrable THEN ''YES''::VARCHAR(3)
                ELSE ''NO''::VARCHAR(3)
            END)::VARCHAR(3)                                       AS is_deferrable
      ,
        (
            CASE
                WHEN c.condeferred THEN ''YES''::VARCHAR(3)
                ELSE ''NO''::VARCHAR(3)
            END)::VARCHAR(3)                                       AS initially_deferred
       FROM 
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_namespace nc,
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_namespace nr,
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_constraint c,
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_class r
      WHERE 
              r.oid > 16384
         AND (nc.oid = c.connamespace) 
         AND (nr.oid = r.relnamespace) 
         AND (c.conrelid = r.oid) 
         AND (c.contype NOT IN (''t'', ''x'')) 
         AND (r.relkind = ''r''::"char") AND (NOT pg_is_other_temp_schema(nr.oid)) 
         AND (pg_has_role(r.relowner, ''USAGE'') 
               OR has_table_privilege(r.oid, ''INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES'') 
               OR has_any_column_privilege(r.oid, ''INSERT, UPDATE, REFERENCES''::text)
             )
    UNION ALL
     SELECT 
         ' || quote_literal( _db_name ) || '::VARCHAR(128)                           AS constraint_catalog
       , nr.nspname::VARCHAR(128)                                  AS constraint_schema
       , (nr.oid::VARCHAR(19) || ''_''::VARCHAR(1) || r.oid::VARCHAR(19) || ''_''::VARCHAR(1) || a.attnum::VARCHAR(19) || ''_not_null''::VARCHAR(19) )::VARCHAR(1024) 
                                                                   AS constraint_name
       , ' || quote_literal( _db_name ) || '::VARCHAR(128)                          AS table_catalog
       , nr.nspname::VARCHAR(128)                                  AS table_schema
       , r.relname::VARCHAR(128)                                   AS table_name
       , ''CHECK''::VARCHAR(16)                                    AS constraint_type
       , ''NO''::VARCHAR(3)                                        AS is_deferrable
       , ''NO''::VARCHAR(3)                                        AS initially_deferred
       FROM 
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_namespace nr,
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_class r,
        ' || quote_ident( _db_name ) || '.pg_catalog.pg_attribute a
      WHERE 
             r.oid     > 16384
         AND nr.oid    = r.relnamespace
         AND r.oid     = a.attrelid
         AND a.attnotnull 
         AND a.attnum  > 0
         AND NOT a.attisdropped 
         AND r.relkind = ''r''::"char"  
         AND NOT pg_is_other_temp_schema(nr.oid)
      ';

      --RAISE INFO '_sql=%', _sql;
      RETURN QUERY EXECUTE _sql ;

   END LOOP;
   
   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$
;

-- ALTER FUNCTION table_constraints_p( DataTypesIfAny )
--    SET search_path = pg_catalog,pg_temp;
   
COMMENT ON FUNCTION table_constraints_p( VARCHAR, VARCHAR ) IS 
'Description:
Existing constraints on user tables as per information_schema.table_constraints. 
  
Examples:
  SELECT * FROM table_constraints_p( ''yellowbrick'' ) 
  SELECT * FROM table_constraints_p( $$%financials$$ )   
  
Arguments:
. _db_like VARCHAR - (reqd) LIKE pattern for name of database(s) to query.

Version:
. 2020.10.22 - Yellowbrick Technical Support
. 2021.11.21 - Integrated with YbEasyCli
'
;