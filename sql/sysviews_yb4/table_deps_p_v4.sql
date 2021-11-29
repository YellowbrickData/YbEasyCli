/* ****************************************************************************
** table_deps_p_v4()
**
** Recursive list of SQL views dependent upon a table or view.
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
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2018.09.22 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**  view_catalog | view_schema | view_name | table_catalog | table_schema | table_name
** --------------+-------------+-----------+---------------+--------------+------------
**  my_db        | public      | v1_1      | my_db         | public       | t1
**  ...
**  my_db        | public      | v2_2      | my_db         | public       | v1_2
*/


/* ****************************************************************************
** Uses information_schema.view_table_usage as the return type.
*/

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE table_deps_p( _db_name      VARCHAR(128)
                                        , _schema_ilike VARCHAR(128) DEFAULT '%'
                                        , _table_ilike  VARCHAR(128) DEFAULT '%'
                                        ) 
RETURNS SETOF information_schema.view_table_usage AS
$$
DECLARE

   _matches   INTEGER := -1;
   _pred      TEXT    := '';
   _rows      INTEGER := 0;

   _sql       TEXT         := '';
   _ret_rec  information_schema.view_table_usage%ROWTYPE;     
   
   _fn_name   VARCHAR(256) := 'table_deps_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   */   
   SET TRANSACTION       READ ONLY;

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    


  _pred := '
      OR 
      (    table_schema ILIKE ' || quote_literal(_schema_ilike) 
   || ' AND table_name  ILIKE ' || quote_literal(_table_ilike)   
   || ')';
  
  WHILE _matches <> 0
  LOOP
    _matches := 0;
    _sql     := 'SELECT 
      *
    FROM ' || _db_name || '.information_schema.view_table_usage
      WHERE (0 = 1 )' || 
      _pred   || '
      ORDER BY table_schema, table_name
    ';
    _pred := '';
               
    FOR _ret_rec IN EXECUTE( _sql ) 
    LOOP
      _pred := _pred || '
      OR (table_schema = ' || quote_literal(_ret_rec.view_schema) || ' AND table_name = ' || quote_literal(_ret_rec.view_name)   || ')';
      _matches := _matches + 1;
      RETURN NEXT _ret_rec;
    END LOOP;
    
  END LOOP;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$$ 
LANGUAGE 'plpgsql' 
VOLATILE
CALLED ON NULL INPUT
SECURITY DEFINER
;

-- ALTER FUNCTION table_deps_p( VARCHAR, VARCHAR, VARCHAR )
--    SET search_path = pg_catalog,pg_temp;
   
COMMENT ON FUNCTION table_deps_p( VARCHAR, VARCHAR, VARCHAR ) IS 
'Description:
Recursive list of SQL views dependent upon a table or view.
  
Examples:
  SELECT * FROM table_deps_p( ''yellowbrick'' );
  
  SELECT view_schema, view_name, '' depends on '' AS depends_on, table_schema, table_name
  FROM table_deps_p( $$yellowbrick$$, $$pub%$$ )
  ORDER BY view_schema, view_name ;
  
Arguments:
. _db_name      - (reqd) Database in which table/views reside.
. _schema_ilike - (opt)  Schema for table or view to find dependents of.
                         Default: ''%''
. _table_ilike  - (reqd) Table or view to find dependents of.
                                 Default: ''%''

Notes:
. Does not find dependencies in foreign databases (i.e. x-db queries).
. Does not support special (double-quoted) names.

Version:
. 2020.10.30 - Yellowbrick Technical Support
'
;



