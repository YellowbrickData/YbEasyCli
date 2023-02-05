/* ****************************************************************************
** table_p()
**
** All user tables in all databases. Similar to ybsql "\d".
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
** . 2022.12.27 - ybCliUtils inclusion.
** . 2021.05.08 - Yellowbrick Technical Support 
** . 2020.11.09 - Yellowbrick Technical Support 
** . 2020.10.30 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example result:
** 
 table_id | db_name  | schema_name |      table_name       | owner_name  |    dist    |         sort_key         | clstr_keys | prtn_keys
----------+----------+-------------+-----------------------+-------------+------------+--------------------------+------------+-----------
    26719 | ybprd001 | admin       | ansi_mer_ctgy         | yellowbrick | replicated | ansi_mer_ctgy_cd         |            |
    26730 | ybprd001 | admin       | ar_stmt_mtch_key_type | yellowbrick | random     | ar_stmt_mtch_key_type_cd |            |
    26770 | ybprd001 | admin       | area_prc              | yellowbrick | replicated | area_prc_type_cd         |            |
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS table_t CASCADE
;

CREATE TABLE table_t
   (
      table_id        BIGINT
    , db_name         VARCHAR( 128 )
    , schema_name     VARCHAR( 128 ) 
    , table_name      VARCHAR( 128 )    
    , owner_name      VARCHAR( 128 ) 
    , dist            VARCHAR( 140 )    
    , sort_key        VARCHAR( 128 )    
    , clstr_keys      VARCHAR( 512 )    
    , prtn_keys       VARCHAR( 512 )    
   )
;
  

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE table_p( _db_ilike     VARCHAR DEFAULT '%'
                        , _schema_ilike VARCHAR DEFAULT '%'
                        , _table_ilike  VARCHAR DEFAULT '%' 
                        , _yb_util_filter VARCHAR DEFAULT 'TRUE'                                  
                        )
   RETURNS SETOF table_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _pred         TEXT := '';
   _sql          TEXT := '';

   _fn_name   VARCHAR(256) := 'table_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
   _ret_rec table_t%ROWTYPE;   
  
BEGIN  

   -- Append sysviews:proc_name to ybd_query_tags
   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';
   
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);  

   _pred := 'WHERE  '
   || '     db_name     ILIKE ' || quote_literal( _db_ilike ) 
   || ' AND schema_name ILIKE ' || quote_literal( _schema_ilike )
   || ' AND table_name  ILIKE ' || quote_literal( _table_ilike  ) 
   || ' AND table_id    > 16384 '
   || ' AND schema_name != ''sys'' 
        AND ' || _yb_util_filter 
   || CHR(10);

   _sql := '
   WITH relations AS
   (
      SELECT
        t.database_id                                            AS database_id
      , t.schema_id                                              AS schema_id
      , t.table_id                                               AS table_id       
      , t.name                                                   AS table_name
      , owner_id                                                 AS owner_id
      , CASE
        WHEN t.distribution <> ''hash'' THEN t.distribution
        ELSE t.distribution || ''('' || k.distribution_key || '')''
      END                                                        AS dist
      , k.sort_key                                               AS sort_key
      , k.cluster_keys                                           AS clstr_keys
      , k.partition_keys                                         AS prtn_keys      
      FROM sys.vt_table_info t
      LEFT OUTER JOIN 
      (
         SELECT
            table_id               AS table_id
          , MAX (sort_key)         AS sort_key
          , MAX (distribution_key) AS distribution_key
          , MAX (cluster_keys)     AS cluster_keys
          , MAX (partition_keys)   AS partition_keys
         FROM
            sys.vt_table_keys
         GROUP BY
            table_id
      ) k ON k.table_id = t.table_id      
      
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
      r.table_id::BIGINT            AS table_id
    , d.name::VARCHAR( 128 )        AS db_name
    , s.name::VARCHAR( 128 )        AS schema_name
    , r.table_name::VARCHAR( 128 )  AS table_name
    , o.owner_name::VARCHAR( 128 )  AS owner_name
    , dist::VARCHAR( 140 )          AS dist    
    , sort_key::VARCHAR( 128 )      AS sort_key    
    , clstr_keys::VARCHAR( 512 )    AS clstr_keys    
    , prtn_keys::VARCHAR( 512 )     AS prtn_keys     
    
   FROM relations     r
   JOIN sys.database  d ON r.database_id = d.database_id
   JOIN sys.schema    s ON r.database_id = s.database_id AND r.schema_id = s.schema_id
   LEFT JOIN owners   o ON r.owner_id    = o.owner_id
   ' || _pred || '
   ORDER BY db_name, schema_name, table_name
   ';
   
   -- RAISE INFO '_sql=%', _sql;
   RETURN QUERY EXECUTE _sql ;
 
   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );

END;   
$proc$ 
;


COMMENT ON FUNCTION table_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR ) IS 
$cmnt$Description:
All user tables across all databases metadata similar to ybsql "\d".

Examples:
  SELECT * FROM table_p( );
  SELECT * FROM table_p( 'my_db', 's%') WHERE owner_name != 'yellowbrick';
  SELECT * FROM table_p( '%', '%qtr%' ,'%fact%');  
  
Arguments:
. _db_ilike     - (optional) An ILIKE pattern for the database name. i.e. 'yellowbrick%'.
                  The defauuls is '%'
. _schema_ilike - (optional) An ILIKE pattern for the schema name. i.e. '%qtr%'.
                  The default is '%'
. _table_ilike  - (optional) An ILIKE pattern for the table name.  i.e. 'fact%'.
                  The default is '%'
. _yb_util_filter - (internal) Used by YbEasyCli.

Version:
. 2022.12.27 - Yellowbrick Technical Support 
$cmnt$
;
