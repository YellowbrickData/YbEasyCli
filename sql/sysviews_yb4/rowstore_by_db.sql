/* ****************************************************************************
** rowstore_size_detail_p()
**
** Details of user tables in the rowstore.
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
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**    oid   | table_schema |      table_name     | row_estimate | total_size | index_size | toast_size | table_size
** ---------+--------------+---------------------+--------------+------------+------------+------------+------------
**     5076 | sys          | _log_authentication |     33603472 | 11 GB      | 0 bytes    |            | 11 GB
**     5003 | sys          | shardstore          |       413984 | 227 MB     | 127 MB     |            | 100 MB
**  8358834 | public       | help_t              |            0 | 32 kB      | 0 bytes    | 32 kB      | 0 bytes
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS rowstore_size_detail_t CASCADE
;

CREATE TABLE rowstore_size_detail_t
(
   oid          BIGINT
 , table_schema VARCHAR (128)
 , table_name   VARCHAR (128)
 , row_estimate BIGINT
 , total_size   VARCHAR (16)
 , index_size   VARCHAR (16)
 , toast_size   VARCHAR (16)
 , table_size   VARCHAR (16)
)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE rowstore_size_detail_p()
RETURNS SETOF rowstore_size_detail_t AS 
$$
DECLARE

   _sql       TEXT         := '';
   _ret_rec  rowstore_size_detail_t%ROWTYPE;     
   
   _fn_name   VARCHAR(256) := 'rowstore_size_detail_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   /* Txn read_only to protect against potential SQL injection attacks on sp that take args
   SET TRANSACTION       READ ONLY;
   */
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT
     oid::BIGINT                 AS oid
   , table_schema::VARCHAR(128)  AS table_schema
   , table_name::VARCHAR(128)    AS table_name
   , row_estimate::BIGINT        AS row_estimate
   , pg_size_pretty(total_bytes) AS total
   , pg_size_pretty(index_bytes) AS index
   , pg_size_pretty(toast_bytes) AS toast
   , pg_size_pretty(table_bytes) AS table
   FROM
     ( SELECT *
       , total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
       FROM
         ( SELECT
             c.oid
           , nspname                               AS table_schema
           , relname                               AS table_name
           , c.reltuples                           AS row_estimate
           , pg_total_relation_size(c.oid)         AS total_bytes
           , pg_indexes_size(c.oid)                AS index_bytes
           , pg_total_relation_size(reltoastrelid) AS toast_bytes
           FROM
             pg_class               c
             LEFT JOIN pg_namespace n
               ON n.oid = c.relnamespace
           WHERE
             relkind = ''r''
             AND nspname NOT IN (''pg_catalog'', ''information_schema'')
             AND total_bytes > 0
             -- AND c.relname LIKE ''yb%''
         ) pc
     )     a
   ORDER BY
     row_estimate DESC
   ';

   FOR _ret_rec IN EXECUTE( _sql ) 
   LOOP
      RETURN NEXT _ret_rec;
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

-- ALTER FUNCTION rowstore_size_detail_p()
--   SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION rowstore_size_detail_p() IS 
'Description:
Size of rowstore data in user tables.
  
Examples:
  SELECT * FROM rowstore_size_detail_p() 
  SELECT * FROM rowstore_size_detail_p() ORDER BY total_size DESC LIMIT 30;
  
Arguments:
. none

Notes:
. This is only the space consumed by rows still in the rowstore (e.g. in the front-end).
. The columnstore storage space is not included in these numbers.

Version:
. 2020.06.15 - Yellowbrick Technical Support 
'
;
