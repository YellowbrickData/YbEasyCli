 /* ****************************************************************************
** column_dstr_p()
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
** . 2022.12.27 - Cosmetic update.
** . 2021.12.09 - ybCliUtils inclusion.
** . 2020.07.26 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**        column_name        | magnitude | rows_per | to | to_rows_per | distincts | max_rows | tot_rows
** --------------------------+-----------+----------+----+-------------+-----------+----------+----------
**  premdb.public.match.htid | 10^2      |      100 | to |         999 |        31 |      424 |     7885
**  premdb.public.match.htid | 10^1      |       10 | to |          99 |        15 |       99 |      721
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS column_dstr_t CASCADE
;

CREATE TABLE column_dstr_t
   (
      column_name        VARCHAR( 256 )
    , magnitude          VARCHAR( 16 )
    , rows_per           BIGINT
    , "to"               CHAR( 2 )
    , to_rows_per        BIGINT
    , distincts          BIGINT
    , max_rows           BIGINT
    , tot_rows           BIGINT
   )
;
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE column_dstr_p(  
      _db_name     VARCHAR
    , _schema_name VARCHAR  
    , _table_name  VARCHAR  
    , _column_name VARCHAR
    , _log_n       NUMERIC DEFAULT 10
   )
   RETURNS SETOF column_dstr_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'proc_name_p';   
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
   _fqtn      VARCHAR(386) := quote_ident( _db_name ) 
                              || '.' || quote_ident( _schema_name ) 
                              || '.' || quote_ident( _table_name  )
                           ;
   _fqcn      VARCHAR(386) := _fqtn
                              || '.' || quote_ident( _column_name )
                           ;
  
BEGIN  

   -- Prefix ybd_query_tags with the procedure name
   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags );
   --PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);   

   _sql := 'SELECT 
      ' || quote_literal( _fqcn ) || '::VARCHAR(256)        AS fqcn
    , (' || quote_literal( _log_n || '^') || '|| magnitude)::VARCHAR(16)
                                                            AS magnitude
    , rows_per_dstnct::BIGINT                               AS rows_per_dstnct
    , ''to''::CHAR(2)                                       AS "to"
    , to_rows_per_dstnct::BIGINT                            AS to_rows_per_dstnct
    , distincts::BIGINT                                     AS distincts
    , max_rows::BIGINT                                      AS max_rows
    , tot_rows::BIGINT                                      AS tot_rows
   FROM ( SELECT 
            TRUNC (log (' || _log_n || ', rows))                                      AS magnitude
          , ' || _log_n || '^ (TRUNC (log (' || _log_n || ', rows))   ) ::INTEGER     AS rows_per_dstnct
          , (' || _log_n || '^(TRUNC (log (' || _log_n || ', rows)) +1) ::INTEGER) -1 AS "to_rows_per_dstnct"
          , COUNT(*)                                                                  AS distincts
          , MAX (rows)                                                                AS max_rows
          , SUM (rows)                                                                AS tot_rows
         FROM (  SELECT 
               ' || quote_ident( _column_name ) || ' AS aggr_col
                , COUNT(*)        AS rows
               FROM ' || _fqtn || '
               GROUP BY 1
            ) sq_dstnct
         GROUP BY 1, 2, 3
      ) dstnct
   ORDER BY 1, 3 DESC
   ';

   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE  'SET ybd_query_tags  TO ' || quote_literal( _prev_tags );
   
END;   
$proc$
;

   
COMMENT ON FUNCTION column_dstr_p( VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMERIC ) IS 
$cmnt$Description:
Distribution of rows per distinct values for column grouped on a logarithmic scale 

For example, if you had an "invoices" table "cust_id" column with 224 rows unevenly 
distributed across 4 cust_id values like:
cust_id rows
------- ----
1007       7
1011      11
1065      65
1141     141

Your output using powers of 10 would resemble
magnitude | rows_per | to | to_rows_per | distincts | max_rows | tot_rows
----------+----------+----+-------------+-----------+----------+----------
 10^0     |        1 | to |           9 |         1 |        7 |        7
 10^1     |       10 | to |          99 |         2 |       65 |       76
 10^2     |      100 | to |         999 |         1 |      141 |      141

  
Examples:
  SELECT * FROM column_dstr_p( 'my_database', 'my_schema', 'my_table', 'my_column' );
  SELECT * FROM column_dstr_p( 'yellowbrick', 'sys', 'shardstore', 'table_id', 4 );
  
Arguments:
. _db_name     VARCHAR - (required) Database name  
. _schema_name VARCHAR - (required) Schema name 
. _table_name  VARCHAR - (required) Table name
. _column_name VARCHAR - (required) Column name
. _log_n       NUMERIC - (optional) use log and powers of n. Default 10.  

Notes:
. Do not double-quote names. If necessary, they will be double-quoted by the 
  procedure

Version:
. 2022.12.27 - Yellowbrick Technical Support
$cmnt$
;



