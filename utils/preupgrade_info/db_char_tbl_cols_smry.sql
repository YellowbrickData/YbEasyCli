/* db_char_tbl_cols_smry.sql
**
** Summmary of user tables with CHAR cols in the current database.
**
** Revision History:
** . 2026.01.19(rek) - Added left join to return a row when there are no CHAR columns. 
**                     
** . 2025.03.14 (rek) - Initial version.
*/

WITH char_tbl_cols AS
(  SELECT
      current_database()    AS db_name
    , COUNT(DISTINCT c.oid) AS char_tables
    , COUNT(*)              AS char_cols
   FROM pg_catalog.pg_class     AS c
   JOIN pg_catalog.pg_attribute AS a  ON c.oid      = a.attrelid
   JOIN pg_catalog.pg_type      AS pt ON a.atttypid = pt.oid
   WHERE pt.typname  = 'bpchar'
      AND a.attnum  > 0
      AND c.relkind = 'r'
      AND c.oid     >= 16384
   GROUP BY 1
)
, dbs AS
(  SELECT datname FROM pg_database WHERE datname=current_database()
)
SELECT 
   datname                      AS db_name
 , NVL(char_tables, 0)::INTEGER AS char_tables
 , NVL(char_cols,   0)::INTEGER AS char_cols
FROM dbs                AS d 
LEFT JOIN char_tbl_cols AS ctc ON d.datname = ctc.db_name
;
