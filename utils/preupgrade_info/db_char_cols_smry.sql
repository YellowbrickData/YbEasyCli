/* db_char_cols_smry.sql
**
** Summmary of user tables with CHARin the current database.
**
** Revision History:
** . 2025.03.14 (rek) - Added CTE with left join so aways returns a row even 
**                      when there are no CHAR columns.
** . 2025.03.14 (rek) - Initial version.
**
** (c) 2025 Yellowbrick Data Corporation.
** . This script is provided free of charge by Yellowbrick Data Corporation as a 
**   convenience to its customers.
** . This script is provided "AS-IS" with no warranty whatsoever.
** . The customer accepts all risk in connection with the use of this script, and
**   Yellowbrick Data Corporation shall have no liability whatsoever.
*/

/* ****************************************************************************
** Example results:
**    db_name  | char_tables | char_cols
** ------------+-------------+-----------
**  a_database |           1 |         2
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
SELECT 
   datname                      AS db_name
 , NVL(char_tables, 0)::INTEGER AS char_tables
 , NVL(char_cols,   0)::INTEGER AS char_cols
FROM pg_database        AS d 
LEFT JOIN char_tbl_cols AS ctc ON d.datname = ctc.db_name
;

