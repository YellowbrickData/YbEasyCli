/* ****************************************************************************
** load_p()
**
** Transformed subset of sys.load columns for active bulk loads.
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
** . 2021.04.20 - Yellowbrick Technical Support (for YB version 5.0)
** . 2020.06.15 - Yellowbrick Technical Support 
** . 2020.02.09 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**  start_time | sid | db_name | schema_name | table_name | user_name | hostname | state 
** ------------+-----+---------+-------------+------------+-----------+----------+-------
** ... 
** ... | secs | insrt_rows_m | errors | insrt_mb | net_mb | parse_mb | net_mbps | cmpr
** ... +------+--------------+--------+----------+--------+----------+----------+------
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS load_t CASCADE
;

CREATE TABLE load_t
   (
      start_time   TIMESTAMP
    , sid          BIGINT
    , db_name      VARCHAR (128)
    , schema_name  VARCHAR (128)
    , table_name   VARCHAR (128)
    , username     VARCHAR (128)
    , hostname     VARCHAR (128)
    , state        VARCHAR (128)
    , secs         NUMERIC (18,1)
    , insrt_rows_m NUMERIC (21,3)
    , errors       BIGINT
    , insrt_mb     NUMERIC (18,1)
    , net_mb       NUMERIC (18,1)
    , parse_mb     NUMERIC (18,1)
    , net_mbps     NUMERIC (18,1)
    , cmpr         VARCHAR (42)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE load_p()
   RETURNS SETOF load_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS
$proc$
DECLARE

   _sql       TEXT         := '';

   _fn_name   VARCHAR(256) := 'load_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   /* Txn read_only to protect against potential SQL injection attack overwrites
   */
   SET TRANSACTION       READ ONLY;
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := 'SELECT
      date_trunc (''secs'', start_time)::TIMESTAMP         AS start_time
    , session_id                                           AS sid
    , TRIM (database_name)::VARCHAR(128)                   AS db_name
    , TRIM (schema_name)::VARCHAR(128)                     AS schema_name
    , TRIM (table_name)::VARCHAR(128)                      AS table_name
    , TRIM (username)::VARCHAR(128)                        AS username
    , TRIM (client_hostname)::VARCHAR(128)                 AS hostname
    , state::VARCHAR(128)                                  AS state
    , ROUND (elapsed_ms    / 1000.00, 1)::NUMERIC(18,1)                   AS secs
    , ROUND (inserted_rows / 1000000.0000, 3)::NUMERIC(21,3)              AS insrt_rows_m
    , error_rows                                                          AS errors
    , ROUND (inserted_bytes / 1024^2 ::NUMERIC (19, 4), 1)::NUMERIC(18,1) AS insrt_mb
    , ROUND (sent_bytes     / 1024^2 ::NUMERIC (19, 4), 1)::NUMERIC(18,1) AS net_mb
    , ROUND (parsed_bytes   / 1024^2 ::NUMERIC (19, 4), 1)::NUMERIC(18,1) AS parse_mb
    , CASE
         WHEN elapsed_ms = 0 THEN 0
         ELSE ROUND ( ( (sent_bytes / elapsed_ms) * (1000.0000) / 1024.00^2), 1)
      END::NUMERIC(18,1) AS net_mbps
    , CASE
         WHEN sent_bytes = 0 THEN 0
         ELSE ROUND ( (parsed_bytes / inserted_bytes::decimal (19, 4)), 1)
      END || '':1'' AS cmpr
   FROM
      sys.load
   ORDER BY
      start_time
   ';

   RETURN QUERY EXECUTE _sql;

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;

-- ALTER FUNCTION load_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION load_p() IS 
'Description:
Transformed subset of sys.load columns for currently active bulk loads.

Bulkloads do not include rows loaded to the rowstore via \copy or INSERT...VALUES.
  
Examples:
  SELECT * FROM load_p();
  
Arguments:
. none

Version:
. 2021.04.20 - Yellowbrick Technical Support 
'
;