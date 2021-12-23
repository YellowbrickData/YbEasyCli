/* ****************************************************************************
** log_bulk_xfer_p()
**
** Transformed subset of sys.load and sys.unload for active bulk transfers.
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
** . 2021.12.09 - ybcli inclusion.
** . 2021.05.08 - Yellowbrick Technical Support 
*/

/* ****************************************************************************
**  Example results:
**
**      start_time      |  type  | db_name | username |    hostname     |  state  | secs | rows | bytes | mbps
** ---------------------+--------+---------+-----------+-----------------+---------+------+------+-------+------
**  2021-04-22 20:54:10 | ybload | kick    | kick      | LAPTOP-RLSCI6RN | ERROR   |   66 |    0 |     0 |    0
**  2021-04-22 20:54:58 | ybload | kick    | kick      | LAPTOP-RLSCI6RN | RUNNING |   18 |    0 |     0 |    0
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS log_bulk_xfer_t CASCADE
;

CREATE TABLE log_bulk_xfer_t
   (
      start_time   TIMESTAMP
    , type         VARCHAR(12)
    , db_name      VARCHAR(128)
    , username     VARCHAR(128)
    , hostname     VARCHAR(128)
    , state        VARCHAR(128)
    , secs         NUMERIC(12,1)
    , rows         BIGINT
    , bytes        BIGINT    
    , mbps         NUMERIC(12,1)
   )
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE log_bulk_xfer_p()
   RETURNS SETOF log_bulk_xfer_t 
   LANGUAGE 'plpgsql' 
   VOLATILE
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';

   _fn_name   VARCHAR(256) := 'log_bulk_xfer_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- SET TRANSACTION       READ ONLY;
   
   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;    

   _sql := '
   SELECT
      date_trunc (''secs'', start_time)::TIMESTAMP   AS start_time
    , ''ybload''::VARCHAR(12)                        AS type
    , database_name::VARCHAR(128)                    AS db_name
    , username::VARCHAR(128)                         AS username
    , client_hostname::VARCHAR(128)                  AS hostname
    , state::VARCHAR(128)                            AS state
    , ROUND (elapsed_ms / 1000.00, 1)::NUMERIC(12,1) AS secs
    , inserted_rows                                  AS rows
    , inserted_bytes                                 AS bytes
    , CASE
         WHEN elapsed_ms = 0 THEN 0
         ELSE ROUND ( ( (sent_bytes / elapsed_ms) * (1000.0000) / 1024.00^2), 1)
      END::NUMERIC(12,1)                             AS mbps
   FROM
      sys.log_load
   UNION ALL
   SELECT
      date_trunc (''secs'', start_time) ::TIMESTAMP  AS start_time
    , ''ybunload''                                   AS type
    , database_name::VARCHAR(128)                    AS db_name
    , username::VARCHAR(128)                         AS username
    , client_hostname::VARCHAR(128)                  AS hostname
    , state::VARCHAR(128)                            AS state
    , ROUND (elapsed_ms / 1000.00, 1)::NUMERIC(12,1) AS secs
    , sent_rows                                      AS rows
    , sent_bytes                                     AS bytes
    , CASE
         WHEN elapsed_ms = 0 THEN 0
         ELSE ROUND ( ( (sent_bytes / elapsed_ms) * (1000.0000) / 1024.00^2), 1)
      END::NUMERIC(12,1)                             AS mbps
   FROM
      sys.log_unload
   ORDER BY
      start_time
   ';

   RETURN QUERY EXECUTE _sql; 

   /* Reset ybd_query_tags back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ; 
   
END;   
$proc$ 
;


COMMENT ON FUNCTION log_bulk_xfer_p() IS 
'Description:
Transformed subset completed bulk transfers (ybload & ybunload) from sys.log_load and sys.log_unload.

Bulkloads do not include rows loaded to the rowstore via \copy or INSERT...VALUES.
  
Examples:
  SELECT * FROM log_bulk_xfer_p();
  
Arguments:
. none

Version:
. 2021.12.09 - Yellowbrick Technical Support 
'
;