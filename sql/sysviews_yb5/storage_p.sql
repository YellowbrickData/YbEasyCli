/* ****************************************************************************
** public.storage_p()
**
** Aggregated summary of appliance storage for data, spill, other, and total space.
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
** Version History:
** . 2021.04.21 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support
** . 2020.02.16 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  workers | data_gb | data_pct | to_gc_gb | to_gc_pct | other_gb | other_pct | spill_gb | spill_pct | used_gb | used_pct | free_gb | free_pct | total_gb
** ---------+---------+----------+----------+-----------+----------+-----------+----------+-----------+---------+----------+---------+----------+----------
**        8 |   11199 |      9.3 |      176 |       0.1 |    30046 |      25.0 |       28 |       0.0 |   41450 |     34.5 |   78733 |     65.5 |   120183
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS public.storage_t CASCADE
;

CREATE TABLE public.storage_t
   (
      workers   INTEGER
    , data_gb   NUMERIC (19, 0)
    , data_pct  NUMERIC (19, 1)
    , to_gc_gb  NUMERIC (19, 0)
    , to_gc_pct NUMERIC (19, 1)     
    , other_gb  NUMERIC (19, 0)
    , other_pct NUMERIC (19, 1)    
    , spill_gb  NUMERIC (19, 0)
    , spill_pct NUMERIC (19, 1)
    , used_gb   NUMERIC (19, 0)
    , used_pct  NUMERIC (19, 1)
    , free_gb   NUMERIC (19, 0)
    , free_pct  NUMERIC (19, 1)
    , total_gb  NUMERIC (19, 0)
   )
;

/* ****************************************************************************
** Create the procedure.
*/
CREATE PROCEDURE public.storage_p()
   RETURNS SETOF public.storage_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;   
     
  
BEGIN  

   _sql := 'SET ybd_query_tags  TO ''' || _tags || '''';
   EXECUTE _sql ;   
  
  --TODO Kick what is this special number, I had to change '<' to '<='
  _sql := '
  WITH to_gc AS
     (  SELECT worker                    AS worker
         , SUM( size_comp_mib ) * 1000^2 AS to_gc_bytes
        FROM sys.shardstore
        WHERE end_xid <= 72057594037927935
        GROUP BY 1
     )
  
   SELECT
     COUNT(*)::INTEGER                                          AS workers
   , ROUND(( SUM( s.data_gb ) ), 0 )::NUMERIC (19, 0)                            AS data_gb
   , ROUND(( SUM( s.data_gb ) / SUM( s.total_gb ) ) * 100, 1 )::NUMERIC (19, 1)  AS data_pct
   , ROUND(( SUM( s.to_gc_gb ) ), 0 )::NUMERIC (19, 0)                           AS to_gc_gb
   , ROUND(( SUM( s.to_gc_gb ) / SUM( s.total_gb ) ) * 100, 1 )::NUMERIC (19, 1) AS to_gc_pct   
   , ROUND(( SUM( s.spill_gb ) ), 0 )::NUMERIC (19, 0)                           AS spill_gb
   , ROUND(( SUM( s.spill_gb ) / SUM( s.total_gb ) ) * 100, 1 )::NUMERIC (19, 1) AS spill_pct
   , ROUND(( SUM( s.other_gb ) ), 0 )::NUMERIC (19, 0)                           AS other_gb
   , ROUND(( SUM( s.other_gb ) / SUM( s.total_gb ) ) * 100, 1 )::NUMERIC (19, 1) AS other_pct
   , ROUND(( SUM( used_gb ) ), 0 )::NUMERIC (19, 0)                              AS used_gb
   , ROUND(( SUM( used_gb ) / SUM( total_gb ) ) * 100, 1 )::NUMERIC (19, 1)      AS used_pct
   , ROUND(( SUM( free_gb ) ), 0 )::NUMERIC (19, 0)                              AS free_gb
   , ROUND(( SUM( free_gb ) / SUM( total_gb ) ) * 100, 1 )::NUMERIC (19, 1)      AS free_pct
   , ROUND( SUM( total_gb ), 0 )::NUMERIC (19, 0)                                AS total_gb
  FROM(  
         SELECT
          ( distributed_bytes + replicated_bytes + random_bytes )                        AS data_bytes
         , data_bytes     / 1024.0^3                                                     AS data_gb
         ,( scratch_bytes / 1024.0^3 )                                                   AS spill_gb
         ,( used_bytes    - ( data_bytes + scratch_bytes + tg.to_gc_bytes ) )::NUMERIC / 1024.0^3 AS other_gb
         , (tg.to_gc_bytes / 1024.0^3)::NUMERIC                                                     AS to_gc_gb
         , used_bytes     / 1024.0^3                                                     AS used_gb
         , free_bytes     / 1024.0^3                                                     AS free_gb
         , total_bytes    / 1024.0^3                                                     AS total_gb
        FROM sys.storage ss
           JOIN to_gc    tg ON ss.worker_id = tg.worker 
   ) s
  ';

  RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tagsops back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;    

END;   
$proc$ 
;

-- ALTER FUNCTION storage_p()
--    SET search_path = pg_catalog,pg_temp;

COMMENT ON FUNCTION storage_p() IS 
'Description:
Aggregated appliance storage for data, spill, other, and total space.
  
Examples:
  SELECT * FROM storage_p();

Arguments:
. None

Notes:
. This is worker storage across all nodes; front end rowstore space is not included.
. "other" includes uncommitted shards (i.e. running loads, etc..) and system overhead.

Version:
. 2021.04.21 - Yellowbrick Technical Support 
'  
;