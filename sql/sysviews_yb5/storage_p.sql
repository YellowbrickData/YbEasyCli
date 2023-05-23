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
** . 2022.09.20 - Fix end_xid for GC filter
** . 2021.12.09 - ybCliUtils inclusion.
** . 2021.04.21 - Yellowbrick Technical Support
** . 2020.06.15 - Yellowbrick Technical Support
** . 2020.02.16 - Yellowbrick Technical Support
*/

/* ****************************************************************************
**  Example results:
**
**  chassis | wpc | workers | data_gb | data_pct | to_gc_gb | to_gc_pct | other_gb | other_pct | spill_gb | spill_pct | used_gb | used_pct | free_gb | free_pct | total_gb
** ---------+-----+---------+---------+----------+----------+-----------+----------+-----------+----------+-----------+---------+----------+---------+----------+----------
**        1 |   8 |       8 |   79964 |     65.5 |        0 |       0.0 |      174 |       0.1 |    24418 |      20.0 |  104556 |     85.6 |   17535 |     14.4 |   122091
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS public.storage_t CASCADE
;

CREATE TABLE public.storage_t
   (
      chassis   INTEGER
    , wpc       INTEGER
    , workers   INTEGER
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
  
  _sql := 'WITH to_gc AS
     (  SELECT SUM( size_comp_mib ) * 1024^2   AS to_gc_bytes
        FROM sys.shardstore
        WHERE end_xid != 72057594037927935
     )
   , usage AS
     (  SELECT 
           COUNT(*)                                                                           AS workers
         , SUM( ss.distributed_bytes + ss.replicated_bytes + ss.random_bytes )                AS data_bytes
         , SUM( ss.scratch_bytes )                                                            AS spill_bytes
         , SUM( ss.used_bytes 
               -( ss.distributed_bytes + ss.replicated_bytes + ss.random_bytes 
                  + ss.scratch_bytes + NVL( tg.to_gc_bytes, 0 ) 
                ) 
              )                                                                               AS other_bytes
         , SUM( nvl( tg.to_gc_bytes, 0 ) ) ::NUMERIC                                          AS to_gc_bytes
         , SUM( ss.used_bytes )                                                               AS used_bytes
         , SUM( ss.free_bytes )                                                               AS free_bytes
         , SUM( ss.total_bytes )                                                              AS total_bytes
        FROM sys.storage AS ss
        CROSS JOIN to_gc AS tg
     )
   , workers AS
     (  SELECT 
           MAX(chassis_id)              + 1 AS chassis
         , COUNT( DISTINCT logical_id )     AS workers_per_chassis
         , COUNT(*)                         AS workers
        FROM sys.worker
        WHERE role = ''MEMBER''
     )
         SELECT
     w.chassis::INTEGER                                                                        AS chassis
   , w.workers_per_chassis::INTEGER                                                            AS wpc
   , w.workers::INTEGER                                                                        AS workers
   , ROUND(( u.data_bytes  / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS data_gb
   , ROUND(( u.data_bytes  / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS data_pct
   , ROUND(( u.to_gc_bytes / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS to_gc_gb
   , ROUND(( u.to_gc_bytes / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS to_gc_pct
   , ROUND(( u.other_bytes / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS other_gb
   , ROUND(( u.other_bytes / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS other_pct
   , ROUND(( u.spill_bytes / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS spill_gb
   , ROUND(( u.spill_bytes / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS spill_pct
   , ROUND(( used_bytes    / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS used_gb
   , ROUND(( used_bytes    / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS used_pct
   , ROUND(( free_bytes    / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS free_gb
   , ROUND(( free_bytes    / u.total_bytes::NUMERIC( 19, 1 ) ) * 100, 1 )::NUMERIC( 19, 1 )    AS free_pct
   , ROUND(( total_bytes   / 1024.0^3 ), 0 )::NUMERIC( 19, 0 )                                 AS total_gb
  FROM usage         AS u
  CROSS JOIN workers AS w
  ';

  RETURN QUERY EXECUTE _sql ;

   /* Reset ybd_query_tagsops back to its previous value
   */
   _sql := 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   EXECUTE _sql ;    

END;   
$proc$ 
;


COMMENT ON FUNCTION storage_p() IS 
$cmnt$Description:
Aggregated appliance storage for data, spill, to GC, other, and total space.
  
Examples:
  SELECT * FROM storage_p();

Arguments:
. None

Notes:
. This is worker storage across all nodes; front end rowstore space is not included.
. "other" includes uncommitted shards (i.e. running loads, etc..) and system overhead.

Version:
. 2022.09.20 - Yellowbrick Technical Support 
$cmnt$
;
