/* ****************************************************************************
** public.storage_by_worker_p()
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
** . 2022.12.27 - YbEasyCli inclusion.
** . 2022.09.21 - Yellowbrick Technical Support.
*/

/* ****************************************************************************
**  Example results:
**
**  chassis | id |              worker_id               | data_gb | data_pct | to_gc_gb | to_gc_pct | other_gb | other_pct | spill_gb | spill_pct | used_gb | used_pct | free_gb | free_pct | total_gb
** ---------+----+--------------------------------------+---------+----------+----------+-----------+----------+-----------+----------+-----------+---------+----------+---------+----------+----------
**        0 |  0 | 00000000-0000-0000-0000-38b8ebd014cd |    9998 |     65.5 |        0 |       0.0 |       22 |       0.1 |     3052 |      20.0 |   13072 |     85.7 |    2189 |     14.3 |    15261
**        0 |  1 | 00000000-0000-0000-0000-38b8ebd01342 |    9982 |     65.4 |        0 |       0.0 |       22 |       0.1 |     3052 |      20.0 |   13056 |     85.5 |    2206 |     14.5 |    15261
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS public.storage_by_worker_t CASCADE
;

CREATE TABLE public.storage_by_worker_t
   (
      chassis   INTEGER
    , id        INTEGER
    , worker_id UUID
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
CREATE PROCEDURE public.storage_by_worker_p()
   RETURNS SETOF public.storage_by_worker_t
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

   EXECUTE 'SET ybd_query_tags  TO ''' || _tags || '''';
  
  _sql := 'WITH to_gc AS
     (  SELECT worker                    AS worker
         , SUM( size_comp_mib ) * 1000^2 AS to_gc_bytes
        FROM sys.shardstore
        WHERE end_xid != 72057594037927935
        GROUP BY 1
     )
   , usage AS
     (  SELECT ss.worker_id                                                                   AS worker_id
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
        FROM sys.storage   AS ss
           LEFT JOIN to_gc AS tg ON ss.worker_id = tg.worker
           group by 1
     )
   , workers AS
     (  SELECT chassis_id
         , logical_id
         , worker_id
        FROM sys.worker
        WHERE role = ''MEMBER''
     )
  SELECT w.chassis_id                                                                          AS chassis
   , w.logical_id                                                                              AS id
   , w.worker_id                                                                               AS worker_id
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
  FROM usage      AS u
     JOIN workers AS w
  USING( worker_id )
  ORDER BY 1, 2
  ';

  RETURN QUERY EXECUTE _sql ;

   -- Reset ybd_query_tags to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';  

END;   
$proc$ 
;


COMMENT ON FUNCTION storage_by_worker_p() IS 
$cmnt$Description:
Aggregated appliance storage space by worker including: data, to GC, spill, other
, and total space.
  
Examples:
  SELECT * FROM storage_by_worker_p();

Arguments:
. None

Notes:
. This is worker storage across all blades; front end rowstore space is not included.
. "other" includes uncommitted shards (i.e. running loads, etc..) and system overhead.

Version:
. 2022.09.21 - Yellowbrick Technical Support 
$cmnt$  
;