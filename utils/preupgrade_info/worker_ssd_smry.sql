/* worker_ssd_smry.sql
** 
** Summary of YB EFFECTIVE column store BY SSD space usage.
**
** Derived the storage_drive_smry query but with minor name and 1 col change.
**
** Effective in this case means the space usage given the worst SDD.
** Looking at actual space usage this way is more meaningful as the space usage
** reported by SMC and system views is based on an average across SSDs. However,
** . An appliance is effectively out of data storage space when any SSD becomes
**   full. For example, if you have heavily skewed data, 20% of storage space
**   could still be free but YB is "out of space" becuase one SSD is full.
** . There is an inherient assumption that all SSDs are the same size but there
**   are (very uncommon) situations where that may not be the case.
**  
** Columns:                 
** . ssd_gb          - The size of the SMALLEST SSD becomes the gating size.
** . max_data_gb     - The max amount of data in GB on any SSD.
** . avg_data_gb     - The average amount of data in GB across all SSDs.
** . tmp_gb          - The spill space in GB of the smallest SSD.
** . max_used_gb     - The max data on an SSD plus min spill GB is effectively
**                     the most full SSD.
** . min_free_gb     - The amount of free space on the smallest SSD assuming 
**                     the ssd_max_used_gb.
** . max_data_pct    - The max effective usage % of any SSD (using *eff*)
** . spill_pct       - The allocated temp space %.
** . used_pct        - The % used space (data + spill) assuming SSDs are all
**                     the size of the smallest SSD
** . wrkrs           - The number of active (not expected) workers.  
** . ssds            - The number of active (not expected) SSDs
** . ttl_data_gb     - The total GB used for data+spill assuming smallest SSDs.
** . ttl_tmp_gb      - The total GB that would be used for temp/spill assuming
**                     all SSDs are the size of the smallest SSD.
** . ttl_free_gb     - The total GB of free space assuming all SSDs are the size
**                     of the smallest SSD.
** . ttl_gb          - The total GB of storage space assuming all SSDs are the 
**                     size of the smallest SSD.
**
** Example output:
**   ssd_gb | avg_data_gb | max_data_gb | tmp_gb | max_used_gb | min_free_gb | max_used_pct | tmp_pct | used_pct | wrkrs | ssds | ttl_data_gb | ttl_tmp_gb | ttl_free_gb | ttl_gb
**  --------+-------------+-------------+--------+-------------+-------------+--------------+---------+----------+-------+------+-------------+------------+-------------+--------
**     1788 |          21 |          23 |    358 |         381 |        1408 |         21.3 |    20.0 |     21.3 |    12 |   96 |        1994 |      34368 |      135286 | 171648  
**        
** History:
** . 2026-01-17 (rek) - Iinitial version derived from storage_drive_smry.sql.
*/
WITH drive_storage_smry AS
(  SELECT
     FLOOR(MIN(total_bytes)               / 1024.0^3)                    AS "ssd_gb"
   , CEIL(AVG(used_bytes - scratch_bytes) / 1024.0^3)                    AS "avg_data_gb"            
   , CEIL(MAX(used_bytes - scratch_bytes) / 1024.0^3)                    AS "max_data_gb"  
   , CEIL(MIN(scratch_bytes)              / 1024.0^3)                    AS "tmp_gb"
   , max_data_gb + tmp_gb                                                AS "max_used_gb"
   , FLOOR(MIN(free_bytes)                / 1024.0^3)                    AS "min_free_gb"
   , ROUND((max_used_gb * 100)            / ssd_gb, 1)::NUMERIC(4,1)     AS "max_used_pct"
   , ROUND((tmp_gb * 100)                 / ssd_gb, 1)::NUMERIC(4,1)     AS "tmp_pct"
   , ROUND((max_used_gb * 100)            / ssd_gb, 1)::NUMERIC(4,1)     AS "used_pct"
   , COUNT(DISTINCT worker_id)                                           AS "wrkrs"
   , COUNT(*)                                                            AS "ssds"
   , CEIL(SUM(used_bytes - scratch_bytes) / 1024.0^3)                    AS "ttl_data_gb"
   , CEIL((tmp_gb * ssds))                                               AS "ttl_tmp_gb"
   , (ssd_gb * ssds) - (ttl_data_gb + ttl_tmp_gb)                        AS "ttl_free_gb"
   , (ssd_gb * ssds)                                                     AS "ttl_gb"
  FROM sys.drive_storage
)

SELECT * FROM drive_storage_smry
;
