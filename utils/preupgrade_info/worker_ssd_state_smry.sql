/* storage_drive_state_smry.sql
**
** Check of SSD wear, errors, and firmware versions.
**
** Example Result:
**   status | num_ssds | avg_pct_wear | max_pct_wear | avg_media_errors | max_media_errors | firmware_versions
**  --------+----------+--------------+--------------+------------------+------------------+-------------------
**        0 |       96 |            0 |            4 |                0 |                0 | EDA7202Q
**  
** Revision History:
** . 2025.02.03 (rek) - Inital version
*/

\o delete.me
SET enable_subquery_remove_useless_order_by TO 'off';
\o

WITH ssd_states_cte AS
(  SELECT
     status            AS status
   , firmware_version  AS firmware_version
   , SUM(percent_wear) AS sum_pct_wear
   , MAX(percent_wear) AS max_pct_wear
   , SUM(media_errors) AS sum_media_errors
   , MAX(media_errors) AS max_media_errors
   , COUNT(*)          AS num_ssds
  FROM sys.drives
  GROUP BY status, firmware_version
  ORDER BY status, firmware_version
)

SELECT
   status                                   AS status
 , SUM(num_ssds)                            AS num_ssds
 , SUM(sum_pct_wear)     / SUM(num_ssds)    AS avg_pct_wear
 , MAX(max_pct_wear)                        AS max_pct_wear
 , SUM(sum_media_errors) / SUM(num_ssds)    AS avg_media_errors
 , MAX(max_media_errors)                    AS max_media_errors
 , string_agg(firmware_version, ',')        AS firmware_versions
FROM ssd_states_cte
GROUP BY status, firmware_version
ORDER BY 1
;