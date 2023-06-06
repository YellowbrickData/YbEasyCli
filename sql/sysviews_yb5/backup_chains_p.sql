/* ****************************************************************************
** backup_chains_p()
**
** Existing backup chains with creating and snapshot info.
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
** . 2023.06.05 - Cosmetic code updates.
** . 2023.05.15 - ybCliUtils inclusion.
*/

/* ****************************************************************************
**  Example results:
**
**  database_id | database_name |   chain_name   | chain_type | history  |    creation_time    | chain_days |    last_snapshot    | snapshot_days |               policy
** -------------+---------------+----------------+------------+----------+---------------------+------------+---------------------+---------------+------------------------------------
**        40673 | db_a          | April2021      | backup     | previous | 2021-03-30 18:33:44 |        797 |                     |               |
**        40673 | db_a          | db_a_202111    | backup     | previous | 2021-11-01 15:57:10 |        581 | 2021-11-01 15:57:10 |           581 |
**        40673 | db_a          | db_a_2022Nov   | backup     | current  | 2022-11-16 02:07:26 |        202 | 2022-11-16 02:07:26 |           202 |
**        40673 | db_a          | default        | backup     | current  | 2023-01-07 01:42:18 |        150 | 2023-01-07 01:42:18 |           150 | "excludedSchemas":["excluded\\_1"]
*/

/* ****************************************************************************
** Create a table to define the rowtype that will be returned by the procedure.
*/
DROP TABLE IF EXISTS backup_chains_t CASCADE
;

CREATE TABLE backup_chains_t
(
   database_id   INT8                       
 , database_name VARCHAR(128)    
 , chain_name    VARCHAR(256)     
 , chain_type    VARCHAR(16)   
 , history       VARCHAR(16) 
 , creation_time TIMESTAMP  
 , chain_days    INT4    
 , last_snapshot TIMESTAMP
 , snapshot_days INT4                      
 , policy        VARCHAR(60000)                       
          

)
;


/* ****************************************************************************
** Create the procedure.
*/
CREATE OR REPLACE PROCEDURE backup_chains_p( 
      _trunc_policy   BOOLEAN DEFAULT 'FALSE'
    , _yb_util_filter VARCHAR DEFAULT 'TRUE' 
   )
   RETURNS SETOF backup_chains_t
   LANGUAGE 'plpgsql' 
   VOLATILE
   CALLED ON NULL INPUT
   SECURITY DEFINER
AS 
$proc$
DECLARE

   _sql       TEXT         := '';
   
   _fn_name   VARCHAR(256) := 'backup_chains_p';
   _prev_tags VARCHAR(256) := current_setting('ybd_query_tags');
   _tags      VARCHAR(256) := CASE WHEN _prev_tags = '' THEN '' ELSE _prev_tags || ':' END || 'sysviews:' || _fn_name;    
  
BEGIN  

   -- Prefix ybd_query_tags with procedure name
   EXECUTE 'SET ybd_query_tags  TO ' || quote_literal( _tags ); 
   PERFORM sql_inject_check_p('_yb_util_filter', _yb_util_filter);   

   _sql := 'WITH curr_chains AS
     (SELECT  database_id       AS database_id
            , policy            AS policy
            , for_replication   AS for_replication
            , MAX(creation_time)AS creation_time
     FROM     sys.backup_chains
     GROUP BY database_id, policy, for_replication
     )
   , all_chains AS
     (SELECT   d.database_id::INT8                                                                                 AS database_id
             , d.name::VARCHAR(128)                                                                                AS database_name
             , ac.chain_name::VARCHAR(256)                                                                         AS chain_name
             , DECODE( for_replication, ''t'',''replication'',''backup'')::VARCHAR(16)                             AS chain_type             
             , CASE WHEN ac.creation_time = cc.creation_time THEN ''current'' ELSE ''previous'' END::VARCHAR(16)   AS history
             , date_trunc(''secs'', ac.creation_time)::TIMESTAMP                                                   AS creation_time
             , ceil(extract(epoch FROM(CURRENT_TIMESTAMP - ac.creation_time)) /(60 * 60 * 24))::INT4               AS chain_days
             , date_trunc(''secs'', bs.creation_time)::TIMESTAMP                                                   AS last_snapshot
             , ceil(extract(epoch FROM(CURRENT_TIMESTAMP - bs.creation_time)) /(60 * 60 * 24))::INT4               AS snapshot_days
             , (CASE ' || quote_literal(_trunc_policy) || '::BOOLEAN 
                  WHEN ''F'' THEN TRANSLATE(policy, ''{}'','''')
                  ELSE       SUBSTR(TRANSLATE(policy,''{}'',''''), 1, 6) 
               END)::VARCHAR(60000)                                                                                AS policy             
     FROM      sys.backup_chains ac
     LEFT JOIN curr_chains cc
        USING (database_id, policy, for_replication)
     LEFT JOIN sys.backup_snapshots bs
        ON ac.last_backup_point_id = bs.snapshot_name
        AND cc.database_id         = bs.database_id
     JOIN      sys.database d
        ON cc.database_id = d.database_id
     )
   SELECT   *
   FROM     all_chains
   ORDER BY 1, 2, 6
   ';

   RETURN QUERY EXECUTE _sql;

   -- Reset ybd_query_tags back to its previous value
   EXECUTE 'SET ybd_query_tags  TO ''' || _prev_tags || '''';
   
END;   
$proc$
;

   
COMMENT ON FUNCTION backup_chains_p( BOOLEAN, VARCHAR ) IS 
$cmnt$Description:
Existing backup chains with creating and snapshot info. 

Useful in finding unnecessary existing backup chains for deletion.
  
Examples:
  SELECT * FROM backup_chains_p();
  SELECT * FROM backup_chains_p()    WHERE chain_type != 'replication';
  SELECT * FROM backup_chains_p('f') WHERE chain_days > 45;  
  
Arguments:
. _trunc_policy   BOOLEAN - (optl ) Truncate the chain policy desc at 6 chars.
                            The policy is the schema exclude list if used.  
                            DEFAULT 'FALSE'
. _yb_util_filter VARCHAR - (intrn) Used by YbEasyCli.
                            DEFAULT 'TRUE' 
Version:
. 2023.06.05 - Yellowbrick Technical Support
$cmnt$
;
