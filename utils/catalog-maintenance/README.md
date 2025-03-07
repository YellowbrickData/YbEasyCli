# Overview

This directory contains SQL and bash snippets for use in YB catalog maintenance activities.
The subset of these that you use will depend upon your environment and catalog
maintenance goals.

If you are unsure of what to use, check the article
[Catalog VACUUM in Yellowbrick](https://support.yellowbrick.com/hc/en-us/articles/17702073770771-Catalog-VACUUM-in-Yellowbrick).

If you still have questions, open a ticket with Yellowbrick Technical Support.

# Prerequisites

1. Linux host with ybsql access to the YB instance.
2. YB superuser access (you can use a manager node).
3. If you are not running from the manager node, then `YBHOST`, `YBUSER`, and
   `YBPASSWORD` environment variables must all be set. i.e.:

```bash
export YBHOST=10.10.10.10
export YBUSER=yellowbrick
export YBPASSWORD=yellowbrick
```

# Installation

1. Get the latest Zip file from YbEasyCli reporsitory (TODO: link).
2. Copy the Zip file to some directory on your Linux host. i.e. ~
3. `unzip` the file
4. `cd` into the created directory. i.e. `cd catalog_maint`
5. `chmod +x *.sh`

# Usage

It would be beneficial for the maintenance activity if:

- write jobs have been stopped
- there are no replication jobs running
- there is limited load on the appliance
- there is not a large number of databases causing continual GC and auto-analyze jobs running

## Notes

- This will create blocking locks
- Sometimes it could be necessary to stop all services and start only front-end PostgreSQL service to avoid continuous blocking locks by system processes running by Lime (like GC, auto-analyze etc) - **this DOES NOT apply to CloudNative platform**

1. Display/capture old backup snapshots, long running non-idle sessions and overall system catalog usage:

   `./run_checks.sh` (this will produce `run_checks_<TIMESTAMP>.out.log` file with the captured screen output) or `ybsql -Xq -d yellowbrick -f catalog_pre_checks.sql` (this will just dump everything on the screen, no log would be created).

2. If this is a replication source or target, stop the replication jobs **on the source**:

   1. `ybsql -Xq -d yellowbrick -f gen_replication_stop_start.sql` - this will generate two files:

      1. `do_replication_stop.out.sql`: pause all running replicas
      2. `do_replication_restart.out.sql`: start all previously paused replicas with 60 seconds interval

   2. Run `ybsql -Xqe -h replication_source -d yellowbrick -f do_replication_stop.out.sql` to stop all active replication jobs

3. Prepare the system for catalog maintenance (it will save the current WLM profile, apply WLM maintenace mode and disable autovacuum, in that order):

   `./system_prep.sh pre`

4. If necessary, activate PG-only mode (**this will cause downtime**):

   If the appliance is configured with LDAP, run `./start_pg_mode_with_ldap.sh`; if there's no LDAP, use `./start_pg_mode_no_ldap.sh` - this will stop **all** services and then start only PostgreSQL.

1. Execute the main catalog vacuum script: by default (without any parameters) it will spawn 4 parallel processes/jobs, for **user database catalog maintenance only**. You can use `+global` command line switch to also include the global catalog maintenance, which will be started as additional Job 0. Usage:

   `./catalog_dbs_maint.sh [+global]`

   There's a way to start global catalog maintenance only, as a separate standalone process, if needed:

   `ybsql -Xqte -d yellowbrick -f catalog_yb_maint.sql`

6. Only if PG-only mode was activated before the maintenance, restart everything:

   1. If the appliance is configured with LDAP, run `./stop_pg_mode_with_ldap.sh`; if there's no LDAP, use `./stop_pg_mode_no_ldap.sh` - this will stop only runnig PostgreSQL.
   2. Run `ybcli database start` and check the status with `ybcli system status`

7. Once the maintenance is finished, run post-maintenance steps (it will reactivate previously used WLM profile and enable autovacuum - **THIS IS VERY IMPORTANT!**):

   `./system_prep.sh post`

1. If this is a replication source or target, restart the replication jobs **on the source** (if there are many replicas, use `screen` so as not to lose the session):

   `ybsql -Xqe -h replication_source -d yellowbrick -f do_replication_restart.out.sql`

9. Run checks again to see the result of the maintenance activity:

   `./run_checks.sh`

10. Optionally, if you want to keep the logs for historical/reporting purposes, compress them (the script will show the name of the archive once done):

    `./compress_logs.sh`

# Script Summary

**NOTE**: striked out scripts are deprecated, but are in the reporsitory still.

|Name                                 |Description                                                             |
|:------------------------------------|------------------------------------------------------------------------|
|`run_checks.sh`                      |Display important system checks and save them to a log file (essentialy a convenience wrapper around `catalog_pre_checks.sql` script)|
|`system_prep.sh`                     |Disables/enables autovacuum and turns WLM maintenance mode on/off in a single shot, accepts a single parameter, which could be `pre` or `post`|
|`catalog_dbs_maint.sh`               |The main maintenance script, runs the global system catalog script (`catalog_yb_maint.sql`) as Job 0 (**only** when `+global` command line switch is used) and four user database catalog scripts (`catalog_db_maint_[1-4].sql`) as concurrent processes|
|`catalog_yb_maint.sql`               |`VACUUM FULL` a few "yellowbrick" database catalog tables|
|`catalog_db_maint.sql`               |`VACUUM FULL` a few non-shared PG catalog tables, local to the current DB|
|`catalog_db_vacuum_full.sql`         |`VACUUM FULL` the entire current database|
|`catalog_pre_checks.sql`             |Check important system states prior to starting catalog maintenance|
|`do_clean_sys_aalog.sql`             |Remove orphan rows from sys.aalog, only needs to be run once after upgrade to 5.2.17 or later|
|`do_clean_sys_log_authentication.sql`|Remove rows from `sys.log_authentication` older than 90 days|
|`do_clean_sys_log_session.sql`       |Remove rows from `sys.log_session` older than 90 days|
|~~`do_disable_gc.sh`~~               |Disable GC via the Lime client|
|~~`do_reenable_gc.sh`~~              |Re-enable GC via the Lime client if disabled in maintenance|
|`do_wlm_profile_set_maintenance.sh`  |Enable/disable WLM maintenance profile, gets called by `system_prep.sh`|
|~~`dump_dbs_catalog_sizes.sh`~~      |Dump catalog sizes for all DBs to disk|
|`get_db_catalog_table_sizes.sql`     |Display total/index/toast sizes for catalog tables (if the current database is "yellowbrick", then all shared tables would be included in the output, otherwise only non-shared tables are displayed), sorted descending by total size|
|`get_long_running_txns.sql`          |Show non-idle sessions running for more than 60 seconds|
|`get_backup_chains.sql`              |Show all backup chains on the appliance|
|`get_old_backup_chains.sql`          |Show backup chains with most recent snapshot older than 30 days, show backup chain executive summary report, generate `DROP BACKUP CHAIN` SQL script for old chains|
|`get_user_database_sizes.sql`        |Show full report on user database sizes, sorted descending by size|
|`get_user_database_sizes_summary.sql`|Show user database sizes executive summary, sorted descending by size|
|`get_system_catalog_total_size_summary.sql`|Show global and user system catalogs executive summary, along with total system catalog utilization (similar to what `ybcli status storage` shows)|
|`gen_catalog_dbs_maint.sql`          |Generate SQL files to VACUUM user databases in parallel|
|`gen_replication_stop_start.sql`     |Generate SQL scripts to pause/resume all running replication jobs|
|`start_pg_mode_no_ldap.sh`           |Start YB in PG only mode when configured without LDAP|
|`start_pg_mode_with_ldap.sh`         |Start YB in PG only mode when configured with LDAP|
|`stop_pg_mode_no_ldap.sh`            |Stop YB in PG only mode when configured without LDAP|
|`stop_pg_mode_with_ldap.sh`          |Stop YB in PG only mode when configured with LDAP|
|`compress_logs.sh`                   |Put all generated files (scripts and logs) into a single `.tgz` archive and delete them|
|**Generated scripts**||
|`drop_old_backup_chains.out.sql`     |Generated by `get_old_backup_chains.sql`, contains `DROP BACKUP CHAIN` statements for all chains with the most recent snapshot older than 30 days. **Needs reviewing before executing!** Contains a safeguard against accidental run - the very first line is `\q`, which makes ybsql exit immediately.|
|`do_replication_stop.out.sql`        |Generated by `gen_replication_stop_start.sql` to pause all running replication jobs|
|`do_replication_restart.out.sql`     |Generated by `gen_replication_stop_start.sql` to resume all previously active replication jobs|
|`catalog_dbs_maint_0.out.sql`        |Generated by `gen_catalog_dbs_maint.sql`, it's just a copy of `catalog_yb_maint.sql` file to serve as Job 0|
|`catalog_dbs_maint_[1-4].out.sql`    |Generated by `gen_catalog_dbs_maint.sql`|
|`wlm_profile_maintenance.out.sql`    |Generated by `do_wlm_profile_set_maintenance.sh`|
|`wlm_profile_reactivate_<timestamp>.out.sql`      |Generated by `do_wlm_profile_set_maintenance.sh`|

# More detailed description of certain scripts

## Shell scripts

### `run_checks.sh`

Runs `catalog_pre_checks.sql`, which collects and displays the following information:

1. Potentially stale backup chains (those with the most recent snapshot older than 30 days)
2. All non-idle sessions running for longer than 60 seconds
3. System catalog full details and summary
4. Active replicas

### `system_prep.sh`

- Run it before/after catalog maintenance.
- Accepts one parameter, which can be `pre` (use it before starting the maintenance) or `post` (use it after the maintenance is done).
- When used with `pre` parameter:
  - Generates a SQL script to reactivate the current WLM profile.
  - Activates maintenance WLM profile on the appliance.
  - Sets autovacuum to OFF
- When used with `post` parameter:
  - Reactivates previously saved WLM profile using the SQL script generated on `pre` step.
  - Sets catalog autovacuum to ON

**YOU MUST**:

- Disable autovacuum before doing `VACUUM FULL`
- Re-enable autovacuum when done with catalog maintenance.

### `do_disable_gc.sh`

Disables Garbage Collector via the Lime client.

- Can only be run from the manager node debug shell.
- Typically done if > 8 databases.
- It is **CRITICAL** to renable GC after maintenace is done!
- Run `do_reenable_gc.sh` if you ran this script.

### `do_reenable_gc.sh`

Re-enables GC via the lime client if disabled in maintenance.

Can only be run from a manager node debug shell.

### `do_wlm_profile_set_maintenance.sh`

- Sets the current WLM profile to the maintenance one (for CN instances this is applied to all currently running clusters).
- Generates the file `wlm_profile_reactivate_<timestamp>.out.sql` to set the active WLM profile(s) back to its original value(s).
- For CN instances it generates `ALTER WLM PROFILE` SQL statements for all currently running clusters.
- Used by `system_prep.sh`

### `start_pg_mode_no_ldap.sh`

Starts YB in PG only mode when configured with LDAP.

**WARNING**: This should not be used without YB Technical Support.

- You can put your appliance into a state where you cannot restart it and/or have damaged the catalog.
- If you use this, you need to stop YB using the `stop_pg_mode_with_ldap.sh` before attempting to restart YB.

### `start_pg_mode_with_ldap.sh`

Starts YB in PG only mode when configured with LDAP.

**WARNING**: This should not be used without YB Technical Support.

- You can put your appliance into a state where you cannot restart it and/or have damaged the catalog.
- If you use this, you need to stop YB using `stop_pg_mode_with_ldap.sh` before attempting to restart YB.

### `stop_pg_mode_no_ldap.sh`

Stops YB if running in PG only mode when configured with LDAP. You would use this only when YB was started in PG only mode (i.e. when started with `stop_pg_mode_no_ldap.sh`).

### `stop_pg_mode_with_ldap.sh`

Stops YB if running in PG only mode when configured with LDAP. You would use this only when YB was started in PG only mode (i.e. when started with `stop_pg_mode_no_ldap.sh`).


## SQL scripts

### `catalog_db_maint.sql`

- `VACUUM FULL` a few of the non-shared PG tables commonly needing it.
- Operates only in the current database.
- This does not lock shared objects as `VACUUM FULL` of the entire database.
- It can be blocked and block write operations like `CTAS`, `ANALYZE`, `GC`, etc.
- This is much faster and less problematic than `VACUUM FULL` on the entire DB.

### `catalog_yb_maint.sql`

Runs `VACUUM FULL` on "yellowbrick" database catalog tables typically needing it:

- Shared `sys.*` catalog tables
- Shared `pg_catalog.*` tables
- Non-shared `pg_catalog.*` tables

This is meant only to be run against the "yellowbrick" database.

### `do_clean_sys_log_authentication.sql`

- Removes rows from `sys.log_authentication` older than 90 days.
- Does a `\COPY` of data to be removed to a CSV file before deleting.
- You should `VACUUM FULL` the table after deleting old rows.

### `do_clean_sys_log_session.sql`

- Removes rows from `sys.log_session` older than 90 days.
- Does a `\COPY` of data to be removed to a CSV file before deleting.
- You should `VACUUM FULL` the table after deleting old rows.
- Uses `end_time` instead of `start_time` as sessions can span multiple days.

### `do_reactivate_wlm_profile_<timestamp>.out.sql`

A SQL file generated by `do_wlm_profile_set_maintenance.sh` to set the active WLM profile back to its original value.

- A timestamp is added as a safety guard against accidentally running `do_wlm_profile_set_maintenance.sh` the second time, as otherwise it would just overwrite previously generated file.
- For CN instances it restores previously activated WLM profiles on all running clusters.

### `gen_catalog_dbs_maint.sql`

- Generates four SQL files (`catalog_dbs_maint_[1-4].sql`)
- These files each run `catalog_db_maint.sql` on a different 1/4 of the databases
- These SQL files are run as separate processes (forks) by `catalog_dbs_maint.sh`
