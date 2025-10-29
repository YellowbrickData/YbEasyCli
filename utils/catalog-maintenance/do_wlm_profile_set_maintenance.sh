#!/bin/bash
export YBDATABASE=yellowbrick

MAJOR_VER=$(ybsql -XAqt -c "WITH a AS (SELECT current_setting('yb_server_version') AS ver) SELECT split_part(ver, '.', 1) FROM a AS major")
[ $? -ne 0 ] && { echo "ERROR: couldn't get version information, exiting" ; exit 1 ; }

ACTIVATE_SEC=30
MAINTENANCE_SQL=wlm_profile_maintenance.out.sql
# NOTE: just to be on the safe side and not overwrite it
REACTIVATE_SQL=wlm_profile_reactivate_$(date +%Y%m%d_%H%M%S).out.sql

if [ $MAJOR_VER -eq 5 ] ; then
	ybsql -XAqt <<SQL
\set ON_ERROR_STOP 1
\o $REACTIVATE_SQL
SELECT format('ALTER WLM PROFILE %I ACTIVATE $ACTIVATE_SEC WITH CANCEL;', name) AS sql FROM sys.wlm_active_profile WHERE active = 't';
\o
\set ECHO queries
ALTER WLM PROFILE maintenance ACTIVATE $ACTIVATE_SEC WITH CANCEL;
SQL
else
	# NOTE: Need to reconnect between profile activations as it kills the current connection
	SQL="SELECT format('\\c yellowbrick'||chr(10)||'USE CLUSTER %I; ALTER WLM PROFILE %I ACTIVATE $ACTIVATE_SEC WITH CANCEL;', cluster_name, :wlm_profile) AS sql FROM sys.cluster WHERE state = 'RUNNING' ORDER BY cluster_name;"
	ybsql -XAqt <<SQL
\set ON_ERROR_STOP 1
\set wlm_profile active_wlm_profile_name
\o $REACTIVATE_SQL
$SQL
\o
SELECT '''maintenance''' AS wlm_profile
\gset
\o $MAINTENANCE_SQL
$SQL
\o
\set ECHO queries
\i $MAINTENANCE_SQL
SQL
fi
[ $? -ne 0 ] && { echo "There were errors when trying to activate maintenance profile, exiting" ; exit 1 ; }
echo "Maintenance profile is now activated. To reactivate previous profile, run $REACTIVATE_SQL"
