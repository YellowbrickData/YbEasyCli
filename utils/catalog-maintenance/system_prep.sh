#!/bin/bash
#
#
export YBDATABASE=yellowbrick
if [ "$1" == "pre" ] ; then
	echo "-- Doing pre-maintenance steps"
	AUTOVACUUM='ALTER SYSTEM SET autovacuum TO off'
	$(dirname $0)/do_wlm_profile_set_maintenance.sh
elif [ "$1" == "post" ] ; then
	echo "-- Doing post-maintenance steps"
	LATEST_WLM=$(ls -1 -t wlm_profile_reactivate_*.out.sql | head -n1)
	if [ -z "$LATEST_WLM" ] ; then
		echo "WARNING: couldn't find WLM reactivation SQL script!"
	else
		ybsql -XAqte -f $LATEST_WLM
	fi
	AUTOVACUUM='ALTER SYSTEM RESET autovacuum'
else
	echo "Usage: $(basename $0) [pre|post]"
	exit
fi


ybsql -Xqte<<SQL
${AUTOVACUUM};
SELECT pg_reload_conf();
-- The \c is necessary so the updated conf value shows
\c
SHOW autovacuum;
SQL


# The system table showing the active WLM profile changes in YB 7 
yb_ver_num="$(ybsql -XAqt -c 'SHOW yb_server_version_num')"

# yb_server_version_num is of the form VMMmm. i.e. 7.4.2 -> 70402
# YB 7.4 introduces legacy tables for pre 7.4 session and authentication log data
if [[ ${yb_ver_num} -lt 70000 ]]; then 
  profile_sql="SELECT name FROM sys.wlm_active_profile WHERE active = TRUE"
else
  profile_sql="SELECT profile_name FROM sys.wlm_active_profile"
fi

active_profile="$(ybsql -XAqt -c "${profile_sql}")"
echo "ACTIVE WLM PROFILE IS NOW '${active_profile}'"