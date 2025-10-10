#!/bin/sh
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
