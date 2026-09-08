#!/bin/bash
#
# do_wlm_profile_set_maintenance.sh
#
# Activate the "maintenance" WLM profile, saving a SQL script that
# reactivates whichever WLM profile was active beforehand.
#
# With --terminate, before activating the maintenance profile this script
# also generates and runs SQL to clear out sessions that would otherwise
# block the profile switch:
#   . pg_terminate_backend() for every backend in the 'client_wait' state
#     whose username is NOT LIKE 'sys_ybd%'.
#   . pg_cancel_backend() for every backend running an active query whose
#     username IS LIKE 'sys_ybd%'.
#
# Usage: do_wlm_profile_set_maintenance.sh [-t|--terminate] [-h|-?|--help|--usage]
#
#     -t | --terminate             Terminate/cancel the blocking sessions
#                                  described above before activating the
#                                  maintenance WLM profile.
#     -h | -? | --help | --usage   Display this help message and exit.
#
# Examples:
#     do_wlm_profile_set_maintenance.sh
#     do_wlm_profile_set_maintenance.sh --terminate
#
# Prerequisites:
# . If not running from the manager node, the YBHOST, YBUSER, and
#   YBPASSWORD environment variables must be set.
#
# Revision History:
# . 2026.09.08 (rek) - Added description, usage()/parse_options(),
#                      -h|-?|--help|--usage, and -t|--terminate (terminate/
#                      cancel blocking sessions before the profile switch).
#

# ---------------------------------------------------------------------------
# INITIALIZATIONS
# ---------------------------------------------------------------------------
SCRIPT_NAME=$(basename "$0")

OPT_TERMINATE=false


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
usage()
# ---------------------------------------------------------------------------
# Print usage/help text by extracting and reprinting this script's own
# header comment block (the "#" lines between the shebang and the first
# blank line), so the header comments are the single source of the usage
# text.
#
# Args:        none
# Outputs:     This file's header comments to stdout.
# Return Code: 0
# Affects:     none
# ---------------------------------------------------------------------------
{
	sed -n '2,/^$/{/^$/!{s/^#[ ]\{0,1\}//;p}}' "$0"
}

parse_options()
# ---------------------------------------------------------------------------
# Parse command-line options, setting the global OPT_TERMINATE variable.
#
# Args:        $* - The script's command-line arguments
# Outputs:     Usage/error text on invalid input.
# Return Code: Does not return on invalid input (exits); 0 on success.
# Affects:     Sets global OPT_TERMINATE
# ---------------------------------------------------------------------------
{
	while [ $# -gt 0 ] ; do
		case "$1" in
			-t|--terminate)
				OPT_TERMINATE=true
				;;
			-h|"-?"|--help|--usage)
				usage
				exit 0
				;;
			*)
				echo "Unknown option: $1" >&2
				echo >&2
				usage >&2
				exit 1
				;;
		esac
		shift
	done
}

terminate_blocking_sessions()
# ---------------------------------------------------------------------------
# Generate and run SQL to clear out sessions that would otherwise block the
# WLM profile switch:
#   . pg_terminate_backend() for every backend in the 'client_wait' state
#     whose username is NOT LIKE 'sys_ybd%'.
#   . pg_cancel_backend() for every backend running an active query whose
#     username IS LIKE 'sys_ybd%'.
#
# Args:        none
# Outputs:     Generated SQL file name; ybsql output/errors.
# Return Code: none (exits 1 on error)
# Affects:     Writes wlm_profile_terminate_<timestamp>.out.sql
# ---------------------------------------------------------------------------
{
	local terminate_sql=wlm_profile_terminate_$(date +%Y%m%d_%H%M%S).out.sql

	echo "-- Terminating/cancelling sessions blocking the WLM profile switch"
	ybsql -XAqt <<-SQL
	\set ON_ERROR_STOP 1
	\o ${terminate_sql}
	/* Terminate all non-sys_ybd_% sessions that are not idle, as they are likely to block the profile switch */	
	SELECT format('SELECT pg_terminate_backend(%s);', pid) AS sql
	FROM pg_stat_activity
	WHERE state != 'idle' AND usename NOT LIKE 'sys_ybd%' AND pid != pg_backend_pid()
	UNION ALL

	/* Cancel (not terminate,)active statements from sys_ybd_% users as they are likely to block the profile switch */
	SELECT format('SELECT pg_cancel_backend(%s);', pid) AS sql
	FROM pg_stat_activity
	WHERE state = 'active'
	  AND usename LIKE 'sys_ybd%';

	\o
	\set ECHO queries
	\i ${terminate_sql}
	SQL
	[ $? -ne 0 ] && { echo "There were errors when trying to terminate/cancel blocking sessions, exiting" ; exit 1 ; }
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
export YBDATABASE=yellowbrick

parse_options "$@"

MAJOR_VER=$(ybsql -XAqt -c "WITH a AS (SELECT current_setting('yb_server_version') AS ver) SELECT split_part(ver, '.', 1) FROM a AS major")
[ $? -ne 0 ] && { echo "ERROR: couldn't get version information, exiting" ; exit 1 ; }

ACTIVATE_SEC=30
MAINTENANCE_SQL=wlm_profile_maintenance.out.sql
# NOTE: just to be on the safe side and not overwrite it
REACTIVATE_SQL=wlm_profile_reactivate_$(date +%Y%m%d_%H%M%S).out.sql

${OPT_TERMINATE} && terminate_blocking_sessions

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
