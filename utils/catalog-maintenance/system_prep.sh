#!/bin/bash
#
# system_prep.sh
#
# Prepare a Yellowbrick system for catalog maintenance, and restore it
# afterwards.
#
# Pre-maintenance (--pre):
#   . Turns autovacuum off (ALTER SYSTEM SET autovacuum TO off).
#   . Activates the "maintenance" WLM profile via do_wlm_profile_set_maintenance.sh.
#
# Post-maintenance (--post):
#   . Reactivates the WLM profile that was active before maintenance began,
#     using the most recent wlm_profile_reactivate_*.out.sql script.
#   . Resets autovacuum back to its default (ALTER SYSTEM RESET autovacuum).
#
# In both cases, the script then reloads the Yellowbrick configuration,
# reports the current autovacuum setting, and reports the WLM profile now
# active.
#
# Revision History:
# . 2026.09.08 - Added description and revision history to header comments.
#                Split pre/post logic into do_pre()/do_post() functions.
#                Added parse_options(), usage(), -h|-?|--help|--usage.
#                Added -t|--terminate, passed through to
#                do_wlm_profile_set_maintenance.sh on --pre.
#
# TODO:
# . none
#

# ---------------------------------------------------------------------------
# INITIALIZATIONS
# ---------------------------------------------------------------------------
SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR=$(dirname "$0")

MODE=
OPT_TERMINATE=false


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------
usage()
# ---------------------------------------------------------------------------
# Print usage/help text.
#
# Args:        none
# Outputs:     Usage text to stdout.
# Return Code: 0
# Affects:     none
# ---------------------------------------------------------------------------
{
	cat <<-USAGE
	Usage: ${SCRIPT_NAME} [--pre]|[--post] [-t|--terminate] [-h|-?|--help|--usage]

	Prepare a Yellowbrick system for catalog maintenance, and restore it
	afterwards.

	    --pre                        Run pre-maintenance steps.
	    --post                       Run post-maintenance steps.
	    -t | --terminate             Pre-maintenance only: also terminate
	                                 sessions still running under the outgoing
	                                 WLM profile (passed through to
	                                 do_wlm_profile_set_maintenance.sh).
	    -h | -? | --help | --usage   This usage text.

	If no options are provided, this usage text is displayed and the script exits.

	Examples:
	    ${SCRIPT_NAME} --pre
	    ${SCRIPT_NAME} --pre --terminate
	    ${SCRIPT_NAME} --post
	USAGE
}

parse_options()
# ---------------------------------------------------------------------------
# Parse command-line options, setting the global MODE and OPT_TERMINATE
# variables.
#
# "pre" and "post" are accepted as undocumented synonyms for --pre and
# --post; only --pre and --post are shown in usage().
#
# Displays usage and exits (rc 1) if:
# . no options are given or an unknown option is passed
# . or neither --pre/pre nor --post/post is given
#
# Args:        $* - The script's command-line arguments
# Outputs:     Usage/error text on invalid input.
# Return Code: Does not return on invalid input (exits); 0 on success.
# Affects:     Sets global MODE ("pre" or "post") and OPT_TERMINATE
# ---------------------------------------------------------------------------
{
	if [ $# -eq 0 ] ; then
		usage
		exit 1
	fi

	while [ $# -gt 0 ] ; do
		case "$1" in
			--pre|pre)
				MODE=pre
				;;
			--post|post)
				MODE=post
				;;
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

	if [ -z "${MODE}" ] ; then
		echo "Error: specify --pre or --post" >&2
		echo >&2
		usage >&2
		exit 1
	fi
}

do_pre()
# ---------------------------------------------------------------------------
# Run the pre-maintenance steps: turn autovacuum off and activate the
# "maintenance" WLM profile. If -t|--terminate was given, sessions still
# running under the outgoing WLM profile are also terminated.
#
# Args:        none
# Outputs:     Progress messages to stdout.
# Return Code: none
# Affects:     Sets global AUTOVACUUM to the SQL used to turn autovacuum off.
# ---------------------------------------------------------------------------
{
	echo "-- Doing pre-maintenance steps"
	AUTOVACUUM='ALTER SYSTEM SET autovacuum TO off'

	if ${OPT_TERMINATE} ; then
		"${SCRIPT_DIR}/do_wlm_profile_set_maintenance.sh" --terminate
	else
		"${SCRIPT_DIR}/do_wlm_profile_set_maintenance.sh"
	fi
}

do_post()
# ---------------------------------------------------------------------------
# Run the post-maintenance steps: reactivate the WLM profile that was active
# before maintenance began, and reset autovacuum back to its default.
#
# Args:        none
# Outputs:     Progress/warning messages to stdout.
# Return Code: none
# Affects:     Sets global AUTOVACUUM to the SQL used to reset autovacuum.
# ---------------------------------------------------------------------------
{
	echo "-- Doing post-maintenance steps"
	LATEST_WLM=$(ls -1 -t wlm_profile_reactivate_*.out.sql 2>/dev/null | head -n1)
	if [ -z "${LATEST_WLM}" ] ; then
		echo "WARNING: couldn't find WLM reactivation SQL script!"
	else
		ybsql -XAqte -f "${LATEST_WLM}"
	fi
	AUTOVACUUM='ALTER SYSTEM RESET autovacuum'
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
export YBDATABASE=yellowbrick

parse_options "$@"

if [ "${MODE}" == "pre" ] ; then
	do_pre
else
	do_post
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
