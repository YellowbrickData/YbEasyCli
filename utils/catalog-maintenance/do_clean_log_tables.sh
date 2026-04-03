#!/bin/bash
#
# do_clean_log_tables.sh
# 
# DELETE rows from the sys _log_authentication and _log_session older than 
# N days. 
# YB 7.4 additionally introduces *_pre_74 versions of these tables.
#
# Args:
# . $1 opt_days_retention (optl) - Days of authentication and session data to retain.
#                              Default: 90
# . $2 COPY_DATA      (optl) - Save a copy of the rows to be deleted. 0 (No) or 1(yes).
#                              Default: 1
#
# Revision History:
# . 2026.03.17 - . Added help, nocopy, anad report options
#                . Arguments are no longer positional
#                . Only use start_time for _log_session* as endtime may be null.
#                  This is not ideal but makes things much simpler
#                . Refactoring to be more consistent with bash best practices.
# TODO:
# . output to specific directory.
# . add noreport option.

###############################################################################
# GLOBAL VARIABLES
###############################################################################
readonly SCRIPT_FILENAME=$(basename $0)                # Script file name
readonly SCRIPT_NAME=$(basename $0| cut -d '.' -f 1 )  # Script file name without ext
readonly TS=$(date +%Y%m%d-%H%M%S)

readonly LOGTABLES_COMMON="session authentication"
readonly LOGTABLES_74="${LOGTABLES_COMMON} authentication_pre_74 session_pre_74"

opt_days_retention=90


###############################################################################
# FUNCTIONS
###############################################################################

function die() 
#------------------------------------------------------------------------------
# Print a message to stderr and then exit the script
# $1 - The message to printf
# $2 - Optional exit code; default 0
#------------------------------------------------------------------------------
{	# This expects 1 or 2 args only where the second is the return code.

   local _ret_cod=${2:-0} 
   
   echo -e "$1"  1>&2
   echo ""
   exit ${_ret_code}
}


function usage()
#------------------------------------------------------------------------------
# Help/usage text
#------------------------------------------------------------------------------
{
    echo "" 
    echo "Usage:  ${SCRIPT_FILENAME} -?|r|[-Cd]"  
    echo "" 
    echo "Delete ." 
    echo "" 
    echo "   [ -C | --nocopy                 ] Do not save a copy of the data to be deleted." 
    echo "   [ -d | --days_retention numDays ] Days of data to retain. DEFAULT: 90." 
    echo "   [ -r | --report                 ] Only report on data retained rows by month. No delete."     
    echo "   [ ?  | --help | --usage         ] display this help message and exit"
    echo ""    
    echo "Examples:"    
    echo "   $SCRIPT_FILENAME --report"    
    echo "   $SCRIPT_FILENAME --nocopy --days_retention 60 "        
    echo ""        
}


function get_opts()
#------------------------------------------------------------------------------
# Process command line arguments  
# $1 - The args array. i.e. "$@"
#------------------------------------------------------------------------------
{
	while [[ $# > 0 ]]
	do
		opt="$1"
		case $opt in         
			-C|--nocopy)
				opt_nocopy=1
				;;
			-d|--days_retention)
				shift
				opt_days_retention="$1"
				;;                                
			-r|--report)
				opt_report=1       
				;;                                   
			-?|--help|--usage)
				usage
				exit 0;
				;;
			*)
				die "FATAL: Unknown option '$1'. Use ${SCRIPT_FILENAME} --help for usage." 1
				;;
		esac
		shift
	done
}


function print_reports()
#------------------------------------------------------------------------------
# Print row by month for the log tables.
# Args:
#   none
# Output:
#   SQL output all goes to STDOUT
# Uses:
# . global ${logtables} but local ${logtable}
# . global ${opt_days_retention}
#------------------------------------------------------------------------------
{
  local logtable
  local sql
  
  for logtable in ${logtables}; do
    sql="SELECT '${logtable}' AS logtable, date_trunc('month', start_time) AS month_begin, COUNT(*) AS rows 
    FROM sys._log_${logtable} 
    GROUP BY 1, 2 ORDER BY 1, 2"
    
    ybsql -c "${sql}"
  done 
}
  

# ##############################################################################
# BODY
# ##############################################################################

echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
get_opts "$@"

yb_ver_num="$(ybsql -XAqt -c 'SHOW yb_server_version_num')"
[[ $? -ne 0 ]] \
  && die "FATAL: Failed to connect to YB, exiting." -1

# yb_server_version_num is of the form VMMmm. i.e. 7.4.2 -> 70402
# YB 7.4 introduces legacy tables for pre 7..4 session and authentication log data
if [[ ${yb_ver_num} -lt 70400 ]]; then 
  logtables="${LOGTABLES_COMMON}"
else
  logtables="${LOGTABLES_74}"
fi

# Always print the rows by month reports
print_reports

# Only if nocopy and report only are not chosen
if [[ -z ${opt_nocopy} && -z ${opt_report} ]]; then
  echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
  echo "Copying  session/authentication data older than ${opt_days_retention} days before deletion."
  for logtable in ${logtables} ; do
    outfile=log_${logtable}_${TS}.csv.gz
    echo "Log ${logtable}: Copying/compressing rows to be deleted to ${outfile}"
    ybsql -XAqt <<SQL|gzip>${outfile}
SET work_mem TO 2000000;
\COPY (SELECT * FROM sys._log_${logtable} WHERE start_time < (CURRENT_DATE - ${opt_days_retention})) TO STDOUT WITH (FORMAT TEXT);
SQL
  done
fi

# Only if report only not chosen.
if [[ -z ${opt_report} ]]; then
  echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
  echo "Deleting  session/authentication data older than ${opt_days_retention} days."
  for logtable in ${logtables} ; do
    echo "Log ${logtable}: Deleting rows older than ${opt_days_retention} days."
    ybsql -XAqt <<SQL 
SET work_mem TO 2000000;
DELETE FROM sys._log_${logtable} WHERE start_time < (CURRENT_DATE - ${opt_days_retention});
SQL
  done
fi

# If the reports only option was choosen, don't print it again as it is printed
# at the begining of the script.
[[ -z ${opt_report} ]] \
   && echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'\
   && print_reports

echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
echo 'DONE'
echo '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'