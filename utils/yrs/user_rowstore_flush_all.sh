# user_rowstore_flush.sh
# 
# Flush user rowstore tables collecting rowstore stats before and after.
#
# Prerequisites:
# . Must be run as a superuser
# . If not run from the manager node the YBHOST, YBUSER, and YBPASWORD
#   env variables need to be set.
# 
# Revision History:
# 2026.02.05 (rek) Major refactoring to support 
#                  . output dirs
#                  . addl yrs queries
#                  . flush of system tables
#
# 2026.01.22 (rek) updated script names and output dir.
# 2024.09.03 (rek) Initial version.
#
# TODO:
# . add option to only run queries vs queries+flush.


###############################################################################
# READONLY VARIABLES
###############################################################################

readonly script_version='2026.02.05.2110'
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly script_file_name="$(echo $(basename $0))"
readonly script_name="$(echo $(basename $0) | cut -f1 -d'.' )"
readonly ybd_path="/mnt/ybdata/ybd/"
readonly ybsql_cmd="ybsql -X -d yellowbrick "
readonly ybsql_qat="${ybsql_cmd} -XqAt  -P footer=off "
readonly outdir="./output/"$( date +"%Y%m%d_%H%M" )
readonly outfile="${outdir}/${script_name}.out"
readonly hr='~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
readonly hr2='=================================================================='
readonly prop_name_width=28


###############################################################################
# FUNCTIONS
###############################################################################


function die() 
#------------------------------------------------------------------------------
# Print a message to stderr and then exit the script
#
# $1 - The message to print
# $2 - Optional exit code; default 1
#------------------------------------------------------------------------------
{	# This expects 1 or 2 args only where the second is the return code.

   local _ret_cod=${2:-1} 
   
   echo -e "$1"  1>&2
   echo ""
   exit ${_ret_code}
}


function print_section_hdr()
#------------------------------------------------------------------------------
# Print property "section" heading name, upper case, in brackets enclosed between
#   dotted lines.
# Args: 
#   $1 _section - The section name
# Uses:
#   prop_name_width - global readonly variable for width of prop/section name field.
# Outputs:
#   Formatted property name its value to std out.
#   TODO: Warnng message if threshold is exceeded
#------------------------------------------------------------------------------
{
  local _section_name=$(echo $1 | tr 'a-z ' 'A-Z_')

  echo ""
  echo "${hr2}"
  echo "[${_section_name}]"
  #echo "${hr}"
}

function print_property()
#------------------------------------------------------------------------------
# Print formatted property name and its value. (Replaces spaces in name with '_').
# Args: 
#   $1 _prop - The property name
#   $2 _val  - The property value
#   TODO: $3 _limit - (optl) Warning threshold as a numeric value.
# Uses:
#   prop_name_width - global readonly variable for width of prop/section name field.
# Outputs:
#   Formatted property name its value to std out.
#   TODO: Warnng message if threshold is exceeded
#------------------------------------------------------------------------------
{
  local _prop="$1"
  local _val="$2"
  local _limit="$3"
  local _pad_char='.'

  printf "%-${prop_name_width}.${prop_name_width}s" "${_prop//$_pad_char/ }" | tr ' ' "${_pad_char}"
  printf ': '"${_val}"
  printf "\n"
}


function run_yrs_queries()
#------------------------------------------------------------------------------
# Run the yrs state SQL queries. 
#   . yrs_file_type_smry.sql - yrs flushed, unflushed, and commmit file summary.
# Args: 
#   none
# Outputs:
#   Each of the yrs queries to stdout.
# Affects:
#   none
#------------------------------------------------------------------------------
{
  print_property 'yrs_query' 'yrs_file_smry.sql'
  ${ybsql_cmd} -f yrs_file_smry.sql
  print_property 'yrs_query' 'yrs_status_smry.sql'
  ${ybsql_cmd} -f yrs_status_smry.sql
  print_property 'yrs_query' 'yrs_file_type_smry.sql'
  ${ybsql_cmd} -f yrs_file_type_smry.sql
  print_property 'yrs_query' "yrs_tables.sql output to ${outdir}/yrs_tables.sql.out"
  ${ybsql_cmd} -f yrs_tables.sql -o "${outdir}/yrs_tables.sql.out"
}


###############################################################################
# MAIN
###############################################################################

function main()
{
  print_section_hdr "script"
  print_property "script_file_name" "${script_file_name}"
  print_property "start_time" $( date +"%Y.%m.%d_%H%M" )
  print_property "${script_file_name} version" "${script_version}"
  print_property "script_dir" "${script_dir}"
  print_property "outdir" "${outdir}"
  
  print_section_hdr "yrs reports before yflush"
  run_yrs_queries

  print_section_hdr "Flush yrs user tables"
  ./yrs_flush_tables.sh "user" "${outdir}"                                                  
  
  print_section_hdr "Flush yrs sys tables"
  ./yrs_flush_tables.sh "sys"  "${outdir}"                                             
  
  print_section_hdr "yrs reports after yflush"
  run_yrs_queries

  print_section_hdr "DONE"
  print_property "output_file"  "${outfile}"
  echo "${hr2}"
  echo ""
}

###############################################################################
# BODY
###############################################################################

mkdir -p ${outdir} > /dev/null || die "Failed to create out directory ${outdir}. Exiting." 1
main | tee ${outfile}
