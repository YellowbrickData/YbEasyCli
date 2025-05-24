# pre_uprade_info.sh
#
# Collect appliance information needed for before YB upgrade.
#
# Inputs:
#   none
#
# Outputs:
# . Console output is tee'd to ouput file
#
# Prerequisites:
#   Run from the manager node as ybdadmin user.
#
# History:
# . 2025.04.07 19:30 (rek) - Addition of print_property_append and dump status.
#                            Added backup chain detail extract.
# . 2025.03.28 10:40 (rek) - Additional refactoring.
#                            Auto generation of zip file.
# . 2025.03.19 21:40 (rek) - Refactor to write ybcli output to file.
# . 2025.03.12 11:40 (rek) - Initial script_version
#
# TODO:
# . ybcli and ybsql file dump indicator
# . Explicity collect backup chain detail
# . Make aged backup chain ui more user friendly
# . Make non-verbose mode that only shows summary of changed props
# . Add functions for:
#   . get_manger_uptimes
#   . get_max_worker_space_used
#   . get_temp_space
#   . get_orphan_snapshot_deps
#   . get_data_skew
#   . help|usage


###############################################################################
# READONLY VARIABLES
###############################################################################

readonly script_version='2025.04.07'
readonly script_file_name="$(echo $(basename $0))"
readonly script_name="$(echo $(basename $0) | cut -f1 -d'.' )"
readonly pg_hba_path="/mnt/ybdata/ybd/postgresql/build/db/data/pg_hba.conf"
readonly prop_name_width=26
readonly min_horizon_age=30
readonly ybsql_cmd="ybsql -d yellowbrick -P footer=off "
readonly ybsql_qat="${ybsql_cmd} -qAt "
readonly outdir="${script_name}_"$( date +"%Y%m%d_%H%M" )
readonly outfile="${script_name}.out"
readonly log_level=0


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


function trim_string()
#------------------------------------------------------------------------------
# Trim leasdng and trailing spaces from a string.
# Args: 
#   $1 - The string to trim
# Returns:
#   The trimmed string
#------------------------------------------------------------------------------
{ # xargs is a simple hack for this; bash string handling is too painful
  echo -e "$1" | xargs
}


function print_section()
#------------------------------------------------------------------------------
# Print property "section" heading name.
# Args: 
#   $1 _section - The property name
# Uses:
#   prop_name_width - global readonly variable for width of prop/section name field.
# Outputs:
#   Formatted property name its value to std out.
#   TODO: Warnng message if threshold is exceeded
#------------------------------------------------------------------------------
{
  local _section=$(echo $1 | tr 'a-z ' 'A-Z_')
  local _val='................................................................'
  local _limit="$3"
  local _pad_char='_'
  local _sec_name_width=$((prop_name_width -2))

  printf "\n\n"
  printf "[${_section}]"
}


function print_property()
#------------------------------------------------------------------------------
# Print formatted property name and its value.
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

  printf "\n"
  printf "%-${prop_name_width}.${prop_name_width}s" "${_prop//$_pad_char/ }" | tr ' ' "${_pad_char}"
  printf ': '"${_val}"
}

function print_property_append()
#------------------------------------------------------------------------------
# Append text to the current formatted property name line.
# Args: 
#   $1 _prop - The string to append to the property line.
# Uses:
#   prop_name_width - global readonly variable for width of prop/section name field.
# Outputs:
#   Formatted property name its value to std out.
#   TODO: Warnng message if threshold is exceeded
#------------------------------------------------------------------------------
{
  local _val="$1"
  local _limit="$2"
  local _pad_char='.'

  printf "${_val}"'; '
}

function info_message()
#------------------------------------------------------------------------------
# Print formatted info message to stderr if debug logging is enabled
# Args: 
#   $1 _prop - The property name
#   $2 _val  - The property value
# Outputs:
#   Formatted property name its value to std out.
#   TODO: Warnng message if threshold is exceeded
#------------------------------------------------------------------------------
{
  local _message="$1"
  [[ log_level -gt 0 ]] && echo "[INFO ] ${_message}" >&2
}


function dump_ybcli_cmd()
#------------------------------------------------------------------------------
# Runs ybcli command and dumps output to file.
#   Always includes '-c' option to turn off color coding in output.
#   Console message written only to std out.
# Args: 
#   $1 _cmd - The command to run under ybcli
# Outputs:
#   File named with the ybcli command but with spaces the "_" character and 
#   suffixed with ".out".
#   Message printed to stderr.
#------------------------------------------------------------------------------
{
  local _cmd="$1"
  local _outfile='ybcli_'"$( echo ${_cmd} | tr ' ' '_' )".out

  info_message "Writing 'ybcli -c ${_cmd}' to '${_outfile}'"
  ybcli -c ${_cmd} > ${_outfile}
  print_property_append "${_outfile}"
}


function dump_ybcli_all()
#------------------------------------------------------------------------------
# Write output of multiple ybcli commands to file for later use.
# Terminal output is written only to STDERR, not STDOUT
# Args: 
#   none
# Outputs:
#   Generates the files
#   . ybcli_config_bmc_local.out
#   . ybcli_config_bmc_remote.out
#   . ybcli_health_network.out
#   . ybcli_health_storage.out
#   . ybcli_status_storage.out
#   . ybcli_status_system.out
#------------------------------------------------------------------------------
{ 
  info_message "Dumping ybcli output to file. This will take multiple minutes"
  print_property 'ybcli_dumps' '(running)'
  dump_ybcli_cmd 'config network bmc local get'
  dump_ybcli_cmd 'config network bmc remote get'
  dump_ybcli_cmd 'health network'
  dump_ybcli_cmd 'health storage'
  dump_ybcli_cmd 'status storage'
  dump_ybcli_cmd 'status system'
}


function dump_ybsql()
#------------------------------------------------------------------------------
# Runs a SQL file using ybcli and dumps the output to file.
# Args: 
#   $1 _fname - The SQL file name
#   $2 _opts  - Additional ybsql options. i.e. -x, -v var=val, etc... 
#               Do not use double-quotes within args.
# Outputs:
#   File with SQL file name suffixed with ".out".
#   Info message printed to stderr.
#------------------------------------------------------------------------------
{
  local _fname="$1"
  local _opts="$2"
  local _outfile="${_fname}.out"

  info_message "Writing '${ybsql_cmd} ${_opts} -f ${_fname}' to '${_outfile}'"
  ${ybsql_cmd} ${_opts} -f ${_fname}  > ${_outfile}
  print_property_append "${_fname}"

}

function dump_ybsql_all()
#------------------------------------------------------------------------------
# Write output of multiple ybsql db metrcis to file for later use.
# Terminal output is written only to STDERR, not STDOUT
# Args: 
#   none
# Outputs:
#   Generates the files
#   . sys_database_smry.sql.out

#------------------------------------------------------------------------------
{ 
  info_message "Dumping db metric qery output to file."
  print_property 'ybsql_dumps' '(running)'
  dump_ybsql 'sys_database_smry.sql' '-x -t'
  
  # We do want headers and dividers for this query
  dump_ybsql aged_backup_chains_smry.sql '-v min_horizon_age=30'
  
  # We do want headers and dividers for this query
  dump_ybsql aged_backup_chains.sql '-v min_horizon_age=30'
}


function get_manager_ips()
#------------------------------------------------------------------------------
# Print virtual, active, and standby manager node ip addresses
# Args: 
#   none
# Inputs:
#   ybcli_health_network.out file must already have been created.
#------------------------------------------------------------------------------
{
  local _virtual_ip=""
  local _local_ip=""
  local _remote_ip=""
  
  print_property 'active_mgr_name' "$(hostname)"
  
  grep -P '^\s+(Floating|Customer)' ybcli_health_network.out \
  | tr ':-' ' ' \
  | awk  '{print $1, $6}' > manager_ips.out
  
  # A loop is in a pipeline runs in a sub-process so variables are not updated
  # in the source shell. So write to and then read from file. (easier than <<< seq3) 
  while IFS= read -r line
  do
    if [[ ${line} =~ ^Floating ]]; then 
      _virtual_ip="$( echo ${line} | cut -d ' ' -f 2 )";
    elif [[ ${line} =~ ^Customer ]]; then
      if [[ "${_local_ip}" == "" ]]; then 
        _local_ip="$( echo ${line} | cut -d ' ' -f 2 )";
      else
        _remote_ip="$( echo ${line} | cut -d ' ' -f 2 )";
      fi
    else 
      echo "ERROR: line is not valid: '${line}'."
    fi
  done < manager_ips.out
  
  print_property 'virtual_ip'    "${_virtual_ip}"
  print_property 'active_mgr_ip' "${_local_ip}"
  print_property 'remote_mgr_ip' "${_remote_ip}"
}


function get_yb_version()
{ 
  local _sql='SELECT version()'
  local _ver="$( ${ybsql_qat} -c "${_sql}" | awk '{print $NF}')"
  print_property 'yb_version' "${_ver}"
}


function get_kernel_version()
{
  local _kernel_version="$( uname -a | awk '{print $3}' )"
  print_property 'kernel_version' "${_kernel_version}"
}


function get_char_mode()
{ # Not in pg_settings unless has been overridden
  local _sql='SHOW pg_char_compatibility_mode'
  local _mode="$( ${ybsql_qat} -c "${_sql}" | awk '{print $NF}')"
  print_property 'pg_char_compatibility_mode' "${_mode}"
}


function get_replicated_dbs()
{
  local num_rplctd_dbs="$(  ${ybsql_qat} -c 'SELECT COUNT(*) FROM sys.replica')"
  local num_rplc_paused="$( ${ybsql_qat} -c 'SELECT COUNT(*) FROM sys.replica WHERE status=$$PAUSED$$')"
  print_property 'replicated_dbs'  "${num_rplctd_dbs}"
  print_property 'replicas_paused' "${num_rplc_paused}"
}


function get_ldap_status()
{
  local _sql="SELECT COUNT(*) FROM sys.config WHERE key = 'factory.pidList' AND value like '%ldap%'"
  local _ldap_enabled=$( ${ybsql_qat} -c "${_sql}" )
  print_property 'ldap_enabled' "${_ldap_enabled}"
}


function get_kerberos_status()
{
  local _kerberos_enabled="$(sudo grep -P -i -c '^hostssl.*\s*gss\s*' ${pg_hba_path})"
  print_property 'kerberos_enabled' "${_kerberos_enabled}"
}


function get_protegrity_status()
{ # If protegrity is enabled it shows up in in the addons line:
  #Add-ons : Protegrity: Installed: YES - Version: 9.1.0.0.43 - Enabled: YES - Manager Running: OK - Blade Running: OK
  local _protegrity_addon="$( grep 'Protegrity' ybcli_status_system.out )"
  local _protegrity_status='0';
  [[ -n ${_protegrity_addon} ]] && _protegrity_status='1'
  print_property 'protegrity_status' "${_protegrity_status}"
}


function get_encryption_status()
{
  local _encryption_status="$( grep -c 'Encryption.*Ready' ybcli_status_system.out )"
  print_property 'encryption_status' "${_encryption_status}"
}


function get_heartbeat_status()
{ # default heartbeat is 15 secs
  local _heartbeat_secs="$(grep -i 'workerMIATime' /mnt/ybdata/ybd/lime/build/conf/lime.properties \
                         | cut -d '=' -f 2 )"
  print_property 'heartbeat_secs' "${_heartbeat_secs} (default=15)"
}


function get_manager_drive_wear()
#------------------------------------------------------------------------------
# Print life used for local and remote manager node SSDs and NVMEs. 
# Args: 
#   none
# Inputs:
#   ybcli_health_storage.out file must already have been created.
# Outputs:
#   The 4 metrics: mgr_local_ssd_life_used , mgr_local_nvme_life_used
#                 ,mgr_remote_ssd_life_used, mgr_remote_nvme_life_used
#------------------------------------------------------------------------------
{ # TODO: extract out health metric. .i.e OK, Warn, Critical
  local _status='OK'
  local _mgr_local_ssd_life_used=''
  local _mgr_local_nvme_life_used=''
  local _mgr_remote_ssd_life_used=''
  local _mgr_remote_nvme_life_used=''
  
  _mgr_local_ssd_life_used=$(grep -Pzo '^(?s).*?(?=^Remote)' ybcli_health_storage.out \
  | grep -P 'sd[a-d].*life used' \
  | awk '{print $3, $7 }' \
  | sort \
  | paste -sd ','
  )
  print_property 'mgr_local_ssd_life_used'    "${_mgr_local_ssd_life_used}"
  
  _mgr_local_nvme_life_used=$(grep -Pzo '^(?s).*?(?=^Remote)' ybcli_health_storage.out \
  | grep -P 'nvme.*life used' \
  | awk '{print $3, $7 }' \
  | sort \
  | paste -sd ','
  )
  print_property 'mgr_local_nvme_life_used'    "${_mgr_local_nvme_life_used}"
  
  _mgr_remote_ssd_life_used=$(grep -Pzo '^Remote.*\n(?s:.)*' ybcli_health_storage.out \
  | grep -P 'sd[a-d].*life used' \
  | awk '{print $3, $7 }' \
  | sort \
  | paste -sd ','
  )
  print_property 'mgr_remote_ssd_life_used'    "${_mgr_remote_ssd_life_used}"
  
  
  _mgr_remote_nvme_life_used=$(grep -Pzo '^Remote.*\n(?s:.)*' ybcli_health_storage.out \
  | grep -P 'sd[a-d].*life used' \
  | awk '{print $3, $7 }' \
  | sort \
  | paste -sd ','
  )
  print_property 'mgr_remote_nvme_life_used'   "${_mgr_remote_nvme_life_used}"
}


function get_database_smry()
#------------------------------------------------------------------------------
# Print out database summary statistics genrated from sys.database.
# Args: 
#   none
# Inputs:
#   ybsql_sys_database_smry.out file must already have been created.
# Outputs:
#   There are 15 metrics including data size and storage, db, and table counts
#     , etc..
#------------------------------------------------------------------------------
{ local _prop=''
  local _val=''
  
  while IFS='|' read -r _prop _val
  do
    _prop=$(trim_string ${_prop})
    _val=$(trim_string ${_val})
    print_property "${_prop}"    "${_val}"
  done < sys_database_smry.sql.out
}

  
function get_bmc_ips()
{
  local _bmc_local_ip=""
  local _bmc_remote_ip=""
  
  _bmc_local_ip="$(  grep 'IP address' ybcli_config_network_bmc_local_get.out  | tr -d ' ' | cut -d ':' -f  2 )"
  print_property 'bmc_local_ip' "${_bmc_local_ip}"
  _bmc_remote_ip="$( grep 'IP address' ybcli_config_network_bmc_remote_get.out | tr -d ' ' | cut -d ':' -f  2 )"
  print_property 'bmc_remote_ip' "${_bmc_remote_ip}"
}


function get_catalog_size()
{
  local _catalog_size="$( grep 'Catalog' ybcli_status_storage.out | cut -d ':' -f 2 )"
  print_property 'catalog_size' "${_catalog_size}"  
}

function get_backup_chain_smry()
{ 
  while IFS= read -r line
  do
    [[ -n "${line}" ]] && print_property 'backup_chains_age_gt_30' "${line}"
  done < aged_backup_chains_smry.sql.out
}

function get_pg_custom_settings()
#------------------------------------------------------------------------------
# Print out system settings from postgresql.auto.conf file and pg_settings view.
# Args: 
#   none
# Inputs:
#   none
# Outputs:
#   pg_custom_sys_setting AND pg_custom_user_setting
#------------------------------------------------------------------------------
{
  sudo cat /mnt/ybdata/ybd/postgresql/build/db/data/postgresql.auto.conf \
  | grep -v -P '^#' > pg_custom_sys_settings.out
  
  while IFS= read -r line
  do
    print_property 'pg_custom_sys_setting' "${line}"
  done < pg_custom_sys_settings.out

  dump_ybsql "pg_custom_user_settings.sql"
  while IFS= read -r line
  do
    print_property 'pg_custom_user_setting' "${line}"
  done < pg_custom_user_settings.sql.out
  
}


###############################################################################
# MAIN
###############################################################################
function main()
{

  # Must be run from the manager node as ybdadmin user.
  sudo ls -1 ${pg_hba_path} > /dev/null
  [[ $? -ne 0 ]] && die '"pg_hba.conf" not found. Must be run from the manager node as "ybdadmin".' 1


  print_property "${script_file_name}" "${script_version}"
  

  # Dump ybcli and SQL output used by later functions
  print_section 'ybcli_info' 
  dump_ybcli_all
  print_section 'ybsql_queries' 
  dump_ybsql_all

  # Manager node configuration
  print_section 'manager node config' 
  get_manager_ips
  get_bmc_ips
  get_yb_version
  get_kernel_version
  
  print_section 'database configuration' 
  get_char_mode
  get_ldap_status
  get_kerberos_status
  get_protegrity_status
  get_encryption_status
  get_heartbeat_status
  
  # Manager node status and health
  print_section 'manager node status' 
  # get_manger_uptimes
  get_manager_drive_wear
  
  # Database status
  print_section 'database metrics'
  get_catalog_size
  get_database_smry
  get_replicated_dbs
  get_backup_chain_smry
  #get_worker_space_used
  #get_orphan_backup_deps
  #get_data_skew
  
  print_section 'database custom settings'
  get_pg_custom_settings
  
  # Done
  print_section 'DONE' 
  mkdir -p ${outdir} > /dev/null
  [[ $? -ne 0 ]] && die "Failed to create out directory ${outdir}. Exiting." 1
  mv *.out ${outdir}/
  tar -czf ${outdir}.tgz ${outdir}
  print_property 'generated_zip_file' "${outdir}.tgz"

}


###############################################################################
# BODY
###############################################################################

main | tee ${outfile}
echo ""