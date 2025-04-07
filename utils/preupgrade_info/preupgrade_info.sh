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
# . 2024.03.12 11:40 (rek)  Initial script_version
#
# TODO:
# . Optimize to read ybcli system status only once. It is very time consuming.
# . Optimize to read ybcli health network only once. It is time consuming.
# . Explicity collect backup chain detail
# . Make aged backup chain ui more user friendly
# . Make non-verbose mode that only shows summary of changed props


###############################################################################
# DECLARATIONS
###############################################################################

readonly script_version='2025.03.12'
readonly script_file_name="$(echo $(basename $0))"
readonly script_name="$(echo $(basename $0) | cut -f1 -d'.' )"
readonly pg_hba_path="/mnt/ybdata/ybd/postgresql/build/db/data/pg_hba.conf"
readonly prop_name_width=26
readonly min_horizon_age=30
readonly ybsql_cmd='ybsql -qAt -d yellowbrick'
readonly outfile="${script_name}.out"

###############################################################################
# FUNCTIONS
###############################################################################


function trim_string()
#------------------------------------------------------------------------------
# Trim leasdng and trailing spaces from a string.
# Args: 
#   $1 - The string to trim
# Outputs:
#   The trimmed string
#------------------------------------------------------------------------------
{ # xargs is a simple hack for this; bash string handling is too painful
  echo -e "$1" | xargs
}


function print_property()
#------------------------------------------------------------------------------
# Print formatted property name and its value.
# Args: 
#   $1 - The message to print
#   $2 - Optional exit code; default 0
# Outputs:
#   properyt
#------------------------------------------------------------------------------
{
  local _prop="$1"
  local _val="$2"
  local _pad_char='.'

  printf "%-${prop_name_width}.${prop_name_width}s" "${_prop//$_pad_char/ }" | tr ' ' "${_pad_char}"
  printf ':'"${_val}\n"
}


function get_manager_ips()
{
  local _virtual_ip=""
  local _local_ip=""
  local _remote_ip=""
  
  print_property 'active_mgr_name' "$(hostname)"
  
  # A loop is in a pipeline runs in a sub-process so variables are not updated
  # in the source shell. So write to and then read from file. (easier than <<< seq3)
  ybcli -c  health network            \
  | grep -P '^\s+(Floating|Customer)' \
  | tr ':-' ' '                       \
  | awk  '{print $1, $6}'             > health_network.out
  
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
  done < health_network.out
  
  print_property 'virtual_ip'    "${_virtual_ip}"
  print_property 'active_mgr_ip' "${_local_ip}"
  print_property 'remote_mgr_ip' "${_remote_ip}"
}


function get_curr_release()
{ 
  local _sql='SELECT version()'
  local _ver="$( $ybsql_cmd -c "${_sql}" | awk '{print $NF}')"
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
  local _mode="$( $ybsql_cmd -c "${_sql}" | awk '{print $NF}')"
  print_property 'pg_char_compatibility_mode' "${_mode}"
}

function get_replicated_dbs()
{
  local num_rplctd_dbs="$(  ${ybsql_cmd} -c 'SELECT COUNT(*) FROM sys.replica')"
  local num_rplc_paused="$( ${ybsql_cmd} -c 'SELECT COUNT(*) FROM sys.replica WHERE status=$$PAUSED$$')"
  print_property 'replicated_dbs'  "${num_rplctd_dbs}"
  print_property 'replicas_paused' "${num_rplc_paused}"
}

function get_ldap_status()
{
  local _sql="SELECT COUNT(*) FROM sys.config WHERE key = 'factory.pidList' AND value like '%ldap%'"
  local _ldap_enabled=$( ${ybsql_cmd} -c "${_sql}" )
  print_property 'ldap_enabled' "${_ldap_enabled}"
}

function get_kerberos_status()
{
  local _kerberos_enabled="$(sudo grep -P -i -c '^hostssl.*\s*gss\s*' ${pg_hba_path})"
  print_property 'kerberos_enabled' "${_kerberos_enabled}"
}

function get_protegrity_status()
{
  local _protegrity_addon="$(ybcli -c system status | grep 'Protegrity')"
  local _protegrity_status='0';
  [[ -n ${_protegrity_addon} ]] && _protegrity_status='1'
  print_property 'protegrity_status' "${_protegrity_status}"
}

function get_encryption_status()
{
  local _encryption_status="$(ybcli -c system status | grep -c 'Encryption.*Ready')"
  print_property 'encryption_status' "${_encryption_status}"
}

function get_heartbeat_status()
{ # default heartbeat is 15 secs
  local _heartbeat_secs="$(grep -i 'workerMIATime' /mnt/ybdata/ybd/lime/build/conf/lime.properties | cut -d '=' -f 2 )"
  print_property 'heartbeat_secs' "${_heartbeat_secs} (default=15)"
}


function get_bmc_ips()
{
  local _bmc_local_ip=""
  local _bmc_remote_ip=""
  
  _bmc_local_ip="$( ybcli config network bmc local  get | grep 'IP address' | tr -d ' ' | cut -d ':' -f  2 )"
  print_property 'bmc_local_ip' "${_bmc_local_ip}"
  _bmc_remote_ip="$( ybcli config network bmc remote  get | grep 'IP address' | tr -d ' ' | cut -d ':' -f  2 )"
  print_property 'bmc_remote_ip' "${_bmc_remote_ip}"
}


function get_catalog_size()
{
  local _catalog_size="$( ybcli -c status storage | grep 'Catalog' | cut -d ':' -f 2 )"
  print_property 'catalog_size' "${_catalog_size}"  
}

function get_backup_chain_smry()
{ # We do want headers and dividers for this query
  ybsql -d yellowbrick --pset="footer=off" -f aged_backup_chains_smry.sql \
        -v min_horizon_age=30 \
        -o aged_backup_chains_smry.out
  
  print_property '' ''
  while IFS= read -r line
  do
    [[ -n "${line}" ]] && print_property 'backup_chains_age_gt_30' "${line}"
  done < aged_backup_chains_smry.out
}

function get_pg_custom_settings()
{
  sudo cat /mnt/ybdata/ybd/postgresql/build/db/data/postgresql.auto.conf \
  | grep -v -P '^#' > pg_custom_sys_settings.out
  
  print_property '' ''
  while IFS= read -r line
  do
    print_property 'pg_custom_sys_setting' "${line}"
  done < pg_custom_sys_settings.out

  ybsql -d yellowbrick --pset="footer=off" -c "WITH settings AS
  (  SELECT
       usename
     , unnest (useconfig) AS setting
    FROM
       pg_user
  ) 
  SELECT
     usename
  , setting
  FROM settings
  WHERE setting NOT LIKE 'ybd_ldap_external%'
  ORDER BY usename, setting
  " > pg_custom_user_settings.out
  
  print_property '' ''
  while IFS= read -r line
  do
    print_property 'pg_custom_user_setting' "${line}"
  done < pg_custom_user_settings.out
  
}


###############################################################################
# MAIN
###############################################################################
function main()
{
  print_property "${script_file_name}" "${script_version}"
  get_manager_ips
  get_curr_release
  get_kernel_version
  get_char_mode
  get_replicated_dbs
  get_ldap_status
  get_kerberos_status
  get_protegrity_status
  get_encryption_status
  get_heartbeat_status
  get_bmc_ips
  get_catalog_size
  get_backup_chain_smry
  get_pg_custom_settings
}


###############################################################################
# BODY
###############################################################################

# Must be run from the manager node as ybdadmin user.
sudo ls -1 ${pg_hba_path} > /dev/null
if [[ $? -ne 0 ]]
then
  echo '"pg_hba.conf" not found. Must be run from the manager node as "ybdadmin".'
  exit 1
fi

main | tee ${outfile}

print_property '' ''
print_property 'DONE' "Output logged to '${outfile}'. "
echo ""