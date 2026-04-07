# connection_fns.sh
#
# Functions:
# . do_connect()
# . is_appliance()
# . is_manager()
# . is_mgr_connection()
# . is_cn()
# . get_instance_type()
# . get_default_cluster()
# . get_system_cluster()
# . get_outdir()
# . get_yb_ver_num()
#
# Revision History:
# 2026.03.09 - Add additional functions for yrs CN functionality.
# 2026.03.09 - Initial version
#
# ???
#. Do we want to use -X if not manager node becuase connection props might be 
#  set in ".ybsqlrc"

###############################################################################
# CONNECTION FUNCTIONS
###############################################################################


function do_connect()
#------------------------------------------------------------------------------
# Attempt a ybsql connection using the environment plus and additional passed args.
#
# Args:
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   IF connection fails, YB* env variables.
# Return Code:
#   0 if success, 1 if error
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local pwd_display="UNSET"
  local rc=0

  # Attempt a frontend, not backend connection. In CN a backend connection may spawn a cluster resume
  ybsql $@ -qAt -c " SELECT current_user" -o delete.me
  rc=$?

  if [[ ${rc} -ne 0 ]]; then
    if [[ "${is_manager}" == "no" ]]; then
      # If YBPASSWORD is set obfuscate it before displaying
      if [[ -n ${YBPASSWORD} ]]; then
        local pwd_len=${#YBPASSWORD}
        if (( pwd_len > 2 )); then
          local middle=$(printf '%*s' $((pwd_len-2)) '' | tr ' ' '*')
          pwd_display="${YBPASSWORD:0:1}${middle}${YBPASSWORD: -1}"
        else
          pwd_display="${YBPASSWORD}"
        fi
      fi

      echo "Using"
      echo "YBHOST    =${YBHOST:-UNSET}    "
      echo "YBUSER    =${YBUSER:-UNSET}    "
      echo "YBPASSWORD=${pwd_display}"
      echo "YBSSLMODE =${YBSSLMODE:-UNSET} "
    fi
  fi

  return ${rc}
}


function is_appliance()
#------------------------------------------------------------------------------
# Is this running on an appliance manager node.
#
# Args: 
#   none
# Returns:
#   0 if success, 1 if error
# Outputs:
#   "yes" or "no"
# Affects:
#   none
#------------------------------------------------------------------------------
{
  echo "not implemented"
}


function is_manager()
#------------------------------------------------------------------------------
# Is this running on a manager node.
#
# Args: 
#   none
# Outputs:
#   "t" for TRUE, "f" for 
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local ybcli_path="$(which ybcli)"

  # If ybcli is found in the path this is assumed to be an appliance manager node.
  if [ -n "${ybcli_path}" ]
  then
    echo "t" 
  else
    echo "f"
  fi
}


function is_mgr_connection()
#------------------------------------------------------------------------------
# Attempt a ybsql connection using the environment plus and additional passed 
# args to check if the connection is from the manager node as a trusted user.
#
# Args:
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   "yes" or "no"
# Return Code:
#   0 if success, 1 if error
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local pwd_display="UNSET"
  local rc=0
  local is_manager_conn="unknown"

  # Returns 't' for TRUE and 'f' for FALSE
  is_manager_conn=$(ybsql $@ -qAt -c "SELECT ((NVL(client_hostname,'')||NVL(client_ip_address,''))='') AS is_manager_conn FROM sys.session WHERE process_id = pg_backend_pid()" )
  rc=$?

  echo ${is_manager_conn}

  return ${rc}
}


function is_cn()
#------------------------------------------------------------------------------
# Is the cluster we are connecting to an CloudNative instance.
#
# Args:
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   "t" or "f"
# Return Code:
#   0 if success, 1 if error
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local rc=0
  local is_cn_conn="unknown"

  # Returns 't' for TRUE and 'f' for FALSE
  is_cn_conn=$(ybsql $@ -d yellowbrick -qAt -c "SELECT (hardware_instance_type_id != '10000000-0000-0000-0000-000000000001') AS is_cn FROM sys.cluster  WHERE is_system_cluster =TRUE" )
  rc=$?

  echo ${is_cn_conn}

  return ${rc}
}


function get_default_cluster()
#------------------------------------------------------------------------------
# Get the default cluster name from sys.cluster
# On appliances there is only one cluster; "yellowbrick"
#
# Args:
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   The cluster name
# Return Code:
#   0 if success, 1 if error
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local rc=0
  local default_cluster="unknown"

  default_cluster=$(ybsql $@ -d yellowbrick -qAt -c "SELECT cluster_name FROM sys.cluster WHERE is_default_cluster=TRUE" )
  rc=$?
  echo ${default_cluster}

  return ${rc}
}


function get_system_cluster()
#------------------------------------------------------------------------------
# Get the system cluster name from sys.cluster
# On appliances there is only one cluster; "yellowbrick"
#
# Args:
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   The cluster name
# Return Code:
# Return Code:
#   0 if success, 1 if error
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local rc=0
  local system_cluster="unknown"

  # Returns 't' for TRUE and 'f' for FALSE
  system_cluster=$(ybsql $@ -d yellowbrick -qAt -c "SELECT cluster_name FROM sys.cluster WHERE is_system_cluster=TRUE" )
  rc=$?
  echo ${system_cluster}

  return ${rc}
}

function get_outdir()
#------------------------------------------------------------------------------
# Create the output directory for the ybcli util
#
# Args: 
#   $1 util_name (reqd) - The property name
# Outputs:
#   relative directory path based on util name and current timestamp.
# Affects:
#   Creates output directory
# Return Code:
#   0 if success, 1 if error
#------------------------------------------------------------------------------
{
  echo "not implemented"
}


function get_yb_ver_num()
#------------------------------------------------------------------------------
# Wrapper around teh "yb_server_version_num" property which is an integer 
# representation of the yb_server_version_num. i.e. 7.4.3 is 70403
#
# Args: 
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   The "yb_server_version_num" property value.
# Affects:
#   none
# Return Code:
#   0 if success, 1 if error
#------------------------------------------------------------------------------
{
  local yb_ver_num="0"
  local rc=0

  yb_ver_num="$(ybsql $@ -d yellowbrick -qAt -c 'SHOW yb_server_version_num')"
  rc=$?
  echo ${yb_ver_num}

  return ${rc}
}
