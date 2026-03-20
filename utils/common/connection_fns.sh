# connection_fns.sh
#
# Revision History:
# 2024-10-22 - Initial version
#
# ???
#. Do we want to use -X if not manager node becuase connection props might be 
#  set in '.ybsqlrc

###############################################################################
# CONNECTION FUNCTIONS
###############################################################################


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
  local ybcli_path="$(which ybcli)"

  # If ybcli is found in the path this is assumed to be an appliance manager node.
  if [ -n "${ybcli_path}" ]
  then
    echo "yes" 
  else
    echo "no"
  fi
}


function is_manager()
#------------------------------------------------------------------------------
# Is this running on an appliance manager node.
#
# Args: 
#   none
# Outputs:
#   "yes" or "no"
# Affects:
#   none
#------------------------------------------------------------------------------
{
  local ybcli_path="$(which ybcli)"

  # If ybcli is found in the path this is assumed to be an appliance manager node.
  if [ -n "${ybcli_path}" ]
  then
    echo "yes" 
  else
    echo "no"
  fi
  
}


function can_connect()
#------------------------------------------------------------------------------
# Attempt a ybsql connection using the environment plus and additional passed args.
#   
# Args: 
#   $* (optl) - Optional args to pass to ybsql
# Outputs:
#   "yes" or "no"
# Return Code:
#   0 if success, not 0 if error.
# Affects:
#   none
#------------------------------------------------------------------------------
{
  ybsql $@ -c "SELECT * FROM sys.const"
  local rc=$?
  
  if [[ ${rc} -eq 0 ]] ; then
    echo "yes"
  else
    echo "no"
  fi
  
  return ${rc}
}


function set_outdir()
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


function validate_connection()
#------------------------------------------------------------------------------
# Attempt a ybsql connection using the environment plus and additional passed args.
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
  local is_manager=""

  ybsql $@ -c "SELECT * FROM sys.const" -o delete.me
  rc=$?


  if [[ ${rc} -ne 0 ]]; then
    is_manager="$(is_manager)"
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
