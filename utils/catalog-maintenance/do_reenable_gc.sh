# do_renable_gc.sh
#
# Renable GC in the lime client.
# . GC needs to always be reenabled if disabled before catalog maintenance.
# . Only runs in a manager node debug shell.
#
# 2024-05-17
#

ybcli_path=$(which ybcli)

# Run the command only from the manager node.
if [ "${ybcli_path}" == "" ]  
then
  echo "ERROR: This script can only be run from an active manager node bash shell."
  exit 1
else
 echo "gc-cmd --enable" | /mnt/ybdata/ybd/lime/build/bin/client -- 
 echo "gc-cmd --get"    | /mnt/ybdata/ybd/lime/build/bin/client -- 
fi 