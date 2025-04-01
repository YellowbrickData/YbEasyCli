# do_disable_gc.sh
#
# Disable GC via the lime client.
# . Can only be run from the manager node debug shell.
# . Typically done this before catalog maintenance if > 8 databases.
# . It is CRITICAL to renable GC after maintenace is done!
#
# 2024-05-13
#

ybcli_path=$(which ybcli)

# Run the command only from the manager node.
if [ "${ybcli_path}" == "" ]  
then
  echo "ERROR: This script can only be run from an active manager node bash shell."
  exit 1
else
 echo "gc-cmd --disable" | /mnt/ybdata/ybd/lime/build/bin/client -- 
 echo "gc-cmd --get"     | /mnt/ybdata/ybd/lime/build/bin/client -- 
fi 