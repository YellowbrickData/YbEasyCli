# fstrim.sh
#
# Run fstrim on 
# . local manager node drbd mounted file systems
# . local manager node root file system
# . remote manager node root file system
#
# The /mnt/* file systems are mounted on only the primary manager node.
#
# Prerequisites
# . logged into the primary manager node as user 'ybdadmin'
#
# 2026.02.13 (rek)

echo "${0}: WARNING: This script can take 30 minutes or more depending upon the manager node SSD|NVME hardware and state."
curr_drbd_ip="$(ifconfig DRBD_bond | grep 'inet ' | awk '{print $2}')"
echo "curr_drbd_ip  =${curr_drbd_ip}"
if [[ "${curr_drbd_ip}" == "192.168.1.1" ]] 
then
  remote_drbd_ip="192.168.1.2"
else
  remote_drbd_ip="192.168.1.1"
fi
echo "remote_drbd_ip=${remote_drbd_ip}"


# Primary manager node
ulimit -s 2097152
echo -n '['$( date +"%Y-%m-%d %H:%M:%S" )'] Running fstrim on 4 manager node file systems:'
echo -n " /mnt/ybdata"
sudo fstrim /mnt/ybdata 
echo -n " /mnt/rowstore"
sudo fstrim /mnt/rowstore
echo -n " /mnt/rowspool"
sudo fstrim /mnt/rowspool
echo -n " /"
sudo fstrim /
echo ""

# Remote manager node
echo '['$( date +"%Y-%m-%d %H:%M:%S" )'] Running fstrim on remote manager node file system "/"'
ssh ${remote_drbd_ip} 'ulimit -s 2097152; sudo fstrim /'

echo -e '['$( date +"%Y-%m-%d %H:%M:%S" )'] DONE\n'
