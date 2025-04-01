# stop_pg_mode_with_ldap.sh
#
# Stop YB if running in PG only mode when configured with LDAP.
# You would use this only when YB was started in PG only mode 
# (i.e. when started with stop_pg_mode_no_ldap.sh).
#
# 2024-09-11 - Fix sleep and services syntax errors
# 2024-04-13

echo "Stopping PG only mode for no-LDAP"
cd /tmp/ \
  && eval 'export $(sudo cat /mnt/ybdata/certs/ybdmasterkey)' \
  && sudo -u ybduser YBDMASTERKEY=$YBDMASTERKEY $(which ybstop) pg


# Make sure everything is actualy down
sleep 5
services=$( ybstatus | grep -P ':\s+\d+' | wc -l)
if [ ${services} -ne 0 ]
then
	echo "YB did not shut down cleanly."
	echo "Resolve before restarting YB."
else 
	echo "YB PG mode shut down cleanly."
	echo "You can now restart YB."
fi

echo ""
echo "DONE"
echo ""
ybstatus
echo ""