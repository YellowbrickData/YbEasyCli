# start_pg_mode_no_ldap.sh
# 
# Start YB in PG only mode if configured without LDAP.
# WARNING: This should not be used without YB Technical Support. You can put your
#          appliance into a state where you cannot restart it or damage the catalog.
#
# 2024-09-11 - Fix sleep and services syntax errors
# 2024-04-13


echo "Stopping YB before restarting in PG mode."
echo "You should have already put YB in 'Maintenance mode' using WLM before proceeding."
ybcli database stop

# Make sure everything is actually down
sleep 5
services=$( ybstatus | grep -P ':\s+\d+' | wc -l)
if [ ${services} -ne 0 ]
then
	echo "YB did not shut down cleanly. Please resolve this and then restart this script/"
	echo ""
	echo 1
fi

echo "Restarting in PG only mode for no-LDAP"
sudo -u ybduser $(which ybstart) pg

echo ""
echo "DONE"
echo ""
ybstatus
echo ""
