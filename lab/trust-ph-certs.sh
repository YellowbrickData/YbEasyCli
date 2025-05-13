#!/bin/sh
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.
DCSDB=/mnt/ybdata/ybdb/ybconfig.db
DCSAPP=io.yellowbrick.dcs.phcerts
PHPEM=/tmp/phcerts.pem

DCSDATADIR=/mnt/ybdata/ybd/dcs/build/data
AMQDIRSIZE=$(du -bd0 $DCSDATADIR/amq-broker | cut -f1)
AMQERRORS=$(grep -Fc -e 'Persistent store is Full, 100%' $DCSDATADIR/log/dcs.log)
echo ActiveMQ info: errors=$AMQERRORS, used bytes=$AMQDIRSIZE
if [ $AMQDIRSIZE -gt 5000000000 -a $AMQERRORS -gt 0 ] ; then
	echo ActiveMQ directory size: $AMQDIRSIZE bytes, running a clean up, please wait
	sudo systemctl stop ybd-dcs
	sleep 1
	sudo rm -rf $DCSDATADIR
	sudo systemctl start ybd-dcs
fi

ybsql -X -d yellowbrick -c 'SELECT version()'
rm /tmp/phcert-*
openssl s_client -connect phonehome.yellowbrick.io:443 -showcerts </dev/null 2>/dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > $PHPEM
# Note: openssl displays certs in "backwards" order compared to what DCS expects
NUM=$(grep -F -e '-BEGIN CERT' $PHPEM | wc -l)
cat $PHPEM | while read LINE ; do
	echo $LINE | grep -qF 'BEGIN CERT' && ((NUM--))
	echo $LINE >> /tmp/phcert-$NUM
done
date
ls -lh /tmp/phcert-*
RESTART_DCS=NO
for CERT in /tmp/phcert-* ; do
	KEY=cert-${CERT#*-}
	if [ $(sqlite3 $DCSDB "SELECT trim(value, char(10)) FROM config WHERE key='$KEY' AND application='$DCSAPP'" | md5sum | cut -d' ' -f1) = $(md5sum $CERT | cut -d' ' -f1) ] ; then
		echo $KEY is already registered
	else
		RESTART_DCS=YES
		echo registering $KEY
		sqlite3 $DCSDB<<SQL
UPDATE config SET application = application||' '||current_timestamp WHERE application = '$DCSAPP' AND key = '$KEY';
INSERT INTO config (application, key, value, last_updated) VALUES ('$DCSAPP', '$KEY', '$(cat $CERT)', current_timestamp);
SQL
	fi
done
if [ $RESTART_DCS = YES ] ; then
	echo restarting dcs service, please wait
	ybstop dcs
	tail -f /mnt/ybdata/ybd/dcs/build/data/log/dcs.log \
		| grep -m1 -qF -e 'RAMJobStore initialized' -e 'starting MgmtNodeStatusMonitorTask' \
		&& echo 'DCS restarted'
fi
echo Done
