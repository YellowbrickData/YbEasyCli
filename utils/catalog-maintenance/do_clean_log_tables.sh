#!/bin/bash

RETENTION=${1:-90}
TS=$(date +%Y%m%d-%H%M%S)

echo "Log session/authentication data will be retained for the last $RETENTION days"
for LOGTAB in session authentication ; do
	if [[ $LOGTAB == "session" ]] ; then COLUMN=end ; else COLUMN=start ; fi
	OUTPUT=log_${LOGTAB}-${TS}.csv.gz
	echo "Log ${LOGTAB}: Copying/compressing rows to be deleted to ${OUTPUT}"
	ybsql -XAqt <<SQL|gzip>$OUTPUT
SET work_mem TO 2000000;
\COPY (SELECT * FROM sys._log_${LOGTAB} WHERE ${COLUMN}_time < (CURRENT_DATE - $RETENTION)) TO STDOUT WITH (FORMAT TEXT);
DELETE FROM sys._log_${LOGTAB} WHERE ${COLUMN}_time < (CURRENT_DATE - $RETENTION);
SQL
done