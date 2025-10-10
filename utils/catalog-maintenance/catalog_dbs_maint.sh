# catalog_dbs_maint.sh
#
# Generate the 4 catalog_db_maint_*.sql files for all user databases.
# Run 5 parallel processes/jobs: 0 is the global catalog, the rest (1-4) are user databases.
#
# If not running from the manager node, the YBHOST, YBUSER, and YBPASSWORD
# environment variables must be set.
#
export YBDATABASE=yellowbrick
# Generate four catalog maintenance scripts for all user databases
ybsql -Xq -f gen_catalog_dbs_maint.sql
if [ "$1" = "+global" ] ; then
	echo '-- Will also start global catalog maintenance as Job 0'
	cp catalog_yb_maint.sql catalog_dbs_maint_0.out.sql
	START_JOB=0
else
	echo '-- Skipping global catalog maintenance, use +global switch to include it'
	START_JOB=1
fi
# Iterate over each catalog_dbs_maint_*.sql file
echo "-- $(date '+%Y-%m-%d %T') -------------------------------------------------------------------"
for i in $(seq ${START_JOB} 4) ; do
	echo -n "-- Starting job ${i}: catalog_dbs_maint_${i}.out.sql ... "
	ybsql -Xqt -f catalog_dbs_maint_${i}.out.sql -o catalog_dbs_maint_${i}.out.log &
	PIDS[${i}]=$!
	echo "PID = $!"
done
TIGHT=5
while true ; do
	echo -n "-- Sleeping for $TIGHT sec ... "
	sleep $TIGHT
	declare -a FINISHED
	for JOB in ${!PIDS[@]} ; do
		kill -0 ${PIDS[$JOB]} 2>/dev/null || { FINISHED[$JOB]=${PIDS[$JOB]} ; unset PIDS[$JOB] ; }
	done
	echo "waking up @ $(date '+%Y-%m-%d %T'), active job(s) = ${#PIDS[@]} (${!PIDS[@]})"
	for JOB in ${!FINISHED[@]} ; do echo -e "-- Job $JOB (PID=${FINISHED[$JOB]}) has \e[96mfinished\e[0m" ; done
	[ ${#PIDS[@]} -eq 0 ] && break
	ybsql -Xq <<SQL | sed -e 's/[\x5B]/[91m/' -e 's/[\x5C]/[92m/' -e 's/[\x5A]/[0m/'
SELECT split_part(application_name,'-',3) AS job, date_trunc('seconds', now() - last_statement) AS age
	, pid AS pg_pid, session_id, query_id, datname, "query", backend_xid
	, CASE WHEN waiting THEN 'YES' ELSE NULL END::VARCHAR(3) AS wait, state
	, CASE WHEN waiting AND backend_xid IS NULL AND age > INTERVAL '60 seconds' THEN chr(91)||'STUCK' ELSE chr(92)||'OK' END::VARCHAR(8)||chr(90) AS status
FROM pg_stat_activity
WHERE application_name LIKE 'syscat-maint-_' AND "query" ILIKE 'vacuum full %'
ORDER BY application_name;
SQL
	unset FINISHED
done

echo '-- All catalog_db_maint_*.sql jobs complete.'
echo '-- Output written to catalog_dbs_maint_[1-4].out.log'
echo "-- $(date '+%Y-%m-%d %T') -------------------------------------------------------------------"
