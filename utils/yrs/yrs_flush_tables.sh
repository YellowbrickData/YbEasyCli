# yrs_flush_user_tables.sh
#
# Generate SQL to flush YRS user OR sys.* tables and then execute.
# An invocation of this script flushed one or the other based on the args.
#
# This script generates 4 separate SQL files, each of which will be run
# in its own bash thread.
#
# Arguments:
# . $1 flush_type (reqd) - "user" or "sys"
# . $2 outdir     (optl) - output directory relative to script dir OR absolute
#
# Prerequisites:
# . Must be run as a superuser
# . If not run from mgr node, YBHOST, YBUSER, and YBPASSWORD env vars must be set.
#
# Revision History:
# . 2026-02-05 (rek) Initial version. (Replaces prior flushall.sh) (IN PROCESS)
#
# TODO:
# . Refacot to clean up output+sql src dir code

readonly script_version='2026.02.05.2110'
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly script_file_name="$(echo $(basename $0))"
readonly script_name="$(echo $(basename $0) | cut -f1 -d'.' )"

[[ $# -ne 2 ]] && echo "ERROR: Required arguments are type (user or sys) and output directory" \
               && exit 1

flush_type=$1
if [[ "${flush_type}" == "sys" ]]
then 
  sql_file_prefix='yrs_flush_sys_tables'
elif [[ "${flush_type}" == "user" ]] 
then
  sql_file_prefix='yrs_flush_user_tables'
else 
  echo "ERROR: flush type must be 'user' or 'sys'" \
  && exit 1
fi

dfltdir="../output/"$(date '+%Y%m%d')
readonly outdir="${2:-${dfltdir}}"
mkdir -p ${outdir} > /dev/null


#echo "------------------------------------------------------------------------------"
# Generate four files of yflush statements to be executed in separate threads
for i in {1..4} 
do
  gen_sql_file="gen_"${sql_file_prefix}".sql"
  out_sql_file="${outdir}/${sql_file_prefix}_${i}.sql"
  #echo "[$(date '+%Y-%m-%d %T')] (${i}) Generating ${out_sql_file} from ${gen_sql_file}"
  ybsql -qAt -d yellowbrick -v "grouping=${i}" -f "${gen_sql_file}" -o "${out_sql_file}"
done

#echo "Generated:"
#ls -l ${outdir}/${sql_file_prefix}*.sql  


#echo "------------------------------------------------------------------------------"
# Run each *.sql file in a separate thread
for i in {1..4} 
do
  echo "Thread ${i}: ${sql_file_prefix}${i}.sql"
  run_sql_file="${outdir}/${sql_file_prefix}_${i}.sql"
  out_sql_file="${run_sql_file}.out"
  ybsql -qAt -d yellowbrick -f ${run_sql_file} -o ${out_sql_file} &
  # Wait 1 second between forked thread starts.
  sleep 1 
done

echo "------------------------------------------------------------------------------"
echo "All ${sql_file_prefix}*.sql jobs started."
echo "Waiting for jobs to complete."
wait

echo "------------------------------------------------------------------------------"
echo "DONE: $(date '+%Y-%m-%d %T')"
echo "All ${sql_file_prefix}*.sql jobs complete."
echo "Output written to ${outdir}/${sql_file_prefix}_[1-4].sql.out"
echo "------------------------------------------------------------------------------"

