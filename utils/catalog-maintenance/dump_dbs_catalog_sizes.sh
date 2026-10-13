# dump_dbs_catalog_sizes.sh
#
# Iterate over all user dbs and get their catalog sizes and append to file.
# Output is in a mostly fixed outputformat for readability.
#
# Prerequisites:
# . Assumes YBHOST, YBUSER, and YBPASSWORD env variables are set unless running
#   from a manager node.
#
# NOTE: 
# . "global" (e.g. shared) PG tables are output only for the yellowbrick db.
# . This will append, not overwrite an existing catalog_sz_outfile.
#
# 2024-10-22 - Write ybcli output to separate file.
# 2024-09-11 - Add visual separator to output
# 2024-09-03

ybcli_sz_outfile="ybcli_storage.out"
catalog_sz_outfile="dbs_catalog_sizes.out"
ybcli_path=$(which ybcli)

# Only do if ybcli is found in the path
if [ -n "${ybcli_path}" ]
then
  echo "Appending ybcli status storage to ${ybcli_sz_outfile}"
  echo '=========================================================' >> ${ybcli_sz_outfile}
  echo "DATE: $(date)"                                             >> ${ybcli_sz_outfile}
  [ -n "${ybcli_path}" ] && ybcli status storage | grep 'Catalog'  >> ${ybcli_sz_outfile}
  echo ""                                                          >> ${ybcli_sz_outfile}
fi


# Output header before query output
echo "Appending catalog sizes for each db to ${catalog_sz_outfile}"
echo '=========================================================' >> ${catalog_sz_outfile}
echo "DATE: $(date)"                                             >> ${catalog_sz_outfile}
echo ""                                                          >> ${catalog_sz_outfile}
echo '           ts           |                        database_name                         | total_mb | table_mb | index_mb | toast_mb '  >> ${catalog_sz_outfile}
echo '------------------------+--------------------------------------------------------------+----------+----------+----------+----------'  >> ${catalog_sz_outfile}

dbs=$(ybsql -d yellowbrick -qAtc "SELECT name FROM sys.database WHERE name !='yellowbrick' ORDER BY 1" )
for db in ${dbs}
do
	echo -n ".${db}."
	ybsql -qt -d ${db} -f get_db_catalog_size.sql --record-separator '' | grep -v -P '^$' >> ${catalog_sz_outfile}
done
echo ""
echo '--------------------------------------------------------------------------' 
echo "DONE. Catalog size results output to ${catalog_sz_outfile}"
echo '--------------------------------------------------------------------------' 
echo ''