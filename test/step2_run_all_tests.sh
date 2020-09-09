if [ "$1" == "--help" ] ; then
    echo "Usage: `basename $0` args"
    exit 0
fi

test_path=$(dirname "$0")
if [ "$test_path" == "" ] ; then
    test_path="."
    exit 0
fi

export PATH=.:/usr/bin:$PATH
unset YBPASSWORD
unset YBDATABASE
unset YBHOST
unset YBUSER
$test_path/test_run.py $* --verbose 1 yb_get_table_name
$test_path/test_run.py $* --verbose 1 yb_get_table_names
$test_path/test_run.py $* --verbose 1 yb_get_view_name
$test_path/test_run.py $* --verbose 1 yb_get_view_names
$test_path/test_run.py $* --verbose 1 yb_get_column_name
$test_path/test_run.py $* --verbose 1 yb_get_column_names
$test_path/test_run.py $* --verbose 1 yb_get_table_distribution_key
$test_path/test_run.py $* --verbose 1 yb_get_column_type
$test_path/test_run.py $* --verbose 1 yb_get_sequence_names
$test_path/test_run.py $* --verbose 1 yb_ddl_table
$test_path/test_run.py $* --verbose 1 yb_ddl_view
$test_path/test_run.py $* --verbose 1 yb_ddl_sequence
$test_path/test_run.py $* --verbose 1 yb_analyze_columns
$test_path/test_run.py $* --verbose 1 yb_check_db_views
$test_path/test_run.py $* --verbose 1 yb_find_columns
$test_path/test_run.py $* --verbose 1 yb_chunk_dml_by_integer
$test_path/test_run.py $* --verbose 1 yb_chunk_dml_by_date_part
$test_path/test_run.py $* --verbose 1 yb_chunk_dml_by_yyyymmdd_integer
$test_path/test_run.py $* --verbose 1 yb_chunk_optimal_rows
$test_path/test_run.py $* --verbose 1 yb_rstore_query_to_cstore_table
$test_path/test_run.py $* --verbose 1 yb_is_cstore_table
$test_path/test_run.py $* --verbose 1 yb_mass_column_update