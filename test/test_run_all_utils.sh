if [ "$1" == "--help" ] ; then
    echo "Usage: `basename $0` host"
    exit 0
fi

export PATH=.:/usr/bin:$PATH
unset YBPASSWORD
unset YBDATABASE
unset YBHOST
unset YBUSER
test_run.py -h $1 --verbose 1 yb_get_table_name
test_run.py -h $1 --verbose 1 yb_get_table_names
test_run.py -h $1 --verbose 1 yb_get_view_name
test_run.py -h $1 --verbose 1 yb_get_view_names
test_run.py -h $1 --verbose 1 yb_get_column_name
test_run.py -h $1 --verbose 1 yb_get_column_names
test_run.py -h $1 --verbose 1 yb_get_table_distribution_key
test_run.py -h $1 --verbose 1 yb_get_column_type
test_run.py -h $1 --verbose 1 yb_get_sequence_names
test_run.py -h $1 --verbose 1 yb_ddl_table
test_run.py -h $1 --verbose 1 yb_ddl_view
test_run.py -h $1 --verbose 1 yb_ddl_sequence
