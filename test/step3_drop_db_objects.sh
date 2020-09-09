if [ "$1" == "--help" ] ; then
    echo "Usage: `basename $0`"
    exit 0
fi

test_path=$(dirname "$0")
if [ "$test_path" == "" ] ; then
    test_path="."
    exit 0
fi

export PATH=.:/usr/bin:$PATH
unset YBDATABASE
unset YBHOST
unset YBUSER
#test_run.py --verbose 3 drop_objects_db2
#test_run.py --verbose 3 drop_objects_db1
#test_run.py --verbose 3 drop_db2
$test_path/test_run.py --verbose 3 drop_su