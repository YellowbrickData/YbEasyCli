if [ "$1" == "--help" ] ; then
    echo "Usage: `basename $0` host"
    exit 0
fi

export PATH=.:/usr/bin:$PATH
export YBPASSWORD=yellowbrick
unset YBDATABASE
unset YBHOST
unset YBUSER
test_run.py -h $1 -U yellowbrick --verbose 3 create_su
test_run.py -h $1 --verbose 3 create_db2
test_run.py -h $1 --verbose 3 create_objects_db1
test_run.py -h $1 --verbose 3 create_objects_db2
