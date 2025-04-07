test_cases = [
    test_case(
        cmd="""yb_exec_ybtool.py  @{argsdir}/db1 --ybtool_cmd "ybsql -c 'SELECT 1 AS one'" """
        , exit_code=0
        , stdout="""one 
-----
   1
(1 row)"""
        , stderr='')
    , test_case(
        cmd="""yb_exec_ybtool.py  @{argsdir}/db1 --ybtool_cmd "../bin/yb_is_cstore_table.py @args_tmp/db1 --table sys.log_query" """
        , exit_code=0
        , stdout="""True"""
        , stderr='')
]