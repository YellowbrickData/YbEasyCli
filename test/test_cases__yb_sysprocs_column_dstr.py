#TODO sysprocs need many more test cases
#   for regular user test cases the sysview procedures need to be installed
#   for superuser test cases a superuser password needs to be incorporated into the test framework
test_cases = [
    test_case(
        cmd="""yb_sysprocs_column_dstr.py @{argsdir}/db1  --schema dev --table data_types_t --column col1"""
        , exit_code=(0 if Common.is_windows else 1)
        , stdout=""
        , stderr="""yb_sysprocs_column_dstr.py: this report may only be run by a DB super user
or you may ask your DBA to perform the non-super user prerequisites
which require installing the sysviews library and granting permissions""")
]