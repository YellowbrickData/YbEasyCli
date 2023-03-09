#TODO sysprocs need many more test cases
#   for regular user test cases the sysview procedures need to be installed
test_cases = [
    test_case(
        cmd="""yb_sysprocs_column_dstr.py @{argsdir}/db1 --schema dev --table data_types_t --column col1"""
        , exit_code=0
        , stdout="""column                         magnitude      rows  to        to    distincts     max      tot
name                                           per          rows                 rows     rows
                                                             per
-----------------------------  -----------  ------  ----  ------  -----------  ------  -------
{db1}.dev.data_types_t.col1  10^0              1  to         9      1000000       1  1000000"""
        , stderr="")
    , test_case(
        cmd="""yb_sysprocs_column_dstr.py @{argsdir}/db1_su --schema dev --table data_types_t --column col1"""
        , exit_code=0
        , stdout="""column                         magnitude      rows  to        to    distincts     max      tot
name                                           per          rows                 rows     rows
                                                             per
-----------------------------  -----------  ------  ----  ------  -----------  ------  -------
{db1}.dev.data_types_t.col1  10^0              1  to         9      1000000       1  1000000"""
        , stderr="")
]