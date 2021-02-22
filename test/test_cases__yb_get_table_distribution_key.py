test_cases = [
    test_case(
        cmd='yb_get_table_distribution_key.py @{argsdir}/db1 --verbose 3'
        , exit_code=1
        , stdout=""
        , stderr=(
        """yb_get_table_distribution_key.py: error: argument --table is required
for complete help, execute: yb_get_table_distribution_key.py --help"""
        if self.test_py_version == 2
        else """yb_get_table_distribution_key.py: error: the following arguments are required: --table
for complete help, execute: yb_get_table_distribution_key.py --help"""))

    , test_case(
        cmd=
        'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev --table a1_t'
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=
        'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev --table x1_t'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev '
            '--table dist_random_t')
        , exit_code=0
        , stdout='RANDOM'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db1 --schema dev '
            '--table dist_replicate_t')
        , exit_code=0
        , stdout='REPLICATED'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --schema dev '
            '--table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr=''
        , comment=(
            'Use of sys.tables cross database does not work, so no value is '
            'returned. The table can be accessed but is returning no rows\nThe '
            'sys.vt_table_info has the same issue and also is only accessible '
            'by super users.'))

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --schema dev '
            '--table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --owner {user_name} '
            '--schema dev --table a1_t --database {db2}')
        , exit_code=0
        , stdout='col1'
        , stderr='')

    , test_case(
        cmd=(
            'yb_get_table_distribution_key.py @{argsdir}/db2 --owner no_such_owner '
            '--schema dev --table a1_t --database {db2}')
        , exit_code=0
        , stdout=''
        , stderr='')
]