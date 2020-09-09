test_cases = [
    test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col1 --'
        , exit_code=0
        , stdout='BIGINT'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col2 --'
        , exit_code=0
        , stdout='INTEGER'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col3 --'
        , exit_code=0
        , stdout='SMALLINT'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col4 --'
        , exit_code=0
        , stdout='NUMERIC(18,0)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col5 --'
        , exit_code=0
        , stdout='REAL'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col6 --'
        , exit_code=0
        , stdout='DOUBLE PRECISION'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col7 --'
        , exit_code=0
        , stdout='UUID'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col8 --'
        , exit_code=0
        , stdout='CHARACTER VARYING(256)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col9 --'
        , exit_code=0
        , stdout='CHARACTER(1)'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col10 --'
        , exit_code=0
        , stdout='DATE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col11 --'
        , exit_code=0
        , stdout='TIME WITHOUT TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col12 --'
        , exit_code=0
        , stdout='TIMESTAMP WITHOUT TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col13 --'
        , exit_code=0
        , stdout='TIMESTAMP WITH TIME ZONE'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col14 --'
        , exit_code=0
        , stdout='IPV4'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col15 --'
        , exit_code=0
        , stdout='IPV6'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col16 --'
        , exit_code=0
        , stdout='MACADDR'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col17 --'
        , exit_code=0
        , stdout='MACADDR8'
        , stderr='')

    , test_case(
        cmd=
            'yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column colXX --'
        , exit_code=0
        , stdout=''
        , stderr='')

    , test_case(
        cmd=('yb_get_column_type.py @{argsdir}/db1 --schema dev '
            '--table data_types_t --column col1 -- {db2}')
        , exit_code=0
        , stdout='BIGINT'
        , stderr='')

    , test_case(
        cmd='yb_get_column_type.py @{argsdir}/db1 --schema dev col1 --'
        , exit_code=2
        , stdout=''
        , stderr=(
        """usage: yb_get_column_type.py [database] [options]
yb_get_column_type.py: error: the following arguments are required: --table, --column"""
        if self.test_py_version == 3
        else """usage: yb_get_column_type.py [database] [options]
yb_get_column_type.py: error: argument --table is required"""))
]