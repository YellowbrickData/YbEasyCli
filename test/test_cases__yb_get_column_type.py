test_cases = [
    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col1'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='BIGINT',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col2'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='INTEGER',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col3'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='SMALLINT',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col4'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='NUMERIC(18,0)',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col5'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='REAL',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col6'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='DOUBLE PRECISION',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col7'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='UUID',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col8'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='CHARACTER VARYING(256)',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col9'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='CHARACTER(1)',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col10'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='DATE',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col11'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='TIME WITHOUT TIME ZONE',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col12'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='TIMESTAMP WITHOUT TIME ZONE',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col13'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='TIMESTAMP WITH TIME ZONE',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col14'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='IPV4',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col15'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='IPV6',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col16'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='MACADDR',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t col17'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='MACADDR8',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_type.py -h %s -U %s --schema dev -D %s data_types_t colXX'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='',
        stderr=''),

    test_case(
        cmd=('yb_get_column_type.py -h %s -U %s --schema dev -D %s %s '
             'data_types_t col1') % (self.host,
                                     self.test_user_name,
                                     self.test_db1,
                                     self.test_db2),
        exit_code=0,
        stdout='BIGINT',
        stderr=''),

    test_case(
        cmd='yb_get_column_type.py -h %s -U %s --schema dev -D %s col1' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=2,
        stdout='',
        stderr=(
        "usage: yb_get_column_type.py [database] table column [options]\n"
            "yb_get_column_type.py: error: the following arguments are "
            "required: column" ))
]
