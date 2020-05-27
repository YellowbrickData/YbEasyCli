test_cases = [
    test_case(
        cmd=
        'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s a1_t col1' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s a1_v col1' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s a1_t colXX'
        % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s '
            'data_types_t col10') % (self.host,
                                     self.test_user_name,
                                     self.test_db1),
        exit_code=0,
        stdout='col10',
        stderr=''),

    test_case(
        cmd=
        'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s %s a1_v col1'
        % (self.host, self.test_user_name, self.test_db1, self.test_db2),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_column_name.py -h %s -U %s --conn_schema dev -D %s %s a1_v '
            'col1 extra_arg') % (self.host,
                                 self.test_user_name,
                                 self.test_db1,
                                 self.test_db2),
        exit_code=2,
        stdout='',
        stderr="""usage: yb_get_column_name.py [database] object column [options]
yb_get_column_name.py: error: unrecognized arguments: extra_arg""")
]
