test_cases = [
    test_case(
        cmd='yb_get_column_names.py -h %s -U %s --conn_schema dev -D %s a1_t' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd='yb_get_column_names.py -h %s -U %s --conn_schema dev -D %s a1_v' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='col1',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_column_names.py -h %s -U %s --conn_schema dev -D %s '
            'data_types_t') % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""col1
col2
col3
col4
col5
col6
col7
col8
col9
col10
col11
col12
col13
col14
col15
col16
col17""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_column_names.py -h %s -U %s --conn_schema dev -D %s "
            "--NOTlike '%%1%%' -- data_types_t") % (self.host,
                                                    self.test_user_name,
                                                    self.test_db1),
        exit_code=0,
        stdout="""col2
col3
col4
col5
col6
col7
col8
col9""",
        stderr='')
]
