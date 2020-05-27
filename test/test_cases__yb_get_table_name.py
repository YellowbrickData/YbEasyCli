test_cases = [
    test_case(
        cmd='yb_get_table_name.py -h %s -U %s -D %s --conn_schema dev a1_t' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='a1_t',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_table_name.py -h %s -U %s -D %s --conn_schema dev --schema '
            'prod b1_t') % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='prod.b1_t',
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_name.py -h %s -U %s -D %s --conn_schema dev --schema "
            "prod %s c1_t") % (self.host,
                               self.test_user_name,
                               self.test_db1,
                               self.test_db2),
        exit_code=0,
        stdout='prod.c1_t',
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_name.py -h %s -U %s -D %s --conn_schema dev --schema "
            "prod %s c1_t extra_pos_arg") % (self.host,
                                             self.test_user_name,
                                             self.test_db1,
                                             self.test_db2),
        exit_code=2,
        stdout='',
        stderr="""usage: yb_get_table_name.py [database] table [options]
yb_get_table_name.py: error: unrecognized arguments: extra_pos_arg""")
]
