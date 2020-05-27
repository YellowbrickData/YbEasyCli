test_cases = [
    test_case(
        cmd='yb_get_view_name.py -h %s -U %s -D %s --conn_schema dev a1_v' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='a1_v',
        stderr=''),

    test_case(
        cmd=(
            'yb_get_view_name.py -h %s -U %s -D %s --conn_schema dev --schema '
            'prod b1_v') % (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout='prod.b1_v',
        stderr=''),

    test_case(
        cmd=(
            "yb_get_view_name.py -h %s -U %s -D %s --conn_schema dev "
            "--schema prod %s c1_v") % (self.host,
                                        self.test_user_name,
                                        self.test_db1,
                                        self.test_db2),
        exit_code=0,
        stdout='prod.c1_v',
        stderr=''),

    test_case(
        cmd=(
            "yb_get_view_name.py -h %s -U %s -D %s --conn_schema dev "
            "--schema prod %s c1_v extra_pos_arg") % (self.host,
                                                      self.test_user_name,
                                                      self.test_db1,
                                                      self.test_db2),
        exit_code=2,
        stdout='',
        stderr="""usage: yb_get_view_name.py [database] view [options]
yb_get_view_name.py: error: unrecognized arguments: extra_pos_arg""")
]
