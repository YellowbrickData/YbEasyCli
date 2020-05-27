test_cases = [
    test_case(cmd='yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev' %
              (self.host, self.test_user_name, self.test_db1),
              exit_code=0,
              stdout="""a1_seq
b1_seq
c1_seq""",
              stderr=''),

    test_case(
        cmd=(
            'yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev '
            '--schemas dev prod') % (self.host,
                                     self.test_user_name,
                                     self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
prod.a1_seq
prod.b1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1%%'") % (self.host,
                                                    self.test_user_name,
                                                    self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
prod.a1_seq
prod.b1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1%%' --NOTlike '%%c%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
prod.a1_seq
prod.b1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1%%' --NOTin b1_seq") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.c1_seq
prod.a1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_seq c1_seq") % (self.host,
                                                    self.test_user_name,
                                                    self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.c1_seq
prod.a1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_seq c1_seq --like '%%1%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
prod.a1_seq
prod.b1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_seq c1_seq --like '%%1%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
prod.a1_seq
prod.b1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_sequence_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_seq c1_seq --like '%%1%%' --owner %s") %
            (self.host, self.test_user_name,
             self.test_db1, self.test_user_name),
        exit_code=0,
        stdout="""dev.a1_seq
dev.b1_seq
dev.c1_seq
prod.a1_seq
prod.b1_seq
prod.c1_seq""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_view_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_seq c1_seq data_vypes_seq --like '%%1%%' "
            "--owner no_such_user") % (self.host,
                                       self.test_user_name,
                                       self.test_db1),
        exit_code=0,
        stdout='',
        stderr='')
]
