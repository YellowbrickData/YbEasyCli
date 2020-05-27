import test_run

test_cases = [
    test_case(cmd='yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev' %
              (self.host, self.test_user_name, self.test_db1),
              exit_code=0,
              stdout="""a1_t
b1_t
c1_t
data_types_t
dist_random_t
dist_replicate_t""",
              stderr=''),

    test_case(
        cmd=(
            'yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev '
            '--schemas dev prod') % (self.host,
                                     self.test_user_name,
                                     self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
dev.c1_t
dev.data_types_t
dev.dist_random_t
dev.dist_replicate_t
prod.a1_t
prod.b1_t
prod.c1_t
prod.data_types_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1\_%%'") % (self.host,
                                                      self.test_user_name,
                                                      self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
dev.c1_t
prod.a1_t
prod.b1_t
prod.c1_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1\_%%' --NOTlike '%%c%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
prod.a1_t
prod.b1_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --like '%%1\_%%' --NOTin b1_t") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.c1_t
prod.a1_t
prod.c1_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_t c1_t data_types_t") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.c1_t
dev.data_types_t
prod.a1_t
prod.c1_t
prod.data_types_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_t c1_t data_types_t --like '%%1\_%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
dev.c1_t
dev.data_types_t
prod.a1_t
prod.b1_t
prod.c1_t
prod.data_types_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_t c1_t data_types_t --like '%%1\_%%'") %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
dev.c1_t
dev.data_types_t
prod.a1_t
prod.b1_t
prod.c1_t
prod.data_types_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_t c1_t data_types_t --like '%%1\_%%' "
            "--owner %s") % (self.host,
                             self.test_user_name,
                             self.test_db1,
                             self.test_user_name),
        exit_code=0,
        stdout="""dev.a1_t
dev.b1_t
dev.c1_t
dev.data_types_t
prod.a1_t
prod.b1_t
prod.c1_t
prod.data_types_t""",
        stderr=''),

    test_case(
        cmd=(
            "yb_get_table_names.py -h %s -U %s -D %s --conn_schema dev "
            "--schemas dev prod --in a1_t c1_t data_types_t --like '%%1%%' "
            "--owner no_such_user") % (self.host,
                                       self.test_user_name,
                                       self.test_db1),
        exit_code=0,
        stdout='',
        stderr='')
]
