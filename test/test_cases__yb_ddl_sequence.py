test_cases = [
    test_case(
        cmd='yb_ddl_sequence.py -h %s -U %s -D %s --conn_schema dev --like a1_seq' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_seq
-- Schema: dev
--------------------------------------------
CREATE SEQUENCE a1_seq START WITH 1000448;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_sequence.py -h %s -U %s -D %s --conn_schema dev --schemas '
            'dev prod --like a1_seq') % (self.host,
                                       self.test_user_name,
                                       self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_seq
-- Schema: dev
--------------------------------------------
CREATE SEQUENCE a1_seq START WITH 1000448;
-- SHOW DDL
-- Name: a1_seq
-- Schema: prod
--------------------------------------------
CREATE SEQUENCE a1_seq START WITH 1000448;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_sequence.py -h %s -U %s -D %s --conn_schema dev --schemas '
            'dev prod --with_schema --like a1_seq') % (self.host,
                                                       self.test_user_name,
                                                       self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_seq
-- Schema: dev
--------------------------------------------
CREATE SEQUENCE dev.a1_seq START WITH 1000448;
-- SHOW DDL
-- Name: a1_seq
-- Schema: prod
--------------------------------------------
CREATE SEQUENCE prod.a1_seq START WITH 1000448;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_sequence.py -h %s -U %s -D %s --conn_schema dev  --schemas '
            'dev prod --with_db --like a1_seq') % (self.host,
                                                   self.test_user_name,
                                                   self.test_db1),
        exit_code=0,
        stdout=("""-- SHOW DDL
-- Name: a1_seq
-- Schema: dev
--------------------------------------------
CREATE SEQUENCE %(test_db1)s.dev.a1_seq START WITH 1000448;
-- SHOW DDL
-- Name: a1_seq
-- Schema: prod
--------------------------------------------
CREATE SEQUENCE %(test_db1)s.prod.a1_seq START WITH 1000448;"""
                % {"test_db1": self.test_db1}),
        stderr='')
]
