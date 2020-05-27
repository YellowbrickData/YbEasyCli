test_cases = [
    test_case(
        cmd='yb_ddl_table.py -h %s -U %s -D %s --conn_schema dev --like a1_t' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_t
-- Schema: dev
-----------------------
CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);""",
        stderr=''),

    test_case(
        cmd=
        ('yb_ddl_table.py -h %s -U %s -D %s --conn_schema dev  '
         '--schemas dev prod --like a1_t') % (self.host,
                                             self.test_user_name,
                                             self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_t
-- Schema: dev
-----------------------
CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);
-- SHOW DDL
-- Name: a1_t
-- Schema: prod
-----------------------
CREATE TABLE a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);""",
        stderr=''),

    test_case(
        cmd=
        ('yb_ddl_table.py -h %s -U %s -D %s --conn_schema dev  '
         '--schemas dev prod --with_schema --like a1_t') % (self.host,
                                                            self.test_user_name,
                                                            self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_t
-- Schema: dev
-----------------------
CREATE TABLE dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);
-- SHOW DDL
-- Name: a1_t
-- Schema: prod
-----------------------
CREATE TABLE prod.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);""",
        stderr=''),

    test_case(
        cmd=
        ('yb_ddl_table.py -h %s -U %s -D %s --conn_schema dev  '
         '--schemas dev prod --with_db --like a1_t') % (self.host,
                                                        self.test_user_name,
                                                        self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_t
-- Schema: dev
-----------------------
CREATE TABLE %(test_db1)s.dev.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);
-- SHOW DDL
-- Name: a1_t
-- Schema: prod
-----------------------
CREATE TABLE %(test_db1)s.prod.a1_t (
    col1 INTEGER
)
DISTRIBUTE ON (col1);""" % {"test_db1": self.test_db1},
        stderr='')
]
