test_cases = [
    test_case(
        cmd='yb_ddl_view.py -h %s -U %s -D %s --conn_schema dev --like a1_v' %
        (self.host, self.test_user_name, self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_v
-- Schema: dev
---------------------
CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM a1_t;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_view.py -h %s -U %s -D %s --conn_schema dev  --schemas '
            'dev prod --like a1_v') % (self.host,
                                       self.test_user_name,
                                       self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_v
-- Schema: dev
---------------------
CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM a1_t;
-- SHOW DDL
-- Name: a1_v
-- Schema: prod
---------------------
CREATE VIEW a1_v AS
 SELECT a1_t.col1
   FROM prod.a1_t;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_view.py -h %s -U %s -D %s --conn_schema dev  --schemas '
            'dev prod --with_schema --like a1_v') % (self.host,
                                                     self.test_user_name,
                                                     self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_v
-- Schema: dev
---------------------
CREATE VIEW dev.a1_v AS
 SELECT a1_t.col1
   FROM a1_t;
-- SHOW DDL
-- Name: a1_v
-- Schema: prod
---------------------
CREATE VIEW prod.a1_v AS
 SELECT a1_t.col1
   FROM prod.a1_t;""",
        stderr=''),

    test_case(
        cmd=(
            'yb_ddl_view.py -h %s -U %s -D %s --conn_schema dev  --schemas '
            'dev prod --with_db --like a1_v') % (self.host,
                                                 self.test_user_name,
                                                 self.test_db1),
        exit_code=0,
        stdout="""-- SHOW DDL
-- Name: a1_v
-- Schema: dev
---------------------
CREATE VIEW %(test_db1)s.dev.a1_v AS
 SELECT a1_t.col1
   FROM a1_t;
-- SHOW DDL
-- Name: a1_v
-- Schema: prod
---------------------
CREATE VIEW %(test_db1)s.prod.a1_v AS
 SELECT a1_t.col1
   FROM prod.a1_t;""" % {"test_db1": self.test_db1},
        stderr='')
]
