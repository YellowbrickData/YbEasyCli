#!/usr/bin/env python3
"""Run tests and report results."""

import os
import sys
sys.path.append('../')
import yb_common


class test_case:
    """Contains structures for running tests and checking results."""
    def __init__(self, cmd, exit_code, stdout, stderr, comment=''):
        self.cmd = cmd
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.comment = comment

    def run(self, type, common):
        """Run the test.

        :param type: The type of the database object
        :param common: An instance of the `common` class
        """
        self.type = type
        self.cmd_results = common.call_cmd('../%s' % (self.cmd))
        self.check()
        print('Test: %s, Type: %s, Command: %s' %
              (common.color('Passed', fg='green') if self.passed else
               common.color('Failed', fg='red'), self.type, self.cmd))
        if not self.passed:
            self.print_test_comparison()

    def check(self):
        """Check test results.

        :return: True if the actual results match the expected results,
                 False otherwise.
        """
        self.passed = (
            True if (self.exit_code == self.cmd_results.exit_code and
                     self.stdout.strip() == self.cmd_results.stdout.strip() and
                     self.stderr.strip() == self.cmd_results.stderr.strip())
            else False)

    def print_test_comparison(self):
        """Print a comparison between actual and expected results."""
        print("Exit Code Expected: '%d', Returned: '%d'" %
              (self.exit_code, self.cmd_results.exit_code))
        print("STDOUT Expected: '%s', Returned: '%s'" %
              (self.stdout.strip(), self.cmd_results.stdout.strip()))
        print("STDERR Expected: '%s', Returned: '%s'" %
              (self.stderr.strip(), self.cmd_results.stderr.strip()))


class execute_test_action:
    """Initiate testing"""
    def __init__(self):
        exec(open('%s' % ('test_constants.py'), 'r').read())

        if 'yellowbrick' not in sys.argv:
            os.environ["YBUSER"] = self.test_user_name
            os.environ["YBPASSWORD"] = self.test_user_password
            os.environ["YBDATABASE"] = self.test_db1

        common = self.init_common()
        self.host = common.args.host

        queries_create_su = [
            "CREATE USER %s CREATEDB LOGIN PASSWORD '%s'" %
            (self.test_user_name, self.test_user_password),
            "CREATE DATABASE %s OWNER %s" %
            (self.test_db1, self.test_user_name),
            "GRANT CONNECT ON DATABASE %s TO %s" %
            (self.test_db1, self.test_user_name)
        ]

        queries_drop_su = [
            "DROP DATABASE %s" % self.test_db1,
            "DROP USER %s" % (self.test_user_name)
        ]

        queries_create_db2 = ["CREATE DATABASE %s" % self.test_db2]

        queries_drop_db2 = ["DROP DATABASE %s" % self.test_db2]

        ddl_dev_types_t = """CREATE TABLE %s.data_types_t (
    col1 BIGINT
    , col2 INTEGER
    , col3 SMALLINT
    , col4 DECIMAL
    , col5 REAL
    , col6 DOUBLE PRECISION
    , col7 UUID
    , col8 VARCHAR
    , col9 CHAR
    , col10 DATE
    , col11 TIME
    , col12 TIMESTAMP
    , col13 TIMESTAMP WITH TIME ZONE
    , col14 IPV4
    , col15 IPV6
    , col16 MACADDR
    , col17 MACADDR8
) DISTRIBUTE ON (col1)
"""

        queries_create_objects_db1 = [
            "CREATE SCHEMA dev",
            "CREATE TABLE dev.a1_t (col1 INT) DISTRIBUTE ON (col1)",
            "CREATE TABLE dev.b1_t (col1 INT) DISTRIBUTE ON (col1)",
            "CREATE TABLE dev.c1_t (col1 INT) DISTRIBUTE ON (col1)",
            "CREATE TABLE dev.dist_random_t (col1 INT) DISTRIBUTE RANDOM",
            "CREATE TABLE dev.dist_replicate_t (col1 INT) DISTRIBUTE REPLICATE",
            ddl_dev_types_t % 'dev',
            "CREATE VIEW dev.a1_v AS SELECT * FROM dev.a1_t",
            "CREATE VIEW dev.b1_v AS SELECT * FROM dev.b1_t",
            "CREATE VIEW dev.c1_v AS SELECT * FROM dev.c1_t",
            "CREATE SEQUENCE dev.a1_seq START WITH 1000000",
            "CREATE SEQUENCE dev.b1_seq START WITH 1000000",
            "CREATE SEQUENCE dev.c1_seq START WITH 1000000",
            "CREATE SCHEMA prod",
            "CREATE TABLE prod.a1_t (col1 INT) DISTRIBUTE ON (col1)",
            "CREATE TABLE prod.b1_t (col1 INT) DISTRIBUTE ON (col1)",
            "CREATE TABLE prod.c1_t (col1 INT) DISTRIBUTE ON (col1)",
            ddl_dev_types_t % 'prod',
            "CREATE VIEW prod.a1_v AS SELECT * FROM prod.a1_t",
            "CREATE VIEW prod.b1_v AS SELECT * FROM prod.b1_t",
            "CREATE VIEW prod.c1_v AS SELECT * FROM prod.c1_t",
            "CREATE SEQUENCE prod.a1_seq START WITH 1000000",
            "CREATE SEQUENCE prod.b1_seq START WITH 1000000",
            "CREATE SEQUENCE prod.c1_seq START WITH 1000000"
        ]

        queries_drop_objects_db1 = [
            "DROP SEQUENCE prod.c1_seq", "DROP SEQUENCE prod.b1_seq",
            "DROP SEQUENCE prod.a1_seq", "DROP VIEW prod.c1_v",
            "DROP VIEW prod.b1_v", "DROP VIEW prod.a1_v",
            "DROP TABLE prod.data_types_t", "DROP TABLE prod.c1_t",
            "DROP TABLE prod.b1_t", "DROP TABLE prod.a1_t", "DROP SCHEMA prod",
            "DROP SEQUENCE dev.c1_seq", "DROP SEQUENCE dev.b1_seq",
            "DROP SEQUENCE dev.a1_seq", "DROP VIEW dev.c1_v",
            "DROP VIEW dev.b1_v", "DROP VIEW dev.a1_v",
            "DROP TABLE dev.data_types_t", "DROP TABLE dev.dist_replicate_t",
            "DROP TABLE dev.dist_random_t", "DROP TABLE dev.c1_t",
            "DROP TABLE dev.b1_t", "DROP TABLE dev.a1_t", "DROP SCHEMA dev"
        ]

        queries_create_objects_db2 = queries_create_objects_db1
        queries_drop_objects_db2 = queries_drop_objects_db1

        if common.args.action[0] == 'create_su':
            for query in queries_create_su:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'drop_su':
            for query in queries_drop_su:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'create_db2':
            for query in queries_create_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'drop_db2':
            for query in queries_drop_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'create_objects_db1':
            for query in queries_create_objects_db1:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'drop_objects_db1':
            for query in queries_drop_objects_db1:
                cmd_results = common.ybsql_query(query)

        os.environ["YBDATABASE"] = self.test_db2

        if common.args.action[0] == 'create_objects_db2':
            for query in queries_create_objects_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action[0] == 'drop_objects_db2':
            for query in queries_drop_objects_db2:
                cmd_results = common.ybsql_query(query)

        # Actions beginning with `yb` refer to the yb utility scripts in this
        # package, e.g. `yb_get_table_name` or `yb_get_column_names`
        if common.args.action[0][0:2] == 'yb':
            # Test cases are defined in files within this directory
            #   (see files with prefix `test_cases__`)
            # We need to execute the relevant test case file and bring
            # the list of `test_case` objects into the local scope
            _ldict = locals()
            exec(open('test_cases__%s.py' %
                      (common.args.action[0]), 'r').read(),
                 globals(),
                 _ldict)
            for test_case in _ldict['test_cases']:
                test_case.run(common.args.action[0], common)

    def init_common(self):
        """Initialize the common class.

        This initialization performs argument parsing and login verification.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common(description='Build test objects.',
                                  positional_args_usage='action',
                                  object_type='table')

        common.args_add_positional_args()
        common.args_add_optional()
        common.args_add_connection_group()

        common.args_process()

        return common


execute_test_action()
