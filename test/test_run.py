#!/usr/bin/env python3
"""Run tests and report results."""

import os
import sys
path = os.path.dirname(__file__)
if len(path) == 0:
    path = '.'
sys.path.append('%s/../' % path)

import time
import re
import shutil
import getpass
import yb_common


class test_case:
    """Contains structures for running tests and checking results."""
    def __init__(self, cmd, exit_code, stdout, stderr, comment='', map_out={}):
        self.cmd = cmd.format(**get.format)
        self.exit_code = exit_code
        self.stdout = stdout.format(**get.format)
        self.stderr = stderr.format(**get.format)
        self.comment = comment
        self.map_out = map_out

    def run(self, common, test):
        """Run the test.

        :param common: An instance of the `common` class
        :test the ordinal of the test in a list of test cases
        """
        cmd = '%s/../%s' % (path, self.cmd)
        if common.args.python_exe:
            cmd = '%s %s' % (common.args.python_exe, cmd)

        self.cmd_results = common.call_cmd(cmd)

        self.check()

        if common.args.test or common.args.print_test:
            run = '%s: %s' % (common.color('Test runs', style='bold')
                , cmd)
        else:
            run = ('%s: %s --test %d'
                % (common.color('To run', style='bold')
                    , ' '.join(sys.argv), test))

        print(
            '%s: %s, %s' % (
                common.color('Test %d' % test, style='bold')
                , common.color('Passed', fg='green')
                    if self.passed
                    else common.color('Failed', fg='red')
                , run))
        if common.args.print_output:
            sys.stdout.write(self.cmd_results.stdout)
            sys.stderr.write(self.cmd_results.stderr)
        if not self.passed and common.args.print_diff:
            self.print_test_comparison(common)

    def check(self):
        """Check test results.

        :return: True if the actual results match the expected results,
                 False otherwise.
        """
        map_out = {r'\x1b[^m]*m' : ''}
        map_out.update(self.map_out)
        for regex in map_out.keys():
            rec = re.compile(regex)
            self.cmd_results.stdout = rec.sub(map_out[regex], self.cmd_results.stdout)
            self.cmd_results.stderr = rec.sub(map_out[regex], self.cmd_results.stderr)
            self.stdout = rec.sub(map_out[regex], self.stdout)
            self.stderr = rec.sub(map_out[regex], self.stderr)

        self.passed = (
            self.exit_code == self.cmd_results.exit_code
            and self.stdout.strip() == self.cmd_results.stdout.strip()
            and self.stderr.strip() == self.cmd_results.stderr.strip())

    def print_test_comparison(self, common):
        """Print a comparison between actual and expected results."""
        print("%s: %d, %s: %d" % (
            common.color('Exit Code Expected', style='bold')
            , self.exit_code
            , common.color('Returned', style='bold')
            , self.cmd_results.exit_code))
        print("%s: %s%s%s\n%s: %s%s%s" % (
            common.color('STDOUT Expected', style='bold')
            , common.color('>!>', style='bold')
            , self.stdout.strip()
            , common.color('<!<', style='bold')
            , common.color('STDOUT Returned', style='bold')
            , common.color('>!>', style='bold')
            , self.cmd_results.stdout.strip()
            , common.color('<!<', style='bold')))
        print("%s: %s%s%s\n%s: %s%s%s" % (
            common.color('STDERR Expected', style='bold')
            , common.color('>!>', style='bold')
            , self.stderr.strip()
            , common.color('<!<', style='bold')
            , common.color('STDERR Returned', style='bold')
            , common.color('>!>', style='bold')
            , self.cmd_results.stderr.strip()
            , common.color('<!<', style='bold')))

class get:
    exec(open('%s/%s' % (path, 'settings.py'), 'r').read())
    format = {
        'host' : host
        , 'user_name' : test_user_name
        , 'user_password' : test_user_password
        , 'db1' : test_db1
        , 'db2' : test_db2
        , 'argsdir' : '%s/args_tmp' % (path)}

class execute_test_action:
    """Initiate testing"""
    def __init__(self):
        common = self.init_common()

        self.set_db_user(common)

        self.check_args_dir()

        queries_create_su = [
            "CREATE USER %s CREATEDB LOGIN PASSWORD '%s'" % (
                get.test_user_name
                , get.test_user_password)
            , "CREATE DATABASE %s OWNER %s" % (
                get.test_db1
                , get.test_user_name)
            , "GRANT CONNECT ON DATABASE %s TO %s" % (
                get.test_db1, get.test_user_name)
        ]

        queries_drop_su = [
            "DROP DATABASE %s" % get.test_db1
            , "DROP DATABASE %s" % get.test_db2
            , "DROP USER %s" % (get.test_user_name)
        ]

        queries_create_db2 = ["CREATE DATABASE %s" % get.test_db2]

        queries_drop_db2 = ["DROP DATABASE %s" % get.test_db2]

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
    , col18 BOOLEAN
    , col19 INTEGER
) DISTRIBUTE ON (col1)
"""

        ddl_dev_types_t__data = """INSERT INTO %s.data_types_t
WITH
digits AS (
    SELECT 0::BIGINT AS digit
    UNION ALL SELECT 1
    UNION ALL SELECT 2
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
    UNION ALL SELECT 6
    UNION ALL SELECT 7
    UNION ALL SELECT 8
    UNION ALL SELECT 9
)
, seq AS (
    SELECT
        d1.digit + 1
        + d10.digit * 10
        + d100.digit * 100
        + d1000.digit * 1000
        + d10000.digit * 10000
        + d100000.digit * 100000
        AS seq
        , seq::VARCHAR(32) AS seq_char
        , LENGTH(seq_char) AS seq_char_len
    FROM
        digits AS d1
        CROSS JOIN digits AS d10
        CROSS JOIN digits AS d100
        CROSS JOIN digits AS d1000
        CROSS JOIN digits AS d10000
        CROSS JOIN digits AS d100000
--    WHERE
--        seq <= 500
)
, cols AS (
    SELECT
        seq::BIGINT AS col1
        , ((SELECT MAX(seq) FROM seq) - seq + 1)::INTEGER AS col2
        , (seq / 100 + 1)::SMALLINT AS col3
        , (col1 * col2)::NUMERIC(18,0) AS col4
        , (col4 / (col3 * 1.0))::REAL AS col5
        , (col4 / (col3 * 1.0))::DOUBLE PRECISION AS col6
        , (SUBSTR('1234567890abcdef1234567890abcdef', 1, 32-seq_char_len) || seq_char)::UUID AS col7
        , SUBSTR(col4::VARCHAR(32), 1, 2) AS s1
        , SUBSTR(col4::VARCHAR(32), 3, 2) AS s2
        , SUBSTR(col4::VARCHAR(32), 5, 2) AS s3
        , SUBSTR(col4::VARCHAR(32), 7, 2) AS s4
        , DECODE(TRUE, s1 = '', 0, s1::INT > 90, 0, s1::INT) AS val1
        , DECODE(TRUE, s2 = '', 0, s2::INT > 90, 0, s2::INT) AS val2
        , DECODE(TRUE, s3 = '', 0, s3::INT > 90, 0, s3::INT) AS val3
        , DECODE(TRUE, s4 = '', 0, s4::INT > 90, 0, s4::INT) AS val4
        , SUBSTR((val1 + 100)::VARCHAR(3), 2, 2) AS str1
        , SUBSTR((val2 + 100)::VARCHAR(3), 2, 2) AS str2
        , SUBSTR((val3 + 100)::VARCHAR(3), 2, 2) AS str3
        , SUBSTR((val4 + 100)::VARCHAR(3), 2, 2) AS str4
        , CHR(33 + val1) || CHR(33 + val2) || CHR(33 + val3) || CHR(33 + val4) AS col8
        , CHR(34 + val1) AS col9
        , '2020/01/01'::DATE + (val1 * val2) AS col10
        , '01:01:01'::TIME + MAKE_INTERVAL(0,0,0,0,0,0,val2*val3) AS col11
        , '2020/01/01'::DATE::TIMESTAMP + MAKE_INTERVAL(0,0,0,0,0,0,val1*val2*val3*val4+1) AS col12
        , '2020/01/01'::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'America/New_York' + MAKE_INTERVAL(0,0,0,0,0,0,val1*val2*val3*val4+1) AS col13
        , (str1 || '.' || str2 || '.' || str3 || '.' || str4)::IPV4 AS col14
        , (str1 || ':' || str2 || ':' || str3 || ':' || str4
        || ':' || str1 || ':' || str2 || ':' || str3 || ':' || str4)::IPV6 AS col15
        , (str1 || ':' || str2 || ':' || str3 || ':' || str4
        || ':' || str1 || ':' || str2)::MACADDR AS col16
        , (str1 || ':' || str2 || ':' || str3 || ':' || str4
        || ':' || str1 || ':' || str2 || ':' || str3 || ':' || str4)::MACADDR8 AS col17
        , DECODE(col2 %% 2, 0, TRUE, FALSE) AS col18
        , TO_CHAR(col10, 'YYYYMMDD')::INTEGER AS col19
    FROM
        seq
--    WHERE FALSE
)
SELECT
    col1
    , col2
    , col3
    , col4
    , col5
    , col6
    , col7
    , col8
    , col9
    , col10
    , col11
    , col12
    , col13
    , col14
    , col15
    , col16
    , col17
    , col18
    , col19
FROM 
    cols
ORDER BY 1
"""

        queries_create_objects_db1 = [
            'CREATE SCHEMA dev'
            , 'CREATE TABLE dev.a1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.b1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.c1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.dist_random_t (col1 INT) DISTRIBUTE RANDOM'
            , 'CREATE TABLE dev.dist_replicate_t (col1 INT) DISTRIBUTE REPLICATE'
            , ddl_dev_types_t % 'dev'
            , 'CREATE VIEW dev.a1_v AS SELECT * FROM dev.a1_t'
            , 'CREATE VIEW dev.b1_v AS SELECT * FROM dev.b1_t'
            , 'CREATE VIEW dev.c1_v AS SELECT * FROM dev.c1_t'
            , 'CREATE SEQUENCE dev.a1_seq START WITH 1000000'
            , 'CREATE SEQUENCE dev.b1_seq START WITH 1000000'
            , 'CREATE SEQUENCE dev.c1_seq START WITH 1000000'
            , 'CREATE SCHEMA "Prod"'
            , 'CREATE TABLE "Prod".a1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE "Prod".b1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE "Prod"."C1_t" ("Col1" INT) DISTRIBUTE ON ("Col1")'
            , ddl_dev_types_t % '"Prod"'
            , 'CREATE VIEW "Prod".a1_v AS SELECT * FROM "Prod".a1_t'
            , 'CREATE VIEW "Prod".b1_v AS SELECT * FROM "Prod".b1_t'
            , 'CREATE VIEW "Prod"."C1_v" AS SELECT * FROM "Prod"."C1_t"'
            , 'CREATE SEQUENCE "Prod".a1_seq START WITH 1000000'
            , 'CREATE SEQUENCE "Prod".b1_seq START WITH 1000000'
            , 'CREATE SEQUENCE "Prod"."C1_seq" START WITH 1000000'
            , ddl_dev_types_t__data % 'dev']

        queries_drop_objects_db1 = [
            'DROP SEQUENCE "Prod"."C1_seq"', 'DROP SEQUENCE "Prod".b1_seq'
            , 'DROP SEQUENCE "Prod".a1_seq', 'DROP VIEW "Prod"."C1_v"'
            , 'DROP VIEW "Prod".b1_v', 'DROP VIEW "Prod".a1_v'
            , 'DROP TABLE "Prod".data_types_t', 'DROP TABLE "Prod"."C1_t"'
            , 'DROP TABLE "Prod".b1_t', 'DROP TABLE "Prod".a1_t', 'DROP SCHEMA "Prod"'
            , 'DROP SEQUENCE dev.c1_seq', 'DROP SEQUENCE dev.b1_seq'
            , 'DROP SEQUENCE dev.a1_seq', 'DROP VIEW dev.c1_v'
            , 'DROP VIEW dev.b1_v', 'DROP VIEW dev.a1_v'
            , 'DROP TABLE dev.data_types_t', 'DROP TABLE dev.dist_replicate_t'
            , 'DROP TABLE dev.dist_random_t', 'DROP TABLE dev.c1_t'
            , 'DROP TABLE dev.b1_t', 'DROP TABLE dev.a1_t', 'DROP SCHEMA dev']

        queries_create_objects_db2 = queries_create_objects_db1.copy()
        queries_drop_objects_db2 = queries_drop_objects_db1.copy()

        queries_create_objects_db1.extend([
            # create broken views
            'CREATE TABLE dev.dropped_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE "Prod".dropped_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE VIEW "Prod"."Dropped_v" AS SELECT * FROM "Prod".dropped_t'])

        queries_create_objects_db2.extend([
            # create broken views
            'CREATE VIEW dev.broken1_v AS SELECT * FROM %s."Prod".dropped_t' % get.test_db1
            , 'CREATE VIEW dev.broken2_v AS SELECT * FROM %s."Prod"."Dropped_v"' % get.test_db1
            , 'CREATE VIEW dev."Broken3_v" AS SELECT * FROM dev.broken1_v'
            , 'CREATE VIEW "Prod".broken1_v AS SELECT * FROM %s.dev.dropped_t' % get.test_db1])

        queries_drop_objects_db2.extend([
            'DROP VIEW "Prod".broken1_v'
            , 'DROP VIEW dev."Broken3_v"'
            , 'DROP VIEW dev.broken2_v'
            , 'DROP VIEW dev.broken1_v'])

        queries_upfront_db1_drops = [
            'DROP VIEW "Prod"."Dropped_v"'
            , 'DROP TABLE dev.dropped_t'
            , 'DROP TABLE "Prod".dropped_t']

        if common.args.action == 'create_su':
            for query in queries_create_su:
                cmd_results = common.ybsql_query(query)
            #check if the test user can login
            self.set_db_user(common
                , user = get.test_user_name)
            print("Testing '%s' DB login, this may take 2 minutes..."
                % get.test_user_name)
            for i in range(1,21):
                time.sleep(5)
                print("Attempting DB login after %d seconds..." % (1 * 5))
                cmd_results = common.ybsql_query("SELECT 1")
                if cmd_results.exit_code == 0:
                    break
            if cmd_results.exit_code == 0:
                print("DB login succeeded...")
            else:
                print("DB login failed...")

        if common.args.action == 'drop_su':
            for query in queries_drop_su:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'create_db2':
            for query in queries_create_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'drop_db2':
            for query in queries_drop_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'create_objects_db1':
            for query in queries_create_objects_db1:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'drop_objects_db1':
            for query in queries_drop_objects_db1:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'upfront_objects_drops_db1':
            for query in queries_upfront_db1_drops:
                cmd_results = common.ybsql_query(query)

        os.environ["YBDATABASE"] = get.test_db2

        if common.args.action == 'create_objects_db2':
            for query in queries_create_objects_db2:
                cmd_results = common.ybsql_query(query)

        if common.args.action == 'drop_objects_db2':
            for query in queries_drop_objects_db2:
                cmd_results = common.ybsql_query(query)

        # Actions beginning with `yb` refer to the yb utility scripts in this
        # package, e.g. `yb_get_table_name` or `yb_get_column_names`
        if common.args.action[0:2] == 'yb':
            # Test cases are defined in files within this directory
            #   (see files with prefix `test_cases__`)
            # We need to execute the relevant test case file and bring
            # the list of `test_case` objects into the local scope
            _ldict = locals()
            exec(open('%s/test_cases__%s.py'
                % (path, common.args.action), 'r').read()
                , globals()
                , _ldict)
            if common.args.test:
                _ldict['test_cases'][common.args.test-1].run(
                    common, common.args.test)
            else:
                # run test cases
                test = 1
                print(
                    '%s: %s, %s: %s'
                    % (
                        common.color('Testing', style='bold')
                        , common.args.action
                        , common.color('Running', style='bold')
                        , ' '.join(sys.argv)))
                for test_case in _ldict['test_cases']:
                    test_case.run(common, test=test)
                    test += 1

    def check_args_dir(self):
        """Check if the dynamic sd args directory has changed.
        If yes recreate the static dd args directory """
        sd = '%s/%s' % (path, 'args')     # source directory
        dd = '%s/%s' % (path, 'args_tmp') # destination directory

        sd_ts = []
        if os.path.isdir(sd):
            sd_files = os.listdir(sd)
            for filename in sd_files:
                sd_ts.append(os.path.getmtime('%s/%s' % (sd, filename)))
        else:
            sd_files = []

        dd_ts = []
        if os.path.isdir(dd):
            dd_files = os.listdir(dd)
            for filename in dd_files:
                dd_ts.append(os.path.getmtime('%s/%s' % (dd, filename)))

        if (True #TODO forcing rewrite of args_tmp directory on every call due to {argsdir} needing to be dynamic on every call
            or len(dd_ts) == 0
            or max(sd_ts) > min(dd_ts)
            or os.path.getmtime('%s/%s' % (path, 'settings.py')) > min(dd_ts)):
            shutil.rmtree(path=dd, ignore_errors=True)
            os.mkdir(path=dd)
            for filename in sd_files:
                with open('%s/%s' % (sd, filename), 'r') as file:
                    data = file.read().format(**get.format)
                    open('%s/%s' % (dd, filename), "w").write(data)

    def set_db_user(self, common, user=None):
        if (user == get.create_user_name
            or (not user
                    and ('create_su' in sys.argv 
                        or 'drop_su' in sys.argv))):
            os.environ["YBUSER"] = get.create_user_name
            os.environ["YBPASSWORD"] = getpass.getpass(
                "Enter the password for user %s: "
                    % get.create_user_name)
        elif (user == get.test_user_name
            or not user):
            os.environ["YBUSER"] = get.test_user_name
            os.environ["YBPASSWORD"] = get.test_user_password
            os.environ["YBDATABASE"] = get.test_db1

        os.environ["YBHOST"] = get.host
        common.args.current_schema = None
        common.args.host = get.host

    def init_common(self):
        """Initialize the common class.

        This initialization performs argument parsing.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `common` class
        """
        common = yb_common.common()

        common.args_process_init(
            description='Run unit test actions.'
            , positional_args_usage='action')

        common.args_add_positional_args()
        common.args_add_optional()
        
        args_test_optional_grp = common.args_parser.add_argument_group(
            'Test optional arguments')
        args_test_optional_grp.add_argument("--test"
            , type=int, default=None
            , help="unit test number to execute")
        args_test_optional_grp.add_argument("--print_test", "--pt"
            , action="store_true"
            , help="instead of the test command display what the test ran")
        args_test_optional_grp.add_argument("--print_output", "--po"
            , action="store_true"
            , help="print the test output")
        args_test_optional_grp.add_argument("--print_diff", "--pd"
            , action="store_true"
            , help="if the test fails, print the diff of the expected "
            "verse actual result")
        args_test_optional_grp.add_argument("--python_exe"
            , default=None
            , help="python executable to run tests with, this allows testing "
                "with different python versions, defaults to 'python3'")

        common.args_process(has_conn_args=False)

        if common.args.python_exe:
            if os.access(common.args.python_exe, os.X_OK):
                cmd_results = common.call_cmd('%s --version'
                    % common.args.python_exe)
                self.test_py_version = (
                    int(cmd_results.stderr.split(' ')[1].split('.')[0]))
            else:
                sys.stderr.write("'%s' is not found or not executable..."
                    % common.args.python_exe)
                exit(2)
        else:
            self.test_py_version = 3

        return common


execute_test_action()
