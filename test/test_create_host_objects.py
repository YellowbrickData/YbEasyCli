#!/usr/bin/env python3
"""Run tests and report results."""

import os, stat
import time
import sys
path = os.path.dirname(__file__)
if len(path) == 0:
    path = '.'
sys.path.append('%s/../' % path)

try:
    import configparser                  # for python3
except:
    import ConfigParser as configparser  # for python2

if hasattr(__builtins__, 'raw_input'):   # for python2
    input=raw_input

import getpass
from yb_util import util
import yb_common
from yb_common import text
from yb_common import db_connect


class create_objects:
    """Initiate testing"""

    def __init__(self):
        args_handler = self.init_args()
        args_handler.args.conn_db = 'yellowbrick'
        db_conn = yb_common.db_connect(args_handler)

        if not(db_conn.ybdb['has_create_user'] and db_conn.ybdb['has_create_db']):
            yb_common.common.error('You must login as a user with create database/'
                'user permission to create all the required test database objects...')

        configFilePath = '%s/%s' % (os.path.expanduser('~'), '.YbEasyCli')
        config = configparser.ConfigParser()
        config.read(configFilePath)

        section = '%s_%s' % ('test', db_conn.env['host'])
        if config.has_section(section):
            yb_common.common.error("A test environment has already been set up for '%s',"
                " first run test_drop_host_objects.py to clean up the old host objects..."
                    % db_conn.env['host'], color='yellow')

        print('\nThe util testing framework requires a test user and 2 test databases.\n'
            "The test user may be an existing user, if the user doesn't exist it will be created.\n"
            "If you continue the test user password will be stored in the ~/.YbEasyCli file.\n"
            "Additionally, 2 test databases will be created, these databases must not already exist.\n")

        config.add_section(section)

        while True:
            test_user = input("    Supply the DB test user name: ")
            if test_user != '':
                config.set(section, 'user', test_user)
                break

        while True:
            test_pwd = getpass.getpass("    Supply the password for '%s': "
                 % (yb_common.text.color(test_user, 'cyan')))
            if test_pwd != '':
                config.set(section, 'password', test_pwd)
                break

        cmd_results = db_conn.ybsql_query("""SELECT TRUE FROM sys.user WHERE name = '%s'""" % (test_user))
        if cmd_results.stdout.strip() == 't':
            # exits on failed connection
            self.get_db_conn(test_user, test_pwd, db_conn.env['conn_db'], db_conn.env['host'])
            #test_user_db_conn = self.get_db_conn(test_user, test_pwd
            #    , db_conn.env['conn_db'], db_conn.env['host'], on_fail_exit=False)
        else:
            cmd_results = db_conn.ybsql_query("""CREATE USER %s PASSWORD '%s'"""
                % (test_user, test_pwd))
            cmd_results.on_error_exit()
            print("\nCreated database user '%s'..." % yb_common.text.color(test_user, 'cyan'))

            self.test_user_login(test_user, test_pwd
                , db_conn.env['conn_db'], db_conn.env['host'])

        while True:
            test_db_prefix = input("\n    Supply the database prefix for the 2 test dbs: ")
            if test_db_prefix != '':
                test_db1 = '%s_db1' % test_db_prefix
                test_db2 = '%s_db2' % test_db_prefix
                config.set(section, 'db1', test_db1)
                config.set(section, 'db2', test_db2)
                break

        cmd_results = db_conn.ybsql_query(
            "CREATE DATABASE {db1} ENCODING=LATIN9; CREATE DATABASE {db2} ENCODING=UTF8;"
            " ALTER DATABASE {db1} OWNER TO {user};"
            " ALTER DATABASE {db2} OWNER TO {user};"
            " GRANT CONNECT ON DATABASE {db1} TO {user};"
            " GRANT CONNECT ON DATABASE {db2} TO {user};".format(
                db1 = test_db1, db2 = test_db2, user = test_user))
        cmd_results.on_error_exit()
        print("\nCreated '%s' as LATIN9 DB..." % yb_common.text.color(test_db1, 'cyan'))
        print("Created '%s' as UTF8 DB..." % yb_common.text.color(test_db2, 'cyan'))

        config_fp = open(configFilePath, 'w')
        config.write(config_fp)
        config_fp.close()
        os.chmod(configFilePath, stat.S_IREAD | stat.S_IWRITE)

        self.config = config
        self.host = db_conn.env['host']
        self.section = section

        self.create_db_objects()

    def get_db_conn(self, user=None, pwd=None, conn_db=None, host=None, on_fail_exit=True):
        env = db_connect.create_env(
            dbuser=user
            , pwd=pwd
            , conn_db=conn_db
            , host=host)
        return db_connect(env=env, on_fail_exit=on_fail_exit)

    def test_user_login(self, user, pwd, db, host):
        print("Testing '%s' DB login, this may take 2 minutes..."
            % yb_common.text.color(user, 'cyan'))
        for i in range(0,20):
            if i > 0:
                time.sleep(5)
                print("Attempting DB login after %d seconds..." % (i * 5))
            conn = self.get_db_conn(user, pwd, db, host, on_fail_exit=False)
            if conn.connected:
                break
        if conn.connected:
            print("DB login succeeded...")
        else:
            print("DB login failed...")
            conn.connect_cmd_results.on_error_exit()
            exit(1)


    def init_args(self):
        """Initialize the args class.

        This initialization performs argument parsing.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `args` class
        """
        cnfg = util.config_default.copy()
        cnfg['description'] = 'Create test user, database, and database objects.'
        cnfg['positional_args_usage'] = None

        args_handler = yb_common.args_handler(cnfg, init_default=False)

        args_handler.args_process_init()

        args_handler.args_add_optional()
        args_handler.args_add_connection_group()
        args_handler.args = args_handler.args_process()

        return args_handler


    def create_db_objects(self):

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

        ddl_test_raise_p = """CREATE PROCEDURE %s."test_Raise_p"(
    dummy_test_arg_1 BIGINT DEFAULT 1
    , dummy_test_arg_2 VARCHAR(1) DEFAULT 'xxxx'
)
    RETURNS INTEGER
    SET client_min_messages=NOTICE
    LANGUAGE 'plpgsql'
AS $CODE$
BEGIN
    RAISE INFO 'Test RAISE INFO, is always displayed regardless of client_min_message!';
    RAISE INFO '';
    --
    RAISE DEBUG 'Test RAISE DEBUG!';
    RAISE LOG 'Test RAISE LOG!';
    RAISE NOTICE 'Test RAISE NOTICE!';
    RAISE WARNING 'Test RAISE WARNING!';
    RAISE EXCEPTION 'Test RAISE EXCEPTION!';
    RETURN -1;
END;$CODE$
"""

        ddl_test_error_p = """CREATE OR REPLACE PROCEDURE %s.test_error_p(
    dummy_test_arg_1 BIGINT DEFAULT 1
)
    LANGUAGE 'plpgsql'
    AS $CODE$
DECLARE
    v_rec RECORD;
    v_state TEXT;
    v_msg TEXT;
    v_detail TEXT;
    v_hint TEXT;
    v_context TEXT;
BEGIN
    EXECUTE 'SELECT * FROM "Does Not Exist"' INTO v_rec;
EXCEPTION
    WHEN SQLSTATE '42P01' THEN
        GET STACKED DIAGNOSTICS
            v_state   = returned_sqlstate,
            v_msg     = message_text,
            v_detail  = pg_exception_detail,
            v_hint    = pg_exception_hint,
            v_context = pg_exception_context;
        RAISE INFO 'SQLERRM ====> %%', SQLERRM;
        RAISE INFO 'SQLSTATE ===> %%', SQLSTATE;
        RAISE INFO 'v_state ====> %%', v_state;
        RAISE INFO 'v_msg ======> %%', v_msg;
        RAISE INFO 'v_detail ===> %%', v_detail;
        RAISE INFO 'v_hint =====> %%', v_hint;
        RAISE INFO 'v_context ==> %%', v_context;
    WHEN OTHERS THEN
        NULL;
END;$CODE$
"""

        ddl_query_definer_p = """CREATE PROCEDURE %s.query_definer_p(
    BIGINT DEFAULT 1
    , NUMERIC(10,2) DEFAULT 1
)
    LANGUAGE 'plpgsql' 
    VOLATILE
    SECURITY DEFINER
AS $CODE$
DECLARE
    v_rec RECORD;
BEGIN
    FOR v_rec IN SELECT * FROM sys.query
    LOOP
        RAISE INFO '%%', v_rec;
    END LOOP;
END;$CODE$
"""

        ddl_get_data_types_p = """CREATE PROCEDURE %s.get_data_types_p()
 LANGUAGE plpgsql
AS $CODE$
DECLARE
 v_rec RECORD;
BEGIN
    SELECT * INTO v_rec FROM dev.data_types_t WHERE FALSE;
    RAISE INFO '%%', pg_typeof(v_rec.col1);
    RAISE INFO '%%', pg_typeof(v_rec.col2);
    RAISE INFO '%%', pg_typeof(v_rec.col3);
    RAISE INFO '%%', pg_typeof(v_rec.col4);
    RAISE INFO '%%', pg_typeof(v_rec.col5);
    RAISE INFO '%%', pg_typeof(v_rec.col6);
    RAISE INFO '%%', pg_typeof(v_rec.col7);
    RAISE INFO '%%', pg_typeof(v_rec.col8);
    RAISE INFO '%%', pg_typeof(v_rec.col9);
    RAISE INFO '%%, pg_typeof(v_rec.col10);
    RAISE INFO '%%', pg_typeof(v_rec.col11);
    RAISE INFO '%%', pg_typeof(v_rec.col12);
    RAISE INFO '%%', pg_typeof(v_rec.col13);
    RAISE INFO '%%', pg_typeof(v_rec.col14);
    RAISE INFO '%%', pg_typeof(v_rec.col15);
    RAISE INFO '%%', pg_typeof(v_rec.col16);
    RAISE INFO '%%', pg_typeof(v_rec.col17);
    RAISE INFO '%%', pg_typeof(v_rec.col18);
    RAISE INFO '%%', pg_typeof(v_rec.col19);
END;$CODE$
"""

        queries_create_objects_db1 = [
            'CREATE SCHEMA dev'
            , 'CREATE TABLE dev.a1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.b1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.c1_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE dev.dist_random_t (col1 INT) DISTRIBUTE RANDOM'
            , 'CREATE TABLE dev.dist_replicate_t (col1 INT) DISTRIBUTE REPLICATE'
            , ddl_dev_types_t % 'dev'
            , ddl_test_raise_p % 'dev'
            , ddl_test_error_p % 'dev'
            , ddl_query_definer_p % 'dev'
            , ddl_get_data_types_p % 'dev'
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
            , ddl_test_raise_p % '"Prod"'
            , ddl_test_error_p % '"Prod"'
            , ddl_query_definer_p % '"Prod"'
            , ddl_get_data_types_p % '"Prod"'
            , 'CREATE VIEW "Prod".a1_v AS SELECT * FROM "Prod".a1_t'
            , 'CREATE VIEW "Prod".b1_v AS SELECT * FROM "Prod".b1_t'
            , 'CREATE VIEW "Prod"."C1_v" AS SELECT * FROM "Prod"."C1_t"'
            , 'CREATE SEQUENCE "Prod".a1_seq START WITH 1000000'
            , 'CREATE SEQUENCE "Prod".b1_seq START WITH 1000000'
            , 'CREATE SEQUENCE "Prod"."C1_seq" START WITH 1000000'
            , ddl_dev_types_t__data % 'dev']

        queries_create_objects_db2 = queries_create_objects_db1.copy()

        queries_create_objects_db1.extend([
            # create broken views
            'CREATE TABLE dev.dropped_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE TABLE "Prod".dropped_t (col1 INT) DISTRIBUTE ON (col1)'
            , 'CREATE VIEW "Prod"."Dropped_v" AS SELECT * FROM "Prod".dropped_t'])

        queries_create_objects_db2.extend([
            # create broken views
            'CREATE VIEW dev.broken1_v AS SELECT * FROM %s."Prod".dropped_t' % self.config.get(self.section, 'db1')
            , 'CREATE VIEW dev.broken2_v AS SELECT * FROM %s."Prod"."Dropped_v"' % self.config.get(self.section, 'db1')
            , 'CREATE VIEW dev."Broken3_v" AS SELECT * FROM dev.broken1_v'
            , 'CREATE VIEW "Prod".broken1_v AS SELECT * FROM %s.dev.dropped_t' % self.config.get(self.section, 'db1')])

        queries_upfront_db1_drops = [
            'DROP VIEW "Prod"."Dropped_v"'
            , 'DROP TABLE dev.dropped_t'
            , 'DROP TABLE "Prod".dropped_t']

        db1_conn = self.get_db_conn(
            user=self.config.get(self.section, 'user')
            , pwd=self.config.get(self.section, 'password')
            , conn_db=self.config.get(self.section, 'db1')
            , host=self.host)

        db2_conn = self.get_db_conn(
            user=self.config.get(self.section, 'user')
            , pwd=self.config.get(self.section, 'password')
            , conn_db=self.config.get(self.section, 'db2')
            , host=self.host)

        print("\nCreating '%s' database objects..." % yb_common.text.color(self.config.get(self.section, 'db1'), 'cyan'))
        for query in queries_create_objects_db1:
            print(query)
            cmd_results = db1_conn.ybsql_query(query)

        print("\nCreating '%s' database objects..." % yb_common.text.color(self.config.get(self.section, 'db2'), 'cyan'))
        for query in queries_create_objects_db2:
            print(query)
            cmd_results = db2_conn.ybsql_query(query)

        print("\nDropping select '%s' database objects..." % yb_common.text.color(self.config.get(self.section, 'db1'), 'cyan'))
        for query in queries_upfront_db1_drops:
            print(query)
            cmd_results = db1_conn.ybsql_query(query)


create_objects()