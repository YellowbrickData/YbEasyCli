#!/usr/bin/env python3
"""Run tests and report results."""

import os, stat
import sys
import time
import re
import shutil
import getpass
import difflib

path = os.path.dirname(__file__)
if len(path) == 0:
    path = '.'
sys.path.append('%s/../bin' % path)

try:
    import configparser                  # for python3
except:
    import ConfigParser as configparser  # for python2

if hasattr(__builtins__, 'raw_input'):   # for python2
    input=raw_input

from yb_common import ArgsHandler, Common, DBConnect, Text, Util

class drop_objects:
    """Initiate testing"""
    def __init__(self):
        args_handler = self.init_args()
        args_handler.args.conn_db = 'yellowbrick'
        db_conn = DBConnect(args_handler)

        if not(db_conn.ybdb['has_create_user'] and db_conn.ybdb['has_create_db']):
            Common.error('You must login as a user with create database/'
                'user permission to drop the test database/user objects...')

        configFilePath = '%s/%s' % (os.path.expanduser('~'), '.YbEasyCli')
        config = configparser.ConfigParser()
        config.read(configFilePath)
        section = '%s_%s' % ('test', db_conn.env['host'])

        if config.has_section(section):
            test_user = config.get(section, 'user')
            print("\nDropping the '%s' test environment, including the '%s' and '%s' databases.\n"
                % (Text.color(db_conn.env['host'], 'cyan')
                    , Text.color(config.get(section, 'db1'), 'cyan')
                    , Text.color(config.get(section, 'db2'), 'cyan')))

            answered_yes = input("    Enter(yes) to continue: ").lower() == 'yes'
            if answered_yes:
                cmd_results = db_conn.ybsql_query(
                    "DROP DATABASE IF EXISTS {db1}; DROP DATABASE IF EXISTS {db2};".format(
                        db1 = config.get(section, 'db1'), db2 = config.get(section, 'db2')))
                cmd_results.write()
                if cmd_results.stderr != '':
                    exit(cmd_results.exit_code)
                print("\nDropped databases '%s' and '%s', if they existed..."
                    % (Text.color(config.get(section, 'db1'), 'cyan')
                        , Text.color(config.get(section, 'db2'), 'cyan')))

                config.remove_section(section)
                config_fp = open(configFilePath, 'w')
                config.write(config_fp)
                config_fp.close()
                os.chmod(configFilePath, stat.S_IREAD | stat.S_IWRITE)

                answered_yes = input("\n    Enter(yes) to drop user '%s': "
                    % Text.color(test_user, 'cyan')).lower() == 'yes'
                if answered_yes:
                    cmd_results = db_conn.ybsql_query( 
                        "DROP USER IF EXISTS %s" % test_user)
                    cmd_results.write()
                    if cmd_results.stderr != '':
                        exit(cmd_results.exit_code)
                    print("\nDropped user '%s', if existed..."
                        % (Text.color(test_user, 'cyan')))
            else:
                print(Text.color('\nExiting without clean up...', 'yellow'))
        else:
            Common.error("There is no test environment setup for '%s' in your"
                " '~/.YbEasyCli file', run 'test_create_host_objects.py'"
                " to set up this host" % db_conn.env['host'], color='yellow')

    def get_db_conn(self, user=None, pwd=None, conn_db=None, host=None):
        env = DBConnect.create_env(
            dbuser=user
            , pwd=pwd
            , conn_db=conn_db
            , host=host)
        return DBConnect(env=env)

    def init_args(self):
        """Initialize the args class.

        This initialization performs argument parsing.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `args` class
        """
        cnfg = Util.config_default.copy()
        cnfg['description'] = 'Drop test user, database, and database objects.'
        cnfg['positional_args_usage'] = None

        args_handler = ArgsHandler(cnfg, init_default=False)

        args_handler.args_process_init()

        args_handler.args_add_optional()
        args_handler.args_add_connection_group()
        args_handler.args = args_handler.args_process()

        return args_handler

drop_objects()
