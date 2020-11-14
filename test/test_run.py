#!/usr/bin/env python3
"""Run tests and report results."""

import os
import sys
import glob
path = os.path.dirname(sys.argv[0])
if len(path) == 0:
    path = '.'
sys.path.append('%s/../' % path)

try:
    import configparser                  # for python3
except:
    import ConfigParser as configparser  # for python2

import time
import re
import shutil
import getpass
import yb_common
import difflib
from yb_common import text
from yb_common import db_connect

class test_case:
    """Contains structures for running tests and checking results."""
    def __init__(self, cmd, exit_code, stdout, stderr, comment='', map_out={}):
        self.cmd = cmd.format(**get.format)
        self.exit_code = exit_code
        self.stdout = stdout.format(**get.format)
        self.stderr = stderr.format(**get.format)
        self.comment = comment
        self.map_out = map_out

    def run(self, args, config, case, test_name):
        """Run the test case.

        :param args: An instance of the `args` class
        :case the ordinal of the test case in a list of test cases
        """
        cmd = '%s/../%s' % (path, self.cmd)
        if args.python_exe:
            cmd = '%s %s' % (args.python_exe, cmd)

        section = 'test_%s' % args.host
        os.environ['YBPASSWORD'] = config.get(section, 'password')

        self.cmd_results = yb_common.common.call_cmd(cmd)

        self.check()

        if args.case or args.print_test:
            run = '%s: %s' % (text.color('Test runs', style='bold')
                , cmd)
        else:
            if '--all' in sys.argv:
                running = ('%s --test_name %s'
                    % (' '.join(sys.argv).replace(' --all', ''), test_name))
            else:
                running = ' '.join(sys.argv)
            run = ('%s: %s --case %d'
                % (text.color('To run', style='bold')
                    , running, case))

        print(
            '%s: %s, %s' % (
                text.color('Test case %d' % case, style='bold')
                , text.color('Passed', fg='green')
                    if self.passed
                    else text.color('Failed', fg='red')
                , run))
        if args.print_output:
            sys.stdout.write(self.cmd_results.stdout)
            sys.stderr.write(self.cmd_results.stderr)
        if not self.passed and args.print_diff:
            self.print_test_comparison()

    def check(self):
        """Check test results.

        set self.passed to True if the actual results match the
        expected results, False otherwise.
        """
        #start by mapping out all text colors/styles
        map_out = {r'\x1b[^m]*m' : ''}

        map_out.update(self.map_out)
        for regex in map_out.keys():
            rec = re.compile(regex)
            self.cmd_results.stdout = rec.sub(map_out[regex], self.cmd_results.stdout)
            self.cmd_results.stderr = rec.sub(map_out[regex], self.cmd_results.stderr)
            self.stdout = rec.sub(map_out[regex], self.stdout)
            self.stderr = rec.sub(map_out[regex], self.stderr)

        self.stdout = self.stdout.strip()
        self.stderr = self.stderr.strip()
        self.cmd_results.stdout = self.cmd_results.stdout.strip()
        self.cmd_results.stderr = self.cmd_results.stderr.strip()

        self.passed = (
            self.exit_code == self.cmd_results.exit_code
            and self.stdout == self.cmd_results.stdout
            and self.stderr == self.cmd_results.stderr)

    def print_test_std_comparison(self, std, std1, std2):
        if std1 != std2:
            d = difflib.Differ()

            good_stdout = std1.splitlines(keepends=True)
            bad_stdout = std2.splitlines(keepends=True)
            diff = list(d.compare(bad_stdout, good_stdout))
            for i in range(0,len(diff)):
                if diff[i][0] in ('-', '+', '?'):
                    color = {'-':'red', '+':'green', '?':'yellow'}[diff[i][0]]
                    if diff[i][0] == '?':
                        diff[i] = text.color(diff[i], fg=color, style='bold')
                    else:
                        diff[i] = text.color(diff[i], fg=color)
            print('\n------------------\n%s %s\n------------------' % (
                text.color(std, style='bold')
                , text.color('differences', fg='red')))
            sys.stdout.writelines(diff)

    def print_test_comparison(self):
        """Print a comparison between actual and expected results."""
        if self.exit_code != self.cmd_results.exit_code:
            print("%s: %s, %s: %s" % (
                text.color('Exit Code Expected', style='bold')
                , text.color(str(self.exit_code), fg='green')
                , text.color('Returned', style='bold')
                , text.color(str(self.cmd_results.exit_code), fg='red')))

        self.print_test_std_comparison(
            'STDOUT', self.stdout, self.cmd_results.stdout)

        self.print_test_std_comparison(
            'STDERR', self.stderr, self.cmd_results.stderr)

class get:
    def __init__(self, args, config):
        section = 'test_%s' % args.host
        get.format = {
            'host' : args.host
            , 'user_name' : config.get(section, 'user')
            , 'user_password' : config.get(section, 'password')
            , 'db1' : config.get(section, 'db1')
            , 'db2' : config.get(section, 'db2')
            , 'argsdir' : '%s/args_tmp' % (path)}

class execute_test_action:
    """Initiate testing"""
    def __init__(self):
        self.init_config()
        args = self.init_args()
        get(args, self.config)

        self.check_args_dir()

        for test_case_file in self.test_case_files:
            self.load_test_cases(test_case_file)

    def load_test_cases(self, test_case_file):
        # Test cases are defined in files within this directory
        #   (see files with prefix `test_cases__`)
        # We need to exec the relevant test case file and bring
        # the list of `test_case` objects into the local scope
        _ldict = locals()

        matches = re.search('test_cases__(.*)\.py', test_case_file, re.DOTALL)
        test_name = matches.group(1)

        exec(open(test_case_file, 'r').read()
            , globals()
            , _ldict)
        if self.args.case:
            _ldict['test_cases'][self.args.case-1].run(
                self.args, self.config, self.args.case, test_name)
        else:
            # run test cases
            if '--all' in sys.argv:
                running = ('%s --test_name %s'
                    % (' '.join(sys.argv).replace(' --all', ''), test_name))
            else:
                running = ' '.join(sys.argv)
            print(
                '%s: %s, %s: %s'
                % (
                    text.color('Testing', style='bold')
                    , test_name
                    , text.color('Running', style='bold')
                    , running))
            case = 1
            for test_case in _ldict['test_cases']:
                test_case.run(self.args, self.config, case, test_name)
                case += 1

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
            or max(sd_ts) > min(dd_ts)):
            shutil.rmtree(path=dd, ignore_errors=True)
            os.mkdir(path=dd)
            for filename in sd_files:
                with open('%s/%s' % (sd, filename), 'r') as file:
                    data = file.read().format(**get.format)
                    open('%s/%s' % (dd, filename), "w").write(data)

    def get_db_conn(self, user=None, pwd=None, db_conn=None):
        env = db_connect.create_env(
            dbuser=user
            , pwd=pwd
            , conn_db=db_conn
            , host=get.host)
        return db_connect(env=env)

    def init_args(self):
        """Initialize the args class.

        This initialization performs argument parsing.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `args` class
        """
        args_handler = yb_common.args_handler()

        args_handler.args_process_init(
            description='Run unit test cases on utility.'
            , positional_args_usage=[])

        args_handler.args_add_positional_args()
        args_handler.args_add_optional()

        args_test_required_grp = args_handler.args_parser.add_argument_group(
            'test required arguments')
        args_test_required_grp.add_argument("--test_name", "--tn", "-t"
            , dest="name"
            , help="the test case name to run, like 'yb_get_table_name'"
                " for the test case file 'test_cases__yb_get_table_name.py'")
        args_test_required_grp.add_argument("--all"
            , action="store_true"
            , help="run test cases for all the test case files")

        args_test_optional_grp = args_handler.args_parser.add_argument_group(
            'test optional arguments')
        args_test_optional_grp.add_argument(
            "--host", "-h", "-H"
            , dest="host", help="database server hostname,"
                " overrides YBHOST env variable, the host where the tests are run")
        args_test_optional_grp.add_argument("--case"
            , type=int, default=None
            , help="unit test case number to execute")
        args_test_optional_grp.add_argument("--print_test", "--pt"
            , action="store_true"
            , help="instead of the test command display what the test ran")
        args_test_optional_grp.add_argument("--print_output", "--po"
            , action="store_true"
            , help="print the test output")
        args_test_optional_grp.add_argument("--print_diff", "--pd"
            , action="store_true"
            , help="if the test fails, print the diff of the expected"
                " verse actual result")
        args_test_optional_grp.add_argument("--python_exe"
            , default=None
            , help="python executable to run tests with, this allows testing"
                " with different python versions, defaults to 'python3'")

        args = args_handler.args_process()

        if args.python_exe:
            if os.access(args.python_exe, os.X_OK):
                cmd_results = yb_common.common.call_cmd('%s --version'
                    % args.python_exe)
                self.test_py_version = (
                    int(cmd_results.stderr.split(' ')[1].split('.')[0]))
            else:
                yb_common.common.error("'%s' is not found or not executable..."
                    % args.python_exe)
        else:
            self.test_py_version = 3

        if not args.host and os.environ.get("YBHOST"):
            args.host = os.environ.get("YBHOST")
        elif not args.host and len(self.config.hosts) == 1:
            args.host = self.config.hosts[0]

        if (args.host and not (args.host in self.config.hosts)):
            yb_common.common.error("the '%s' host is not configured for testing,"
                " run 'test_create_host_objects.py' to create host db objects for testing"
                    % args.host, color='white')
        elif len(self.config.hosts) == 0:
            yb_common.common.error("currently there are no hosts configures for testing,"
                " run 'test_create_host_objects.py' to create host db objects for testing"
                    , color='white')
        elif len(self.config.hosts) > 1 and not args.host:
            yb_common.common.error("currently there is more than 1 host(%s) configures for testing,"
                " use the --host option or YBHOST environment variable to select a host"
                    % self.config.hosts, color='white')

        if bool(args.name) == args.all: # exclusive or
            yb_common.common.error("either the option --test_name or --all must be specified not both")

        self.test_case_files = []
        if args.name:
            test_case_file_path = '%s/test_cases__%s.py' % (path, args.name)
            if os.access(test_case_file_path, os.R_OK):
                self.test_case_files.append(test_case_file_path)
            else:
                yb_common.common.error("test case '%s' has no test case file '%s'..."
                    % (args.name, test_case_file_path))
        else:
            for test_case_file_path in glob.glob("%s/test_cases__*.py" % path):
                if os.access(test_case_file_path, os.R_OK):
                    self.test_case_files.append(test_case_file_path)
        self.test_case_files.sort()

        self.args = args
        return args


    def init_config(self):
        configFilePath = '%s/%s' % (os.path.expanduser('~'), '.YbEasyCli')

        config = configparser.ConfigParser()
        config.read(configFilePath)
        config.hosts = []
        for section in config.sections():
            if section[0:5] == 'test_':
                config.hosts.append(section[5:])
        self.config = config


execute_test_action()