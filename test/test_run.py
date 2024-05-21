#!/usr/bin/env python3
"""Run tests and report results."""

import os
import sys
import glob
path = os.path.dirname(sys.argv[0])
if len(path) == 0:
    path = '.'
sys.path.append('%s/../bin/' % path)

try:
    import configparser                  # for python3
except:
    import ConfigParser as configparser  # for python2

import time
import string
import re
import shutil
import getpass
import difflib
from yb_common import ArgsHandler, Cmd, Common, DBConnect, Text, Util

class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

class test_case:
    """Contains structures for running tests and checking results."""
    def __init__(self, cmd, exit_code, stdout, stderr, comment='', map_out=[]):
        self.cmd = cmd.format(**get.format)
        # fix output_template args in test cases where they are double brackets 
        # TODO fix this so that double brackets aren't required
        self.cmd = self.cmd.replace('{{', '{').replace('}}', '}')
        
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
        cmd = '%s/../bin/%s' % (path, self.cmd)
        if args.python_exe:
            cmd = '%s %s' % (args.python_exe, cmd)

        section = 'test_%s' % args.host

        os.environ.pop('YBPASSWORD', None)

        if Common.is_windows:
            # .py files can be associated with all kinds of applications
            # (like MS VS Code or py.exe wrapper or something completely different).
            # We, however, want our command-line python.exe, so it's much better
            # to explicitly reuse the same python interpreter which is executing this script:
            cmd = sys.executable + ' ' + cmd
            # in Windows the file argument @file_name needs to be placed in single quotes
            cmd = re.sub(r'(\s)(\@[^\s]*)', r"\1'\2'", cmd)

        self.cmd_results = Cmd(cmd)

        self.check()

        if args.case or args.print_test:
            run = '%s: %s' % (Text.color('Test runs', style='bold')
                , cmd)
        else:
            if '--all' in sys.argv:
                running = ('%s --test_name %s'
                    % (' '.join(sys.argv).replace(' --all', ''), test_name))
            else:
                running = ' '.join(sys.argv)
            if Common.is_windows:
                # in Windows the py script requires to be run with an explicit 'python' command
                running = 'python %s' % running                
            run = ('%s: %s --case %d'
                % (Text.color('To run', style='bold')
                    , running, case))

        print(
            '%s: %s, %s' % (
                Text.color('Test case %d' % case, style='bold')
                , Text.color('Passed', fg='green')
                    if self.passed
                    else Text.color('Failed', fg='red')
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
        map_out = [ { 'regex' : re.compile(r'\x1b[^m]*m'), 'sub' : ''} ]

        map_out.extend(self.map_out)
        for mo in map_out:
            regex = mo['regex']
            self.cmd_results.stdout = regex.sub(mo['sub'], self.cmd_results.stdout)
            self.cmd_results.stderr = regex.sub(mo['sub'], self.cmd_results.stderr)
            self.stdout = regex.sub(mo['sub'], self.stdout)
            self.stderr = regex.sub(mo['sub'], self.stderr)

        self.stdout = self.stdout.strip()
        self.stderr = self.stderr.strip()
        self.cmd_results.stdout = self.cmd_results.stdout.strip()
        self.cmd_results.stderr = self.cmd_results.stderr.strip()

        if Common.is_windows or Common.is_cygwin:
            self.cmd_results.stdout = self.cmd_results.stdout.replace('\r', '')
            self.cmd_results.stderr = self.cmd_results.stderr.replace('\r', '')

        self.passed = (
            self.exit_code == self.cmd_results.exit_code
            and self.stdout == self.cmd_results.stdout
            and self.stderr == self.cmd_results.stderr)

    def print_test_std_comparison(self, std, std1, std2):
        if std1 != std2:
            d = difflib.Differ()

            #good_stdout = std1.splitlines(keepends=True)
            #bad_stdout = std2.splitlines(keepends=True)
            good_stdout = std1.splitlines(True)
            bad_stdout = std2.splitlines(True)
            diff = list(d.compare(bad_stdout, good_stdout))
            for i in range(0,len(diff)):
                if diff[i][0] in ('-', '+', '?'):
                    color = {'-':'red', '+':'green', '?':'yellow'}[diff[i][0]]
                    if diff[i][0] == '?':
                        diff[i] = Text.color(diff[i], fg=color, style='bold')
                    else:
                        diff[i] = Text.color(diff[i], fg=color)
            print('\n------------------\n%s %s\n------------------' % (
                Text.color(std, style='bold')
                , Text.color('differences', fg='red')))
            sys.stdout.writelines(diff)

    def print_test_comparison(self):
        """Print a comparison between actual and expected results."""
        if self.exit_code != self.cmd_results.exit_code:
            print("%s: %s, %s: %s" % (
                Text.color('Exit Code Expected', style='bold')
                , Text.color(str(self.exit_code), fg='green')
                , Text.color('Returned', style='bold')
                , Text.color(str(self.cmd_results.exit_code), fg='red')))

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
        # print(get.format) --debug, will return a dictionary

        db_conn = self.get_db_conn(get.format)
        self.ybdb_version_major = db_conn.ybdb['version_major']

        self.check_args_dir()

        for test_case_file in self.test_case_files:
            self.load_test_cases(test_case_file)

    def load_test_cases(self, test_case_file):
        # Test cases are defined in files within this directory
        #   (see files with prefix `test_cases__`)
        # We need to exec the relevant test case file and bring
        # the list of `test_case` objects into the local scope
        _ldict = locals()

        matches = re.search(r'test_cases__(.*)\.py', test_case_file, re.DOTALL)
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
            if Common.is_windows:
                # in Windows the py script requires to be run with an explicit 'python' command
                running = 'python %s' % running
            print(
                '%s: %s, %s: %s'
                % (
                    Text.color('Testing', style='bold')
                    , test_name
                    , Text.color('Running', style='bold')
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
            #os.mkdir(path=dd)
            os.mkdir(dd)
            for filename in sd_files:
                with open('%s/%s' % (sd, filename), 'r') as file:
                    data = file.read().format(**get.format)
                    open('%s/%s' % (dd, filename), "w").write(data)

    def get_db_conn(self, conf_dict):
        env = DBConnect.create_env(
            dbuser=conf_dict['user_name']
            , pwd=conf_dict['user_password']
            , conn_db=conf_dict['db1']
            , host=conf_dict['host'])
        return DBConnect(env=env)

    def init_args(self):
        """Initialize the args class.

        This initialization performs argument parsing.
        It also provides access to functions such as logging and command
        execution.

        :return: An instance of the `args` class
        """
        cnfg = Util.config_default.copy()
        cnfg['description'] = 'Run unit test cases on utility.'
        cnfg['positional_args_usage'] = None

        args_handler = ArgsHandler(cnfg, init_default=False)

        args_handler.args_process_init()

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
                cmd_results = Cmd('%s --version'
                    % args.python_exe)
                #some python versions return the version in stdout some in stderr
                self.test_py_version = (
                    int((cmd_results.stdout if cmd_results.stdout else cmd_results.stderr)
                        .split(' ')[1].split('.')[0]))
            else:
                Common.error("'%s' is not found or not executable..."
                    % args.python_exe)
        else:
            self.test_py_version = sys.version_info[0]

        if not args.host and os.environ.get("YBHOST"):
            args.host = os.environ.get("YBHOST")
        elif not args.host and len(self.config.hosts) == 1:
            args.host = self.config.hosts[0]

        if (args.host and not (args.host in self.config.hosts)):
            Common.error("the '%s' host is not configured for testing,"
                " run 'test_create_host_objects.py' to create host db objects for testing"
                    % args.host, color='white')
        elif len(self.config.hosts) == 0:
            Common.error("currently there are no hosts configures for testing,"
                " run 'test_create_host_objects.py' to create host db objects for testing"
                    , color='white')
        elif len(self.config.hosts) > 1 and not args.host:
            Common.error("currently there is more than 1 host(%s) configures for testing,"
                " use the --host option or YBHOST environment variable to select a host"
                    % self.config.hosts, color='white')

        if bool(args.name) == args.all: # exclusive or
            Common.error("either the option --test_name or --all must be specified not both")

        self.test_case_files = []
        if args.name:
            test_case_file_path = '%s/test_cases__%s.py' % (path, args.name)
            if os.access(test_case_file_path, os.R_OK):
                self.test_case_files.append(test_case_file_path)
            else:
                Common.error("test case '%s' has no test case file '%s'..."
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