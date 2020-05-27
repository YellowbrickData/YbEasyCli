#!/usr/bin/env python3
"""Performs functions such as argument parsing, login verification, logging,
and command execution that are common to all utilities in this package.
"""

import argparse
import getpass
import os
import platform
import re
import subprocess
import sys
import traceback
from datetime import datetime


class common:
    """This class contains functions used for argument parsing, login
    verification, logging, and command execution.
    """

    def __init__(self, description, object_type, positional_args_usage=None):
        self.positional_args_usage = positional_args_usage
        self.object_type = object_type

        self.version = 20190617
        self.nl = '\n'    # newline character
        self.gs = '\x1D'  # group seperator character

        self.start_ts = datetime.now()

        self.util_dir_path = os.path.dirname(os.path.realpath(__file__))
        self.util_file_name = os.path.basename(os.path.realpath(__file__))
        self.util_name = self.util_file_name.split('.')[0]

        self.text_color = {
            'black': 0,
            'red': 1,
            'green': 2,
            'yellow': 3,
            'blue': 4,
            'purple': 5,
            'cyan': 6,
            'white': 7
        }

        self.text_style = {
            'no_effect': 0,
            'bold': 1,
            'underline': 2,
            'italic': 3,
            'negative2': 5
        }

        self.database = None
        self.schema = None

        self.args_process_init(description, positional_args_usage)

    def args_process_init(self, description, positional_args_usage):
        """Create an ArgumentParser object.

        :param description: Text to display before the argument help
        :param positional_args_usage: Description of how positional arguments
                                      are used
        """
        # Pass in a custom formatter to trim help messages to max width of 100
        formatter = lambda prog: argparse.HelpFormatter(prog, width=100)
        self.args_parser = argparse.ArgumentParser(
            description=description,
            usage="%%(prog)s %s[options]" %
            (self.positional_args_usage +
             ' ' if self.positional_args_usage else ''),
            add_help=False,
            formatter_class=formatter)

    def args_add_positional_args(self):
        """Add positional arguments to the class's ArgumentParser object."""
        if self.positional_args_usage:
            for arg in self.positional_args_usage.split(' '):
                # Optional arguments are surrounded by square brackets
                trimmed_arg = arg.lstrip('[').rstrip(']')
                is_optional = len(trimmed_arg) != len(arg)

                self.args_parser.add_argument(
                    trimmed_arg,
                    nargs=('?' if is_optional else 1),
                    help="%s%s to process" %
                    (('optional ' if is_optional else ''), trimmed_arg))

    def args_add_connection_group(self):
        """Add conceptual grouping to improve the display of help messages.

        Creates a new group for arguments related to connection.
        """
        conn_grp = self.args_parser.add_argument_group('connection arguments')
        conn_grp.add_argument(
            "--host",
            "-h",
            "-H",
            dest="host",
            help=
            "specify database server hostname, overrides YBHOST env variable")
        conn_grp.add_argument(
            "--port",
            "-p",
            "-P",
            dest="port",
            help=(
                "specify database server port, overrides YBPORT env variable, "
                "the default port is 5432"))
        conn_grp.add_argument(
            "--dbuser",
            "-U",
            dest="dbuser",
            help="specify database user, overrides YBUSER env variable")
        conn_grp.add_argument(
            "--conn_db",
            "-d",
            "-db",
            "-D",
            dest="conn_db",
            help=
            "specify database to connect to, overrides YBDATABASE env variable"
        )
        conn_grp.add_argument("--conn_schema",
                              dest="conn_schema",
                              help="specify schema name to connect to")
        conn_grp.add_argument(
            "-W",
            action="store_true",
            help=
            "prompt for password instead of using the YBPASSWORD env variable")

    def args_add_filter_group(self, keep_args=['ALL'], remove_args=[]):
        """Add conceptual grouping to improve the display of help messages.

        Creates a new group for arguments related to filtering.

        :param keep_args: A list of arguments to keep, represented by
                          space-separated strings (Default value = ['ALL'])
        :param remove_args: A list of arguments to remove, represented by
                            space-separated strings (Default value = [])
        """
        filter_grp = self.args_parser.add_argument_group(
            '%s filter arguments' % self.object_type)

        if (('ALL' in keep_args or '--owner' in keep_args)
                and '--owner' not in remove_args):
            filter_grp.add_argument("--owner",
                                    dest="owner",
                                    help="filter by owner of %s/s" %
                                    self.object_type)
        if (('ALL' in keep_args or '--schema' in keep_args)
                and '--schema' not in remove_args):
            filter_grp.add_argument("--schema",
                                    dest="schemas",
                                    nargs="?",
                                    metavar="SCHEMA",
                                    help="schema to process")
        if ('ALL' in keep_args or '--schemas'
                in keep_args) and '--schemas' not in remove_args:
            filter_grp.add_argument(
                "--schemas",
                dest="schemas",
                nargs="+",
                action='append',
                help=("list of schemas to process, ALL to process every "
                      "schema, adds a schema name prefix to all %s/s" %
                      self.object_type))
        if ('ALL' in keep_args
                or '--in' in keep_args) and '--in' not in remove_args:
            filter_grp.add_argument(
                "--in",
                dest="in_list",
                nargs="+",
                action='append',
                metavar="%s_NAME" % self.object_type.upper(),
                help=("filter for %s/s in the list, use '\"Name\"' notation "
                      "for case dependent names" % self.object_type))
            filter_grp.add_argument(
                "--NOTin",
                dest="not_in_list",
                nargs="+",
                action='append',
                metavar="%s_NAME" % self.object_type.upper(),
                help=("filter for %s/s NOT in the list, use '\"Name\"' "
                      "notation for case dependent names" % self.object_type))
            filter_grp.add_argument(
                "--like",
                dest="like_pattern",
                nargs="+",
                action='append',
                metavar="PATTERN",
                help="filter for %s/s like the pattern/s" % self.object_type)
            filter_grp.add_argument(
                "--NOTlike",
                dest="not_like_pattern",
                nargs="+",
                action='append',
                metavar="PATTERN",
                help=("filter for %s/s NOT like the pattern/s" %
                      self.object_type))

    def args_add_optional(self):
        """Add conceptual grouping  to improve the display of help messages.

        Creates a new group for optional arguments.
        """
        self.args_parser.add_argument(
            "--help",
            "-u",
            "-?",
            "--usage",
            action="help",
            help="display this help message and exit")
        self.args_parser.add_argument(
            "--verbose",
            type=int,
            default=0,
            help="display verbose execution{1 - info, 2 - debug, 3 - extended}",
            choices=range(1, 4))
        self.args_parser.add_argument(
            "--nocolor",
            action="store_true",
            help="turn off colored text output")
        self.args_parser.add_argument(
            "--version",
            "-v",
            action="store_true",
            help="display the program version and exit")

    def args_build_filter(self):
        """Build the filter clause for the query."""
        if isinstance(self.args.schemas, str):
            self.args.schemas = [[self.args.schemas]]

        # Flatten the following args if they exist
        if self.args.in_list:
            self.args.in_list = sorted(set(sum(self.args.in_list, [])))

        if self.args.not_in_list:
            self.args.not_in_list = sorted(set(sum(self.args.not_in_list, [])))

        if self.args.like_pattern:
            self.args.like_pattern = sorted(set(sum(self.args.like_pattern,
                                                    [])))

        if self.args.not_like_pattern:
            self.args.not_like_pattern = sorted(set(sum(
                self.args.not_like_pattern, [])))

        if self.args.schemas:
            self.args.schemas = sorted(set(sum(self.args.schemas, [])))

        or_objects = []
        and_objects = []
        if self.args.in_list:
            objects = []
            for name in self.args.in_list:
                if name[0] == '"':
                    objects.append(name.replace('"', "'"))
                else:
                    objects.append("'%s'" % name.lower())
            or_objects.append(
                '<object_column_name> IN (%s)' % ', '.join(objects))

        if self.args.like_pattern:
            for pattern in self.args.like_pattern:
                or_objects.append(
                    "LOWER(<object_column_name>) LIKE LOWER('%s')" % pattern)

        if len(or_objects) > 0:
            and_objects.append('(%s)' % ' OR '.join(or_objects))

        if self.args.not_in_list:
            objects = []
            for name in self.args.not_in_list:
                if name[0] == '"':
                    objects.append(name.replace('"', "'"))
                else:
                    objects.append("'%s'" % name.lower())
            and_objects.append(
                '<object_column_name> NOT IN (%s)' % ', '.join(objects))

        if self.args.not_like_pattern:
            for pattern in self.args.not_like_pattern:
                and_objects.append(
                    "LOWER(<object_column_name>) NOT LIKE LOWER('%s')\n" %
                    pattern)

        if self.args.schemas:
            if 'ALL' in self.args.schemas:
                and_objects.append("<schema_column_name> LIKE '%'")
            else:
                objects = []
                for name in self.args.schemas:
                    objects.append("'%s'" % name)
                and_objects.append(
                    '<schema_column_name> IN (%s)' % ', '.join(objects))
        else:
            and_objects.append('<schema_column_name> = CURRENT_SCHEMA')

        if self.args.owner:
            and_objects.append("<owner_column_name> = '%s'" % self.args.owner)

        return '\n    AND '.join(and_objects)

    def args_process(self):
        """Process arguments.

        Convert argument strings to objects and assign to the class. Then
        update the OS environment variables related to ybsql to match what was
        passed to this script. Finally, attempt to verify login credentials
        based on those variables.
        """
        self.args = self.args_parser.parse_args()
        if self.args.version:
            print("yb command line utils %s" % self.version)
            exit(0)

        # Check if 'filter arguments' is defined
        if any("filter arguments" in group.title
               for group in self.args_parser._action_groups):
            self.filter_clause = self.args_build_filter()

        # Get and set operating system environment variables related to ybsql
        if self.args.host:
            os.environ["YBHOST"] = self.args.host
        else:
            if os.environ.get("YBHOST") is None:
                raise TypeError("No host specified")
            else:
                self.args.host = os.environ.get("YBHOST")
        if self.args.port:
            os.environ["YBPORT"] = self.args.port
        if os.environ.get("YBPORT") is None:
            os.environ["YBPORT"] = '5432'  # default port
        if self.args.dbuser:
            os.environ["YBUSER"] = self.args.dbuser
        if self.args.conn_db:
            os.environ["YBDATABASE"] = self.args.conn_db

        self.login_verify()

    def login_verify(self):
        """Attempt to verify login credentials.

        Exits the program with a code if login fails.
        """
        if self.args.verbose >= 1:
            print(
                'Connecting to Host: %s, Port: %s, DB User: %s, Database: %s, '
                'Schema: %s'
                % (self.color(os.environ.get("YBHOST"), fg='cyan'),
                   self.color(os.environ.get("YBPORT"), fg='cyan'),
                   self.color(os.environ.get("YBUSER"), fg='cyan'),
                   self.color('<user default>' if os.environ.get("YBDATABASE")
                              is None else os.environ.get("YBDATABASE"),
                              fg='cyan'),
                   self.color('<user default>' if self.args.conn_schema is None
                              else self.args.conn_schema,
                              fg='cyan')))

        if os.environ.get("YBUSER") is not None:
            if self.args.W or os.environ.get("YBPASSWORD") is None:
                os.environ["YBPASSWORD"] = getpass.getpass(
                    "Enter the password for user %s: " %
                    self.color(os.environ.get("YBUSER"), fg='cyan'))
        # We are missing YBUSER
        # Set an invalid password to skip the ybsql password prompt
        else:
            os.environ["YBPASSWORD"] = '-*-force bad password-*-'

        cmd_results = self.ybsql_query(
            "SELECT CURRENT_DATABASE() || '.' || CURRENT_SCHEMA")
        if cmd_results.exit_code == 0:
            self.schema = cmd_results.stdout.split('.')[1].strip()
            # if --schema arg was set check if the schema is valid
            if len(self.schema) == 0:
                err = self.color('util: FATAL: schema "%s" does not exist\n' %
                                 self.args.conn_schema,
                                 fg='red')
                sys.stderr.write(err)
                exit(2)
            if hasattr(self.args, 'database') and self.args.database:
                self.database = self.args.database
            else:
                self.database = cmd_results.stdout.split('.')[0].strip()
        else:
            sys.stderr.write(
                self.color(cmd_results.stderr.replace('ybsql', 'util'),
                           fg='red'))
            exit(cmd_results.exit_code)

    def call_cmd(self, cmd_str, stack_level=2):
        """Spawn a new process to execute the given command.

        Example: results = call_cmd('env | grep -i path')

        :param cmd_str: The string representing the command to execute
        :param stack_level: A number signifying the limit of stack trace
                            entries (Default value = 2)
        :return: The result produced by running the given command
        """
        if hasattr(self, 'args') and self.args.verbose >= 2:
            trace_line = traceback.extract_stack(None, stack_level)[0]
            self.log('In file: %s, Function: %s, Line: %s%sExecuting: %s'
                     % (self.color(trace_line[0], 'cyan'),
                        self.color(trace_line[2], 'cyan'),
                        self.color(trace_line[1], 'cyan'),
                        self.gs, cmd_str))

        start_time = datetime.now()
        p = subprocess.Popen(cmd_str,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
        (stdout, stderr) = map(bytes.decode, p.communicate())
        end_time = datetime.now()
        results = argparse.Namespace(exit_code=p.returncode,
                                     stdout=stdout,
                                     stderr=stderr)

        if hasattr(self, 'args') and self.args.verbose >= 2:
            self.log(
                'Execution duration: %s%sExit code: %s%sStdout: %s%sStderr: %s'
                % (self.color(end_time - start_time, fg='cyan'),
                   self.gs,
                   self.color(str(results.exit_code),
                              fg=('red' if results.exit_code else 'cyan')),
                   self.gs,
                   results.stdout.rstrip(),
                   self.gs,
                   self.color(results.stderr.rstrip(), fg='red')))

        return results

    def call_util_cmd(self, util_cmd, stack_level=2):
        """A wrapper for `call_cmd` used when running in verbose mode

        When verbose is turned on, the output of `call_cmd` will contain more
        info than exepected causing an execution error. To mitigate this issue,
        this wrapper calls `call_cmd` twice when verbose mode is turned on.
          - First time it produces the debug output
          - Second time it strips the verbose setting and returns the desired
            output without debug info

        :param util_cmd: The string representing the command to execute
        :param stack_level: A number signifying the limit of stack trace
                            entries (Default value = 2)
        :return: The result produced by running the given command
        """
        cmd_results = self.call_cmd(util_cmd)
        if self.args.verbose:
            util_cmd_wo_verbose = re.sub(r'(.*)--verbose [0-9](.*)', r'\1\2',
                                         util_cmd)
            cmd_results = self.call_cmd(util_cmd_wo_verbose, stack_level)

        return cmd_results

    def ybsql_query(self, sql_statement):
        """Run and evaluate a query using ybsql.

        :param sql_statement: The SQL command string
        :return: The result produced by running the given command
        """
        if self.args.conn_schema:
            sql_statement = "SET SCHEMA '%s';%s" % (self.args.conn_schema,
                                                    sql_statement)

        return self.call_cmd("""ybsql -t -A -q -X <<eof
%s
eof""" % sql_statement,
                             stack_level=3)

    def color_str(self, fg='white', bg='black', style='no_effect'):
        """Return a formatted string.

        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string formatted with color and style
        """
        return '\033[%d;%d;%dm' % (self.text_style[style.lower()],
                                   30 + self.text_color[fg.lower()],
                                   40 + self.text_color[bg.lower()])

    def color(self, text, fg='white', bg='black', style='no_effect'):
        """Style a given string with color.

        :param text: The input string
        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string with added color
        """
        colored_text = '%s%s%s' % (self.color_str(fg, bg, style),
                                   text,
                                   self.color_str())
        return text if self.args.nocolor else colored_text

    def log(self, str):
        """Log text with color and formatting

        For a more readable log entry, format the input string by using
         '\x1D' as a group separator
         '\n' as a line separator

        :param str: The string to log
        """
        line_1_prefix = '=====>'
        line_x_prefix = '----->'
        default_color = '' if self.args.nocolor else self.color_str()
        print(self.color(self.ts(), fg='green'))

        for str_group in str.split(self.gs):
            current_color = ''
            str_lines = str_group.split(self.nl)

            for line in range(len(str_lines)):
                print('%s%s%s%s' %
                      (default_color,
                       line_x_prefix if line else line_1_prefix,
                       current_color,
                       str_lines[line]))
                last_color_loc = str_lines[line].rfind('\033')
                if last_color_loc != -1:
                    current_color = str_lines[line][last_color_loc:(
                        last_color_loc + len(self.color_str()))]

        sys.stdout.flush()

    def ts(self):
        """Get the current time (for time stamping)"""
        return str(datetime.now())


# Standalone tests
# Example: yb_common.py -h YB14 -U denav -D denav --verbose 3
if __name__ == "__main__":
    common = common('Common Standalone Debug Run', 'object')
    common.args_add_positional_args()
    common.args_add_optional()
    common.args_add_connection_group()
    common.args_add_filter_group()
    common.args_process()

    if common.args.verbose >= 3:
        # Print extended information on the environment running this program
        print('--->%s\n%s' % ("(common.call_cmd('lscpu')).stdout",
                              common.call_cmd('lscpu').stdout))
        print('--->%s\n%s' % ("platform.platform()", platform.platform()))
        print('--->%s\n%s' % ("platform.python_implementation()",
                              platform.python_implementation()))
        print('--->%s\n%s' % ("sys.version", sys.version))
        print('--->%s\n%s' % ("common.args", common.args))
        print('--->%s\n%s' % ("common.filter_clause", common.filter_clause))
        print('--->%s\n%s' % ("common.util_dir_path", common.util_dir_path))
        print('--->%s\n%s' % ("common.util_file_name", common.util_file_name))
        print('--->%s\n%s' % ("common.util_name", common.util_name))
        print('--->%s\n%s' % ("common.version", common.version))
