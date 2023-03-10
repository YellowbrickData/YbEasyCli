#!/usr/bin/env python3
import os, sys
import argparse
import gzip
import getpass
import time
import re
import readline
import cmd
import glob
from datetime import datetime
from datetime import timedelta
from tabulate import tabulate

default_path = '/home/guest/scratch/cloud-commander/output'

class Text:
    colors = {
        'black': 0
        , 'red': 1
        , 'green': 2
        , 'yellow': 3
        , 'blue': 4
        , 'purple': 5
        , 'cyan': 6
        , 'white': 7
    }

    styles = {
        'no_effect': 0
        , 'bold': 1
        , 'underline': 2
        , 'italic': 3
        , 'negative2': 5
    }

    nocolor = False

    @staticmethod
    def color_str(fg='white', bg='black', style='no_effect'):
        """Return a formatted string.

        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string formatted with color and style
        """
        #return '\033[%d;%d;%dm' % (
        #    Text.styles[style.lower()]
        #    , 30 + Text.colors[fg.lower()]
        #    , 40 + Text.colors[bg.lower()])
        return '\033[%d;%dm' % (
            Text.styles[style.lower()]
            , 30 + Text.colors[fg.lower()]);

    @staticmethod
    def color(txt, fg='white', bg='black', style='no_effect'):
        """Style a given string with color.

        :param txt: The text input string
        :param fg: Foreground color string (Default value = 'white')
        :param bg: Background color string (Default value = 'black')
        :param style: Text style string (Default value = 'no_effect')
        :return: A string with added color
        """
        colored_text = '%s%s%s' % (
            Text.color_str(fg, bg, style), txt, Text.color_str())
        return txt if Text.nocolor else colored_text


class tabCompleter(object):

    def pathCompleter(self,text,state):
        return [x for x in tabCompleter.dirs_in_list(text)][state]

    @staticmethod
    def dirs_in_list(text):
        dirs = []
        for dir in glob.glob(text+'*'):
            if os.path.isdir(dir):
                dirs.append(dir)
        if len(dirs) == 1:
            dirs[0] = dirs[0] + '/'
        return dirs

class GetClusterDirectory():

    def __init__(self, dir = '/'):
        if os.path.isdir(dir):
            self.dir = dir + ('' if dir[-1] == '/' else '/')
            if GetClusterDirectory.isClusterDir(dir):
                return
        else:
            print("'%s' is not a valid directory..." % dir)
            exit(1)

        t = tabCompleter()
        readline.set_completer_delims('\t')
        readline.parse_and_bind("tab: complete")
        readline.set_completer(t.pathCompleter)

        os.chdir(self.dir)
        while True:
            if sys.version_info.major == 2:
                dir = raw_input('%s %s' % (Text.color('Cluster:', style='bold'), self.dir))
            else:
                dir = input('%s %s' % (Text.color('Cluster:', style='bold'), self.dir))
            dir = self.dir + dir

            if os.path.isdir(dir):
                self.dir = dir + ('' if dir[-1] == '/' else '/')
                if GetClusterDirectory.isClusterDir(dir):
                    return

            print("'%s' is not a cluster directory..." % dir)

    @staticmethod
    def isClusterDir(dir):
        found = False
        os.chdir(dir)
        sub_dirs = list(filter(os.path.isdir, os.listdir(dir)))
        if len(sub_dirs):
            found = True
            for sub_dir in sub_dirs:
                if not re.match(r'^\d{4}-\d{2}$', sub_dir):
                    found = False
        return found


class check_lime_status:

    def __init__(self):

        self.init_constants()

        self.args_process('Summarizes the lime-status.log/s of the specified cluster and returns state changes')

        print("Summarizing: %s%s YBOS: %s" % ( \
            Text.color(self.files[0], fg='cyan') \
            , (' Created at: %s' % (Text.color(time.ctime(os.path.getmtime(self.files[0])), fg='cyan')) if len(self.files) == 1 else '') \
            , self.get_version_for_lime_status(self.files[0])))
        if len(self.files) > 1:
            print("through: %s%s YBOS: %s" % ( \
                Text.color(self.files[-1], fg='cyan') \
                , ' Created at: %s' % (Text.color(time.ctime(os.path.getmtime(self.files[-1])), fg='cyan'))
                , self.get_version_for_lime_status(self.files[-1])))
        print

        self.parse_lime_status_file()


    def parse_lime_status_file(self):
        keys = self.get_all_keys()
        f = read_array_of_files(self.files)

        entry = ''
        previous_entry = ''
        line = str(f.readline())
        last_entry_state = 'healthy'
        last_entry_ts = None
        first_unhealthy_ts = None
        entry_table = []
        unknown_keys = []
        while line:
            next_line = str(f.readline())
            entry = entry + line
            if next_line[0:4].isdigit() or not next_line:
                if not self.args.unmask:
                    entry = self.mask_entry(entry)
                current_entry = '\n'.join(entry.split('\n')[2:])
                #current_entry = self.mask_entry(current_entry)
                if current_entry != previous_entry or not next_line:
                    (entry_state, entry_ts, entry_row) = self.parse_lime_status_entry(entry, keys)
                    is_entry_healthy = entry_state == 'healthy'
                    if not is_entry_healthy and first_unhealthy_ts is None:
                        first_unhealthy_ts = entry_ts
                    if last_entry_ts is not None:
                        color = fg=('green' if last_entry_state == 'healthy' else ('yellow' if last_entry_state == 'warning' else 'red'))
                        entry_table[-1][1] = Text.color(str(entry_ts - last_entry_ts), fg=color)
                        entry_table[-1][2] = Text.color(str(entry_ts - (last_entry_ts if last_entry_state == 'healthy' else first_unhealthy_ts)), fg=color)
                    if is_entry_healthy:
                        first_unhealthy_ts = None
                    last_entry_ts = entry_ts
                    last_entry_state = entry_state
                    if next_line:
                        entry_table.append(entry_row)
                entry = ''
                previous_entry = current_entry

            line = next_line

        f.close()


        # remove table columns that contain a constant value in all rows
        # print those values out first pivoted
        key_value = []
        key_constant = []
        for key in keys:
            key_value.append(None)
            key_constant.append(False if key in self.no_name_value else True)

        for entry in entry_table:
            for i in range(0, len(keys)):
                if key_value[i] is None and entry[i+3].strip() != '':
                    key_value[i] = entry[i+3]
                if key_constant[i] and entry[i+3].strip() != '' and key_value[i] != entry[i+3]:
                    key_constant[i] = False

        constants = []
        for i in range(len(keys)-1, -1, -1):
            if key_constant[i]:
                for entry in entry_table:
                    del entry[i+3]
                constants.insert(0, [keys[i] + ':', key_value[i]])
                del keys[i]

        print('\n'.join(tabulate(constants, tablefmt="simple").split('\n')[1:-1]) + '\n')


        #print entry_ts #last time needs to use this instead of local time for last record
        #TODO should build last delta time based on last entry_ts to take into account for different timezones

        headers = ['Timestamp %s' % Text.color(self.entry_tz, fg='cyan'), 'Duration', 'Total\nState\nDuration']
        for key in keys:
            if key in self.token_def_keys:
                headers.append(self.token_defs[self.token_def_keys.index(key)]['header'])
            else:
                headers.append(key)

        if self.args.columns > 0 and self.args.columns < len(headers):
            headers = headers [0:self.args.columns]
            for i in range(0, len(entry_table)):
                entry_table[i] = entry_table[i][0:self.args.columns]

        print(tabulate(entry_table, headers=headers, tablefmt="simple"))
        last_update_delta = (datetime.now() - datetime.fromtimestamp(read_array_of_files.current_file_ts))

        age_color = 'green'
        if timedelta(minutes=1) < last_update_delta and last_update_delta <= timedelta(minutes=5):
            age_color = 'yellow'
        if last_update_delta > timedelta(minutes=5):
            age_color = 'red'

        print('%s  %s %s %s'
            % (last_entry_ts
                , Text.color('timestamp of last log entry summarized, which is', fg='cyan')
                , Text.color(str(last_update_delta).split('.')[0], fg=(age_color) )
                , Text.color('old', fg='cyan') ) )


    def mask_entry(self, entry):
        # swap 'LDAP' timestamp with constant string, this timestamp limits the summarization
        new_entry = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}[-+]\d{4}", Text.color("<... time>", fg='yellow'), entry, 0, re.MULTILINE)
        # swap 'Parity Rebuilder' MiB counter with constant string, this counter limits the summarization
        new_entry = re.sub(r"\d+\s+MiB\s+\/", Text.color("<... Mib> ", fg='yellow') + '/', new_entry, 0, re.MULTILINE)
        # swap 'Shard Rewriter' MiB counter with constant string, this counter limits the summarization
        new_entry = re.sub(r"\d+\s+shards remaining", Text.color("<... shards remaining>", fg='yellow'), new_entry, 0, re.MULTILINE)
        new_entry = re.sub(r"[1-9]\d*\s+shards pending", Text.color("<... shards pending>", fg='yellow'), new_entry, 0, re.MULTILINE)
        # swap 'Metadata Backlog' with constant string, this counter limits the summarization
        new_entry = re.sub(r"\d+ \{[^\}]*\}", Text.color("<... metadata>", fg='yellow'), new_entry, 0, re.MULTILINE)
        return new_entry

    def get_all_keys(self):
        f = read_array_of_files(self.files)
        line = str(f.readline())
        keys = []

        while line:
            key = line.split(':')[0].strip()
            if key not in keys and not key[0:4].isdigit() and key != 'Services':
                keys.append(key)
            line = str(f.readline())

        f.close()
        return keys


    def parse_lime_status_entry(self, entry, keys):
        entry_row = [''] * (len(keys) + 3)
        lines = entry.strip().split('\n')
        line_count = 0
        entry_state = 'healthy'
        for line in lines:
            #print line
            token = line.split(':')
            key = token[0].strip()
            if line_count == 0:
                entry_ts = datetime.strptime(line[0:19], '%Y-%m-%dT%H:%M:%S')
                entry_row[0] = entry_ts
                self.entry_tz = line[23:].split(' ', 1)[0]
                main_status = (line.split('|')[2]).replace(' lime-status ', '').strip()
                if main_status != 'command output':
                    entry_row[keys.index('Offline reason') + 3] = Text.color(main_status, fg='red')
                    entry_state = 'bad'
            elif key in keys:
                value = ':'.join(token[1:]).strip()
                color = None
                if key in self.health_keys:
                    if value in self.token_defs[self.token_def_keys.index(key)]['healthy']:
                        color = 'green'
                    elif value in self.token_defs[self.token_def_keys.index(key)]['warning']:
                        entry_state = entry_state if entry_state == 'bad' else 'warning'
                        color = 'yellow'
                    else:
                        entry_state = 'bad'
                        color = 'red'
                entry_row[keys.index(key) + 3] = value if color is None else Text.color(value, fg=color)

            line_count += 1

        #return (is_entry_healthy, entry_ts, entry_row)
        return (entry_state, entry_ts, entry_row)


    def args_process(self, description):
        formatter = lambda prog: argparse.HelpFormatter(prog,width=100)
        self.args_parser = argparse.ArgumentParser(
            description = description
            , usage = "%(prog)s [options]"
            , add_help = False
            , formatter_class = formatter
        )

        self.args_add_required()
        self.args_add_optional()

        self.args = self.args_parser.parse_args()

        if self.args.version:
            print('%s %s' % (self.util_name, self.version))
            exit(0)

        self.files = []
        if self.args.file is None:
            if self.args.cluster_dir[-1] != '/':
                self.args.cluster_dir += '/'
            self.cluster_dir = GetClusterDirectory(self.args.cluster_dir).dir
            if not os.access(self.cluster_dir, os.R_OK):
                sys.stdout.write("The cluster directory '%s' is not readable...\n" % (self.cluster_dir))
                exit(1)

            if not self.args.date:
                #looking for file with tomorrows timestamp as the latest file maybe from a future timezone
                file_date = datetime.now() + timedelta(days=1)
                if not os.access('%s%s' % (self.cluster_dir, file_date.strftime("%Y-%m/%d")), os.R_OK):
                    file_date = datetime.now()
            else:
                file_date = datetime.strptime(self.args.date, '%Y%m%d')
            if self.args.days:
                file_date = (file_date - timedelta(days=(int(self.args.days) - 1)))

            for ct in range(int(self.args.days)):
                date_dir = file_date.strftime("%Y-%m/%d")
                self.files.append('%s%s/lime-status.log' % (self.cluster_dir, date_dir))
                file_date = file_date + timedelta(days=1)
        else:
            self.files.append(self.args.file)

        file_count = 0
        while file_count < len(self.files):
            if not os.access(self.files[file_count], os.R_OK):
                if not os.access(self.files[file_count] + '.gz', os.R_OK):
                    sys.stdout.write("The file '%s' is not readable...\n" % self.files[file_count])
                    exit(1)
                else:
                    self.files[file_count] += '.gz'
            file_count += 1

    def get_version_for_lime_status(self, lime_status_file):
        version_file = lime_status_file.replace('lime-status', '*version*') + '*'
        version_file = glob.glob(version_file)
        if len(version_file):
            version_file = version_file[0]
            version_type = version_file.split('/')[-1].split('.')[0]
            file_type = version_file.split('/')[-1].split('.')[-1]
        if len(version_file) and os.access(version_file, os.R_OK):
            if file_type == 'gz':
                f = gzip.open(version_file, 'r')
                data = f.read().decode()
            else:
                f = open(version_file, 'r')
                data = f.read()
            f.close()
            if version_type == 'version': # reading version.log file
                version = data
            elif version_type == 'sw_component_versions': # reading sw_component_versions.log file
                version = re.search(
                    r".*ybd:.*?version:\s*([^\s]*)"
                    , str(data)
                    , re.DOTALL).group(1)
            else: # parsing w_component_versions.log file
                version = re.search(
                    r".*installer_pkg.*?version:\s*([^\s]*)"
                    , str(data)
                    , re.DOTALL).group(1)
            version = Text.color(version.strip(), fg='cyan')
        else:
            version = Text.color('unknown', fg='yellow')

        return version


    def args_add_optional(self):
        optional_grp = self.args_parser.add_argument_group('optional arguments')

        optional_grp.add_argument("--help", "-h", "-?", "--usage", action="help", help="display this help message and exit")
        optional_grp.add_argument("--nocolor", action="store_true", help="turn off colored text output")
        optional_grp.add_argument("--version", "-v", action="store_true", help="display the program version and exit")
        optional_grp.add_argument("--date", "-d", dest="date", help="the date of the log file to summarize, an 8 digit number in the form of YYYYMMDD")
        optional_grp.add_argument("--for_x_days", "-x", dest="days", default="1", help="the number of days/logs to summarize")
        optional_grp.add_argument("--unmask", "-u", action="store_true", help="By deafult LDAP, Parity Rebuilder, Metadata Backlog, and Shard Rewrite are masked to improve summarization of the log, use this option to unmask the values")
        optional_grp.add_argument("--first_n_columns", "-n", type=int, dest="columns", default="0", help="display the the first n columns only")


    def args_add_required(self):
        required_grp = self.args_parser.add_argument_group('required arguments option 1')
        required_grp.add_argument("--cluster_dir", "-c", dest="cluster_dir", help="parent directory where cluster dated log directeries are located, defaults to '%s'.  If dated directories aren't located in the requested directory use interface to traverse to the desired directory." % default_path, default=default_path)

        required_grp = self.args_parser.add_argument_group('required arguments option 2')
        required_grp.add_argument("--file", "-f", dest="file", help="lime status log file to summarize")


    def init_constants(self):
        self.version = 20211213

        self.util_dir_path = os.path.dirname(os.path.realpath(__file__))
        self.util_file_name = os.path.basename(os.path.realpath(__file__))
        self.util_name = self.util_file_name.split('.')[0]

        self.token_defs = [
            {'key' : 'Ready'                        , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Ready'}
            , {'key' : 'Online'                     , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Online'}
            , {'key' : 'Offline reason'             , 'healthy' : ['NOT_OFFLINE']     , 'warning' : []                                 , 'header' : 'Offline\nreason'}
            , {'key' : 'Readonly'                   , 'healthy' : ['false']           , 'warning' : ['true']                           , 'header' : 'Read\nonly'}
            , {'key' : 'Serving'                    , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Serv\ning'}
            , {'key' : 'Shutting down'              , 'healthy' : ['false']           , 'warning' : []                                 , 'header' : 'Shutt\ning\ndown'}
            , {'key' : 'FSCKing'                    , 'healthy' : ['false']           , 'warning' : []                                 , 'header' : 'FSCK\ning'}
            , {'key' : 'Parity Rebuilder'           , 'healthy' : ['Not Running']     , 'warning' : ['Running']                        , 'header' : 'Parity\nRebuilder'}
            , {'key' : 'Parity Rebuilder Enabled'   , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Parity\nRebuilder\nEnabled'}
            , {'key' : 'Parity Rebuilder Fast Path' , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Parity\nRebuilder\nFast\nPath'}
            , {'key' : 'Cluster Auto Reconfigure'   , 'healthy' : ['true']            , 'warning' : []                                 , 'header' : 'Cluster\nAuto\nRecon\nfigure'}
            , {'key' : 'Cluster Degraded'           , 'healthy' : ['NOT_DEGRADED']    , 'warning' : ['MISSING_WORKERS','MISSING_DRIVES','VIRTUAL_FILES'], 'header' : 'Cluster\nDegraded'}
            , {'key' : 'Missing Workers'                                                                                             , 'header' : 'Missing\nWorkers'}
            , {'key' : 'Shard Rewriter'             , 'healthy' : ['0 shards pending', 'Not Running', 'Check is currently disabled'], 'warning' : ['<... shards pending>']           , 'header' : 'Shard Rewriter'}
            , {'key' : 'Quiesced'                   , 'healthy' : ['false']           , 'warning' : ['true']                           , 'header' : 'Quiesced'}
        ]

        self.no_name_value = ['Missing Workers', 'Workers Rebuilding']

        self.token_def_keys = []
        self.health_keys = []
        for token_def in self.token_defs:
            self.token_def_keys.append(token_def['key'])
            if 'healthy' in token_def:
                self.health_keys.append(token_def['key'])


class read_array_of_files:

    current_file_ts = None

    def __init__(self, files):
        self.current_file = 0
        self.files = files
        self.f = self.open(self.current_file)


    def open(self, file_ordinal):
        self.is_gzip = self.files[file_ordinal].split(".")[-1] == 'gz'
        read_array_of_files.current_file_ts = os.path.getmtime(self.files[file_ordinal])
        return gzip.open(self.files[file_ordinal], mode='rt') \
            if self.is_gzip \
            else open(self.files[file_ordinal], 'r')


    def readline(self):
        line = self.f.readline()
        if not line:
            self.current_file += 1
            if self.current_file < len(self.files):
                self.f.close()
                self.f = self.open(self.current_file)
                line = self.f.readline()
        return line


    def close(self):
        return self.f.close()


#standalone tests
if __name__ == "__main__":
    check_lime_status()
