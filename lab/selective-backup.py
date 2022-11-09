# (c) 2022 Yellowbrick Data Corporation.
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.

import re, os, sys, six, subprocess, uuid, shutil
import sqlite3
from argparse import ArgumentParser

def message(text, level='INFO', exitcode=0):
	colormap = {'INFO': '\033[92m', 'WARN': '\033[93m', 'ERROR': '\033[91m',}
	print('[{}{:6}\033[0m] {}'.format(colormap[level] if level in colormap else '\033[96m', level, text))
	if exitcode: exit(exitcode)

def meta(op, remove_backup=False):
	for filename in metadata:
		fqfn = os.path.join(bundle, filename)
		if os.path.isfile(fqfn):
			(shutil.move if remove_backup else shutil.copyfile)(fqfn + (backup_tail if op == 'restore' else ''), fqfn + (backup_tail if op == 'backup' else ''))

def get_backup_details(snapshot_name):
	insert = 'insert into "public"."{}" values'
	patterns = {
		'backup_info'    : {'regex': '\((\d+)(?:,\s*\S+){{4}},\s*\'{}\''.format(snapshot_name),},
		'backup_location': {'columns': 'id int, backup_id int, location text',
							'regex': '\((\d+),\s*(\d+),\s*\'([^\']+)',},
		'backup_file'    : {'columns': 'id int, location_id int, filename text',
							'regex': '\((\d+),\s*(\d+),\s*\'(\S+)\',\s*\'catalog\',',},
		'object_info'    : {'columns': 'id int, backup_id int, objtype text, objname text, dbsize int, backupsize int, rowcount int',
							'regex': '\((\d+),\s*(\d+),\s*\'(\w+)\',\s*\'([^\']+)\',\s*\'[^\']+\',\s*(\d+),\s*(\d+),\s*(\d+)',},
	}
	backup = {'id': None, 'catalog': None, 'details': [],}
	db = sqlite3.connect(':memory:')
	for table,properties in patterns.items():
		if 'columns' in properties:
			db.execute('create table {} ({})'.format(table, properties['columns']))
	with open(os.path.join(bundle, metadata[0])) as f:
		for line in f:
			# NOTE: there's some room for file read optimization here ;)
			for table,val in patterns.items():
				start = insert.format(table)
				if line.lower().startswith(start):
					m = re.match(val['regex'], line[len(start):], re.I)
					if m:
						if table == 'backup_info':
							backup['id'] = m.group(1)
						else:
							db.execute('insert into {} values ({})'.format(table, ','.join('?'*len(m.groups()))), m.groups())
	if backup['id']:
		data = db.execute('SELECT bl.location, bf.filename FROM backup_file AS bf JOIN backup_location AS bl ON bf.location_id = bl.id WHERE bl.backup_id = ?', (backup['id'],)).fetchall()
		if len(data) == 1:
			backup['catalog'] = (os.path.normpath(data[0][0][6 if os.name == 'nt' else 5:]), os.path.normpath(data[0][1][1:]),)
		else:
			raise ValueError('Expected to get only one catalog file record, got this instead: {}'.format(data))
		backup['details'] = db.execute('SELECT objtype, objname, dbsize, backupsize, rowcount FROM object_info WHERE backup_id = ? AND objtype != ?;', (backup['id'], 'CATALOG',)).fetchall()
	else:
		raise ValueError('Couldn\'t find backup snapshot ID in the metadata file')
	return backup

try:
	FileNotFoundError
except NameError:
	FileNotFoundError = IOError

envvars = {'host': ('h','YBHOST',), 'port': ('p','YBPORT',5432,), 'dbname': ('d','YBDATABASE',), 'username': ('U','YBUSER',), 'password': ('P','YBPASSWORD',)}
parser = ArgumentParser(description='Yellowbrick selective backup tool', add_help=False)
parser.add_argument('-v', '--version', action = 'version', version = '%(prog)s 0.2b')
parser.add_argument('-b', '--bundle', required=True, help='backup bundle location')
parser.add_argument('-c', '--chain', default='default', help='backup chain name')
for long_opt,short_opt,varname,var_exists,default in [(x,y[0],y[1],y[1] in os.environ,None if len(y)<3 else y[2]) for x,y in envvars.items()]:
	parser.add_argument('-' + short_opt, '--' + long_opt, required = not (var_exists or default), default = os.environ[varname] if var_exists else default)
args = parser.parse_args()
for arg,val in vars(args).items():
	if arg in envvars:
		os.environ[envvars[arg][1]] = str(val)

bundle = os.path.abspath(args.bundle)
snapshot_name = 'check_' + str(uuid.uuid4()).replace('-', '_')
metadata, backup_tail = ('metaDB.sql', 'metaDB.mv.db',), '.' + snapshot_name + '.tmp'

message('backing up metadata catalog')
meta('backup')
message('calling ybbackup to take an incremental backup')
try:
	subprocess.check_call(['ybbackup', '--chain', args.chain, '--name', snapshot_name, '--logfile', snapshot_name + '.inc.log', '--inc', bundle])
except (FileNotFoundError, subprocess.CalledProcessError) as e:
	message('incremental backup attempt failed, exiting', 'ERROR', exitcode=e.returncode if hasattr(e, 'returncode') else -1)
message('checking if there are any data changes')
try:
	backup_data = get_backup_details(snapshot_name)
except Exception as e:
	message('couldn\'t get the last incremental backup details: {}'.format(e), 'ERROR', -1)
if backup_data['details']:
	message('data changes detected', 'STATUS')
	header = ('Object type', 'Object name', 'Data change in database, bytes', 'Data backup size, bytes', 'Rows affected')
	backup_data['details'].insert(0, header)
	maxlength = {i: max(map(len,map(str,row))) for i,row in enumerate(zip(*backup_data['details']))}
	backup_data['details'].insert(1, ['-'*i for i in maxlength.values()])
	for rown, row in enumerate(backup_data['details']):
		sys.stdout.write('| ')
		for coln, cell in enumerate(row):
			vtype = ('s' if isinstance(cell, six.string_types) else ',d')
			sys.stdout.write('{:{}{}}'.format(cell, maxlength[coln], vtype) + ' |' + ('\n' if coln == len(header) - 1 else ' '))
	exit_code = 0
else:
	message('no data changes detected', 'STATUS')
	# Clean up stuff
	message('removing temporary backup snapshot {} from the database'.format(snapshot_name))
	try:
		subprocess.check_call(['ybsql', '-c', "drop backup snapshot '" + snapshot_name + "'"])
	except (OSError, FileNotFoundError, subprocess.CalledProcessError) as e:
		message('couldn\'t remove backup snapshot: {}'.format(e), 'WARN')
	catalog_file = os.path.normpath(os.path.join(*backup_data['catalog']))
	message('removing catalog backup file {}'.format(catalog_file))
	if os.path.isfile(catalog_file):
		item = catalog_file
		os.remove(item)
		while item:
			item = os.path.dirname(item)
			try:
				os.rmdir(item)
			except OSError as e:
				break
	else:
		message('catalog backup file {} not found'.format(catalog_file), 'WARN')
	message('restoring metadata catalog')
	meta('restore', remove_backup=True)
	exit_code = -2
message('done')
exit(exit_code)
