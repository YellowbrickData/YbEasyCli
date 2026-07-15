import requests, sqlite3, time, sys, argparse, os, json, codecs
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.3b')
parser.add_argument('-p', '--pause'   , default=5
	, help='pause between polls in seconds, default: %(default)s', type=int)
parser.add_argument('-d', '--local-db', default='wlm-rule-events-{ts}.db'.format(ts=datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
	, help='sqlite3 database file name on local file system to keep the capture results, default: %(default)s')
parser.add_argument('-i', '--ignore'  , default='begin,end,disabled,ignore'
	, help='event types to ignore (comma-separated list, default: %(default)s), use NONE to include everything')
parser.add_argument('--inc-user'    , help='capture events only for specified user(s)')
parser.add_argument('--inc-database', help='capture events only for specified database(s)')
parser.add_argument('--inc-query'   , help='capture events only for queries that contain the specified substring')
parser.add_argument('--display'     , help='display captured events live', action='store_true')
parser.add_argument('--save-json'   , help='save WLM JSON response'      , action='store_true')
# TODO: add safeguards
#parser.add_argument('-f', '--free-space-prc', default=25     , help = 'free disk space percentage to keep as a safeguard against disk fill, default: %(default)s', type=int)
#parser.add_argument('-s', '--local-db-size' , default=1024   , help = 'local database size limit in MB, default: %(default)s', type=int)
args = parser.parse_args()

colors = {
'disabled': '\033[90m', # dark grey
'timeout' : '\033[33m', # yellow
'set'     : '\033[32m', # green
'ignore'  : '\033[35m', # purple
'info'    : '\033[34m', # blue
'throttle': '\033[31m', # red
'_cyan'   : '\033[36m', # cyan
'_default': '\033[39m', # grey
'_off'    : '\033[0m',
}

def pretty_print(cur, tsid):
	cur.execute('SELECT * FROM query WHERE id IN (SELECT DISTINCT query_id FROM event WHERE tsid = ?) ORDER BY id', (tsid,))
	for query in cur.fetchall():
		print('Query ID: {on}{qid}{off}, Database: {on}{db}{off}, User: {on}{user}{off}, SQL: {on}{sql}{off}'.format(
			qid=query['id'], db=query['db_name'], user=query['user_name'], sql=query['query_text'], on=colors['_cyan'], off=colors['_off']))
		cur.execute("SELECT ts, substr(substr('        ',length(nano))||nano,1,3) AS nano, rule_type, event_type, rule_name, event_msg FROM v_event WHERE query_id = ? AND tsid = ?", (query['id'], tsid))
		for event in cur.fetchall():
			color = colors[event['event_type']] if event['event_type'] in colors else colors['_default']
			print('{ts:25s} [{nano:3s}] {rt:20s} {on}{et:24s}  {rule:40s} {msg}{off}'.format(
				ts=event['ts'], nano=event['nano'], rt=event['rule_type'], et=event['event_type'], rule=event['rule_name'], msg=event['event_msg'], on=color, off=colors['_off']))

db = sqlite3.connect(args.local_db)
db.row_factory = sqlite3.Row
cur = db.cursor()
tables = {
	'query': {'columns': ['id INTEGER PRIMARY KEY', 'db_name TEXT NOT NULL', 'user_name TEXT NOT NULL', 'query_text TEXT NOT NULL', ], 'constraints': [], 'rows': [], },
	'event': {'columns': ['tsid INTEGER NOT NULL', 'query_id INTEGER NOT NULL', 'rule_type TEXT NOT NULL', 'rn INTEGER NOT NULL', 'ern INTEGER NOT NULL'
			, 'ts TIMESTAMP NOT NULL', 'nano TEXT NOT NULL', 'event_type TEXT NOT NULL', 'rule_name TEXT NOT NULL', 'event_msg TEXT NOT NULL', ]
			, 'constraints': ['CONSTRAINT pk_event PRIMARY KEY (query_id, rule_type, ts, nano, ern)'], 'rows': [], },
}
for table, tdata in tables.items():
	cur.execute('CREATE TABLE IF NOT EXISTS {table} ({columns})'.format(table=table, columns=','.join(tdata['columns'] + tdata['constraints'])))
cur.execute("""CREATE VIEW IF NOT EXISTS v_event AS SELECT tsid, query_id, rule_type, ts, nano, event_type, rule_name, event_msg
FROM event ORDER BY tsid, rn DESC, ern, CASE rule_type WHEN 'submit' THEN 1 WHEN 'prepare' THEN 2 WHEN 'compile' THEN 3 WHEN 'runtime' THEN 4 WHEN 'completion' THEN 5 ELSE 256 END""")

query_attrs = ('executionId', 'databaseName', 'userName', 'SQLQueryText',)
trace_attrs = ('timestamp', 'nano', 'type', 'ruleName', 'message')
poll = 1

user_filter     = args.inc_user.split(',')     if args.inc_user     else None
database_filter = args.inc_database.split(',') if args.inc_database else None
event_filter    = args.ignore.split(',')       if args.ignore and args.ignore != 'NONE' else None
query_filter    = args.inc_query.lower()       if args.inc_query    else None

try:
	while True:
# TODO: print free disk %
		print('Poll #{poll:06d} - datetime: {ts}, local DB size: {dbsize} MB'.format(poll=poll, ts=datetime.now().strftime('%Y %b %d, %H:%M:%S'), dbsize=round(os.path.getsize(args.local_db)/1024**2,2)))
		resp = requests.get('http://127.0.0.1:8181/jolokia/read/io.yellowbrick:container=lime,name=Workload%20Manager/RuleResults', headers={'Authorization': 'Basic bm90OnVzZWQ='})
		resp.raise_for_status()
		data = resp.json()
		if args.save_json:
			with codecs.open(os.path.splitext(args.local_db)[0] + '.{:06d}.json'.format(poll), mode='w', encoding='utf-8') as f:
				f.write(json.dumps(data, indent=4))
		for key in tables: tables[key]['rows'] = []
		print('\tEvent blob size: {} bytes, number of events captured: {}'.format(len(resp.text), len(data['value'])))
		tsid = int(time.time())
		for rn, event in enumerate(data['value']):
			if (user_filter and event['userName'] not in user_filter) or (database_filter and event['databaseName'] not in database_filter): continue
			if query_filter and query_filter not in event['SQLQueryText'].lower(): continue
			tables['query']['rows'].append(tuple(event[attr] for attr in query_attrs))
			# NOTE: any profile activate/reload event wouldn't have 'trace' block, hence we have to check for its existence
			if 'trace' not in event or not event['trace']: continue
			for ern, trace in enumerate(event['trace']):
				if event_filter and trace['type'] in event_filter: continue
				tables['event']['rows'].append((tsid, event['executionId'], event['ruleType'], rn, ern) + tuple(trace[attr] for attr in trace_attrs))
		for table, tdata in tables.items():
			cur.executemany('INSERT OR IGNORE INTO {table} VALUES ({placeholders})'.format(table=table, placeholders=','.join(['?']*len(tdata['columns']))), tdata['rows'])
			print('\t{table}: {rows} rows inserted'.format(rows=cur.rowcount if cur.rowcount >= 0 else 0, table=table))
		db.commit()
		poll += 1
		if args.display: pretty_print(cur, tsid)
		print('\tSleeping for {} seconds'.format(args.pause))
		time.sleep(args.pause)
except requests.exceptions.HTTPError as err:
	print('Could not get an HTTP response: {}'.format(err))
except KeyboardInterrupt:
	print('Ctrl+C pressed, exiting')
finally:
	print('Closing local DB connection')
	db.close()
