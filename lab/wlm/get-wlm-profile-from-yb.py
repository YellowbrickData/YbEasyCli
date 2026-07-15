# (c) 2023 Yellowbrick Data Corporation.
#
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.

import argparse, psycopg2, json, sys, os

envvars = {'host': ('h','YBHOST',), 'port': ('p','YBPORT',5432,), 'dbname': ('d','YBDATABASE',), 'username': ('U','YBUSER',), 'password': ('P','YBPASSWORD',)}
parser = argparse.ArgumentParser(prog = sys.argv[0], description = 'Exports WLM configuration in JSON format', add_help = False)
parser.add_argument('--version', action = 'version', version = '%(prog)s beta 0.1')
parser.add_argument('--profile', required = True)
for long_opt,short_opt,varname,var_exists,default in [(x,y[0],y[1],y[1] in os.environ,None if len(y)<3 else y[2]) for x,y in envvars.items()]:
	parser.add_argument('-' + short_opt, '--' + long_opt, required = not (var_exists or default), default = os.environ[varname] if var_exists else default)
args = parser.parse_args()

info = {
	'profile': '''SELECT name, active, default_pool, updated::text AS updated
FROM sys.wlm_profile
WHERE name = %s AND activated IS NULL AND deactivated IS NULL''',
	'resourcePools': '''SELECT PROFILE, name, min_concurrency, max_concurrency, queue_size, requested_memory, next_memory_queue, max_spill_pct, maximum_row_limit, maximum_wait_limit, maximum_exec_time_limit, next_exec_time_limit_queue, changed_by, updated::text AS updated
FROM sys.wlm_resource_pool
WHERE PROFILE = %s AND activated IS NULL AND deactivated IS NULL
ORDER BY name''',
'rules': '''SELECT profile, name, editstate, priority, TYPE, VERSION, enabled, superuser, javascript, changed_by, updated::text AS updated
FROM sys.wlm_classification_rule
WHERE (PROFILE = %s OR PROFILE IS NULL) AND activated IS NULL AND deactivated IS NULL
ORDER BY priority''',
}

result = {}
with psycopg2.connect(host = args.host, port = args.port, dbname = args.dbname, user = args.username, password = args.password) as con:
	with con.cursor() as cur:
		for name, sql in info.items():
			cur.execute(sql, (args.profile,))
			rows = cur.fetchall()
			data = []
			for row in rows:
				data.append(dict(zip([desc[0] for desc in cur.description], row)))
			if name == 'profile':
				assert len(rows) == 1, 'more than one row returned for WLM profile'
				result.update(data[0])
			elif name in ('resourcePools', 'rules',):
				result.update({name: data})
print(json.dumps(result, indent=4))
