#  (c) 2024 Yellowbrick Data Corporation.
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.

import re, json, argparse

def transform_value(area, key, value):
	if area == 'pools':
		if key == 'requested_memory' and value[-1] != '%':
			value += 'MB'
		elif key == 'max_spill_pct':
			value = int(value.split('.')[0])
	if area == 'rules' and key == 'javascript':
		quote = '$$'
	elif isinstance(value, str):
		quote = "'"
	else:
		quote = ''
	retval = f'{quote}{value}{quote}'
	return retval

def rename(obj, old_profile, new_profile):
# NOTE: the following will also "fix" the names which are not prepended by the profile name,
#       for example, renaming "my_pool_name" to "my_profile_name: my_pool_name"
	# return new_profile + (obj[len(old_profile):] if obj.startswith(old_profile) else f': {obj}')
# NOTE: the following will not touch the original object name if it doesn't start with the old profile name
	return (new_profile + obj[len(old_profile):]) if obj.startswith(old_profile) else obj

__version__ = '0.1b'
parser = argparse.ArgumentParser(prog = 'Yellowbrick WLM JSON to SQL conversion tool', description = 'Converts WLM profile exported as JSON from the SMC to SQL', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--version', action = 'version', version = '%(prog)s {v}'.format(v = __version__))
parser.add_argument('-f', '--json-file'           , required = True    , help = 'Exported WLM profile JSON file name')
parser.add_argument('-r', '--rename-profile'                           , help = 'Rename the original profile on importing')
parser.add_argument('-g', '--include-global-rules', action='store_true', help = 'Import global rules as well')
args = parser.parse_args()

print('\n'.join((r"\set ECHO 'queries'", r'\set ON_ERROR_STOP on', 'BEGIN;', )))
with open(args.json_file) as a:
	wlm = json.load(a)
create = 'CREATE WLM'
profile = args.rename_profile if args.rename_profile else f'{wlm["name"]}'
print(f"{create} PROFILE \"{profile}\" (DEFAULT_POOL \"{rename(wlm['default_pool'], wlm['name'], profile)}\");")

print('-- WLM pools')
for p in wlm['resourcePools']:
	p['profile'] = profile
	pool = rename(p["name"], wlm['name'], profile)
	options = f',\n\t'.join([f'{k} {transform_value("pools", k, v)}' for k,v in p.items() if v and k not in ('changed_by', 'updated', 'name',)])
	print(f'{create} RESOURCE POOL \"{pool}\" (\n\t{options}\n);')

print('-- WLM rules')
mapping = {'priority': 'rule_order',}
for r in wlm['rules']:
	if r['profile']:
		r['profile'] = profile
	elif not args.include_global_rules:
		continue
# NOTE: here's a useful side-effect - if you choose to import all global rules,
#       they would also be renamed as "profile_name: global_rule_name", and this is good,
#       because otherwise there's a good chance they would clash with existing global rules,
#       which would make the import fail.
	rule = rename(r["name"], wlm['name'], profile)
	r['javascript'] = re.sub(rf'\b{wlm["name"]}\b', profile, r['javascript'])
	options = f',\n\t'.join([f'{mapping[k] if k in mapping else k} {transform_value("rules", k, v)}' for k,v in r.items() if v and k not in ('changed_by', 'updated', 'name', 'editstate',)])
	print(f'{create} RULE \"{rule}\" (\n\t{options}\n);')
print('COMMIT;')
