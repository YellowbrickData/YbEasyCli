#  (c) 2022 Yellowbrick Data Corporation.
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.

# NOTE on certificate hashes
# 'show ssl trust' command returns certificate hashes in the first column, which is the same as calculated by 'openssl x509 -noout -subject -subject_hash -in cert.crt'
# Trying to calculate the same hash other than by calling openssl is painful and not worth the effort, see:
# 1. https://stackoverflow.com/questions/66055956/openssl-how-to-get-x509-subject-hash-manually
# 2. https://stackoverflow.com/questions/71004481/what-does-openssl-x509-hash-calculate-the-hash-of
# 3. https://stackoverflow.com/questions/30059107/get-x509-certificate-hash-with-openssl-library
# Also, openssl has changed the algo in one of the releases, and there's nothing preventing it from doing it again in the future, so different versions produce different hashes for the same cert - unreliable for the script purposes.
# That's why the script calculates MD5 hash on certificates internally for checking if SSL trust exists between YB appliances, and uses the original hash (as returned by 'show ssl trust') only for revoking trust when requested.

import os, re, hashlib, argparse, subprocess
from getpass  import getpass
from tempfile import mkdtemp
from shutil   import rmtree

__version__ = '1.1'

def run_ybsql(envvars, params = ['-Aqt',], sql = 'select version()'):
	env = os.environ.copy()
	env.update(envvars)
	command = [r'ybsql'] + ['-d', 'yellowbrick'] + params
	if sql:
		command += ['-c', sql]
	return subprocess.check_output(command, env = env)

parser = argparse.ArgumentParser(prog = 'Yellowbrick Replication SSL Trust tool', description = 'Adds, revokes or checks SSL trust between source/target replication members.\nSource and target connection string format:\n\tusername[/password]@host[:port]', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--version', action = 'version', version = '%(prog)s {v}'.format(v = __version__))
parser.add_argument('-s', '--source', required = True, help = 'Replication source appliance connection string')

target = parser.add_mutually_exclusive_group(required = True)
target.add_argument('-t', '--target', help = 'Replication target appliance connection string')
target.add_argument('-l', '--loopback', action = 'store_true', help = 'Create self-trust for loopback replication, only the source connection have to be specified')

parser.add_argument('-x', '--export'  , action = 'store_true', help = 'Export entire SSL truststore into PEM file')
parser.add_argument('-r', '--revoke'  , action = 'store_true', help = 'Revoke mutual SSL trust between replication source and target appliances')
parser.add_argument('-c', '--create'  , action = 'store_true', help = 'Create mutual SSL trust between replication source and target appliances')
parser.add_argument('-W', '--password', action = 'store_true', help = 'Force interactive password prompt')
args = parser.parse_args()

if args.loopback:
	args.target = args.source

# YB appliance connection string format: username/password@host:port (port and password are optional, either YBPASSWORD envvar or .ybpass entry work fine)
rx = re.compile(r'(?P<YBUSER>\S+?)(?:/(?P<YBPASSWORD>.+))?@(?P<YBHOST>\S+)(?::(?P<YBPORT>\d+))?')
pk = 'password typed in'
clusters = {
	'source': {'env': {}, 'ssl': {'ca'    : None}, 'trust': {}, pk: False, },
	'target': {'env': {}, 'ssl': {'system': None}, 'trust': {}, pk: False, },
}
# NOTE: a PEM file could contain multiple certificates (the whole chain of trust for example), but only the first one gets ingested by 'import ssl trust' command, which makes sense as we don't need the entire chain of trust, the leaf certificate is enough for this use case.
# NOTE: below I'm extracting only the base64-encoded part of the first certificate (leaf) to calculate MD5 hash
formalize = lambda x: re.search('-+BEGIN CERTIFICATE-+([^-]+)-+END CERTIFICATE-+',re.sub('[\r\n]','',x)).group(1)
md5 = lambda x: hashlib.md5(x.encode()).hexdigest()
for cluster in clusters.keys():
	matches = rx.match(vars(args)[cluster])
	if matches:
		clusters[cluster]['env'] = {k:v for k,v in matches.groupdict().items() if v}
	else:
		print("Couldn't parse the {cluster} connection string".format(cluster = cluster))
		exit(1)

passvar = 'YBPASSWORD'
if passvar in os.environ and not args.password and (passvar not in clusters['source']['env'] or passvar not in clusters['target']['env']):
	print('{var} envronment variable detected'.format(var = passvar))
# Import System cert, CA cert and SSL trust store contents from source/target
for k,v in clusters.items():
	print('Checking {cluster} ({host}) ...'.format(cluster = k, host = v["env"]["YBHOST"]))
	if args.password and not v[pk]: # forced password entry is requested and no password manually entered yet
		v['env'][passvar] = getpass('Enter password for "{user}" user: '.format(cluster = v['env']['YBHOST'], user = v['env']['YBUSER']))
		clusters[k][pk] = True
	hostname, version = v['env']['YBHOST'], run_ybsql(v['env']).decode().strip().split()[-1]
	for cert in v['ssl']:
		clusters[k]['ssl'][cert] = run_ybsql(v['env'], sql = 'show ssl {cert}'.format(cert = cert)).decode().strip()
		if args.export:
			with open('{cluster}-{host}-{cert}.pem'.format(cluster = k, host = hostname, cert = cert), 'w') as f:
				f.write(clusters[k]['ssl'][cert])
	trust_store = run_ybsql(v['env'], params = ['-F', '\x1F', '-R', '\x1E', '-Aqt'], sql = 'show ssl trust').decode().strip()
	if trust_store:
		for n, line in enumerate(trust_store.split('\x1E')):
			ybhash, details, cert = [x.strip() for x in line.split('\x1F')]
			clusters[k]['trust'][md5(formalize(cert))] = {'hash': ybhash, 'details': details, 'cert': cert}
			if args.export:
				with open('{cluster}-{host}-truststore-{n}.pem'.format(cluster = k, host = hostname, n = n), 'w') as f:
					f.write(cert)
	print('\tversion = {v:15s}, truststore has {n:3d} entries'.format(h = hostname, v = version, n = len(clusters[k]['trust'])))

# Display, create or revoke SSL trust
for trust in ('source:target:system', 'target:source:ca'):
	trustor, trustee, cert = trust.split(':')
	maxwidth = max(len(clusters[trustor]['env']['YBHOST']), len(clusters[trustee]['env']['YBHOST']))
	msg = '{tr} ({ctr:<{w}}) {msg} {te} ({cte:<{w}}) {c:6} certificate in its truststore'.format(
		tr = trustor, w = maxwidth, ctr = clusters[trustor]['env']['YBHOST'], te = trustee, cte = clusters[trustee]['env']['YBHOST'], c = cert.upper(), msg = '{r}')
	cert_md5 = md5(formalize(clusters[trustee]['ssl'][cert]))
	trusted = cert_md5 in clusters[trustor]['trust']
	print(msg.format(r = '\033[92malready has\033[0m ' if trusted else '\033[91mdoesn\'t have\033[0m'))
	if trusted and args.revoke:
		print('Revoking trust for {te} from {tr}...'.format(te = trustee, tr = trustor))
		run_ybsql(clusters[trustor]['env'], sql = "revoke '{hash}' from ssl trust".format(hash = clusters[trustor]['trust'][cert_md5]['hash']))
	if not trusted and args.create:
		print('Creating trust for {te} on {tr}...'.format(te = trustee, tr = trustor))
		run_ybsql(clusters[trustor]['env'], sql = "import ssl trust from '{cert}'".format(cert = clusters[trustee]['ssl'][cert]))
