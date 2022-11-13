#  (c) 2022 Yellowbrick Data Corporation.
# NOTE:
# - This script is provided free of charge by Yellowbrick Data Corporation as a convenience to its customers.
# - This script is provided "AS-IS" with no warranty whatsoever.
# - The customer accepts all risk in connection with the use of this script, and Yellowbrick Data Corporation shall have no liability whatsoever.

import os, re, hashlib, argparse, subprocess

def run_ybsql(envvars, params = ['-Aqt',], sql = 'select version()'):
	os.environ.update(envvars)
	command = [r'ybsql'] + params + ['-c', sql]
	return subprocess.check_output(command)

parser = argparse.ArgumentParser(prog = 'Yellowbrick Replication SSL Trust tool', description = 'Adds, removes or checks SSL trust between source/target replication members')
parser.add_argument('--version', action = 'version', version = '%(prog)s beta 0.2')
parser.add_argument('-s', '--source', required = True)
parser.add_argument('-x', '--export', action = 'store_true')
parser.add_argument('-r', '--remove', action = 'store_true')
parser.add_argument('-i', '--create', action = 'store_true')
target = parser.add_mutually_exclusive_group(required = True)
target.add_argument('-t', '--target')
target.add_argument('-l', '--loopback', action = 'store_true')
args = parser.parse_args()

if args.loopback:
	args.target = args.source

# YB appliance connection string format: username/password@host:port/database, port and password are optional (either YBPASSWORD envvar or .ybpass entry work fine)
rx = re.compile(r'(?P<YBUSER>\w+)(?:/(?P<YBPASSWORD>.+))?@(?P<YBHOST>\S+?)(?::(?P<YBPORT>\d+))?/(?P<YBDATABASE>\w+)')
clusters = {'source': {'env': {}, 'ssl': {'ca': None}, 'trust': {}}, 'target': {'env':{}, 'ssl': {'system': None}, 'trust': {}}}
# NOTE: a PEM file could contain multiple certificates (the whole chain of trust for example), but only the first one gets ingested by 'import ssl trust' command
# NOTE: below I'm extracting only the base64-encoded part of the first certificate
formalize = lambda x: re.search('-+BEGIN CERTIFICATE-+([^-]+)-+END CERTIFICATE-+',re.sub('[\r\n]','',x)).group(1)
md5 = lambda x: hashlib.md5(x.encode()).hexdigest()
for cluster in clusters.keys():
	matches = rx.match(vars(args)[cluster])
	if matches:
		clusters[cluster]['env'] = {k:v for k,v in matches.groupdict().items() if v}
	else:
		print("Couldn't parse the {cluster} connection string".format(cluster = cluster))
		exit(1)
for k,v in clusters.items():
	# print(k,v)
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
			#print('[{h}]\n[{d}]\n[{c}]'.format(h = ybhash, d = details, c = cert))
			# NOTE: Why all this hassle with MD5 hashes if we already have a hash returned by 'show ssl trust'? Well, the thing is - I have no idea what this hash is, it looks like CRC32 but what was it calculated for (details, certificate, something else)? Without knowing it I can only use it as an ID when I want to remove an entry from the truststore.
			clusters[k]['trust'][md5(formalize(cert))] = {'hash': ybhash, 'details': details, 'cert': cert}
			# print(f'-- # {n}: md5={md5(formalize(cert))}, cert=[{formalize(cert)}]')
			if args.export:
				with open('{cluster}-{host}-truststore-{n}.pem'.format(cluster = k, host = hostname, n = n), 'w') as f:
					f.write(cert)
	print('{cluster}: hostname = {h:30s} version = {v:15s}, truststore has {n:3d} entries'.format(cluster = k, h = hostname, v = version, n = len(clusters[k]['trust'])))
for trust in ('source:target:system', 'target:source:ca'):
	trustor, trustee, cert = trust.split(':')
	msg = '{tr} ({ctr}) {msg} {te} ({cte}) {c} certificate in its truststore'.format(tr = trustor, ctr = clusters[trustor]['env']['YBHOST'], te = trustee, cte = clusters[trustee]['env']['YBHOST'], c = cert, msg = '{r}')
	cert_md5 = md5(formalize(clusters[trustee]['ssl'][cert]))
	trusted = cert_md5 in clusters[trustor]['trust']
	# print(f"-- trust: {trust}\n-- cert_md5={cert_md5},-- cert=[{formalize(clusters[trustee]['ssl'][cert])}]")
	print(msg.format(r = 'already has' if trusted else 'does not have'))
	if trusted and args.remove:
		print('removing trust...')
		run_ybsql(clusters[trustor]['env'], sql = "revoke '{hash}' from ssl trust".format(hash = clusters[trustor]['trust'][cert_md5]['hash']))
	if not trusted and args.create:
		print('creating trust...')
		run_ybsql(clusters[trustor]['env'], sql = "import ssl trust from '{cert}'".format(cert = clusters[trustee]['ssl'][cert]))

# print(clusters)
print('Finita')
