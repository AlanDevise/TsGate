#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Build and run exact-version openGemini regression fixtures using local Docker.

Examples:
  python3 environment.py build --version 1.4.1 --state .local-test/og-141.json
  python3 environment.py start --version 1.4.1 --mode cluster --state .local-test/og-141.json
  python3 environment.py wait-database --state .local-test/og-141.json --database test_points
  python3 environment.py inspect --state .local-test/og-141.json
  python3 environment.py stop --state .local-test/og-141.json

Only containers and networks carrying this invocation's unique owner label are removed.
"""
import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

LABEL = 'com.alandevise.tsgate.opengemini.fixture'
VERSIONS = {
    '1.4.1': {
        'commit': '5ff486d020cf52df11d8de73aa4664fd843a4e53',
        'kind': 'binary',
        'sha256': {
            'arm64': '519991bf147778985ddc5c12dc08ea0585946c7b04d3d6fb305f66de49d1c0a6',
            'amd64': 'b5ea28b603657b70cd5e16840ef4a642bd63a65d51c047a25325caf41988208e',
        },
    },
    '1.5.2': {
        'commit': '19ba0e2d9b428579004eb53d37d0c9b23034e2a9',
        'kind': 'source',
        'sha256': 'c14f3bc11b52bd875763453882f76421df498fc4b78001e5cf8f7abcccd09bed',
    },
}

def docker(*args, check=True, timeout=None):
    result = subprocess.run(['docker', *args], text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError('docker ' + ' '.join(args) + ': ' + result.stderr.strip())
    return result

def read_json(path):
    return json.loads(path.read_text())

def save(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False) + '\n')

def architecture():
    arch = docker('info', '--format', '{{.Architecture}}').stdout.strip()
    if arch in ('aarch64', 'arm64'):
        return 'arm64'
    if arch in ('x86_64', 'amd64'):
        return 'amd64'
    raise RuntimeError('Unsupported Docker architecture: ' + arch)

def fingerprint(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def download(url, path, digest):
    if path.exists() and fingerprint(path) == digest:
        return
    request = urllib.request.Request(url, headers={'User-Agent': 'TsGate-regression-fixture'})
    temp = path.with_suffix(path.suffix + '.partial')
    with urllib.request.urlopen(request, timeout=120) as response, temp.open('wb') as output:
        shutil.copyfileobj(response, output)
    actual = fingerprint(temp)
    if actual != digest:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f'Checksum mismatch for {url}: expected {digest}, got {actual}')
    temp.replace(path)

def image_for(version):
    return f'tsgate-opengemini:{version}-{VERSIONS[version]["commit"][:7]}'

def build(version, state_path, force=False):
    spec = VERSIONS[version]
    arch = architecture()
    image = image_for(version)
    context = state_path.parent / 'image-builds' / (version + '-' + arch)
    context.mkdir(parents=True, exist_ok=True)
    fixture_dir = Path(__file__).resolve().parent
    recipe = fixture_dir / ('Dockerfile.' + spec['kind'])
    entrypoint = fixture_dir / 'entrypoint.sh'
    fixture_digest = hashlib.sha256(recipe.read_bytes() + entrypoint.read_bytes()).hexdigest()
    for source in (recipe, entrypoint):
        shutil.copy2(source, context / source.name)
    if spec['kind'] == 'binary':
        url = f'https://github.com/openGemini/openGemini/releases/download/v{version}/openGemini-{version}-linux-{arch}.tar.gz'
        digest = spec['sha256'][arch]
        archive = context / 'release.tar.gz'
    else:
        url = 'https://codeload.github.com/openGemini/openGemini/tar.gz/' + spec['commit']
        digest = spec['sha256']
        archive = context / 'source.tar.gz'
    print('Downloading and verifying ' + url, file=sys.stderr, flush=True)
    download(url, archive, digest)
    provenance = {'architecture': arch, 'release_tag_commit': spec['commit'],
                  'source_commit': spec['commit'] if spec['kind'] == 'source' else None, 'version': version,
                  'artifact_kind': spec['kind'], 'artifact_url': url, 'artifact_sha256': digest,
                  'fixture_sha256': fixture_digest}
    inspected = docker('image', 'inspect', image, check=False)
    if not force and inspected.returncode == 0:
        info = json.loads(inspected.stdout)[0]
        labels = info.get('Config', {}).get('Labels', {}) or {}
        if (labels.get('org.opencontainers.image.revision') == spec['commit']
                and labels.get(LABEL + '.sha256') == fixture_digest and info['Architecture'] == arch):
            return provenance | {'image': image, 'image_id': info['Id'], 'cached': True}
    command = ['docker', 'build', '--platform', 'linux/' + arch, '--progress', 'plain',
               '-f', str(context / ('Dockerfile.' + spec['kind'])),
               '--build-arg', 'VERSION=' + version, '--build-arg', 'SOURCE_COMMIT=' + spec['commit'],
               '--build-arg', 'FIXTURE_SHA256=' + fixture_digest,
               '-t', image, str(context)]
    log = context / 'build.log'
    print('Building ' + image + '; log: ' + str(log), file=sys.stderr, flush=True)
    with log.open('w') as output:
        subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=True)
    info = json.loads(docker('image', 'inspect', image).stdout)[0]
    return provenance | {'image': image, 'image_id': info['Id'], 'build_log': str(log), 'cached': False}

def request(url, body=None, headers=None):
    req = urllib.request.Request(url, data=body, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15) as response:
        data = response.read().decode()
        return {'status': response.status, 'headers': dict(response.headers),
                'body': json.loads(data) if data else None}

def query(url, sql, db=None):
    params = {'q': sql, 'epoch': 'ms'}
    if db:
        params['db'] = db
    result = request(url + '/query?' + urllib.parse.urlencode(params))
    envelope = result['body'] or {}
    errors = [x['error'] for x in envelope.get('results', []) if x.get('error')]
    if envelope.get('error'):
        errors.append(envelope['error'])
    if errors:
        raise RuntimeError(sql + ': ' + '; '.join(errors))
    return envelope

def wait_for(callback, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = callback()
            if value:
                return value
        except (OSError, ValueError, RuntimeError, urllib.error.URLError) as exc:
            last = str(exc)
        time.sleep(1)
    raise RuntimeError('Readiness timeout; last result: ' + str(last))

def node_details(name):
    info = json.loads(docker('inspect', name).stdout)[0]
    ports = info['NetworkSettings']['Ports']
    return {'name': name, 'container_id': info['Id'],
            'sql_url': 'http://127.0.0.1:' + ports['8086/tcp'][0]['HostPort'],
            'meta_url': 'http://127.0.0.1:' + ports['8091/tcp'][0]['HostPort'],
            'ip': next(iter(info['NetworkSettings']['Networks'].values()))['IPAddress']}

def series_rows(result):
    for item in result.get('results', []):
        for series in item.get('series', []):
            for row in series.get('values', []):
                yield dict(zip(series.get('columns', []), row))

def fixture_point_present(result, timestamp):
    """Require the exact fixture point, including millisecond time and tag/field values."""
    return list(series_rows(result)) == [{'time': timestamp, 'node': 'first', 'value': 17}]

def cluster_members(result):
    members = []
    for item in result.get('results', []):
        for series in item.get('series', []):
            columns = series.get('columns', [])
            if 'nodeType' not in columns:
                continue
            for row in series.get('values', []):
                members.append(dict(zip(columns, row)))
    return members

def cluster_ready(state):
    evidence = [query(node['sql_url'], 'SHOW CLUSTER') for node in state['nodes']]
    expected = 3 if state['mode'] == 'cluster' else 1
    members = cluster_members(evidence[0])
    meta = [x for x in members if x.get('nodeType') == 'meta' and x.get('status') == 'alive']
    data = [x for x in members if x.get('nodeType') == 'data' and x.get('status') == 'alive']
    if len(meta) != expected or len(data) != expected:
        return None
    return evidence

def inspect_state(state):
    evidence = {'pings': [], 'binary_versions': [], 'meta_raft': [], 'cluster': []}
    for node in state['nodes']:
        evidence['pings'].append(request(node['sql_url'] + '/ping'))
        evidence['binary_versions'].append(docker('exec', node['name'], 'ts-server', 'version').stdout.strip())
        evidence['meta_raft'].append(request(node['meta_url'] + '/debug?witch=raft-stat', headers={'all': 'y'})['body'])
        evidence['cluster'].append(query(node['sql_url'], 'SHOW CLUSTER'))
    evidence['members'] = cluster_members(evidence['cluster'][0])
    evidence['meta_count'] = sum(x.get('nodeType') == 'meta' for x in evidence['members'])
    evidence['store_count'] = sum(x.get('nodeType') == 'data' for x in evidence['members'])
    evidence['actual_versions'] = [next((v for k, v in p['headers'].items() if k.lower() == 'x-geminidb-version'), None) for p in evidence['pings']]
    raft = evidence['meta_raft'][0]
    evidence['raft_members'] = [{'address': address, 'state': stats.get('state'),
                                 'peer_count': int(stats.get('num_peers', '-1')),
                                 'voter_count': stats.get('latest_configuration', '').count('Suffrage:Voter')}
                                for address, stats in raft.items()]
    evidence['leader_count'] = sum(x['state'] == 'Leader' for x in evidence['raft_members'])
    evidence['binary_git_commits'] = [re.search(r'^git: \S+ ([a-f0-9]+)', text, re.MULTILINE).group(1) for text in evidence['binary_versions']]
    return evidence

def smoke(state):
    db = 'tsgate_fixture_' + state['owner'][-8:]
    replicas = state['replicas']
    first = state['nodes'][0]['sql_url']
    query(first, f'CREATE DATABASE "{db}" REPLICAS {replicas}')
    timestamp = int(time.time() * 1000)
    body = f'fixture_smoke,node=first value=17i {timestamp}'.encode()
    wait_for(lambda: request(first + '/write?' + urllib.parse.urlencode({'db': db, 'precision': 'ms'}), body) or True)
    results = []
    for node in state['nodes']:
        def read():
            answer = query(node['sql_url'], 'SELECT * FROM fixture_smoke', db)
            return answer if fixture_point_present(answer, timestamp) else None
        results.append(wait_for(read))
    retention = query(first, 'SHOW RETENTION POLICIES ON "' + db + '"')
    details = query(first, 'SHOW DATABASES DETAIL')
    database_rows = [row for row in series_rows(details) if row.get('name') == db]
    if len(database_rows) != 1 or int(database_rows[0]['ReplicaN']) != replicas:
        raise RuntimeError('Database metadata does not confirm the configured replica count')
    default_policies = [row for row in series_rows(retention) if row.get('default')]
    if len(default_policies) != 1 or int(default_policies[0]['replicaN']) != replicas:
        raise RuntimeError('Retention policy metadata does not confirm the configured replica count')
    return {'database': db, 'replicas': replicas, 'timestamp_ms': timestamp,
            'read_from_each_sql': results, 'retention_policies': retention,
            'database_detail': details}

def start(args):
    if args.state.exists():
        previous = read_json(args.state)
        if previous.get('containers') and not previous.get('stopped'):
            raise RuntimeError('An existing fixture state is active; stop it before starting a new one')
    image = build(args.version, args.state)
    owner = 'tsgate-og-' + uuid.uuid4().hex[:10]
    network = owner + '-net'
    count = 3 if args.mode == 'cluster' else 1
    state = image | {'owner': owner, 'network': network, 'mode': args.mode,
                     'replicas': 3 if args.mode == 'cluster' else 1, 'containers': [], 'nodes': [],
                     'state_file': str(args.state.resolve()), 'stopped': False}
    save(args.state, state)
    docker('network', 'create', '--label', LABEL + '=' + owner, network)
    try:
        for index in range(1, count + 1):
            name = owner + '-node' + str(index)
            meta_join = ','.join('"node' + str(i) + ':8092"' for i in range(1, count + 1))
            gossip = ','.join('"node' + str(i) + ':8010"' for i in range(1, count + 1))
            docker('create', '--name', name, '--network', network, '--network-alias', 'node' + str(index),
                   '--label', LABEL + '=' + owner, '--memory', '1536m', '--cpus', '2',
                   '-p', '127.0.0.1::8086', '-p', '127.0.0.1::8091',
                   '-e', 'OPENGEMINI_MODE=' + args.mode, '-e', 'OPENGEMINI_NODE_NAME=node' + str(index),
                   '-e', 'OPENGEMINI_META_JOIN=' + meta_join,
                   '-e', 'OPENGEMINI_GOSSIP_MEMBERS=' + gossip, image['image'])
            state['containers'].append(name)
            save(args.state, state)
        for name in state['containers']:
            docker('start', name)
        state['nodes'] = [node_details(name) for name in state['containers']]
        save(args.state, state)
        for node in state['nodes']:
            wait_for(lambda: request(node['sql_url'] + '/ping'))
        print('HTTP ready; waiting for registered meta/store membership', file=sys.stderr, flush=True)
        wait_for(lambda: cluster_ready(state))
        state['smoke'] = smoke(state)
        state['evidence'] = inspect_state(state)
        if any(v != args.version for v in state['evidence']['actual_versions']):
            raise RuntimeError('The running service version does not match the requested version')
        raft = state['evidence']['raft_members']
        if (len(raft) != count or state['evidence']['leader_count'] != 1
                or any(x['voter_count'] != count or x['peer_count'] != count - 1 for x in raft)):
            raise RuntimeError('Raft membership does not confirm the requested meta quorum')
        state['actual_version'] = state['evidence']['actual_versions'][0]
        state['url'] = state['nodes'][0]['sql_url']
        state['urls'] = [n['sql_url'] for n in state['nodes']]
        state['ready'] = True
        save(args.state, state)
        return state
    except Exception:
        collect(state, args.state)
        save(args.state, state)
        raise

def database_ready_partitions(log, database):
    """Read only exact structured completion records for this database."""
    partitions = set()
    for line in log.splitlines():
        try:
            record = json.loads(line)
        except (ValueError, TypeError):
            continue
        if (isinstance(record, dict)
                and record.get('msg') == 'try to transfer leadership finish'
                and record.get('database') == database
                and type(record.get('partition')) is int
                and record['partition'] in (0, 1, 2)):
            partitions.add(record['partition'])
    return partitions


# The official 1.4.1 binary drops Logger.With fields. Its exact source has only
# two TransferLeadership callers: every successful init, and the logged RPC.
# Count all calls and completions; never assign an anonymous finish to a DB.
# Logger buffering and later background RPCs preclude an atomic health guarantee.
# This observes initial CREATE transfers in an exclusive, serial test fixture.
BINARY_141_COMMIT = '42678b4a23e0c7921548f6ceacb812659591d0bf'


def database_readiness_141(log, database):
    """Require a complete pinned-binary log and a balanced global transfer ledger."""
    records = []
    for line in log.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except (ValueError, TypeError) as error:
            raise RuntimeError('Malformed or truncated 1.4.1 store log') from error
        if not isinstance(record, dict):
            raise RuntimeError('Malformed 1.4.1 store log record')
        records.append(record)
    startups = [i for i, record in enumerate(records) if record.get('msg') == 'TSStore starting']
    if len(startups) != 1:
        raise RuntimeError('1.4.1 readiness requires one complete, unrotated store process log')
    startup = records[startups[0]]
    if (startup.get('level') != 'info' or startup.get('version') != '1.4.1'
            or startup.get('commit') != BINARY_141_COMMIT):
        raise RuntimeError('1.4.1 readiness requires the pinned official binary commit')
    sites = {
        '[ASSIGN]init and start node': 'engine/engine_replication.go:83',
        'Start TransferLeadership': 'handler/transfer_leader.go:44',
        'try to transfer leadership finish': 'raftconn/node.go:313',
    }
    failures = {'[ASSIGN]InitAndStartNode failed', 'try to transfer leadership timeout',
                'startRaftNode transferLeaderShip fail', 'TransferLeadership fail'}
    started, finished = 0, 0
    target = []
    for index, record in enumerate(records):
        message = record.get('msg')
        if message in failures:
            raise RuntimeError('1.4.1 data-Raft lifecycle failure: ' + message)
        if message not in sites:
            continue
        if (index <= startups[0] or record.get('level') != 'info'
                or record.get('location') != sites[message]):
            raise RuntimeError('Unexpected or incomplete 1.4.1 data-Raft lifecycle record')
        repeated = record.get('repeated')
        if type(repeated) is not int or repeated < 1:
            raise RuntimeError('Invalid 1.4.1 data-Raft repeated count')
        if message == 'try to transfer leadership finish':
            finished += repeated
            continue
        if not isinstance(record.get('db'), str) or not record['db']:
            raise RuntimeError('Missing database in 1.4.1 data-Raft start record')
        if message == '[ASSIGN]init and start node':
            partition = record.get('pt')
            if type(partition) is not int or partition not in (0, 1, 2):
                raise RuntimeError('Invalid partition in 1.4.1 data-Raft init record')
            if record['db'] == database:
                if repeated != 1:
                    raise RuntimeError('Ambiguous target database in repeated 1.4.1 init record')
                target.append(partition)
        else:
            if any(type(record.get(key)) is not int or record[key] not in (0, 1, 2)
                   for key in ('oldPt', 'newPt')):
                raise RuntimeError('Invalid partition in 1.4.1 transfer RPC record')
        started += repeated
    if len(target) > 1:
        raise RuntimeError('Ambiguous or restarted target database in 1.4.1 store log')
    if finished > started:
        raise RuntimeError('Incomplete 1.4.1 data-Raft transfer ledger')
    ledger = {'started_transfers': started, 'finished_transfers': finished,
              'pending_transfers': started - finished, 'target_partitions': target}
    return set(target) if started == finished else set(), ledger


def wait_database(state, database, timeout=45):
    """Wait for all owned database partitions without writing or transferring leadership."""
    if not isinstance(database, str) or not database.strip():
        raise RuntimeError('wait-database requires a nonempty database name')
    mode, replicas = state.get('mode'), state.get('replicas')
    if mode == 'single' and type(replicas) is int and replicas == 1:
        return {'database': database, 'mode': mode, 'status': 'skipped',
                'reason': 'Single-node fixtures do not require the data-Raft gate'}
    if mode != 'cluster' or type(replicas) is not int or replicas != 3:
        raise RuntimeError('wait-database requires a three-replica cluster fixture')
    owner, containers = state.get('owner'), state.get('containers')
    if (not isinstance(owner, str) or not owner.startswith('tsgate-og-')
            or not isinstance(containers, list) or len(containers) != 3
            or not all(isinstance(name, str) and name.startswith(owner + '-node') for name in containers)
            or len(set(containers)) != 3 or state.get('ready') is not True or state.get('stopped')):
        raise RuntimeError('wait-database requires an active, ready, owned fixture')
    version = state.get('version')
    if version not in VERSIONS:
        raise RuntimeError('wait-database requires an exact supported fixture version')
    deadline = time.monotonic() + timeout

    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise RuntimeError('Database data-Raft readiness timeout: ' + database)
        return min(seconds, 5)

    for name in containers:
        inspected = json.loads(docker('inspect', name, timeout=remaining()).stdout)
        if (len(inspected) != 1
                or inspected[0].get('Config', {}).get('Labels', {}).get(LABEL) != owner):
            raise RuntimeError('Refusing to read a container with a different ownership label: ' + name)
    last = {}
    previous_ledger = None
    while time.monotonic() < deadline:
        last = {}
        ledgers = {}
        try:
            for name in containers:
                result = docker('exec', name, 'cat', '/var/log/opengemini/store.log', timeout=remaining())
                if version == '1.4.1':
                    partitions, ledgers[name] = database_readiness_141(result.stdout, database)
                else:
                    partitions = database_ready_partitions(result.stdout, database)
                if len(partitions) > 1:
                    raise RuntimeError('Ambiguous database partitions on owned node: ' + name)
                last[name] = sorted(partitions)
            found = [partitions[0] for partitions in last.values() if partitions]
            if len(found) != len(set(found)):
                raise RuntimeError('Database partition appears on more than one owned node: ' + repr(last))
            if len(found) == 3 and set(found) == {0, 1, 2}:
                if version != '1.4.1' or previous_ledger == ledgers:
                    answer = {'database': database, 'mode': mode, 'version': version,
                              'status': 'ready', 'partitions': last}
                    if version == '1.4.1':
                        answer.update(transfer_ledgers=ledgers, stable_observations=2)
                    return answer
                previous_ledger = ledgers
            else:
                previous_ledger = None
            if ledgers:
                last['transfer_ledgers'] = ledgers
        except subprocess.TimeoutExpired:
            previous_ledger = None
            last['readTimeout'] = True
        delay = min(0.5, deadline - time.monotonic())
        if delay > 0:
            time.sleep(delay)
    raise RuntimeError('Database data-Raft readiness timeout: ' + database + '; partitions=' + repr(last))


def collect(state, state_path):
    logs = state_path.parent / (state_path.stem + '-logs')
    logs.mkdir(parents=True, exist_ok=True)
    for name in state.get('containers', []):
        output = docker('logs', name, check=False)
        (logs / (name + '.log')).write_text(output.stdout + output.stderr)
        dest = logs / name
        dest.mkdir(exist_ok=True)
        docker('cp', name + ':/var/log/opengemini/.', str(dest), check=False)
        docker('cp', name + ':/tmp/opengemini.conf', str(dest / 'opengemini.conf'), check=False)

def stop(state, state_path):
    collect(state, state_path)
    for name in state.get('containers', []):
        found = docker('inspect', name, check=False)
        if found.returncode:
            continue
        info = json.loads(found.stdout)[0]
        if info['Config']['Labels'].get(LABEL) != state['owner']:
            raise RuntimeError('Refusing to remove a container with a different ownership label: ' + name)
        docker('rm', '-f', name)
    found = docker('network', 'inspect', state['network'], check=False)
    if found.returncode == 0:
        info = json.loads(found.stdout)[0]
        if info['Labels'].get(LABEL) != state['owner']:
            raise RuntimeError('Refusing to remove a network with a different ownership label')
        docker('network', 'rm', state['network'])
    state['stopped'] = True
    save(state_path, state)
    return state

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('command', choices=('build', 'start', 'inspect', 'stop', 'wait-database'))
    parser.add_argument('--version', choices=VERSIONS, default='1.4.1')
    parser.add_argument('--mode', choices=('single', 'cluster'), default='single')
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--force-build', action='store_true')
    parser.add_argument('--database', help='Exact owned database whose data-Raft leader must be ready')
    args = parser.parse_args()
    args.state = args.state.resolve()
    if args.command == 'build':
        result = build(args.version, args.state, args.force_build)
    elif args.command == 'start':
        result = start(args)
    elif args.command == 'wait-database':
        if not args.database:
            parser.error('wait-database requires --database')
        result = wait_database(read_json(args.state), args.database)
    elif args.command == 'stop':
        result = stop(read_json(args.state), args.state)
    else:
        state = read_json(args.state)
        state['evidence'] = inspect_state(state)
        save(args.state, state)
        result = state
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()
