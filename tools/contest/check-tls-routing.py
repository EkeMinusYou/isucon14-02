#!/usr/bin/env python3
"""Exercise generated configurations with nginx 1.24 and an HTTP/SSE fixture."""
import collections
import http.client
import ipaddress
import json
import os
import pathlib
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
NAME = 'isuride-tls-check-' + uuid.uuid4().hex[:10]


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def docker(*args):
    return command('docker', *args)


def port(container, private_port):
    return int(docker('port', container, str(private_port) + '/tcp').rsplit(':', 1)[1])


with tempfile.TemporaryDirectory(prefix=NAME) as temporary:
    work = pathlib.Path(temporary)
    containers = []
    network = False
    try:
        architecture = docker('version', '--format', '{{.Server.Arch}}')
        environment = dict(os.environ, CGO_ENABLED='0', GOOS='linux', GOARCH=architecture)
        command('go', 'build', '-o', str(work / 'tls-router'), '.',
                cwd=ROOT / 'tools/contest/tls-router', env=environment)
        command('go', 'build', '-o', str(work / 'http'), 'testdata/http.go',
                cwd=ROOT / 'tools/contest/tls-router', env=environment)
        for name in ('official', 'dummy'):
            command('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                    '-keyout', str(work / (name + '.key')),
                    '-out', str(work / (name + '.crt')), '-days', '1',
                    '-subj', '/CN=' + ('isuride.xiv.isucon.net' if name == 'official' else 'dummy'),
                    '-addext', 'subjectAltName=DNS:isuride.xiv.isucon.net,DNS:other.xiv.isucon.net',
                    stderr=subprocess.DEVNULL)
        docker('network', 'create', NAME)
        network = True
        subnet = json.loads(docker('network', 'inspect', NAME))[0]['IPAM']['Config'][0]['Subnet']
        addresses = [str(ipaddress.ip_network(subnet)[i]) for i in (2, 3)]
        for index, host in enumerate(('isucon-2', 'isucon-3')):
            config = work / host
            shutil.copytree(ROOT / 'tools/contest/generated/nginx' / host, config, symlinks=True)
            tls = config / 'tls'
            tls.mkdir()
            for source, destination in [('official.crt', '_.xiv.isucon.net.crt'),
                                        ('official.key', '_.xiv.isucon.net.key'),
                                        ('dummy.crt', 'dummy.crt'), ('dummy.key', 'dummy.key')]:
                shutil.copyfile(work / source, tls / destination)
            # Only test addresses and the image's worker user differ from deployment.
            for file in (config / 'tls-listen.conf', config / 'tls-listen-default.conf',
                         config / 'conf.d/upstream.conf', config / 'nginx.conf'):
                text = file.read_text().replace('172.31.7.50', addresses[0])
                text = text.replace('172.31.5.116', addresses[1]).replace('172.31.0.126', addresses[0])
                file.write_text(text.replace('user www-data;', 'user nginx;'))
            logs = work / (host + '-logs')
            logs.mkdir()
            name = NAME + '-' + str(index)
            args = ['run', '-d', '--name', name, '--network', NAME, '--ip', addresses[index],
                    '-p', '127.0.0.1::8443', '-v', str(config) + ':/etc/nginx:ro',
                    '-v', str(ROOT / 'webapp/public') + ':/home/isucon/webapp/public:ro',
                    '-v', str(logs) + ':/var/log/nginx', '-v', str(work) + ':/fixture:ro',
                    '--entrypoint', 'sh']
            script = "nginx -t && exec nginx -g 'daemon off;'"
            if index == 0:
                args += ['-p', '127.0.0.1::443']
                script = (f'/fixture/http & /fixture/tls-router -listen {addresses[0]}:443 '
                          f'-backends {addresses[0]}:8443,{addresses[1]}:8443 & ' + script)
            docker(*args, 'nginx:1.24.0', '-c', script)
            containers.append(name)

        context = ssl.create_default_context(cafile=str(work / 'official.crt'))
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        expected = collections.Counter()

        def request(target, path, *, sni='isuride.xiv.isucon.net', method='GET', body=None,
                    headers=None, stream=False):
            connection = http.client.HTTPSConnection(sni, target, context=context, timeout=5)
            # Preserve SNI/Host while using Docker's ephemeral local mapped port.
            connection._create_connection = lambda address, *a, **kw: socket.create_connection(
                ('127.0.0.1', target), timeout=5)
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            expected[path.split('?', 1)[0]] += 1
            if stream:
                return connection, response
            payload = response.read()
            connection.close()
            return response, payload

        entry = port(containers[0], 443)
        endpoints = [port(container, 8443) for container in containers]
        for container in containers:
            for attempt in range(50):
                if docker('inspect', '--format', '{{.State.Running}}', container) != 'true':
                    raise AssertionError(docker('logs', container))
                if subprocess.run(['docker', 'exec', container, 'test', '-s', '/run/nginx.pid'],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                    break
                time.sleep(0.1)
            assert docker('exec', container, 'nginx', '-t') == ''

        static = (ROOT / 'webapp/public/index.html').read_bytes()
        for sequence in range(8):
            response, payload = request(entry, f'/api/entry-{sequence}')
            assert response.status == 200
        connection, response = request(entry, '/api/keepalive-0', stream=True)
        assert response.status == 200
        response.read()
        original_socket = connection.sock
        connection.request('GET', '/api/keepalive-1')
        response = connection.getresponse()
        assert response.status == 200
        response.read()
        assert connection.sock is original_socket
        expected['/api/keepalive-1'] += 1
        connection.close()
        for target in endpoints + [entry] * 8:
            response, payload = request(target, '/index.html')
            assert response.status == 200 and payload == static
            response, payload = request(target, '/api/echo', method='POST', body='coordinates',
                                        headers={'Cookie': 'chair_session=test'})
            assert response.status == 200
            assert payload == b'POST|isuride.xiv.isucon.net|chair_session=test|coordinates'
            assert response.getheader('X-Measurement-Session-ID') is None
            response, payload = request(target, '/api/internal/_network-check',
                                        headers={'X-Forwarded-For': '127.0.0.1',
                                                 'X-Real-IP': '127.0.0.1'})
            assert response.status == 403

        # The second official SNI resolves to the same HTTP vhost and certificate.
        response, payload = request(entry, '/index.html', sni='other.xiv.isucon.net')
        assert response.status == 200 and payload == static
        # Unknown SNI must still select the original dummy certificate.
        insecure = ssl._create_unverified_context()
        dummy = ssl.PEM_cert_to_DER_cert((work / 'dummy.crt').read_text())
        for target in endpoints + [entry, entry]:
            with socket.create_connection(('127.0.0.1', target), timeout=5) as tcp:
                with insecure.wrap_socket(tcp, server_hostname='unknown.example') as tls:
                    assert tls.getpeercert(binary_form=True) == dummy

        for target in endpoints + [entry, entry]:
            for path in ('/api/app/notification', '/api/chair/notification'):
                start = time.monotonic()
                connection, response = request(target, path, stream=True)
                assert response.status == 200
                assert response.getheader('Content-Type') == 'text/event-stream'
                for event in range(3):
                    assert response.readline() == f'data: {event}\n'.encode()
                    assert response.readline() == b'\n'
                    if event == 0:
                        assert time.monotonic() - start < 0.8, 'first event was buffered'
                assert response.read() == b''
                connection.close()

        # Matcher bypasses the TCP relay and retains the original localhost ACL.
        status = docker('exec', containers[0], 'curl', '-sk', '--resolve',
                        'isuride.xiv.isucon.net:443:127.0.0.1', '-o', '/dev/null',
                        '-w', '%{http_code}', 'https://isuride.xiv.isucon.net/api/internal/_network-check')
        assert status == '404'
        expected['/api/internal/_network-check'] += 1
        # Even a loopback connection to the routed port must be denied.
        status = docker('exec', containers[0], 'curl', '-sk', '--connect-to',
                        f'isuride.xiv.isucon.net:443:{addresses[0]}:8443', '--interface', '127.0.0.1', '-o', '/dev/null',
                        '-w', '%{http_code}', 'https://isuride.xiv.isucon.net/api/internal/_network-check')
        assert status == '403'
        expected['/api/internal/_network-check'] += 1

        time.sleep(1.2)
        all_rows = []
        keepalive_hosts = set()
        raw = work / 'runs' / '20000101-000000' / 'raw'
        raw.mkdir(parents=True)
        for index, host in enumerate(('isucon-2', 'isucon-3')):
            rows = [json.loads(line) for line in (work / (host + '-logs/access.log')).read_text().splitlines()]
            assert rows and any(row['uri'] == '/index.html' for row in rows)
            assert any(row['uri'] == '/api/echo' for row in rows)
            assert any(row['uri'].endswith('/notification') for row in rows)
            routed = [row for row in rows if row['uri'].startswith('/api/entry-')]
            assert len(routed) == 4, 'entry connections were not balanced across both TLS endpoints'
            if any(row['uri'].startswith('/api/keepalive-') for row in rows):
                assert sum(row['uri'].startswith('/api/keepalive-') for row in rows) == 2
                keepalive_hosts.add(host)
            assert all(row['upstream_addr'] in ('', addresses[0] + ':8080') for row in rows)
            all_rows += rows
            shutil.copyfile(work / (host + '-logs/access.log'), raw / f'access-{host}.log')
            print(f'{host}: static/API/SSE and original JSON access log OK ({len(rows)} requests)')
        assert collections.Counter(row['uri'] for row in all_rows) == expected, 'missing or duplicate HTTP log entries'
        assert len(keepalive_hosts) == 1, 'keepalive connection changed its TLS endpoint'
        # Use the existing per-ingress schema without modifying shared raw or the analysis DB.
        schema = (ROOT / 'tools/analysis/schema/access-log.sql').read_text()
        sql = ("SET VARIABLE run_glob = '" + str(raw.parent).replace("'", "''") + "';\n" + schema +
               '\nselect ingress_host,sum(requests) from http_traffic_buckets group by ingress_host order by ingress_host;')
        aggregates = command('duckdb', '-csv', '-noheader', ':memory:', '-c', sql)
        counts = dict(line.split(',') for line in aggregates.splitlines())
        assert set(counts) == {'isucon-2', 'isucon-3'}
        assert sum(map(int, counts.values())) == len(all_rows)
        print('Existing DuckDB per-ingress schema: both hosts and total request count OK')
        print('TLS passthrough, both endpoints, SSE order/flush, internal ACL and single HTTP accounting: PASS')
    finally:
        for container in containers:
            subprocess.run(['docker', 'rm', '-f', container], stdout=subprocess.DEVNULL, check=False)
        if network:
            subprocess.run(['docker', 'network', 'rm', NAME], stdout=subprocess.DEVNULL, check=False)
