#!/usr/bin/env python3
"""Run one capacity probe; retain successful, failed, and timed-out output."""
import argparse
import datetime
import gzip
import hashlib
import json
import os
import pathlib
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--timeout', type=int, default=180)
parser.add_argument('--pin-library', default='/tmp/ruapc-qp-registry-bench/pin_threads.so')
parser.add_argument('--env', action='append', default=[])
args = parser.parse_args()
options = dict(pair.split('=', 1) for pair in args.env)
options['RUAPC_BENCH_RDMA_DEVICE'] = 'mlx5_0'
env = dict(os.environ, **options)
command = ['numactl', '--physcpubind=0-10', '--membind=0', 'env',
           f'LD_PRELOAD={args.pin_library}', args.binary]
started_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
start = time.monotonic()
timed_out = False
try:
    process = subprocess.run(command, env=env, capture_output=True, text=True,
                             timeout=args.timeout)
    stdout, stderr, returncode = process.stdout, process.stderr, process.returncode
except subprocess.TimeoutExpired as error:
    timed_out = True
    def as_text(value):
        return value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
    stdout, stderr, returncode = as_text(error.stdout), as_text(error.stderr), None
events = []
parse_errors = []
for line_number, line in enumerate(stdout.splitlines(), 1):
    if line.startswith('{'):
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as error:
            parse_errors.append({'line': line_number, 'error': str(error)})
result = {
    'started_utc': started_utc,
    'completed_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'binary': args.binary,
    'binary_sha256': hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest(),
    'pin_library_sha256': hashlib.sha256(pathlib.Path(args.pin_library).read_bytes()).hexdigest(),
    'environment': options,
    'command': command,
    'returncode': returncode,
    'timed_out': timed_out,
    'timeout_seconds': args.timeout,
    'json_parse_errors': parse_errors,
    'elapsed_s': time.monotonic() - start,
    'stdout': stdout,
    'stderr': stderr,
    'events': events,
}
raw_bytes = (json.dumps(result, indent=2) + '\n').encode()
artifact_path = pathlib.Path(args.output)
artifact_bytes = gzip.compress(raw_bytes, mtime=0) if artifact_path.suffix == '.gz' else raw_bytes
artifact_path.write_bytes(artifact_bytes)
print(json.dumps({'artifact': str(artifact_path), 'sha256': hashlib.sha256(artifact_bytes).hexdigest(), 'uncompressed_sha256': hashlib.sha256(raw_bytes).hexdigest()}), flush=True)
print(json.dumps({key: value for key, value in result.items()
                  if key not in ['stdout', 'stderr', 'events']}), flush=True)
for event in result['events']:
    if event['phase'] == 'progress':
        continue
    # Full path reports stay in the artifact; keep the terminal summary small.
    summary = {key: value for key, value in event.items() if key not in ['client', 'server']}
    for peer in ['client', 'server']:
        if peer in event:
            summary[f'{peer}_path_count'] = len(event[peer]['paths'])
            summary[f'{peer}_completion_queues'] = event[peer].get('completion_queues')
    print(json.dumps(summary), flush=True)
if returncode or timed_out:
    print(stderr, flush=True)
