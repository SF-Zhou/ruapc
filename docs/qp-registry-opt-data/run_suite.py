#!/usr/bin/env python3
"""Run the unchanged paired workloads serially with a fresh output directory."""
import argparse
from pathlib import Path
import shlex
import subprocess
import sys

here = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary-dir', type=Path,
                    default=Path('/tmp/ruapc-qp-registry-opt-bench'))
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--pairs', type=int, default=5)
parser.add_argument('--groups', nargs='+', choices=['rpc', 'echo', 'remote'],
                    default=['rpc', 'echo', 'remote'])
parser.add_argument('--dry-run', action='store_true')
args = parser.parse_args()
assert args.pairs > 0
assert len(set(args.groups)) == len(args.groups)
pin = args.binary_dir / 'pin_threads.so'
paired = here.parent / 'rdma-capacity-data' / 'run_paired.py'
commands = {
    'rpc': [sys.executable, str(here.parent / 'qp-registry-data' / 'run_multiconn.py'),
            '--baseline', str(args.binary_dir / 'capacity-rpc-baseline'),
            '--current', str(args.binary_dir / 'capacity-rpc-current'),
            '--pin-library', str(pin), '--output-dir', str(args.output_dir),
            '--pairs', str(args.pairs), '--connections', '128', '256'],
    'echo': [sys.executable, str(paired),
             '--baseline', str(args.binary_dir / 'echo-baseline-long'),
             '--current', str(args.binary_dir / 'echo-current-long'),
             '--pin-library', str(pin), '--output', str(args.output_dir / 'echo-long-paired.json'),
             '--pairs', str(args.pairs)],
    'remote': [sys.executable, str(paired),
               '--baseline', str(args.binary_dir / 'remote-memory-baseline'),
               '--current', str(args.binary_dir / 'remote-memory-current'),
               '--pin-library', str(pin),
               '--output', str(args.output_dir / 'remote-memory-extended-paired.json'),
               '--pairs', str(args.pairs), '--kind', 'remote_memory',
               '--warmup', '1000', '--serial', '10000'],
}
outputs = {'rpc': 'rpc-paired.json', 'echo': 'echo-long-paired.json',
           'remote': 'remote-memory-extended-paired.json'}
for group in args.groups:
    assert not (args.output_dir / outputs[group]).exists(), 'Use a new output directory; retain prior samples.'
    if group == 'rpc':
        assert not any(args.output_dir.glob('rpc-*.json.gz')), 'Retain raw files from interrupted runs.'
for group in args.groups:
    print(shlex.join(commands[group]), flush=True)
    if not args.dry_run:
        subprocess.run(commands[group], check=True)
