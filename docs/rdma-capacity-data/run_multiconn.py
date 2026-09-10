#!/usr/bin/env python3
"""Compare two fixed many-stripe RPC binaries in alternating paired order."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', required=True)
parser.add_argument('--current', required=True)
parser.add_argument('--pin-library', required=True)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--pairs', type=int, default=5)
parser.add_argument('--connections', type=int, nargs='+', default=[128, 256])
args = parser.parse_args()
args.output_dir.mkdir(parents=True, exist_ok=True)
runner = Path(__file__).with_name('run_capacity.py')
result = {'pairs': args.pairs, 'connections': args.connections,
          'concurrent_total_ops': 2560000, 'runs': [],
          'binaries': {version: {'path': str(Path(binary).resolve()),
                        'sha256': hashlib.sha256(Path(binary).read_bytes()).hexdigest()}
                       for version, binary in [('baseline', args.baseline), ('current', args.current)]}}
assert result['binaries']['baseline']['sha256'] != result['binaries']['current']['sha256']
output = args.output_dir / 'rpc-paired.json'
def save():
    output.write_text(json.dumps(result, indent=2) + '\n')
for count in args.connections:
    for pair in range(1, args.pairs + 1):
        for version in (['baseline', 'current'] if pair % 2 else ['current', 'baseline']):
            print(f'{count} connections pair {pair}/{args.pairs}: {version}', flush=True)
            artifact = f'rpc-{count}-{version}-{pair}.json'
            command = [sys.executable, str(runner), '--binary', getattr(args, version),
                       '--pin-library', args.pin_library, '--output', str(args.output_dir / artifact),
                       '--env', f'RUAPC_CAPACITY_CONNECTIONS={count}',
                       '--env', 'RUAPC_CAPACITY_TOTAL_OPS=2560000']
            completed = subprocess.run(command, capture_output=True, text=True)
            if completed.returncode:
                raise RuntimeError(completed.stdout + completed.stderr)
            raw = json.loads((args.output_dir / artifact).read_text())
            entry = {'connections': count, 'pair': pair, 'version': version, 'artifact': artifact}
            result['runs'].append(entry)
            save()
            assert raw['returncode'] == 0 and not raw['timed_out'], artifact
            connected = next(event for event in raw['events'] if event['phase'] == 'connected')
            assert connected['error'] is None, artifact
            assert len(connected['client']['paths']) == count, artifact
            assert len(connected['server']['paths']) == count, artifact
            assert all(path['healthy'] for peer in ['client','server'] for path in connected[peer]['paths']), artifact
            metrics = {str(event['tasks']): event['qps'] for event in raw['events'] if event['phase'] == 'throughput'}
            assert set(metrics) == {'64', '1024'}, artifact
            assert any(event['phase'] == 'stopped' for event in raw['events']), artifact
            entry['qps'] = metrics
            save()
summary = {}
for count in args.connections:
    for tasks in ['64', '1024']:
        values = {version: [entry['qps'][tasks] for entry in result['runs']
                           if entry['connections'] == count and entry['version'] == version]
                  for version in ['baseline', 'current']}
        baseline, current = map(statistics.median, (values['baseline'], values['current']))
        summary[f'{count}_connections/{tasks}_tasks'] = {
            'baseline_median_qps': baseline, 'current_median_qps': current,
            'median_change_percent': (current / baseline - 1) * 100,
            'paired_ratio_median_change_percent': (statistics.median(c / b for b, c in zip(values['baseline'], values['current'])) - 1) * 100,
            'baseline_min_max': [min(values['baseline']), max(values['baseline'])],
            'current_min_max': [min(values['current']), max(values['current'])],
        }
result['summary'] = summary
save()
print(json.dumps(summary, indent=2), flush=True)
