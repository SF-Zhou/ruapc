#!/usr/bin/env python3
"""Run identical RDMA benchmark binaries serially in alternating paired order."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse_echo(output):
    group = None
    data = {}
    for line in output.splitlines():
        if line in ('RDMA', 'RDMA (2 endpoints)'):
            group = line
        match = re.search(r'serial\s+(\d+)B:\s+([\d.]+) us/op', line)
        if match:
            data[f'{group}/serial_{match[1]}B_us'] = float(match[2])
        match = re.search(r'concurrent\s+(\d+)B:\s+([\d.]+) kops/s \|\s+([\d.]+) us/op \((\d+) tasks\)', line)
        if match:
            data[f'{group}/concurrent_{match[4]}_kops'] = float(match[2])
            data[f'{group}/concurrent_{match[4]}_us'] = float(match[3])
    if len(data) != 10:
        raise ValueError(f'expected 10 echo metrics, got {data}')
    return data


def parse_remote(output):
    data = {}
    for line in output.splitlines():
        match = re.search(r'(remote_\w+)\s+(\d+) KiB \|\s+(\d+) iters \|\s+([\d.]+) us/op \|\s+([\d.]+) MiB/s \| verified', line)
        if match:
            data[f'{match[1]}/{match[2]}KiB_us'] = float(match[4])
            data[f'{match[1]}/{match[2]}KiB_mibps'] = float(match[5])
    if len(data) != 8:
        raise ValueError(f'expected 8 remote memory metrics, got {data}')
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--current', type=Path, required=True)
    parser.add_argument('--pin-library', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--pairs', type=int, default=9)
    parser.add_argument('--kind', choices=['echo', 'remote_memory'], default='echo')
    parser.add_argument('--serial', type=int, default=50000)
    parser.add_argument('--warmup', type=int, default=5000)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    env_opts = {
        'TOKIO_WORKER_THREADS': '8' if args.kind == 'echo' else '4',
        'RUAPC_BENCH_TRANSPORT': 'RDMA',
        'RUAPC_BENCH_RDMA_DEVICE': 'mlx5_0',
        'RUAPC_BENCH_WARMUP_ITERS': str(args.warmup),
        'RUAPC_BENCH_SERIAL_ITERS': str(args.serial),
    }
    env = dict(os.environ, **env_opts)
    result = {
        'kind': args.kind,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'environment': env_opts,
        'placement': {'runtime_workers': list(range(8 if args.kind == 'echo' else 4)), 'main': 8, 'pollers': [9, 10], 'numa_memory_node': 0},
        'binaries': {version: {'path': str(path), 'sha256': sha256(path)} for version, path in [('baseline', args.baseline), ('current', args.current)]},
        'pin_library': {'path': str(args.pin_library), 'sha256': sha256(args.pin_library)},
        'machine': {'uname': subprocess.check_output(['uname', '-a'], text=True).strip(), 'cpu': next(line.strip().split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), 'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip()},
        'pairs': args.pairs,
        'runs': [],
    }
    def save():
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    for pair in range(1, args.pairs + 1):
        order = ['baseline', 'current'] if pair % 2 else ['current', 'baseline']
        for version in order:
            binary = getattr(args, version).resolve()
            command = ['numactl', '--physcpubind=0-10', '--membind=0', 'env', f'LD_PRELOAD={args.pin_library.resolve()}', str(binary)]
            print(f'pair {pair}/{args.pairs}: {version}', flush=True)
            start = time.monotonic()
            process = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
            run = {'pair': pair, 'version': version, 'command': command, 'elapsed_s': time.monotonic() - start, 'returncode': process.returncode, 'stdout': process.stdout, 'stderr': process.stderr}
            result['runs'].append(run)
            save()
            if process.returncode or 'skipped:' in process.stdout:
                raise RuntimeError(f'benchmark failed: {run}')
            expected_cpus = list(range(8 if args.kind == 'echo' else 4)) + [8, 9, 10]
            actual_cpus = sorted(int(cpu) for cpu in re.findall(r'pin_threads: .* -> cpu=(\d+)', process.stderr))
            if actual_cpus != expected_cpus:
                raise RuntimeError(f'wrong thread placement: {actual_cpus}')
            run['metrics'] = (parse_echo if args.kind == 'echo' else parse_remote)(process.stdout)
            save()
    summary = {}
    for metric in result['runs'][0]['metrics']:
        baseline = [r['metrics'][metric] for r in result['runs'] if r['version'] == 'baseline']
        current = [r['metrics'][metric] for r in result['runs'] if r['version'] == 'current']
        ratios = [c / b for b, c in zip(baseline, current)]
        med_b, med_c = statistics.median(baseline), statistics.median(current)
        summary[metric] = {'baseline_median': med_b, 'current_median': med_c, 'median_change_percent': (med_c / med_b - 1) * 100, 'paired_ratio_median_change_percent': (statistics.median(ratios) - 1) * 100, 'baseline_min_max': [min(baseline), max(baseline)], 'current_min_max': [min(current), max(current)]}
    result['summary'] = summary
    result['completed_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
    print(json.dumps(summary, indent=2), flush=True)

if __name__ == '__main__':
    main()
