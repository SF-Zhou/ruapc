#!/usr/bin/env python3
"""Collect phase timing and process CPU diagnostics without replacing performance samples."""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline', required=True)
p.add_argument('--current', required=True)
p.add_argument('--pin-library', required=True)
p.add_argument('--output-dir', required=True, type=Path)
p.add_argument('--pairs', type=int, default=5)
a = p.parse_args()
a.output_dir.mkdir(parents=True, exist_ok=True)
runner = Path(__file__).with_name('run_capacity.py')
output = a.output_dir / 'remote-diagnostic.json'
data = {'pairs': a.pairs, 'iterations': 10000, 'warmup': 1000,
        'description': 'Separate one-MiB directions; full verification retained. Remote await includes admission, DMA completion and internal RPC, not pure hardware DMA time.',
        'binaries': {v: {'path': getattr(a,v), 'sha256': hashlib.sha256(Path(getattr(a,v)).read_bytes()).hexdigest()} for v in ['baseline','current']},
        'runs': []}
def save():
    output.write_text(json.dumps(data, indent=2) + '\n')
for direction in ['read', 'write']:
    for pair in range(1, a.pairs + 1):
        for version in (['baseline','current'] if pair % 2 else ['current','baseline']):
            print(f'{direction} pair {pair}/{a.pairs}: {version}', flush=True)
            artifact = f'remote-diagnostic-{direction}-{version}-{pair}.json'
            command = [sys.executable,str(runner),'--binary',getattr(a,version),'--pin-library',a.pin_library,'--output',str(a.output_dir/artifact),
                       '--env','RUAPC_BENCH_TRANSPORT=RDMA','--env','RUAPC_BENCH_SERIAL_ITERS=10000','--env','RUAPC_BENCH_WARMUP_ITERS=1000','--env',f'RUAPC_DIAG_DIRECTION={direction}']
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            done = subprocess.run(command,capture_output=True,text=True)
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            assert done.returncode == 0, done.stdout + done.stderr
            raw = json.loads((a.output_dir/artifact).read_text())
            assert raw['returncode'] == 0 and not raw['timed_out'], artifact
            metrics = next(e for e in raw['events'] if e['phase']=='diagnostic')
            data['runs'].append({'direction':direction,'pair':pair,'version':version,'artifact':artifact,'metrics':metrics,
                'all_phases_process_user_cpu_s':after.ru_utime-before.ru_utime,
                'all_phases_process_system_cpu_s':after.ru_stime-before.ru_stime,
                'all_phases_voluntary_context_switches':after.ru_nvcsw-before.ru_nvcsw,
                'all_phases_involuntary_context_switches':after.ru_nivcsw-before.ru_nivcsw})
            save()
summary={}
for direction in ['read','write']:
    for metric in ['wall_us_per_op','verify_us_per_op','remote_await_us_per_op']:
        values={v:[r['metrics'][metric] for r in data['runs'] if r['direction']==direction and r['version']==v] for v in ['baseline','current']}
        b,c=map(statistics.median,(values['baseline'],values['current']))
        summary[f'{direction}/{metric}']={'baseline_median':b,'current_median':c,'median_change_percent':(c/b-1)*100,
             'paired_ratio_median_change_percent':(statistics.median(c/b for b,c in zip(values['baseline'],values['current']))-1)*100,
             'baseline_min_max':[min(values['baseline']),max(values['baseline'])],'current_min_max':[min(values['current']),max(values['current'])]}
data['summary']=summary
save()
print(json.dumps(summary,indent=2),flush=True)
