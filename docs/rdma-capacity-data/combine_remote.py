#!/usr/bin/env python3
"""Combine two complete identical-workload groups, preserving every sample and group."""
import argparse
import datetime
import json
import os
from pathlib import Path
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--initial', required=True, type=Path)
p.add_argument('--confirmation', required=True, type=Path)
p.add_argument('--output', required=True, type=Path)
a = p.parse_args()
groups = [(name, path, json.loads(path.read_text())) for name, path in [('initial_extended', a.initial), ('confirmation', a.confirmation)]]
initial = groups[0][2]
result = {'kind': 'remote_memory', 'generated_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'method': 'All samples from two separately started alternating-order groups; no exclusions. Pair ratios are computed within original group/pair, then pooled.',
          'environment': initial['environment'], 'binaries': initial['binaries'], 'pin_library': initial['pin_library'],
          'groups': [], 'runs': []}
for name, path, data in groups:
    assert data['kind'] == 'remote_memory'
    assert data['environment'] == initial['environment']
    assert data['placement'] == initial['placement']
    assert data['pin_library']['sha256'] == initial['pin_library']['sha256']
    assert all(data['binaries'][v]['sha256'] == initial['binaries'][v]['sha256'] for v in ['baseline','current'])
    assert len(data['runs']) == data['pairs'] * 2 and 'summary' in data
    ref = os.path.relpath(path.resolve(), a.output.resolve().parent)
    result['groups'].append({'name':name,'artifact':ref,'pairs':data['pairs'],
                            'started_utc':data['started_utc'],'completed_utc':data['completed_utc'],'summary':data['summary']})
    for index, run in enumerate(data['runs']):
        assert run['returncode'] == 0 and 'metrics' in run
        result['runs'].append({'group':name,'pair':run['pair'],'version':run['version'],
                              'artifact':ref,'raw_run_index':index,'metrics':run['metrics']})
summary = {}
for metric in result['runs'][0]['metrics']:
    values = {v:[r['metrics'][metric] for r in result['runs'] if r['version']==v] for v in ['baseline','current']}
    b,c = map(statistics.median, (values['baseline'],values['current']))
    summary[metric] = {'baseline_median':b,'current_median':c,'median_change_percent':(c/b-1)*100,
        'paired_ratio_median_change_percent':(statistics.median(c/b for b,c in zip(values['baseline'],values['current']))-1)*100,
        'baseline_min_max':[min(values['baseline']),max(values['baseline'])],
        'current_min_max':[min(values['current']),max(values['current'])]}
result['pairs'] = sum(g['pairs'] for g in result['groups'])
result['summary'] = summary
a.output.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(summary,indent=2))
