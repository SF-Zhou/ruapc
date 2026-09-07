#!/usr/bin/env python3
"""Summarize every paired sample, keeping both values and all paired changes."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--directory', type=Path, default=Path(__file__).parent)
args = parser.parse_args()
result = {
    'change_definition': '100 * (current / baseline - 1); lower is better for latency, higher for throughput',
    'aggregation': 'Each pair has equal weight; no samples or outliers are removed.',
    'groups': {},
}


def summarize_pairs(pairs):
    baseline = [pair['baseline'] for pair in pairs]
    current = [pair['current'] for pair in pairs]
    changes = [100 * (c / b - 1) for b, c in zip(baseline, current)]
    for pair, change in zip(pairs, changes):
        pair['change_percent'] = change
    return {
        'baseline_median': statistics.median(baseline),
        'current_median': statistics.median(current),
        'paired_change_median_percent': statistics.median(changes),
        'paired_change_min_max_percent': [min(changes), max(changes)],
        'baseline_min_max': [min(baseline), max(baseline)],
        'current_min_max': [min(current), max(current)],
        'samples': pairs,
    }


for filename in ['echo-long-paired.json', 'rpc-paired.json',
                 'remote-memory-extended-paired.json']:
    artifact = args.directory / filename
    blob = artifact.read_bytes()
    data = json.loads(blob)
    grouped = {}
    if filename == 'rpc-paired.json':
        for run in data['runs']:
            for tasks, value in run['qps'].items():
                metric = f"{run['connections']}_connections/{tasks}_tasks_qps"
                pairs = grouped.setdefault(metric, {})
                pair = pairs.setdefault(run['pair'], {})
                assert run['version'] not in pair
                pair[run['version']] = value
    else:
        for run in data['runs']:
            for metric, value in run['metrics'].items():
                pairs = grouped.setdefault(metric, {})
                pair = pairs.setdefault(run['pair'], {})
                assert run['version'] not in pair
                pair[run['version']] = value
    metrics = {}
    for metric, pairs in grouped.items():
        assert sorted(pairs) == list(range(1, data['pairs'] + 1)), (filename, metric)
        assert all(set(pair) == {'baseline', 'current'} for pair in pairs.values())
        metrics[metric] = summarize_pairs([
            {'pair': number, **pair} for number, pair in sorted(pairs.items())
        ])
    result['groups'][filename] = {
        'artifact_sha256': hashlib.sha256(blob).hexdigest(),
        'pairs': data['pairs'],
        'metrics': metrics,
    }

output = args.directory / 'paired-summary.json'
output.write_text(json.dumps(result, indent=2) + '\n')
print(output)
