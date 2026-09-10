#!/usr/bin/env python3
"""Read JSON or gzip JSON evidence; --summary omits verbose per-connection paths."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('artifact', type=Path)
parser.add_argument('--summary', action='store_true')
args = parser.parse_args()
blob = args.artifact.read_bytes()
raw = gzip.decompress(blob) if args.artifact.suffix == '.gz' else blob
data = json.loads(raw)
if args.summary:
    summary = {k:v for k,v in data.items() if k not in ['stdout','stderr','events']}
    summary['sha256'] = hashlib.sha256(blob).hexdigest()
    summary['uncompressed_sha256'] = hashlib.sha256(raw).hexdigest()
    summary['events'] = []
    for event in data.get('events', []):
        if event.get('phase') == 'progress':
            continue
        item = {k:v for k,v in event.items() if k not in ['client','server']}
        for peer in ['client','server']:
            if peer in event:
                report = event[peer]
                item[peer] = {'paths':len(report['paths']), 'healthy':sum(p['healthy'] for p in report['paths']), 'completion_queues':report.get('completion_queues')}
        summary['events'].append(item)
    data = summary
print(json.dumps(data,indent=2))
