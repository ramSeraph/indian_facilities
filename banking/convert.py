import json
from pathlib import Path

def clean(s):
    s = s.replace('\\', '')
    return s

for p in Path('data/').glob('*.jsonl'):
    out_name = p.name[:-6] + '_2025.geojsonl'
    out = f'data/RBI_Banks_{out_name}'
    print(out)
    with open(out, 'w') as of:
        with open(p, 'r') as f:
            count = 0
            for line in f:
                item = json.loads(line)
                lon = item.pop('longitude')
                lat = item.pop('lattitude')
                try:
                    lon = float(clean(lon))
                    lat = float(clean(lat))
                except Exception:
                    print(count, line)
                    count += 1
                    continue
                feat = { 'type': 'Feature', 'geometry': { 'type': 'Point', 'coordinates': [lon, lat] }, 'properties': item }
                of.write(json.dumps(feat))
                of.write('\n')
                count += 1

