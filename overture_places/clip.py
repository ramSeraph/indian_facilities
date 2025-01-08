import json

from pathlib import Path
from shapely.geometry import shape
from shapely.prepared import prep


def load_india_shape():
    data = json.loads(Path('data/india-composite.geojson').read_text())
    geom = data['features'][0]['geometry']
    return prep(shape(geom))


india_shape = load_india_shape()

with open('data/overture_places_india.geojsonl', 'w') as outf:
    with open('data/overture_places_india_bbox.geojsonl', 'r') as f:
        for line in f:
            feat = json.loads(line)
            geom = feat['geometry']
            s = shape(geom)
            if not india_shape.intersects(s):
                continue
            outf.write(line)
