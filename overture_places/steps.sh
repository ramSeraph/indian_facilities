#!/bin/sh

mkdir data
cd data
wget https://raw.githubusercontent.com/datameet/maps/master/Country/india-composite.geojson
aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2023-12-14-alpha.0/theme=places/ .
cd -

# clips the data to indian bbox and converts to gpkg
# gpkg because conversion geojsonl crashes duckdb
duckdb -c '.read extract_bbox.sql'

# convert gpkg to geojsonl
ogr2ogr -f GeoJSONSeq data/overture_places_india_bbox.geojsonl data/overture_places_india_bbox.gpkg

# clip to actual India shape
# attempted to do this in extract.sql itself.. but failed miserably
pip install shapely
python clip.py

# tile the data
tippecanoe -P -zg  -J filter.json -o data/overture_places_india.mbtiles --simplify-only-low-zooms --drop-densest-as-needed --extend-zooms-if-still-dropping -l overture-places-india -n overture-places-india -A 'Source: <a href="https://overturemaps.org/overture-december-2023-release-notes/" target="_blank" rel="noopener noreferrer">Overture Maps</a>' data/overture_places_india.geojsonl

# convert to pmtiles
pmtiles convert data/overture_places_india.mbtiles data/overture_places_india.pmtiles
