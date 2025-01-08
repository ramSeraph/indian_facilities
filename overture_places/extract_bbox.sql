INSTALL spatial;
LOAD spatial;
SET memory_limit='2GB';
PRAGMA enable_progress_bar;
CREATE TABLE india_file AS SELECT * FROM ST_Read('data/india-composite.geojson');
CREATE VIEW overture_places AS SELECT * FROM read_parquet('data/type=place/*', filename=true, hive_partitioning=1);
COPY (
  SELECT
    id,
    updatetime,
    version,
    round(confidence, 2) AS confidence,
    JSON(names) AS names,
    JSON(categories) AS categories,
    JSON(websites) AS websites,
    JSON(socials) AS socials,
    JSON(emails) AS emails,
    JSON(phones) AS phones,
    JSON(brand) AS brand,
    JSON(addresses) AS addresses,
    JSON(sources) AS sources,
    ST_GeomFromWKB(geometry) AS geom
  FROM overture_places
  INNER JOIN india_file
  ON
    bbox.minX > ST_XMin(india_file.geom) AND
    bbox.maxX < ST_XMax(india_file.geom) AND
    bbox.minY > ST_YMin(india_file.geom) AND
    bbox.maxY < ST_YMax(india_file.geom)
) TO 'data/overture_places_india_bbox.gpkg' WITH (FORMAT gdal, DRIVER 'gpkg');
