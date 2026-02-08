#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""
Collate all district GeoJSONL files into a single export file.

Usage:
    uv run export.py
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
EXPORT_DIR = DATA_DIR / "export"

SPECIAL_STYLE = "#icon-1899-0288D1"


def classify_station(feature: dict) -> str:
    """Classify station as 'special' or 'regular' based on styleUrl."""
    style = feature.get("properties", {}).get("styleUrl", "")
    return "special" if style == SPECIAL_STYLE else "regular"


def collate_all() -> list[dict]:
    """Collate all district GeoJSONL files into a single list."""
    features = []
    
    for geojsonl_file in sorted(DATA_DIR.glob("*.geojsonl")):
        print(f"Reading {geojsonl_file.name}...")
        with open(geojsonl_file, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    feature = json.loads(line)
                    # Add classification
                    feature["properties"]["station_type"] = classify_station(feature)
                    features.append(feature)
    
    return features


def main():
    features = collate_all()
    
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = EXPORT_DIR / "MP_Police_Stations.geojsonl"
    
    with open(output_file, "w", encoding="utf-8") as f:
        for feature in features:
            f.write(json.dumps(feature, ensure_ascii=False) + "\n")
    
    # Count by type
    special = sum(1 for f in features if f["properties"]["station_type"] == "special")
    regular = len(features) - special
    
    print(f"\nExported {len(features)} stations to {output_file}")
    print(f"  Special: {special}")
    print(f"  Regular: {regular}")


if __name__ == "__main__":
    main()
