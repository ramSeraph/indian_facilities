#!/usr/bin/env -S uv run --with requests --with beautifulsoup4
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests", "beautifulsoup4"]
# ///
"""Fetch SOI CORS station locations and save as GeoJSONL."""

import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://corswebmap.surveyofindia.gov.in/?output=embed"
API_URL = "https://corswebmap.surveyofindia.gov.in/get_stations_soi_api/?state="
OUTPUT_FILE = Path(__file__).parent / "data" / "SOI_CORS_locations.geojsonl"


def get_api_token() -> str:
    """Extract api-token from meta tag on the CORS website."""
    response = requests.get(BASE_URL, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    meta_tag = soup.find("meta", attrs={"name": "api-token"})
    if not meta_tag or not meta_tag.get("content"):
        raise ValueError("Could not find api-token meta tag")
    return meta_tag["content"]


def fetch_stations(token: str) -> dict:
    """Fetch CORS stations GeoJSON using the bearer token."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(API_URL, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def geojson_to_geojsonl(geojson: dict, output_path: Path) -> int:
    """Convert GeoJSON FeatureCollection to GeoJSONL (one feature per line)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    features = geojson.get("features", [])
    with output_path.open("w") as f:
        for feature in features:
            f.write(json.dumps(feature, separators=(",", ":")) + "\n")
    return len(features)


def main():
    print("Fetching API token...")
    token = get_api_token()
    print(f"Got token: {token[:20]}...")

    print("Fetching CORS stations...")
    geojson = fetch_stations(token)

    print(f"Converting to GeoJSONL: {OUTPUT_FILE}")
    count = geojson_to_geojsonl(geojson, OUTPUT_FILE)
    print(f"Saved {count} features to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
