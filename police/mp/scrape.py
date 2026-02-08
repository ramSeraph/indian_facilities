#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "httpx",
#     "beautifulsoup4",
#     "kml2geojson",
# ]
# ///
"""
Scrape MP Police district/battalion/PTS/GRP websites from mppolice.gov.in

Usage:
    uv run scrape.py
"""

import io
import re
import zipfile
import json
from pathlib import Path

import httpx
import kml2geojson
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "data"
STATION_URLS_FILE = DATA_DIR / "station_urls.json"


def _scrape_from_web() -> dict:
    """Scrape police station websites from the MP Police homepage."""
    url = "https://www.mppolice.gov.in/en"
    
    print(f"Fetching {url}...")
    response = httpx.get(url, timeout=30, follow_redirects=True)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find the dropdown with district websites
    select = soup.find("select", {"id": "ctl00_CPH_ddlDistrict"})
    
    if not select:
        select = soup.find("select", {"name": "District"})
    
    if not select:
        raise ValueError("Could not find the district dropdown on the page")
    
    police_stations = {}
    
    for option in select.find_all("option"):
        url_value = option.get("value", "").strip()
        name = option.get_text(strip=True)
        
        if not url_value or url_value == "" or name == "--Select--":
            continue
        
        police_stations[name] = url_value
    
    return police_stations


def get_station_urls() -> dict:
    """Get police station URLs. Loads from cache if available, otherwise scrapes."""
    if STATION_URLS_FILE.exists():
        print(f"Loading from {STATION_URLS_FILE}...")
        with open(STATION_URLS_FILE, encoding="utf-8") as f:
            return json.load(f)
    
    data = _scrape_from_web()
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATION_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Scraped {len(data)} police station websites")
    print(f"Output saved to {STATION_URLS_FILE}")
    
    return data


def extract_google_maps_id(html: str) -> str | None:
    """Extract Google Maps embed ID from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    iframe = soup.find("iframe", src=re.compile(r"google\.com/maps/d"))
    if not iframe:
        return None
    
    src = iframe.get("src", "")
    match = re.search(r"mid=([a-zA-Z0-9_-]+)", src)
    return match.group(1) if match else None


def normalize_district_name(name: str) -> str:
    """Convert district name to a valid filename."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def kml_to_geojson_features(kml_content: bytes, district_name: str, district_url: str) -> list[dict]:
    """Convert KML content to a list of GeoJSON Features with all properties."""
    kml_str = kml_content.decode("utf-8")
    
    # kml2geojson returns a list of FeatureCollections (one per Document/Folder)
    feature_collections = kml2geojson.main.convert(io.StringIO(kml_str))
    
    features = []
    for fc in feature_collections:
        for feature in fc.get("features", []):
            props = feature["properties"]
            
            # Remove unwanted properties
            props.pop("description", None)
            
            # Add district metadata
            props["district"] = district_name
            props["district_url"] = district_url
            
            # Fix mobile numbers stored in scientific notation
            if "Mobile" in props:
                try:
                    props["Mobile"] = str(int(float(props["Mobile"])))
                except (ValueError, TypeError):
                    pass
            
            features.append(feature)
    
    return features


def get_district_geojsonl_path(district_name: str) -> Path:
    """Get the path to the GeoJSONL file for a district."""
    return DATA_DIR / f"{normalize_district_name(district_name)}.geojsonl"


def scrape_district_police_stations(district_name: str, district_url: str, force: bool = False) -> list[dict]:
    """Scrape police station locations from a district website.
    
    Returns a list of GeoJSON Features. Results are cached to a .geojsonl file.
    """
    geojsonl_path = get_district_geojsonl_path(district_name)
    
    # Skip if already cached
    if not force and geojsonl_path.exists():
        print(f"Skipping {district_name} (cached)")
        return []
    
    print(f"Scraping {district_name} from {district_url}...")
    
    try:
        response = httpx.get(district_url, timeout=30, follow_redirects=True)
        response.raise_for_status()
    except Exception as e:
        print(f"  Error fetching {district_url}: {e}")
        return []
    
    map_id = extract_google_maps_id(response.text)
    if not map_id:
        print(f"  No Google Maps embed found for {district_name}")
        return []
    
    print(f"  Found map ID: {map_id}")
    
    # Download KML from Google Maps
    kml_url = f"https://www.google.com/maps/d/kml?mid={map_id}"
    try:
        kml_response = httpx.get(kml_url, timeout=30, follow_redirects=True)
        kml_response.raise_for_status()
    except Exception as e:
        print(f"  Error fetching KML: {e}")
        return []
    
    # KMZ is a zip file containing doc.kml
    try:
        with zipfile.ZipFile(io.BytesIO(kml_response.content)) as zf:
            kml_content = zf.read("doc.kml")
    except Exception as e:
        print(f"  Error extracting KML: {e}")
        return []
    
    features = kml_to_geojson_features(kml_content, district_name, district_url)
    
    # Save to GeoJSONL cache
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(geojsonl_path, "w", encoding="utf-8") as f:
        for feature in features:
            f.write(json.dumps(feature, ensure_ascii=False) + "\n")
    
    print(f"  Found {len(features)} police stations, saved to {geojsonl_path.name}")
    return features


def scrape_all_districts(force: bool = False) -> dict[str, list[dict]]:
    """Scrape police stations from all district websites.
    
    Returns a dict mapping district name to list of GeoJSON Features.
    """
    district_urls = get_station_urls()
    all_stations = {}
    
    # Skip training centers
    skip_prefixes = ("PTC", "PTS", "ITI", "JNPA", "GRP")
    
    for district_name, district_url in district_urls.items():
        if district_name.startswith(skip_prefixes):
            print(f"Skipping {district_name} (training center)")
            continue
        
        features = scrape_district_police_stations(district_name, district_url, force=force)
        if features:
            all_stations[district_name] = features
    
    return all_stations


def main():
    all_stations = scrape_all_districts()
    
    total = sum(len(f) for f in all_stations.values())
    print(f"\nTotal: {total} police stations from {len(all_stations)} districts")


if __name__ == "__main__":
    main()
