import time
import json
import hashlib

import pandas as pd
import requests

IN_CSV = "../data/clamming_sites.csv"
OUT_GEOJSON = "../data/clamming_polygons.geojson"
CACHE_FILE = "../data/geocode_cache.json"

# Nominatim requires a real User-Agent with contact info
HEADERS = {
    "User-Agent": "DNREC-shellfish-survey-map/1.0 (contact: tahmid@udel.edu)"
}


def load_cache():
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def geocode(query, cache):
    key = hashlib.md5(query.encode("utf-8")).hexdigest()
    if key in cache:
        return cache[key]

    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1}

    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not data:
        cache[key] = None
        return None

    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    cache[key] = {
        "lat": lat,
        "lon": lon,
        "display_name": data[0].get("display_name", ""),
    }
    return cache[key]


def square_polygon(lat, lon, dlat=0.01, dlon=0.015):
    # GeoJSON polygon coords are [lon, lat]
    return [
        [lon - dlon, lat - dlat],
        [lon + dlon, lat - dlat],
        [lon + dlon, lat + dlat],
        [lon - dlon, lat + dlat],
        [lon - dlon, lat - dlat],
    ]


def main():
    df = pd.read_csv(IN_CSV)

    required = {"zone_id", "zone_name", "site_name", "geocode_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {sorted(missing)}")

    cache = load_cache()
    features = []

    for _, row in df.iterrows():
        zone_id = str(row["zone_id"]).strip()
        zone_name = str(row["zone_name"]).strip()
        site_name = str(row["site_name"]).strip()
        geocode_name = str(row["geocode_name"]).strip()

        query = f"{geocode_name}, Delaware, USA"

        result = geocode(query, cache)
        time.sleep(1.1)  # be polite to Nominatim

        if result is None:
            print(f"NOT FOUND: {query}")
            continue

        lat, lon = result["lat"], result["lon"]

        # 1 polygon per site (placeholder square around the point)
        coords = square_polygon(lat, lon)
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "site_name": site_name,
                    "polygon_id": "A",
                },
                "geometry": {"type": "Polygon", "coordinates": [coords]},
            }
        )

        # point marker for the site
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "site_name": site_name,
                    "feature_type": "site_point",
                },
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
            }
        )

    save_cache(cache)

    geo = {"type": "FeatureCollection", "features": features}
    with open(OUT_GEOJSON, "w") as f:
        json.dump(geo, f)

    print(f"WROTE: {OUT_GEOJSON}")


if __name__ == "__main__":
    main()
