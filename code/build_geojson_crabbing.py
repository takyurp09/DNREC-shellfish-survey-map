import time
import json
import hashlib

import pandas as pd
import requests

IN_CSV = "../data/crabbing_sites.csv"
OUT_GEOJSON = "../data/crabbing_polygons.geojson"
CACHE_FILE = "../data/geocode_cache.json"

HEADERS = {
    "User-Agent": "DNREC-shellfish-survey-map/1.0 (contact: tahmid@udel.edu)"
}

# Delaware bounding box (left, top, right, bottom)
DE_VIEWBOX = (-75.8, 39.95, -74.85, 38.35)


def load_cache():
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def cache_key(query, extra=""):
    return hashlib.md5((query + "|" + extra).encode("utf-8")).hexdigest()


def nominatim_search(query):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
        "viewbox": ",".join(map(str, DE_VIEWBOX)),
        "bounded": 1,
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def geocode_with_fallbacks(candidates, cache):
    """
    Try multiple queries until one geocodes.
    Cache per-candidate query.
    """
    for q in candidates:
        q = q.strip()
        if not q:
            continue

        k = cache_key(q, extra="DE_VIEWBOX_US")
        if k in cache:
            return cache[k]  # can be dict or None

        data = nominatim_search(q)

        if not data:
            cache[k] = None
            continue

        out = {
            "lat": float(data[0]["lat"]),
            "lon": float(data[0]["lon"]),
            "display_name": data[0].get("display_name", ""),
            "query_used": q,
        }
        cache[k] = out
        return out

    return None


def square_polygon(lat, lon, dlat=0.01, dlon=0.015):
    return [
        [lon - dlon, lat - dlat],
        [lon + dlon, lat - dlat],
        [lon + dlon, lat + dlat],
        [lon - dlon, lat + dlat],
        [lon - dlon, lat - dlat],
    ]


def build_candidates(geocode_name, site_name):
    """
    Make several plausible Nominatim queries.
    Put the most specific first.
    """
    # If geocode_name already contains DE/Delaware/USA, don't double-append too much
    base = geocode_name.strip()
    candidates = [
        base,
        f"{base}, DE",
        f"{base}, Delaware",
        f"{site_name}, DE",
        f"{site_name}, Delaware",
    ]

    # Also try stripping the word "Pier" and "Bridge" sometimes helps
    candidates += [
        base.replace(" Pier", "").strip(),
        base.replace(" Bridge", "").strip(),
        site_name.replace(" Pier", "").strip(),
        site_name.replace(" Bridge", "").strip(),
        f"{site_name.replace(' Pier','').replace(' Bridge','').strip()}, DE",
    ]

    # de-dup while preserving order
    seen = set()
    out = []
    for c in candidates:
        c2 = " ".join(c.split())
        if c2 and c2 not in seen:
            seen.add(c2)
            out.append(c2)
    return out


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

        # OPTIONAL: if you later add lat/lon columns manually, script will use them
        lat = None
        lon = None
        if "lat" in df.columns and "lon" in df.columns:
            try:
                if pd.notna(row["lat"]) and pd.notna(row["lon"]):
                    lat = float(row["lat"])
                    lon = float(row["lon"])
            except Exception:
                lat = None
                lon = None

        if lat is None or lon is None:
            candidates = build_candidates(geocode_name, site_name)
            result = geocode_with_fallbacks(candidates, cache)
            time.sleep(1.1)

            if result is None:
                print(f"NOT FOUND: {geocode_name}  |  {site_name}")
                continue

            lat, lon = result["lat"], result["lon"]

        # polygon placeholder
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

        # point marker
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
