#!/usr/bin/env python3
"""
AQUAVIEW Ultimate Gulf COP — Data Updater

Queries AQUAVIEW STAC API to discover all Gulf of Mexico ocean data,
then fetches latest readings from each item's source ERDDAP/API.
Outputs static JSON files to docs/data/ for GitHub Pages to serve.

Runs in GitHub Actions every 6 hours.

Data sources via AQUAVIEW STAC API:
  - IOOS_SENSORS → ~4,700 Gulf sensors (temp, sal, chl, DO)
  - NDBC         → ~313 Gulf buoys (wind, waves, pressure, SST)
  - COOPS        → ~140 Gulf tide stations (water level, temp)
  - IOOS         → Gulf glider tracks + positions
  - NOAA_GDP     → Drifter trajectories
  - INCIDENT_NEWS→ Oil spills & pollution events
  - PMEL         → Hurricane monitoring probes
  - COASTWATCH   → Ocean current vectors (grid)
"""

import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import time
import sys

# ─── Paths ───────────────────────────────────────────────────────────────────
DOCS_DATA = Path(__file__).parent.parent / "docs" / "data"
LATEST_JSON = DOCS_DATA / "latest.json"
CURRENTS_JSON = DOCS_DATA / "currents.json"
IOOS_SENSORS_JSON = DOCS_DATA / "ioos_sensors.json"
NDBC_MET_JSON = DOCS_DATA / "ndbc_met.json"
COOPS_JSON = DOCS_DATA / "coops.json"
GLIDERS_JSON = DOCS_DATA / "gliders.json"
DRIFTERS_JSON = DOCS_DATA / "drifters.json"
INCIDENTS_JSON = DOCS_DATA / "incidents.json"
PMEL_JSON = DOCS_DATA / "pmel.json"

# ─── AQUAVIEW STAC API ───────────────────────────────────────────────────────
AQUAVIEW_API = "https://aquaview-sfeos-1025757962819.us-east1.run.app"
GULF_BBOX = [-98, 18, -80, 32]  # Gulf of Mexico bounding box

# ─── ERDDAP / Data URLs ─────────────────────────────────────────────────────
NDBC_OCEAN_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.ocean"
NDBC_TXT_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
CURRENTS_URL = (
    "https://coastwatch.noaa.gov/erddap/griddap/"
    "noaacwBLENDEDNRTcurrentsDaily.json?"
    "u_current[(last)][(18):(32)][(-98):(-80)],"
    "v_current[(last)][(18):(32)][(-98):(-80)]"
)


def fetch_url(url, timeout=20):
    """Fetch URL content, return text or None on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AQUAVIEW-Explorer/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


def fetch_json(url, timeout=20):
    """Fetch URL and parse as JSON, return dict or None."""
    text = fetch_url(url, timeout)
    if text:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    return None


def save_json(path, data):
    """Write JSON file and report size."""
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = path.stat().st_size / 1024
    print(f"  → Saved {path.name}: {size_kb:.0f} KB")


# ─── AQUAVIEW STAC Helpers ───────────────────────────────────────────────────

def fetch_aquaview_items(collection, limit=500, extra_params=None):
    """Query AQUAVIEW STAC API for Gulf items from a specific collection."""
    params = {
        "collections": collection,
        "bbox": ",".join(str(b) for b in GULF_BBOX),
        "limit": str(limit),
    }
    if extra_params:
        params.update(extra_params)

    url = f"{AQUAVIEW_API}/search?{urllib.parse.urlencode(params)}"
    data = fetch_json(url, timeout=30)

    if data and "features" in data:
        items = data["features"]
        print(f"  AQUAVIEW {collection}: {len(items)} Gulf items")
        return items

    print(f"  AQUAVIEW {collection}: failed to fetch")
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# COLLECTION UPDATERS
# ═══════════════════════════════════════════════════════════════════════════════


def update_ndbc_hypoxia():
    """Update existing NDBC hypoxia stations (latest.json) — keep existing logic."""
    if not LATEST_JSON.exists():
        print("  WARNING: latest.json not found, skipping hypoxia update")
        return

    with open(LATEST_JSON) as f:
        data = json.load(f)

    print(f"\n── NDBC Hypoxia Stations ──")
    print(f"  Loaded {len(data['stations'])} stations")

    updated = 0
    for station in data["stations"]:
        sid = station["id"]

        # Try .ocean feed first
        ocean_text = fetch_url(NDBC_OCEAN_URL.format(station=sid))
        readings = _parse_ocean_feed(ocean_text)

        if not readings:
            txt_text = fetch_url(NDBC_TXT_URL.format(station=sid))
            readings = _parse_txt_feed(txt_text)

        if readings:
            if "current" not in station or station["current"] is None:
                station["current"] = {}
            for key, val in readings.items():
                if key == "timestamp":
                    station["current"]["last_obs"] = val
                else:
                    station["current"][key] = val
            updated += 1

    data["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with open(LATEST_JSON, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  Updated: {updated}/{len(data['stations'])} stations")


def _parse_ocean_feed(text):
    """Parse NDBC .ocean realtime feed."""
    if not text:
        return None
    lines = text.strip().split("\n")
    if len(lines) < 3:
        return None

    headers = lines[0].split()
    col_map = {h: i for i, h in enumerate(headers)}
    readings = {"do": None, "temp": None, "sal": None, "ph": None, "turb": None}
    timestamp = None

    for line in lines[2:50]:
        parts = line.split()
        if len(parts) < 5:
            continue
        try:
            yr = int(parts[col_map.get("#YY", col_map.get("YY", 0))])
            mo = int(parts[col_map.get("MM", 1)])
            dy = int(parts[col_map.get("DD", 2)])
            hr = int(parts[col_map.get("hh", 3)])
            mn = int(parts[col_map.get("mm", 4)])
        except (ValueError, IndexError):
            continue

        if timestamp is None:
            try:
                timestamp = datetime(yr, mo, dy, hr, mn, tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass

        def get_val(col_name):
            idx = col_map.get(col_name)
            if idx is None or idx >= len(parts):
                return None
            val_str = parts[idx]
            if val_str in ("MM", "99.0", "999.0", "9999.0", "99.00", "999", "9999"):
                return None
            try:
                v = float(val_str)
                if v >= 99 and col_name not in ("DEPTH",):
                    return None
                return v
            except ValueError:
                return None

        if readings["do"] is None:
            readings["do"] = get_val("O2PPM")
        if readings["temp"] is None:
            readings["temp"] = get_val("OTMP")
        if readings["sal"] is None:
            readings["sal"] = get_val("SAL")
        if readings["ph"] is None:
            readings["ph"] = get_val("PH")
        if readings["turb"] is None:
            readings["turb"] = get_val("TURB")
        if readings["do"] is not None and readings["temp"] is not None:
            break

    result = {k: v for k, v in readings.items() if v is not None}
    if timestamp:
        result["timestamp"] = timestamp
    return result if result else None


def _parse_txt_feed(text):
    """Parse NDBC .txt realtime feed for water temp fallback."""
    if not text:
        return None
    lines = text.strip().split("\n")
    if len(lines) < 3:
        return None
    headers = lines[0].split()
    col_map = {h: i for i, h in enumerate(headers)}
    for line in lines[2:10]:
        parts = line.split()
        if len(parts) < 5:
            continue
        wtmp_idx = col_map.get("WTMP")
        if wtmp_idx and wtmp_idx < len(parts):
            try:
                v = float(parts[wtmp_idx])
                if v < 50 and parts[wtmp_idx] != "MM":
                    return {"temp": v}
            except ValueError:
                pass
    return None


def update_currents():
    """Fetch CoastWatch ocean current vectors (CORS bypass for browser)."""
    print("\n── Ocean Currents ──")
    data = fetch_url(CURRENTS_URL, timeout=60)
    if data:
        with open(CURRENTS_JSON, "w") as f:
            f.write(data)
        size_kb = len(data) / 1024
        print(f"  → Currents saved: {size_kb:.0f} KB")
        return True
    else:
        print("  ✗ Failed to fetch current data")
        return False


def update_ioos_sensors():
    """Fetch IOOS sensor network from AQUAVIEW → static JSON."""
    print("\n── IOOS Sensor Network ──")
    items = fetch_aquaview_items("IOOS_SENSORS", limit=500)

    sensors = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        # Handle both Point and other geometry types
        if geom.get("type") == "Point" and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
        elif geom.get("type") == "MultiPoint" and coords:
            lon, lat = coords[0][0], coords[0][1]
        else:
            continue

        variables = props.get("aquaview:variables", [])
        source_url = props.get("aquaview:source_url", "")
        title = props.get("title", item.get("id", "Unknown"))

        sensors.append({
            "id": item.get("id", ""),
            "title": title[:80],
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "vars": variables[:10],  # Cap at 10 vars
            "src": source_url[:200],
            "org": props.get("aquaview:organization", ""),
        })

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(sensors),
        "sensors": sensors,
    }
    save_json(IOOS_SENSORS_JSON, result)
    print(f"  Total: {len(sensors)} sensors")


def update_ndbc_met():
    """Fetch NDBC weather buoys from AQUAVIEW + live readings from NDBC."""
    print("\n── NDBC Weather Buoys ──")
    items = fetch_aquaview_items("NDBC", limit=400)

    buoys = []
    fetched = 0
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        if geom.get("type") != "Point" or len(coords) < 2:
            continue

        sid = item.get("id", "")
        # NDBC station IDs are typically 5 chars (e.g., 42001)
        # Extract from AQUAVIEW item ID if needed
        station_id = sid.split("_")[-1] if "_" in sid else sid

        lat, lon = coords[1], coords[0]
        variables = props.get("aquaview:variables", [])

        buoy = {
            "id": station_id,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "title": props.get("title", station_id)[:80],
            "vars": variables[:8],
        }

        # Fetch latest .txt data for wind/wave/pressure
        if fetched < 200:  # Rate-limit: fetch top 200 buoys
            txt_text = fetch_url(NDBC_TXT_URL.format(station=station_id), timeout=10)
            readings = _parse_ndbc_met_txt(txt_text)
            if readings:
                buoy["current"] = readings
                fetched += 1

        buoys.append(buoy)

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(buoys),
        "buoys": buoys,
    }
    save_json(NDBC_MET_JSON, result)
    print(f"  Total: {len(buoys)} buoys, {fetched} with live data")


def _parse_ndbc_met_txt(text):
    """Parse NDBC .txt for wind, waves, pressure, water temp."""
    if not text:
        return None
    lines = text.strip().split("\n")
    if len(lines) < 3:
        return None

    headers = lines[0].split()
    col_map = {h: i for i, h in enumerate(headers)}
    readings = {}

    for line in lines[2:6]:
        parts = line.split()
        if len(parts) < 10:
            continue

        def get(name):
            idx = col_map.get(name)
            if idx is None or idx >= len(parts):
                return None
            s = parts[idx]
            if s in ("MM", "99.0", "999.0", "9999.0", "99.00", "999", "9999"):
                return None
            try:
                v = float(s)
                return v if v < 999 else None
            except ValueError:
                return None

        if "wspd" not in readings:
            v = get("WSPD")
            if v is not None:
                readings["wspd"] = v
        if "wdir" not in readings:
            v = get("WDIR")
            if v is not None:
                readings["wdir"] = v
        if "wvht" not in readings:
            v = get("WVHT")
            if v is not None:
                readings["wvht"] = v
        if "dpd" not in readings:
            v = get("DPD")
            if v is not None:
                readings["dpd"] = v
        if "pres" not in readings:
            v = get("PRES")
            if v is not None:
                readings["pres"] = v
        if "atmp" not in readings:
            v = get("ATMP")
            if v is not None:
                readings["atmp"] = v
        if "wtmp" not in readings:
            v = get("WTMP")
            if v is not None:
                readings["wtmp"] = v

        if len(readings) >= 4:
            break

    return readings if readings else None


def update_coops():
    """Fetch CO-OPS tidal stations from AQUAVIEW."""
    print("\n── CO-OPS Tidal Stations ──")
    items = fetch_aquaview_items("COOPS", limit=200)

    stations = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        if geom.get("type") != "Point" or len(coords) < 2:
            continue

        sid = item.get("id", "")
        station_id = sid.split("_")[-1] if "_" in sid else sid
        lat, lon = coords[1], coords[0]

        stations.append({
            "id": station_id,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "title": props.get("title", station_id)[:80],
            "vars": props.get("aquaview:variables", [])[:6],
            "src": props.get("aquaview:source_url", "")[:200],
        })

    # Try to get latest water levels from CO-OPS API for first 100 stations
    fetched = 0
    for s in stations[:100]:
        try:
            url = (
                f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
                f"date=latest&station={s['id']}&product=water_level"
                f"&datum=MLLW&units=metric&time_zone=gmt&format=json"
            )
            data = fetch_json(url, timeout=8)
            if data and "data" in data and data["data"]:
                latest = data["data"][-1]
                s["current"] = {
                    "wl": float(latest.get("v", 0)),
                    "time": latest.get("t", ""),
                }
                fetched += 1
        except Exception:
            pass

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(stations),
        "stations": stations,
    }
    save_json(COOPS_JSON, result)
    print(f"  Total: {len(stations)} stations, {fetched} with water levels")


def fetch_glider_track(dataset_id: str, erddap_base: str = "https://gliders.ioos.us/erddap") -> list:
    """Fetch actual trajectory from IOOS Gliders ERDDAP. Returns [[lon, lat], ...]."""
    url = (
        f"{erddap_base}/tabledap/{dataset_id}.json"
        f"?time,latitude,longitude&orderBy(%22time%22)"
    )
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "AQUAVIEW-COP/1.0")
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status != 200:
                return []
            data = json.loads(resp.read().decode())
        rows = data.get("table", {}).get("rows", [])
        if not rows:
            return []
        # Rows are [time, lat, lon] — subsample to max 200 points
        step = max(1, len(rows) // 200)
        track = []
        for i in range(0, len(rows), step):
            _time, lat, lon = rows[i]
            if lat is not None and lon is not None:
                track.append([round(lon, 4), round(lat, 4)])
        # Always include the last point
        _time, last_lat, last_lon = rows[-1]
        if last_lat is not None and last_lon is not None:
            last_pt = [round(last_lon, 4), round(last_lat, 4)]
            if not track or track[-1] != last_pt:
                track.append(last_pt)
        return track
    except Exception as e:
        print(f"    Track fetch failed for {dataset_id}: {e}")
        return []


def update_gliders():
    """Fetch active glider tracks from AQUAVIEW IOOS collection + ERDDAP trajectories."""
    print("\n── Glider Tracks ──")
    items = fetch_aquaview_items("IOOS", limit=100)

    gliders = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        title = props.get("title", item.get("id", ""))

        # Filter for glider missions
        if not any(kw in title.lower() for kw in ("glider", "slocum", "spray", "seaglider", "sg", "ru", "usf")):
            continue

        dataset_id = item.get("id", "")

        # Try fetching real track from ERDDAP first
        erddap_src = props.get("aquaview:source_url", "")
        erddap_base = "https://gliders.ioos.us/erddap"
        if "erddap" in erddap_src:
            try:
                parsed = urllib.parse.urlparse(erddap_src)
                erddap_base = f"{parsed.scheme}://{parsed.netloc}/erddap"
            except:
                pass

        print(f"  Fetching track: {dataset_id}...", end=" ")
        track = fetch_glider_track(dataset_id, erddap_base)
        if track and len(track) > 1:
            print(f"{len(track)} pts")
        else:
            # Fallback to STAC geometry
            coords = geom.get("coordinates", [])
            geom_type = geom.get("type", "")
            if geom_type == "LineString" and len(coords) >= 2:
                track = [[round(c[0], 4), round(c[1], 4)] for c in coords[-100:]]
            elif geom_type == "Point":
                track = [[round(coords[0], 4), round(coords[1], 4)]]
            elif geom_type == "MultiPoint":
                track = [[round(c[0], 4), round(c[1], 4)] for c in coords[-100:]]
            else:
                bbox = item.get("bbox")
                if bbox and len(bbox) >= 4:
                    cx = (bbox[0] + bbox[2]) / 2
                    cy = (bbox[1] + bbox[3]) / 2
                    track = [[round(cx, 4), round(cy, 4)]]
                else:
                    print("skipped (no geometry)")
                    continue
            print(f"{len(track)} pts (from STAC)")

        gliders.append({
            "id": dataset_id,
            "title": title[:80],
            "track": track,
            "vars": props.get("aquaview:variables", [])[:6],
            "start": props.get("start_datetime", props.get("datetime", "")),
            "end": props.get("end_datetime", ""),
            "src": erddap_src[:200],
        })

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(gliders),
        "gliders": gliders,
    }
    save_json(GLIDERS_JSON, result)
    print(f"  Total: {len(gliders)} glider missions")


def update_drifters():
    """Fetch drifter data from AQUAVIEW NOAA_GDP collection."""
    print("\n── Drifter Trajectories ──")
    items = fetch_aquaview_items("NOAA_GDP", limit=50)

    drifters = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        title = props.get("title", item.get("id", ""))
        source_url = props.get("aquaview:source_url", "")

        # Try to fetch recent drifter positions from ERDDAP
        # GDP datasets on ERDDAP typically have lat, lon, time
        if source_url and "erddap" in source_url:
            # Attempt to get last 50 positions
            erddap_base = source_url.rstrip("/")
            dataset_id = erddap_base.split("/")[-1] if "/" in erddap_base else ""

            if dataset_id:
                # Try fetching last positions with Gulf bbox filter
                csv_url = (
                    f"{erddap_base}.json?"
                    f"latitude,longitude,time"
                    f"&latitude>={GULF_BBOX[1]}&latitude<={GULF_BBOX[3]}"
                    f"&longitude>={GULF_BBOX[0]}&longitude<={GULF_BBOX[2]}"
                    f"&orderByLimit(%22time,50%22)"
                )
                data = fetch_json(csv_url, timeout=15)
                if data and "table" in data:
                    rows = data["table"].get("rows", [])
                    if rows:
                        track = [
                            [round(r[1], 4), round(r[0], 4)]  # [lon, lat]
                            for r in rows if r[0] is not None and r[1] is not None
                        ]
                        if track:
                            drifters.append({
                                "id": item.get("id", ""),
                                "title": title[:80],
                                "track": track[-200:],  # Last 200 pts
                                "count": len(track),
                                "src": source_url[:200],
                            })
                            continue

        # Fallback: use geometry from STAC item
        coords = geom.get("coordinates", [])
        geom_type = geom.get("type", "")
        if geom_type == "Point" and len(coords) >= 2:
            drifters.append({
                "id": item.get("id", ""),
                "title": title[:80],
                "track": [[round(coords[0], 4), round(coords[1], 4)]],
                "count": 1,
                "src": source_url[:200],
            })

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(drifters),
        "drifters": drifters,
    }
    save_json(DRIFTERS_JSON, result)
    print(f"  Total: {len(drifters)} drifter datasets")


def update_incidents():
    """Fetch oil spill / pollution incidents from AQUAVIEW INCIDENT_NEWS."""
    print("\n── Incidents (Oil Spills & Pollution) ──")

    # Get recent incidents — last 2 years
    two_years_ago = datetime(datetime.now().year - 2, 1, 1).strftime("%Y-%m-%dT00:00:00Z")
    items = fetch_aquaview_items(
        "INCIDENT_NEWS",
        limit=300,
        extra_params={"datetime": f"{two_years_ago}/.."},
    )

    incidents = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        if geom.get("type") != "Point" or len(coords) < 2:
            continue

        lat, lon = coords[1], coords[0]

        incidents.append({
            "id": item.get("id", ""),
            "title": props.get("title", "Unknown Incident")[:120],
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "date": props.get("datetime", props.get("start_datetime", "")),
            "desc": props.get("description", "")[:300],
            "url": next(
                (l.get("href", "") for l in item.get("links", []) if l.get("rel") == "alternate"),
                "",
            ),
        })

    # Sort by date, newest first
    incidents.sort(key=lambda x: x.get("date", ""), reverse=True)

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(incidents),
        "incidents": incidents,
    }
    save_json(INCIDENTS_JSON, result)
    print(f"  Total: {len(incidents)} incidents")


def update_pmel():
    """Fetch PMEL hurricane monitoring probes from AQUAVIEW."""
    print("\n── PMEL Hurricane Probes ──")
    items = fetch_aquaview_items("PMEL", limit=100)

    probes = []
    for item in items:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        if geom.get("type") != "Point" or len(coords) < 2:
            continue

        lat, lon = coords[1], coords[0]

        probes.append({
            "id": item.get("id", ""),
            "title": props.get("title", item.get("id", ""))[:80],
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "vars": props.get("aquaview:variables", [])[:6],
            "date": props.get("datetime", props.get("start_datetime", "")),
            "src": props.get("aquaview:source_url", "")[:200],
        })

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(probes),
        "probes": probes,
    }
    save_json(PMEL_JSON, result)
    print(f"  Total: {len(probes)} probes")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("AQUAVIEW Ultimate Gulf COP — Data Update")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Ensure output directory exists
    DOCS_DATA.mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    # 1. Existing: NDBC hypoxia stations
    try:
        update_ndbc_hypoxia()
    except Exception as e:
        print(f"  ERROR in NDBC hypoxia: {e}")

    # 2. Existing: Ocean currents
    try:
        update_currents()
    except Exception as e:
        print(f"  ERROR in currents: {e}")

    # 3. NEW: IOOS Sensor Network
    try:
        update_ioos_sensors()
    except Exception as e:
        print(f"  ERROR in IOOS sensors: {e}")

    # 4. NEW: NDBC Weather Buoys
    try:
        update_ndbc_met()
    except Exception as e:
        print(f"  ERROR in NDBC met: {e}")

    # 5. NEW: CO-OPS Tidal Stations
    try:
        update_coops()
    except Exception as e:
        print(f"  ERROR in CO-OPS: {e}")

    # 6. NEW: Glider Tracks
    try:
        update_gliders()
    except Exception as e:
        print(f"  ERROR in gliders: {e}")

    # 7. NEW: Drifters
    try:
        update_drifters()
    except Exception as e:
        print(f"  ERROR in drifters: {e}")

    # 8. NEW: Incidents
    try:
        update_incidents()
    except Exception as e:
        print(f"  ERROR in incidents: {e}")

    # 9. NEW: PMEL
    try:
        update_pmel()
    except Exception as e:
        print(f"  ERROR in PMEL: {e}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Complete in {elapsed:.1f}s")

    # Summary
    files = list(DOCS_DATA.glob("*.json"))
    total_kb = sum(f.stat().st_size for f in files) / 1024
    print(f"Data files: {len(files)} JSON files, {total_kb:.0f} KB total")
    for f in sorted(files):
        print(f"  {f.name}: {f.stat().st_size / 1024:.0f} KB")
    print("=" * 60)


if __name__ == "__main__":
    main()
