#!/usr/bin/env python3
"""
Fetch live NDBC sensor data and update docs/data/latest.json.

Runs in GitHub Actions every 6 hours. No ML dependencies — just fetches
current observations from NDBC realtime feeds and updates station readings.

Data sources:
  - NDBC realtime .ocean feeds (dissolved oxygen, temp, salinity, pH)
  - NDBC realtime .txt feeds (water temp, wind, pressure fallback)
"""

import json
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path


LATEST_JSON = Path(__file__).parent.parent / "docs" / "data" / "latest.json"
CURRENTS_JSON = Path(__file__).parent.parent / "docs" / "data" / "currents.json"

NDBC_OCEAN_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.ocean"
NDBC_TXT_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
CURRENTS_URL = (
    "https://coastwatch.noaa.gov/erddap/griddap/"
    "noaacwBLENDEDNRTcurrentsDaily.json?"
    "u_current[(last)][(18):(32)][(-98):(-80)],"
    "v_current[(last)][(18):(32)][(-98):(-80)]"
)


def fetch_url(url, timeout=15):
    """Fetch URL content, return text or None on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AQUAVIEW-Explorer/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
        return None


def parse_ocean_feed(text):
    """Parse NDBC .ocean realtime feed. Returns latest valid readings dict."""
    if not text:
        return None

    lines = text.strip().split("\n")
    if len(lines) < 3:
        return None

    # Header lines: column names, units, then data
    headers = lines[0].split()
    # Find column indices
    col_map = {}
    for i, h in enumerate(headers):
        col_map[h] = i

    readings = {"do": None, "temp": None, "sal": None, "ph": None, "turb": None}
    timestamp = None

    # Scan first 48 rows (most recent data) for valid readings
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

        # Fill in readings from most recent valid values
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

        # If we have the key readings, stop scanning
        if readings["do"] is not None and readings["temp"] is not None:
            break

    # Remove None values
    result = {k: v for k, v in readings.items() if v is not None}
    if timestamp:
        result["timestamp"] = timestamp

    return result if result else None


def parse_txt_feed(text):
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


def update_station(station):
    """Fetch latest data for a single station. Returns updated current dict or None."""
    station_id = station["id"]

    # Try .ocean feed first (has DO, temp, sal)
    ocean_text = fetch_url(NDBC_OCEAN_URL.format(station=station_id))
    readings = parse_ocean_feed(ocean_text)

    if readings:
        return readings

    # Fall back to .txt feed for water temp
    txt_text = fetch_url(NDBC_TXT_URL.format(station=station_id))
    txt_readings = parse_txt_feed(txt_text)

    return txt_readings


def update_currents():
    """Fetch CoastWatch ocean current data (bypasses browser CORS restrictions)."""
    print("\nFetching ocean current vectors from CoastWatch ERDDAP...")
    data = fetch_url(CURRENTS_URL, timeout=60)
    if data:
        with open(CURRENTS_JSON, "w") as f:
            f.write(data)
        size_kb = len(data) / 1024
        print(f"  ✓ Currents saved: {size_kb:.0f} KB")
        return True
    else:
        print("  ✗ Failed to fetch current data")
        return False


def main():
    # Load existing latest.json
    if not LATEST_JSON.exists():
        print(f"ERROR: {LATEST_JSON} not found")
        return

    with open(LATEST_JSON) as f:
        data = json.load(f)

    print(f"Loaded {len(data['stations'])} stations from latest.json")
    print(f"Previous update: {data.get('updated', 'unknown')}")

    updated_count = 0
    error_count = 0

    for station in data["stations"]:
        station_id = station["id"]
        readings = update_station(station)

        if readings:
            # Update the current readings
            if "current" not in station or station["current"] is None:
                station["current"] = {}

            for key, val in readings.items():
                if key == "timestamp":
                    station["current"]["last_obs"] = val
                else:
                    station["current"][key] = val

            updated_count += 1
            do_str = f"DO={readings.get('do', '—')}" if "do" in readings else ""
            temp_str = f"T={readings.get('temp', '—')}" if "temp" in readings else ""
            print(f"  ✓ {station_id}: {do_str} {temp_str}")
        else:
            error_count += 1

    # Update timestamp
    data["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Write back
    with open(LATEST_JSON, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nStations: {updated_count} updated, {error_count} unavailable")
    print(f"Timestamp: {data['updated']}")

    # Also fetch ocean current data (serves as CORS-free static file for browser)
    update_currents()

    print("\nAll done.")


if __name__ == "__main__":
    main()
