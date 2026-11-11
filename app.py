
from flask import Flask, jsonify, send_from_directory
from skyfield.api import load, EarthSatellite
from datetime import datetime, timezone, timedelta
import requests
import os
import time
import csv
import io
import math

app = Flask(__name__, static_folder=None)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TLE_BASE_URL = "https://celestrak.org/NORAD/elements/gp.php"
SATCAT_URL = "https://celestrak.org/pub/satcat.csv"

TLE_GROUPS = ["active", "stations", "last-30-days"]

TLE_REFRESH_HOURS = 6
SATCAT_REFRESH_HOURS = 24
POSITION_CACHE_SECONDS = 20

ts = load.timescale(builtin=True)

tle_cache = {}
satcat_cache = {
    "time": None,
    "data": {}
}

position_cache = {
    "time": 0,
    "data": None
}


def utc_now():
    return datetime.now(timezone.utc)


def cache_file_for_group(group):
    return os.path.join(BASE_DIR, f"{group}.tle")


def parse_tle_blocks(lines):
    blocks = []

    for i in range(len(lines) - 2):
        name = lines[i].strip()
        line1 = lines[i + 1].strip()
        line2 = lines[i + 2].strip()

        if line1.startswith("1 ") and line2.startswith("2 "):
            if name.startswith("0 "):
                name = name[2:]

            blocks.append((name, line1, line2))

    return blocks


def get_tle_group(group):
    now = utc_now()

    cached = tle_cache.get(group)

    if (
        cached
        and now - cached["time"] < timedelta(hours=TLE_REFRESH_HOURS)
    ):
        return cached["lines"], cached["source"]

    file_path = cache_file_for_group(group)

    try:
        print(f"Downloading CelesTrak group: {group}")

        response = requests.get(
            TLE_BASE_URL,
            params={
                "GROUP": group,
                "FORMAT": "TLE"
            },
            timeout=(10, 40),
            headers={"User-Agent": "CosmoDefense/1.0"}
        )

        response.raise_for_status()

        lines = [
            line.strip()
            for line in response.text.splitlines()
            if line.strip()
        ]

        if not parse_tle_blocks(lines):
            raise ValueError(f"Empty or invalid TLE feed: {group}")

        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(lines))

        tle_cache[group] = {
            "time": now,
            "lines": lines,
            "source": "CelesTrak download"
        }

        return lines, "CelesTrak download"

    except Exception as error:
        print(f"{group} download failed:", error)

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                lines = [
                    line.strip()
                    for line in file
                    if line.strip()
                ]

            if parse_tle_blocks(lines):
                file_time = datetime.fromtimestamp(
                    os.path.getmtime(file_path),
                    tz=timezone.utc
                )

                tle_cache[group] = {
                    "time": file_time,
                    "lines": lines,
                    "source": "Local backup"
                }

                return lines, "Local backup"

        raise RuntimeError(f"No data or backup available for {group}")



def get_satcat_metadata():
    now = utc_now()

    if (
        satcat_cache["time"]
        and now - satcat_cache["time"]
        < timedelta(hours=SATCAT_REFRESH_HOURS)
    ):
        return satcat_cache["data"], "Memory cache"

    file_path = os.path.join(BASE_DIR, "satcat.csv")

    try:
        print("Downloading SATCAT object metadata...")

        response = requests.get(
            SATCAT_URL,
            timeout=(10, 60),
            headers={"User-Agent": "CosmoDefense/1.0"}
        )

        response.raise_for_status()

        csv_text = response.content.decode("utf-8-sig")

        with open(file_path, "w", encoding="utf-8-sig", newline="") as file:
            file.write(csv_text)

        source = "CelesTrak SATCAT"

    except Exception as error:
        print("SATCAT download failed:", error)

        if not os.path.exists(file_path):
            return {}, "Metadata unavailable"

        with open(file_path, "r", encoding="utf-8-sig") as file:
            csv_text = file.read()

        source = "Local SATCAT backup"

    metadata = {}

    reader = csv.DictReader(io.StringIO(csv_text))

    if not reader.fieldnames:
        print("SATCAT ERROR: CSV headers missing")
        return {}, "Invalid SATCAT CSV"

    # Clean CSV column names
    reader.fieldnames = [
        str(name).strip().lstrip("\ufeff").upper()
        for name in reader.fieldnames
    ]

    print("SATCAT columns:", reader.fieldnames)

    if "NORAD_CAT_ID" not in reader.fieldnames:
        print("SATCAT ERROR: NORAD_CAT_ID column missing")
        return {}, "Invalid SATCAT columns"

    if "OBJECT_TYPE" not in reader.fieldnames:
        print("SATCAT WARNING: OBJECT_TYPE column missing")

    for row in reader:
        clean_row = {
            str(key).strip().upper(): str(value or "").strip()
            for key, value in row.items()
            if key is not None
        }

        norad_id = clean_row.get("NORAD_CAT_ID", "").strip()

        if not norad_id:
            continue

        # Normalize IDs such as 25544.0 to 25544
        try:
            norad_id = str(int(float(norad_id)))
        except (ValueError, TypeError):
            continue

        metadata[norad_id] = clean_row

    print("SATCAT records loaded:", len(metadata))

    satcat_cache["time"] = now
    satcat_cache["data"] = metadata

    return metadata, source



def is_space_station(name):
    upper = name.upper()

    station_terms = [
        "ISS",
        "ZARYA",
        "TIANGONG",
        "CSS",
        "CHINESE SPACE STATION"
    ]

    return any(term in upper for term in station_terms)



def classify_object(name, metadata, source_group):
    name_upper = str(name).strip().upper()
    object_type = str(
        metadata.get("OBJECT_TYPE", "")
    ).strip().upper()

    # Explicit metadata gets priority
    if object_type in ("DEB", "DEBRIS"):
        return "debris"

    if object_type in ("R/B", "ROCKET BODY"):
        return "rocket_body"

    # Identify space stations
    if (
        is_space_station(name_upper)
        or "ISS" in name_upper
        or "TIANGONG" in name_upper
        or source_group == "stations"
    ):
        return "station"

    if object_type in ("PAY", "PAYLOAD"):
        return "satellite"

    if "DEB" in name_upper or "DEBRIS" in name_upper:
        return "debris"

    if "R/B" in name_upper or "ROCKET BODY" in name_upper:
        return "rocket_body"

    if "PAYLOAD" in name_upper:
        return "satellite"

    return "unknown"

def safe_float(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def calculate_positions(feeds, metadata):
    current_time = ts.now()

    objects_by_id = {}

    for group, lines in feeds.items():
        for name, line1, line2 in parse_tle_blocks(lines):

            norad_id = line1[2:7].strip()

            if not norad_id:
                continue

            # Prefer the station feed if an object occurs in multiple feeds.
            if (
                norad_id in objects_by_id
                and group != "stations"
            ):
                continue

            try:
                satellite = EarthSatellite(
                    line1,
                    line2,
                    name,
                    ts
                )

                position = satellite.at(current_time)
                point = position.subpoint()

                velocity = position.velocity.km_per_s

                speed_kms = math.sqrt(
                    sum(component * component for component in velocity)
                )

                record = metadata.get(norad_id, {})

                category = classify_object(
                    name,
                    record,
                    group
                )

                inclination = safe_float(line2[8:16])
                mean_motion = safe_float(line2[52:63])

                period = (
                    1440.0 / mean_motion
                    if mean_motion and mean_motion > 0
                    else safe_float(record.get("PERIOD"))
                )

                objects_by_id[norad_id] = {
                    "norad_id": norad_id,
                    "name": name,
                    "category": category,
                    "object_type": record.get("OBJECT_TYPE", "UNK"),
                    "owner": record.get("OWNER") or "Unknown",
                    "launch_date": record.get("LAUNCH_DATE") or "Unknown",
                    "operational_status": (
                        record.get("OPS_STATUS_CODE") or "Unknown"
                    ),
                    "latitude": round(point.latitude.degrees, 4),
                    "longitude": round(point.longitude.degrees, 4),
                    "altitude_km": round(point.elevation.km, 2),
                    "speed_kms": round(speed_kms, 3),
                    "inclination_deg": (
                        round(inclination, 3)
                        if inclination is not None else None
                    ),
                    "period_min": (
                        round(period, 2)
                        if period is not None else None
                    ),
                    "apogee_km": safe_float(record.get("APOGEE")),
                    "perigee_km": safe_float(record.get("PERIGEE")),
                    "source_group": group
                }

            except Exception as error:
                print(f"Skipping {name}:", error)
                continue

    return list(objects_by_id.values())


@app.route("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/satellites")
def satellites():
    global position_cache

    try:
        if (
            position_cache["data"] is not None
            and time.time() - position_cache["time"]
            < POSITION_CACHE_SECONDS
        ):
            return jsonify(position_cache["data"])

        feeds = {}
        feed_sources = {}

        for group in TLE_GROUPS:
            try:
                lines, source = get_tle_group(group)
                feeds[group] = lines
                feed_sources[group] = source

            except Exception as error:
                print(f"Feed unavailable: {group}", error)

        if "active" not in feeds:
            raise RuntimeError(
                "Active satellite feed unavailable. Check connection."
            )

        metadata, metadata_source = get_satcat_metadata()

        tle_ids = set()

        for group, lines in feeds.items():
            for name, line1, line2 in parse_tle_blocks(lines):
                tle_ids.add(line1[2:7].strip())

        matched_ids = tle_ids.intersection(metadata.keys())

        tle_ids = set()

        for group, lines in feeds.items():
            for name, line1, line2 in parse_tle_blocks(lines):
                tle_ids.add(line1[2:7].strip())

        matched_ids = tle_ids.intersection(metadata.keys())

        matched_records = [
        metadata[x] for x in matched_ids
        ]

        type_counts = {}

        for record in matched_records:
            obj_type = record.get("OBJECT_TYPE", "EMPTY").strip()
            if not obj_type:
                obj_type = "EMPTY"

            type_counts[obj_type] = type_counts.get(obj_type, 0) + 1

        print("SATCAT OBJECT_TYPE DISTRIBUTION:", type_counts)

        print("\n========== SATCAT DIAGNOSTICS ==========")
        print("Metadata source:", metadata_source)
        print("SATCAT records:", len(metadata))
        print("TLE unique IDs:", len(tle_ids))
        print("Matched IDs:", len(matched_ids))
        print("Sample TLE ID:", next(iter(tle_ids), "None"))
        print("Sample SATCAT record:", next(iter(metadata.items()), "None"))
        print("========================================\n")

        objects = calculate_positions(feeds, metadata)

        if not objects:
            raise RuntimeError("No orbital positions could be calculated.")

        categories = [
            "satellite",
            "station",
            "rocket_body",
            "debris",
            "unknown"
        ]

        category_counts = {
            category: sum(
                1 for obj in objects
                if obj["category"] == category
            )
            for category in categories
        }

        result = {
            "success": True,
            "source": "CelesTrak GP + SATCAT",
            "tle_sources": feed_sources,
            "metadata_source": metadata_source,
            "position_updated": utc_now().isoformat(),
            "data_type": (
                "Calculated orbital positions from public elements; "
                "not live radar observations."
            ),
            "count": len(objects),
            "category_counts": category_counts,
            "satellites": objects
        }

        position_cache = {
            "time": time.time(),
            "data": result
        }

        print("Orbital objects calculated:", len(objects))

        return jsonify(result)

    except Exception as error:
        print("API error:", error)

        return jsonify({
            "success": False,
            "error": str(error),
            "satellites": []
        }), 502


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        threaded=True
    )
