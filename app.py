# ==============================================================================
# COSMO-DEFENSE  - BACKEND SERVER 
# ==============================================================================
# Yeh file CelesTrak se live/backup TLE satellite data, SATCAT metadata, aur
# NASA/JPL se asteroid data fetch karke Skyfield ke zariye 3D positions calculate
# karti hai aur JSON API ke through frontend (index.html) ko bhejti hai.
# ==============================================================================

from flask import Flask, jsonify, send_from_directory
from skyfield.api import load, EarthSatellite
from datetime import datetime, timezone, timedelta
from collections import Counter
import requests
import os
import re
import time
import csv
import io
import json
import math
import logging
import threading

# Flask app initialize karna (static_folder=None kyunki hum custom route se index.html serve karte hain)
app = Flask(__name__, static_folder=None)

# Server logs ko clean format mein terminal/IDLE par dikhane ke liye
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cosmo")

# Project folder ka base path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# External Space Data APIs (CelesTrak & NASA/JPL)
TLE_BASE_URL = "https://celestrak.org/NORAD/elements/gp.php"
SATCAT_URL = "https://celestrak.org/pub/satcat.csv"
SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb_query.api"

# Kaunse satellite groups load karne hain
TLE_GROUPS = ["active", "stations"]

# Cache aur refresh timings (taaki external servers par unnecessary load na pade)
TLE_REFRESH_HOURS = 6
SATCAT_REFRESH_HOURS = 24
ASTEROID_REFRESH_HOURS = 24
RETRY_AFTER_FAILURE_MINUTES = 15
POSITION_CACHE_SECONDS = 20

# Orbital mechanics constants (Earth gravitational parameter & radius)
EARTH_MU_KM3_S2 = 398600.4418
EARTH_RADIUS_KM = 6378.137

# Skyfield astronomical timescale load karna
ts = load.timescale(builtin=True)

# Thread lock taaki ek sath multiple browser requests aane par duplicate download na ho
refresh_lock = threading.RLock()

# In-memory state dictionaries (live, cached, aur local backup status track karne ke liye)
feed_state = {}
feed_failure = {}
satcat_state = {}
asteroid_state = {}
catalog_cache = {"key": None, "data": None}

position_cache = {
    "time": 0,
    "data": None
}


# ==============================================================================
# 1. HELPER FUNCTIONS (Time, Validation, TLE Parsing)
# ==============================================================================

def utc_now():
    """Current UTC time return karta hai."""
    return datetime.now(timezone.utc)


def iso(dt):
    """Datetime object ko ISO string mein convert karta hai."""
    return dt.isoformat() if dt else None


def cache_file_for_group(group):
    """Har TLE group ke local backup file (.tle) ka path deta hai."""
    return os.path.join(BASE_DIR, f"{group}.tle")


def safe_float(value):
    """Kisi bhi string/number ko safely finite float mein convert karta hai."""
    try:
        if value is None or str(value).strip() == "":
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_norad_id(value):
    """
    TLE ('00900') aur SATCAT ('900') ke NORAD IDs ko ek standard integer string ('900')
    banata hai taaki dono datasets accurately match ho sakein.
    """
    try:
        return str(int(float(str(value).strip())))
    except (ValueError, TypeError, OverflowError):
        return None


def tle_checksum(line):
    """TLE line ke pehle 68 characters ka modulo-10 checksum calculate karta hai."""
    total = 0
    for char in line[:68]:
        if char.isdigit():
            total += int(char)
        elif char == "-":
            total += 1
    return total % 10


def tle_epoch(line1):
    """TLE Line 1 se satellite ke orbital data ka epoch timestamp nikalta hai."""
    try:
        yy = int(line1[18:20])
        day = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy
        return datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=day - 1)
    except (ValueError, TypeError, OverflowError):
        return None


def validate_tle(line1, line2):
    """Check karta hai ki TLE ki dono lines length, NORAD ID aur checksum mein valid hain ya nahi."""
    if len(line1) != 69 or len(line2) != 69:
        return f"bad line length ({len(line1)}/{len(line2)}, expected 69/69)"

    if line1[2:7].strip() != line2[2:7].strip():
        return "NORAD ID differs between line 1 and line 2"

    if normalize_norad_id(line1[2:7]) is None:
        return f"non-numeric NORAD ID '{line1[2:7].strip()}'"

    if not (line1[68].isdigit() and line2[68].isdigit()):
        return "missing checksum digit"

    if tle_checksum(line1) != int(line1[68]):
        return "line 1 checksum mismatch"

    if tle_checksum(line2) != int(line2[68]):
        return "line 2 checksum mismatch"

    return None


def parse_tle_blocks(lines):
    """Raw TLE text lines ko 3-line blocks (Name, Line 1, Line 2) mein todta hai."""
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


# ==============================================================================
# 2. TLE FEED DOWNLOADER & LOCAL BACKUP MANAGER
# ==============================================================================

def feed_public_info(state):
    """Frontend ke Data Quality panel ke liye feed ki current status summary banata hai."""
    now = utc_now()
    data_time = state["data_time"]

    return {
        "status": state["status"],
        "label": {
            "live": "Live download",
            "cached": "Live download (held in memory)",
            "local_backup": "Local backup file",
        }[state["status"]],
        "data_obtained_at": iso(data_time),
        "age_minutes": round((now - data_time).total_seconds() / 60, 1),
        "last_attempt": iso(state.get("last_attempt")),
        "last_error": state.get("error"),
        "records": state["records"],
        "tle_epoch_newest": iso(state.get("epoch_newest")),
        "tle_epoch_oldest": iso(state.get("epoch_oldest")),
    }


def _summarise_feed(lines):
    """Feed ke total records aur newest/oldest epoch ki summary nikalta hai."""
    blocks = parse_tle_blocks(lines)
    epochs = [e for e in (tle_epoch(b[1]) for b in blocks) if e]
    return {
        "records": len(blocks),
        "epoch_newest": max(epochs) if epochs else None,
        "epoch_oldest": min(epochs) if epochs else None,
    }


def get_tle_group(group):
    """
    CelesTrak se TLE download karta hai. Agar memory cache fresh hai toh wahi deta hai,
    aur agar internet fail ho toh local .tle backup file se data load karta hai.
    """
    with refresh_lock:
        now = utc_now()
        state = feed_state.get(group)

        if state:
            if (
                state["origin"] == "live"
                and now - state["data_time"] < timedelta(hours=TLE_REFRESH_HOURS)
            ):
                state["status"] = "cached"
                return state["lines"], feed_public_info(state)

            if (
                state["origin"] == "local_backup"
                and now - state["last_attempt"]
                < timedelta(minutes=RETRY_AFTER_FAILURE_MINUTES)
            ):
                return state["lines"], feed_public_info(state)

        failure = feed_failure.get(group)
        if failure and now - failure["time"] < timedelta(
            minutes=RETRY_AFTER_FAILURE_MINUTES
        ):
            raise RuntimeError(failure["error"])

        file_path = cache_file_for_group(group)
        error_text = None

        try:
            log.info("Downloading CelesTrak group '%s'", group)
            response = requests.get(
                TLE_BASE_URL,
                params={"GROUP": group, "FORMAT": "TLE"},
                timeout=(10, 40),
                headers={"User-Agent": "CosmoDefense/1.0"},
            )
            response.raise_for_status()

            lines = [
                line.strip()
                for line in response.text.splitlines()
                if line.strip()
            ]

            if not parse_tle_blocks(lines):
                raise ValueError(f"Empty or invalid TLE feed: {group}")

            # Atomic file write taaki backup file corrupt na ho
            tmp_path = file_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as file:
                file.write("\n".join(lines))
            os.replace(tmp_path, file_path)

            state = {
                "origin": "live",
                "status": "live",
                "lines": lines,
                "data_time": now,
                "last_attempt": now,
                "error": None,
                **_summarise_feed(lines),
            }
            feed_state[group] = state
            feed_failure.pop(group, None)
            log.info("Group '%s': live download OK (%d records)", group, state["records"])
            return lines, feed_public_info(state)

        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
            log.warning("Group '%s' download failed (%s)", group, error_text)

        # Fallback: Local .tle file se load karna
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                lines = [line.strip() for line in file if line.strip()]

            if parse_tle_blocks(lines):
                file_time = datetime.fromtimestamp(
                    os.path.getmtime(file_path), tz=timezone.utc
                )
                state = {
                    "origin": "local_backup",
                    "status": "local_backup",
                    "lines": lines,
                    "data_time": file_time,
                    "last_attempt": now,
                    "error": error_text,
                    **_summarise_feed(lines),
                }
                feed_state[group] = state
                log.warning(
                    "Group '%s': using local backup (file modified %s, %d records)",
                    group, iso(file_time), state["records"],
                )
                return lines, feed_public_info(state)

        message = f"No data or backup available for {group} ({error_text})"
        feed_failure[group] = {"time": now, "error": message}
        raise RuntimeError(message)


# ==============================================================================
# 3. SATCAT METADATA MANAGER (Owner, Launch Date, Object Type)
# ==============================================================================

def satcat_public_info():
    """SATCAT metadata source ki current status summary deta hai."""
    state = satcat_state
    if not state:
        return {"status": "unavailable", "label": "Metadata unavailable"}

    now = utc_now()
    return {
        "status": state["status"],
        "label": {
            "live": "Live download",
            "cached": "Live download (held in memory)",
            "local_backup": "Local backup file",
            "unavailable": "Metadata unavailable",
        }[state["status"]],
        "data_obtained_at": iso(state.get("data_time")),
        "age_minutes": (
            round((now - state["data_time"]).total_seconds() / 60, 1)
            if state.get("data_time") else None
        ),
        "last_error": state.get("error"),
        "records": len(state.get("data", {})),
    }


def _parse_satcat(csv_text):
    """SATCAT CSV text ko parse karke NORAD ID ke dictionary mein map karta hai."""
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("SATCAT CSV headers missing")

    reader.fieldnames = [
        str(name).strip().lstrip("\ufeff").upper()
        for name in reader.fieldnames
    ]

    if "NORAD_CAT_ID" not in reader.fieldnames:
        raise ValueError("SATCAT NORAD_CAT_ID column missing")

    metadata = {}
    for row in reader:
        clean_row = {
            str(key).strip().upper(): str(value or "").strip()
            for key, value in row.items()
            if key is not None
        }
        norad_id = normalize_norad_id(clean_row.get("NORAD_CAT_ID", ""))
        if norad_id is None:
            continue
        metadata[norad_id] = clean_row

    if not metadata:
        raise ValueError("SATCAT contained no usable rows")
    return metadata


def get_satcat_metadata():
    """SATCAT metadata ko live download ya local backup (satcat.csv) se load karta hai."""
    with refresh_lock:
        now = utc_now()

        if (
            satcat_state
            and satcat_state.get("data")
            and satcat_state["origin"] == "live"
            and now - satcat_state["data_time"]
            < timedelta(hours=SATCAT_REFRESH_HOURS)
        ):
            satcat_state["status"] = "cached"
            return satcat_state["data"], satcat_public_info()

        if (
            satcat_state
            and satcat_state.get("data")
            and satcat_state["origin"] == "local_backup"
            and now - satcat_state["last_attempt"]
            < timedelta(minutes=RETRY_AFTER_FAILURE_MINUTES)
        ):
            return satcat_state["data"], satcat_public_info()

        if (
            satcat_state
            and satcat_state.get("status") == "unavailable"
            and now - satcat_state["last_attempt"]
            < timedelta(minutes=RETRY_AFTER_FAILURE_MINUTES)
        ):
            return {}, satcat_public_info()

        file_path = os.path.join(BASE_DIR, "satcat.csv")
        error_text = None

        try:
            log.info("Downloading SATCAT object metadata")
            response = requests.get(
                SATCAT_URL,
                timeout=(10, 60),
                headers={"User-Agent": "CosmoDefense/1.0"},
            )
            response.raise_for_status()

            csv_text = response.content.decode("utf-8-sig")
            metadata = _parse_satcat(csv_text)

            tmp_path = file_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8-sig", newline="") as file:
                file.write(csv_text)
            os.replace(tmp_path, file_path)

            satcat_state.clear()
            satcat_state.update({
                "origin": "live", "status": "live", "data": metadata,
                "data_time": now, "last_attempt": now, "error": None,
            })
            log.info("SATCAT live download OK (%d records)", len(metadata))
            return metadata, satcat_public_info()

        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
            log.warning("SATCAT download failed (%s)", error_text)

        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8-sig") as file:
                    metadata = _parse_satcat(file.read())

                file_time = datetime.fromtimestamp(
                    os.path.getmtime(file_path), tz=timezone.utc
                )
                satcat_state.clear()
                satcat_state.update({
                    "origin": "local_backup", "status": "local_backup",
                    "data": metadata, "data_time": file_time,
                    "last_attempt": now, "error": error_text,
                })
                log.warning(
                    "SATCAT: using local backup (file modified %s, %d records)",
                    iso(file_time), len(metadata),
                )
                return metadata, satcat_public_info()

            except Exception as error:
                error_text = f"{error_text}; backup unreadable: {error}"
                log.error("SATCAT backup unusable: %s", error)

        satcat_state.clear()
        satcat_state.update({
            "origin": "none", "status": "unavailable", "data": {},
            "data_time": None, "last_attempt": now, "error": error_text,
        })
        return {}, satcat_public_info()


# ==============================================================================
# 4. OBJECT CLASSIFICATION (Satellite, Station, Rocket Body, Debris, Unknown)
# ==============================================================================

STATION_NAME_RE = re.compile(r"^(ISS|CSS)\b")
STATION_EXTRA_NAMES = {
    "POISK", "ZARYA", "UNITY", "ZVEZDA", "DESTINY", "NAUKA", "TIANGONG"
}

DEBRIS_NAME_RE = re.compile(r"\bDEB\b|\bDEBRIS\b")
ROCKET_NAME_RE = re.compile(r"\bR/B\b|\bROCKET BODY\b")


def is_space_station(name):
    """ISS, Tiangong (CSS) aur known station modules ko pehchanta hai."""
    upper = str(name).strip().upper()
    return bool(STATION_NAME_RE.match(upper)) or upper in STATION_EXTRA_NAMES


def classify_object(name, metadata, source_group=None):
  """SATCAT OBJECT_TYPE aur verified naming rules se object category decide karta hai."""
  name_upper = str(name).strip().upper()
  object_type = str(metadata.get("OBJECT_TYPE", "")).strip().upper()

  if object_type in ("DEB", "DEBRIS"):
    return "debris"
  if object_type in ("R/B", "ROCKET BODY") or ROCKET_NAME_RE.search(name_upper):
    return "rocket_body"
  if is_space_station(name_upper):
    return "station"
  if object_type in ("PAY", "PAYLOAD"):
    return "satellite"
  if DEBRIS_NAME_RE.search(name_upper):
    return "debris"
  return "unknown"


OPS_STATUS_LABELS = {
    "+": "Operational",
    "-": "Non-operational",
    "P": "Partially operational",
    "B": "Backup / standby",
    "S": "Spare",
    "X": "Extended mission",
    "D": "Decayed",
    "?": "Unknown",
}


def decode_ops_status(code):
    """SATCAT operational status code ko human-readable label mein badalta hai."""
    code = (code or "").strip()
    if not code:
        return "Not recorded in SATCAT"
    return OPS_STATUS_LABELS.get(code, f"Unrecognised code '{code}'")


# ==============================================================================
# 5. CATALOG BUILDER & SKYFIELD POSITION PROPAGATION
# ==============================================================================

def build_catalog(feeds):
    """Multiple feeds ko merge karke duplicate NORAD IDs hatata hai aur unique objects banata hai."""
    selected = {}
    raw_records = 0
    records_by_group = {}
    unique_by_group = {}
    group_membership = {}
    malformed = []

    for group, lines in feeds.items():
        blocks = parse_tle_blocks(lines)
        records_by_group[group] = len(blocks)
        seen_in_group = set()

        for name, line1, line2 in blocks:
            raw_records += 1

            problem = validate_tle(line1, line2)
            if problem:
                malformed.append({
                    "group": group,
                    "name": name,
                    "norad_id": line1[2:7].strip(),
                    "reason": problem,
                })
                log.warning("Rejected TLE %s [%s] in '%s': %s",
                            name, line1[2:7].strip(), group, problem)
                continue

            norad_id = normalize_norad_id(line1[2:7])
            seen_in_group.add(norad_id)
            group_membership.setdefault(norad_id, []).append(group)

            if norad_id in selected and group != "stations":
                continue

            selected[norad_id] = {
                "norad_id": norad_id,
                "name": name,
                "line1": line1,
                "line2": line2,
                "group": group,
            }

        unique_by_group[group] = len(seen_in_group)

    overlapping = {
        nid: groups for nid, groups in group_membership.items()
        if len(groups) > 1
    }

    return {
        "selected": selected,
        "raw_records": raw_records,
        "records_by_group": records_by_group,
        "unique_by_group": unique_by_group,
        "overlapping_ids": overlapping,
        "malformed": malformed,
    }


def get_catalog():
    """TLE feeds aur SATCAT metadata ko load karke cached unique catalog return karta hai."""
    with refresh_lock:
        feeds = {}
        feed_info = {}

        for group in TLE_GROUPS:
            try:
                lines, info = get_tle_group(group)
                feeds[group] = lines
                feed_info[group] = info
            except Exception as error:
                log.warning("Feed unavailable: %s (%s)", group, error)
                feed_info[group] = {
                    "status": "unavailable",
                    "label": "Unavailable",
                    "last_error": str(error),
                }

        if "active" not in feeds:
            raise RuntimeError(
                "Active satellite feed unavailable (no download and no local backup)."
            )

        metadata, metadata_info = get_satcat_metadata()

        key = (
            tuple((g, feed_state[g]["data_time"]) for g in feeds),
            satcat_state.get("data_time"),
        )

        if catalog_cache["key"] != key:
            catalog_cache["key"] = key
            catalog_cache["data"] = build_catalog(feeds)
            catalog_cache["elements"] = None

        return catalog_cache["data"], feed_info, metadata, metadata_info


def calculate_positions(catalog, metadata):
    """Skyfield SGP4 model se har satellite ka current Latitude, Longitude, Altitude aur Speed nikalta hai."""
    current_time = ts.now()
    objects = []
    failures = []

    for norad_id, item in catalog["selected"].items():
        name = item["name"]
        group = item["group"]
        line1 = item["line1"]
        line2 = item["line2"]

        try:
            satellite = EarthSatellite(line1, line2, name, ts)
            position = satellite.at(current_time)
            sgp4_message = getattr(position, "message", None)

            point = position.subpoint()
            velocity = position.velocity.km_per_s
            speed_kms = math.sqrt(sum(c * c for c in velocity))

            latitude = point.latitude.degrees
            longitude = point.longitude.degrees
            altitude = point.elevation.km

            if not all(math.isfinite(v) for v in (
                latitude, longitude, altitude, speed_kms
            )):
                reason = (
                    f"SGP4: {sgp4_message}" if sgp4_message
                    else "non-finite position/velocity"
                )
                raise ValueError(reason)

            record = metadata.get(norad_id, {})
            has_metadata = bool(record)
            category = classify_object(name, record, group)

            inclination = safe_float(line2[8:16])
            mean_motion = safe_float(line2[52:63])

            period = (
                1440.0 / mean_motion
                if mean_motion and mean_motion > 0
                else safe_float(record.get("PERIOD"))
            )

            objects.append({
                "norad_id": norad_id,
                "name": name,
                "category": category,
                "metadata_matched": has_metadata,
                "object_type": record.get("OBJECT_TYPE") or "UNK",
                "owner": record.get("OWNER") or "Unknown",
                "launch_date": record.get("LAUNCH_DATE") or "Unknown",
                "launch_vehicle": record.get("LAUNCH_VEHICLE") or None,
                "mission_purpose": (
                    record.get("MISSION") or record.get("PAYLOAD_DESC") or None
                ),
                "operational_status": decode_ops_status(
                    record.get("OPS_STATUS_CODE")
                ),
                "latitude": round(latitude, 4),
                "longitude": round(longitude, 4),
                "altitude_km": round(altitude, 2),
                "speed_kms": round(speed_kms, 3),
                "inclination_deg": (
                    round(inclination, 3) if inclination is not None else None
                ),
                "period_min": (
                    round(period, 2) if period is not None else None
                ),
                "apogee_km": safe_float(record.get("APOGEE")),
                "perigee_km": safe_float(record.get("PERIGEE")),
                "source_group": group,
            })

        except Exception as error:
            reason = f"{type(error).__name__}: {error}"
            failures.append({
                "norad_id": norad_id,
                "name": name,
                "source_group": group,
                "tle_epoch": iso(tle_epoch(line1)),
                "reason": reason,
            })
            log.warning("Position failed for %s [%s] (%s): %s",
                        name, norad_id, group, reason)

    return objects, failures


# ==============================================================================
# 6. ORBITAL REGIMES (LEO / MEO / GEO) & CONGESTION ANALYSIS
# ==============================================================================

def orbit_elements(item):
    """TLE mean motion aur eccentricity se apogee, perigee aur semi-major axis nikalta hai."""
    line2 = item["line2"]
    try:
        inclination = float(line2[8:16])
        eccentricity = float("0." + line2[26:33].strip())
        mean_motion = float(line2[52:63])
    except (ValueError, TypeError):
        return None

    if not (mean_motion > 0 and 0 <= eccentricity < 1):
        return None

    n_rad_s = mean_motion * 2 * math.pi / 86400.0
    a_km = (EARTH_MU_KM3_S2 / (n_rad_s ** 2)) ** (1.0 / 3.0)

    perigee = a_km * (1 - eccentricity) - EARTH_RADIUS_KM
    apogee = a_km * (1 + eccentricity) - EARTH_RADIUS_KM

    if not all(math.isfinite(v) for v in (a_km, perigee, apogee)):
        return None

    return {
        "inclination": inclination,
        "eccentricity": eccentricity,
        "a_km": a_km,
        "perigee_km": perigee,
        "apogee_km": apogee,
        "mean_alt_km": a_km - EARTH_RADIUS_KM,
        "epoch": tle_epoch(item["line1"]),
    }


LEO_MAX_APOGEE_KM = 2000.0
GEO_ALT_KM = 35786.0
GEO_BAND_KM = 500.0


def orbit_regime(perigee, apogee):
    """Altitude ke hisaab se orbit ko LEO, MEO, GEO, ya ELLIPTICAL mein classify karta hai."""
    if apogee < LEO_MAX_APOGEE_KM:
        return "LEO"
    if perigee < LEO_MAX_APOGEE_KM:
        return "ELLIPTICAL"
    if (perigee >= GEO_ALT_KM - GEO_BAND_KM
            and apogee <= GEO_ALT_KM + GEO_BAND_KM):
        return "GEO"
    if apogee < GEO_ALT_KM - GEO_BAND_KM:
        return "MEO"
    return "OTHER_HIGH"


REGIME_ORDER = ["LEO", "MEO", "GEO", "ELLIPTICAL", "OTHER_HIGH"]


def catalog_elements():
    """Congestion aur Health endpoints ke liye orbital elements cache karta hai."""
    catalog, feed_info, metadata, metadata_info = get_catalog()

    if catalog_cache.get("elements") is None:
        rows = []
        skipped = 0
        for nid, item in catalog["selected"].items():
            elements = orbit_elements(item)
            if elements is None:
                skipped += 1
                continue
            record = metadata.get(nid, {})
            elements["norad_id"] = nid
            elements["category"] = classify_object(
                item["name"], record, item["group"]
            )
            elements["regime"] = orbit_regime(
                elements["perigee_km"], elements["apogee_km"]
            )
            rows.append(elements)
        catalog_cache["elements"] = {"rows": rows, "skipped": skipped}

    return (
        catalog, feed_info, metadata, metadata_info,
        catalog_cache["elements"]["rows"],
        catalog_cache["elements"]["skipped"],
    )


# ==============================================================================
# 7. ASTEROID DATA FETCHER (NASA / JPL Small-Body Database)
# ==============================================================================

ASTEROID_CACHE_FILE = os.path.join(BASE_DIR, "asteroids_cache.json")
ASTEROID_FIELDS = "full_name,e,a,i,om,w,ma,epoch,moid,H,diameter"


def _parse_asteroids(payload):
    """JPL SBDB JSON response se asteroid ke heliocentric elements parse karta hai."""
    fields = payload["fields"]
    rows = []

    for raw in payload["data"]:
        row = dict(zip(fields, raw))
        rows.append({
            "name": str(row.get("full_name") or "").strip(),
            "eccentricity": safe_float(row.get("e")),
            "semi_major_axis_au": safe_float(row.get("a")),
            "inclination_deg": safe_float(row.get("i")),
            "ascending_node_deg": safe_float(row.get("om")),
            "arg_perihelion_deg": safe_float(row.get("w")),
            "mean_anomaly_deg": safe_float(row.get("ma")),
            "epoch_jd": safe_float(row.get("epoch")),
            "earth_moid_au": safe_float(row.get("moid")),
            "abs_magnitude_h": safe_float(row.get("H")),
            "diameter_km": safe_float(row.get("diameter")),
        })

    if not rows:
        raise ValueError("JPL SBDB returned no rows")
    return rows


def get_asteroids():
    """NASA/JPL se 25 Potentially Hazardous Asteroids fetch karta hai (local backup fallback ke saath)."""
    with refresh_lock:
        now = utc_now()

        if (
            asteroid_state.get("rows")
            and asteroid_state["origin"] == "live"
            and now - asteroid_state["data_time"]
            < timedelta(hours=ASTEROID_REFRESH_HOURS)
        ):
            asteroid_state["status"] = "cached"
            return asteroid_state

        if (
            asteroid_state.get("rows")
            and asteroid_state["origin"] == "local_backup"
            and now - asteroid_state["last_attempt"]
            < timedelta(minutes=RETRY_AFTER_FAILURE_MINUTES)
        ):
            return asteroid_state

        if (
            asteroid_state.get("status") == "unavailable"
            and now - asteroid_state["last_attempt"]
            < timedelta(minutes=RETRY_AFTER_FAILURE_MINUTES)
        ):
            return asteroid_state

        error_text = None

        try:
            log.info("Querying JPL SBDB for potentially hazardous asteroids")
            response = requests.get(
                SBDB_URL,
                params={
                    "fields": ASTEROID_FIELDS,
                    "sb-kind": "a",
                    "sb-group": "pha",
                    "sort": "moid",
                    "limit": 25,
                },
                timeout=(5, 10),
                headers={"User-Agent": "CosmoDefense/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
            rows = _parse_asteroids(payload)

            with open(ASTEROID_CACHE_FILE, "w", encoding="utf-8") as file:
                json.dump({"payload": payload}, file)

            asteroid_state.clear()
            asteroid_state.update({
                "origin": "live", "status": "live", "rows": rows,
                "data_time": now, "last_attempt": now, "error": None,
            })
            return asteroid_state

        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
            log.warning("Asteroid query failed (%s)", error_text)

        if os.path.exists(ASTEROID_CACHE_FILE):
            try:
                with open(ASTEROID_CACHE_FILE, "r", encoding="utf-8") as file:
                    payload = json.load(file)["payload"]
                rows = _parse_asteroids(payload)
                file_time = datetime.fromtimestamp(
                    os.path.getmtime(ASTEROID_CACHE_FILE), tz=timezone.utc
                )
                asteroid_state.clear()
                asteroid_state.update({
                    "origin": "local_backup", "status": "local_backup",
                    "rows": rows, "data_time": file_time,
                    "last_attempt": now, "error": error_text,
                })
                return asteroid_state
            except Exception as error:
                error_text = f"{error_text}; backup unreadable: {error}"

        asteroid_state.clear()
        asteroid_state.update({
            "origin": "none", "status": "unavailable", "rows": [],
            "data_time": None, "last_attempt": now, "error": error_text,
        })
        return asteroid_state


# ==============================================================================
# 8. FLASK API ROUTES (Frontend Endpoints)
# ==============================================================================

def unavailable(feature, reason, status_code=200, **extra):
    """Jab koi data source unavailable ho toh clean JSON response return karta hai."""
    body = {
        "success": True,
        "available": False,
        "feature": feature,
        "status": "DATA UNAVAILABLE",
        "reason": reason,
    }
    body.update(extra)
    return jsonify(body), status_code


def api_error(feature, error):
    """Server-side exception ko log karke safe 502 JSON error bhejta hai."""
    log.error("%s API error: %s", feature, error)
    return jsonify({
        "success": False,
        "available": False,
        "feature": feature,
        "status": "ERROR",
        "error": str(error),
    }), 502


@app.route("/")
def home():
    """Main dashboard HTML page (index.html) serve karta hai."""
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/satellites")
def satellites():
    """Saare tracked Earth-orbiting objects ki live calculated positions aur Data Quality metrics deta hai."""
    global position_cache

    try:
        with refresh_lock:
            if (
                position_cache["data"] is not None
                and time.time() - position_cache["time"]
                < POSITION_CACHE_SECONDS
            ):
                cached = dict(position_cache["data"])
                cached["served_from_position_cache"] = True
                return jsonify(cached)

            started = time.time()

            catalog, feed_info, metadata, metadata_info = get_catalog()
            objects, failures = calculate_positions(catalog, metadata)

            if not objects:
                raise RuntimeError("No orbital positions could be calculated.")

            unique_objects = len(catalog["selected"])
            selected_ids = set(catalog["selected"])

            matched_ids = selected_ids.intersection(metadata.keys())
            unmatched_ids = sorted(selected_ids - matched_ids, key=int)

            unknown_objects = [o for o in objects if o["category"] == "unknown"]
            unknown_no_metadata = sum(
                1 for o in unknown_objects if not o["metadata_matched"]
            )
            unknown_type_unk = len(unknown_objects) - unknown_no_metadata

            owner_no_record = sum(
                1 for o in objects if not o["metadata_matched"]
            )
            owner_blank = sum(
                1 for o in objects
                if o["metadata_matched"] and o["owner"] == "Unknown"
            )

            categories = ["satellite", "station", "rocket_body", "debris", "unknown"]
            category_counts = {
                c: sum(1 for o in objects if o["category"] == c)
                for c in categories
            }

            feed_sources = {
                group: info.get("label", "Unavailable")
                for group, info in feed_info.items()
            }

            data_quality = {
                "tle_objects": unique_objects,
                "metadata_matches": len(matched_ids),
                "metadata_unmatched": len(unmatched_ids),
                "positions_calculated": len(objects),
                "unknown_classifications": len(unknown_objects),
                "missing_owner_metadata": owner_no_record + owner_blank,
                "metadata_match_percent": round(
                    len(matched_ids) / unique_objects * 100, 2
                ) if unique_objects else 0,
                "feed_sources": feed_sources,
                "metadata_source": metadata_info.get("label", "Unavailable"),
                "raw_feed_records": catalog["raw_records"],
                "records_by_feed": catalog["records_by_group"],
                "unique_by_feed": catalog["unique_by_group"],
                "duplicate_ids_across_feeds": len(catalog["overlapping_ids"]),
                "malformed_records_rejected": len(catalog["malformed"]),
                "unique_valid_objects": unique_objects,
                "positions_not_calculated": len(failures),
                "failed_positions": failures[:100],
                "counts_consistent": (
                    unique_objects == len(objects) + len(failures)
                ),
                "unknown_no_metadata_record": unknown_no_metadata,
                "unknown_metadata_type_unk": unknown_type_unk,
                "owner_missing_no_metadata_record": owner_no_record,
                "owner_blank_in_metadata": owner_blank,
                "feeds": feed_info,
                "metadata": metadata_info,
            }

            overall_statuses = {
                info.get("status") for info in feed_info.values()
            } | {metadata_info.get("status")}

            if overall_statuses <= {"live", "cached"}:
                source_summary = "CelesTrak GP + SATCAT (live data)"
            elif overall_statuses & {"local_backup", "unavailable"}:
                source_summary = "CelesTrak GP + SATCAT (Hybrid: Live + Backup)"
            else:
                source_summary = "CelesTrak GP + SATCAT"

            result = {
                "success": True,
                "source": source_summary,
                "tle_sources": feed_sources,
                "metadata_source": metadata_info.get("label", "Unavailable"),
                "feeds": feed_info,
                "metadata": metadata_info,
                "position_updated": utc_now().isoformat(),
                "count": len(objects),
                "data_quality": data_quality,
                "category_counts": category_counts,
                "served_from_position_cache": False,
                "satellites": objects,
            }

            position_cache = {"time": time.time(), "data": result}

            log.info(
                "Positions: %d calculated, %d failed, %d unique objects in %.1fs",
                len(objects), len(failures), unique_objects, time.time() - started,
            )

            return jsonify(result)

    except Exception as error:
        log.error("API error: %s", error)
        return jsonify({
            "success": False,
            "error": str(error),
            "satellites": []
        }), 502


@app.route("/api/congestion")
def congestion():
    """LEO, MEO, GEO orbital regimes aur 100km altitude shells ka object distribution deta hai."""
    try:
        catalog, feed_info, metadata, metadata_info, rows, skipped = (
            catalog_elements()
        )

        if not rows:
            return unavailable(
                "congestion", "No usable orbital elements in the catalogue."
            )

        regimes = {r: 0 for r in REGIME_ORDER}
        by_category = {r: Counter() for r in REGIME_ORDER}
        leo_bins = [0] * int(LEO_MAX_APOGEE_KM // 100)

        for row in rows:
            regimes[row["regime"]] += 1
            by_category[row["regime"]][row["category"]] += 1

            if row["regime"] == "LEO":
                index = int(max(row["mean_alt_km"], 0) // 100)
                if 0 <= index < len(leo_bins):
                    leo_bins[index] += 1

        return jsonify({
            "success": True,
            "available": True,
            "feature": "congestion",
            "objects_analysed": len(rows),
            "objects_skipped_unusable_elements": skipped,
            "regimes": regimes,
            "regimes_by_category": {
                r: dict(c) for r, c in by_category.items()
            },
            "leo_altitude_bins_100km": [
                {"from_km": i * 100, "to_km": (i + 1) * 100, "objects": n}
                for i, n in enumerate(leo_bins)
            ],
            "method": (
                "Altitudes derived from TLE mean motion and eccentricity "
                "(Keplerian, spherical Earth R=6378.137 km)."
            ),
            "feeds": {g: i.get("label") for g, i in feed_info.items()},
            "metadata": metadata_info,
            "generated_at": utc_now().isoformat(),
        })

    except Exception as error:
        return api_error("congestion", error)


@app.route("/api/health")
def orbital_health():
    """Orbital environment ke factual metrics (LEO share, TLE freshness, metadata coverage) deta hai."""
    try:
        catalog, feed_info, metadata, metadata_info, rows, skipped = (
            catalog_elements()
        )

        if not rows:
            return unavailable(
                "health", "No usable orbital elements in the catalogue."
            )

        now = utc_now()
        total = len(rows)

        categories = Counter(r["category"] for r in rows)
        leo_rows = [r for r in rows if r["regime"] == "LEO"]

        ages_days = sorted(
            (now - r["epoch"]).total_seconds() / 86400
            for r in rows if r["epoch"]
        )
        median_age = ages_days[len(ages_days) // 2] if ages_days else None
        stale_7d = sum(1 for a in ages_days if a > 7)
        matched = sum(1 for r in rows if r["norad_id"] in metadata)

        def pct(part, whole):
            return round(part / whole * 100, 2) if whole else None

        metrics = {
            "objects_analysed": total,
            "category_counts": dict(categories),
            "leo_objects": len(leo_rows),
            "leo_share_of_catalogue_percent": pct(len(leo_rows), total),
            "tle_epoch_median_age_days": (
                round(median_age, 2) if median_age is not None else None
            ),
            "tle_epoch_older_than_7_days_percent": pct(stale_7d, len(ages_days)),
            "metadata_coverage_percent": pct(matched, total),
        }

        scope_note = (
            "Loaded feeds: " + ", ".join(feed_info) + ". Metrics reflect active catalog composition and element freshness."
        )

        return jsonify({
            "success": True,
            "available": True,
            "feature": "health",
            "score_status": "MEASURED METRICS ONLY",
            "score_reason": (
                "Composite risk weights are excluded to maintain scientific objectivity; "
                "all underlying orbital population and TLE freshness metrics are shown below."
            ),
            "metrics": metrics,
            "scope_note": scope_note,
            "method": "Computed directly from CelesTrak GP feeds and SATCAT metadata.",
            "feeds": {g: i.get("label") for g, i in feed_info.items()},
            "metadata": metadata_info,
            "generated_at": now.isoformat(),
        })

    except Exception as error:
        return api_error("health", error)


@app.route("/api/history")
def history():
    """SATCAT launch dates ke base par space history timeline (1957 se ab tak) deta hai."""
    try:
        metadata, metadata_info = get_satcat_metadata()

        if not metadata:
            return unavailable(
                "history", "SATCAT metadata is not available.",
                metadata=metadata_info,
            )

        per_year = Counter()
        payload_per_year = Counter()
        dated = []

        for nid, row in metadata.items():
            date_text = row.get("LAUNCH_DATE", "")
            match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_text)
            if not match:
                continue

            year = int(match.group(1))
            per_year[year] += 1
            if row.get("OBJECT_TYPE") == "PAY":
                payload_per_year[year] += 1
            dated.append((date_text, nid, row))

        if not per_year:
            return unavailable(
                "history", "SATCAT contains no parseable launch dates.",
                metadata=metadata_info,
            )

        dated.sort(key=lambda item: (item[0], int(item[1])))

        earliest = [
            {
                "launch_date": date_text,
                "norad_id": nid,
                "name": row.get("OBJECT_NAME"),
                "object_type": row.get("OBJECT_TYPE"),
                "owner": row.get("OWNER") or None,
            }
            for date_text, nid, row in dated[:5]
        ]

        years = sorted(per_year)

        return jsonify({
            "success": True,
            "available": True,
            "feature": "history",
            "title": "Catalogued objects by launch year",
            "first_year": years[0],
            "last_year": years[-1],
            "total_dated_objects": sum(per_year.values()),
            "by_year": [
                {
                    "year": y,
                    "objects": per_year[y],
                    "payloads": payload_per_year[y],
                }
                for y in years
            ],
            "earliest_catalogued_launches": earliest,
            "method": "Counts of catalogued objects in CelesTrak SATCAT grouped by launch year.",
            "metadata": metadata_info,
            "generated_at": utc_now().isoformat(),
        })

    except Exception as error:
        return api_error("history", error)


@app.route("/api/asteroids")
def asteroids():
    """NASA/JPL Small-Body Database se 25 Potentially Hazardous Asteroids deta hai."""
    try:
        state = get_asteroids()

        if state["status"] == "unavailable":
            return unavailable(
                "asteroids",
                "JPL Small-Body Database could not be reached and no local backup exists.",
                error_detail=state.get("error"),
            )

        now = utc_now()

        return jsonify({
            "success": True,
            "available": True,
            "feature": "asteroids",
            "status": state["status"],
            "label": {
                "live": "Live query",
                "cached": "Live query (held in memory)",
                "local_backup": "Local backup file",
            }[state["status"]],
            "source": "NASA/JPL Small-Body Database (SBDB) Query API",
            "data_obtained_at": iso(state["data_time"]),
            "age_minutes": round(
                (now - state["data_time"]).total_seconds() / 60, 1
            ),
            "last_error": state.get("error"),
            "selection": "Top 25 Potentially Hazardous Asteroids (PHAs) with smallest Earth MOID.",
            "orbit_type": "Heliocentric osculating elements",
            "count": len(state["rows"]),
            "asteroids": state["rows"],
        })

    except Exception as error:
        return api_error("asteroids", error)


# Server start point (Python IDLE mein F5 dabane par yahan se run hota hai)



if __name__ == "__main__":
  port = int(os.environ.get("PORT", 5000))
  app.run(host="0.0.0.0", port=port, debug=False, threaded=True)





  
#The END============================            ><


  
