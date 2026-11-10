
from flask import Flask, jsonify, send_from_directory
from skyfield.api import load, EarthSatellite
from datetime import datetime, timezone, timedelta
import requests
import os

app = Flask(__name__, static_folder=None)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ts = load.timescale(builtin=True)

TLE_URL = (
    "https://celestrak.org/NORAD/elements/"
    "gp.php?GROUP=stations&FORMAT=tle"
)

tle_cache = {"time": None, "lines": []}


def get_tle_data():
    now = datetime.now(timezone.utc)

    if (tle_cache["time"] is None or
        now - tle_cache["time"] > timedelta(hours=2)):

        response = requests.get(TLE_URL, timeout=25)
        response.raise_for_status()

        lines = [
            line.strip()
            for line in response.text.splitlines()
            if line.strip()
        ]

        tle_cache["lines"] = lines
        tle_cache["time"] = now

    return tle_cache["lines"]


@app.route("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/satellites")
def satellites():
    try:
        lines = get_tle_data()
        current_time = ts.now()
        objects = []

        for i in range(0, len(lines) - 2, 3):
            name, line1, line2 = lines[i:i + 3]

            try:
                satellite = EarthSatellite(
                    line1, line2, name, ts
                )

                position = satellite.at(current_time)
                point = position.subpoint()

                objects.append({
                    "name": name,
                    "latitude": round(
                        point.latitude.degrees, 3
                    ),
                    "longitude": round(
                        point.longitude.degrees, 3
                    ),
                    "altitude_km": round(
                        point.elevation.km, 2
                    )
                })

            except Exception:
                continue

        return jsonify({
            "success": True,
            "source": "CelesTrak GP orbital elements",
            "data_type": "Calculated positions, not radar observations",
            "updated": datetime.now(timezone.utc).isoformat(),
            "count": len(objects),
            "satellites": objects
        })

    except Exception as error:
        return jsonify({
            "success": False,
            "error": str(error),
            "satellites": []
        }), 502


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False
    )
