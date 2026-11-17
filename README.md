# Cosmo-Defense
An advanced orbital surveillance & planetary defense engine that serves as Earth's digital frontline against space debris and cosmic threats. Combines real-time Space-Track arrays with NASA NeoWs telemetry via Python to intercept and project satellite tactical near-misses and high-risk asteroid vectors onto an interactive, military-grade 3D CesiumJS rendering loop.
# COSMO-DEFENSE // ORBITAL INTELLIGENCE

Passive orbital intelligence and educational space-domain awareness dashboard built with Python (Flask), Skyfield, and CesiumJS.

## Data Sources
- **Earth-Orbiting Objects:** CelesTrak GP (TLE) feeds (`active`, `stations`) and SATCAT metadata (`satcat.csv`).
- **Orbital Propagation:** SGP4 via Skyfield (calculated positions, not live radar observations).
- **Near-Earth Asteroids:** NASA/JPL Small-Body Database (SBDB) Query API (heliocentric osculating elements).

## Run Locally
```cmd
cd /d D:\Cosmo-Defense
pip install -r requirements.txt
python app.py