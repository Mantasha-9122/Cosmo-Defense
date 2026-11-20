# COSMO-DEFENSE

An interactive 3D orbital intelligence and space-domain awareness dashboard built with **Python (Flask)**, **Skyfield (SGP4)**, and **CesiumJS**. It visualizes Earth-orbiting objects alongside the Sun and Potentially Hazardous Asteroids (PHAs) in a Tactical HUD interface.

## Features
- **3D Earth & Heliocentric Views:** Switch between Earth-orbiting objects (`Earth 3D`) and a heliocentric solar system view (`Sun + Asteroids`).
- **Object Classification & Catalog:** Filter and search tracked objects by category (`Satellite`, `Space Station`, `Rocket Body`, `Debris`, `Heliocentric Asteroid`, `Unknown`).
- **Interactive Object Pop-ups & Orbit Tracks:** Click any satellite or asteroid on the 3D globe or catalog to inspect its mission purpose, orbital parameters, and 3D trajectory.
- **Orbital Analytics Panels:** View Data Quality checks, Orbital Health metrics, Congestion shells, and Space History timelines.

## Data Sources
- **Earth-Orbiting Objects:** CelesTrak GP (TLE) feeds (`active`, `stations`) and SATCAT metadata (`satcat.csv`), with local backup support when offline.
- **Orbital Propagation:** SGP4 orbital calculations via **Skyfield**.
- **Near-Earth Asteroids:** NASA/JPL Small-Body Database (SBDB) Query API.

## Project Structure
- `app.py` — Flask backend server, data fetcher/cache manager, and REST API endpoints.
- `index.html` — Single-file Tactical HUD frontend and CesiumJS 3D visualization.
- `requirements.txt` — Python dependencies (`Flask`, `requests`, `skyfield`).
- `*.tle`, `satcat.csv`, `asteroids_cache.json` — Local cache/backup data files.

