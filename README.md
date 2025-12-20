# Chennai-Environmental-Monitoring

## 🛠 Technical Architecture

### 1. Spatial Data Layer
The project utilizes a `chennai_wards.geojson` file containing administrative boundaries. Key properties mapped include:
* **Zone Name:** Categorical identifiers (e.g., "St. Thomas Mount", "Zone 1").
* **Zone ID:** Used for database indexing (`Zone_No`).
* **Geometry:** WGS84 (EPSG:4326) Polygons defining the spatial extent of Chennai's 15 zones.

### 2. Live Monitoring (OpenAQ v3)
We utilize the OpenAQ v3 API to fetch real-time atmospheric data:
* **Pollutants:** PM2.5, NO2, and SO2.
* **Weather Integration:** Cross-referencing windspeed data to calculate pollution dispersion.

### 3. Analytics Pipeline
* **Spatial Join:** Sensors are mapped to administrative zones using PostGIS `ST_Contains`.
* **Risk Scoring:** A custom algorithm weights PM2.5 concentrations against Indian National Air Quality Standards to generate a "Health Risk Index."
