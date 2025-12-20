import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_KEY")
DB_PASS = os.getenv("DB_PASSWORD")

URL_LOCS = "https://api.openaq.org/v3/locations?bbox=80.1,12.8,80.35,13.2"
HEADERS = {"X-API-Key": API_KEY}

# We are adding weather parameters here
INTERESTED_PARAMS = ['pm25', 'no2', 'temperature', 'humidity', 'wind_speed', 'pressure']

def sync_all_aspects():
    try:
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password=DB_PASS)
        cur = conn.cursor()

        # The table remains the same - it can handle any 'pollutant' or 'parameter' name
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chennai_measurements (
                id SERIAL PRIMARY KEY,
                station_name TEXT,
                pollutant TEXT,
                value FLOAT,
                unit TEXT,
                measured_at TIMESTAMP,
                geom GEOMETRY(Point, 4326)
            );
        """)

        print("🔍 Searching for Environmental & Weather data...")
        response = requests.get(URL_LOCS, headers=HEADERS)
        locations = response.json().get('results', [])

        for loc in locations:
            loc_id = loc['id']
            name = loc['name']
            lon, lat = loc['coordinates']['longitude'], loc['coordinates']['latitude']
            sensor_lookup = {s['id']: s['parameter'] for s in loc.get('sensors', [])}

            latest_url = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
            latest_res = requests.get(latest_url, headers=HEADERS).json()
            
            for r in latest_res.get('results', []):
                param_info = sensor_lookup.get(r.get('sensorsId'))
                
                if param_info and param_info['name'] in INTERESTED_PARAMS:
                    print(f"📈 Found {param_info['name']} at {name}: {r['value']}")
                    cur.execute("""
                        INSERT INTO chennai_measurements (station_name, pollutant, value, unit, measured_at, geom)
                        VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    """, (name, param_info['name'], r['value'], param_info['units'], r['datetime']['utc'], lon, lat))
        
        conn.commit()
        print("✅ Success! Air Quality and Weather data are synchronized.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    sync_all_aspects()