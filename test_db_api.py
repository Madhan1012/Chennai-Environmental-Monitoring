import os
import requests
import psycopg2
from dotenv import load_dotenv

# 1. Load your secrets
load_dotenv()
API_KEY = os.getenv("OPENAQ_KEY")
DB_PASS = os.getenv("DB_PASSWORD")

# 2. Test Database Connection
def test_db_connection():
    try:
        print("Checking database connection...")
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password=DB_PASS,
            port="5432"
        )
        print("Database is reachable!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# 3. Fetch Chennai Data (API v3)
def fetch_and_save():
    conn = test_db_connection()
    if not conn:
        return

    # Chennai Bounding Box
    url = "https://api.openaq.org/v3/locations?bbox=80.1,12.8,80.35,13.2&limit=5"
    headers = {"X-API-Key": API_KEY}

    try:
        print("Fetching data from OpenAQ...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            locations = response.json().get('results', [])
            cur = conn.cursor()

            # Create the table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chennai_raw (
                    id SERIAL PRIMARY KEY,
                    station_name TEXT,
                    lat FLOAT,
                    lon FLOAT
                );
            """)

            for loc in locations:
                cur.execute(
                    "INSERT INTO chennai_raw (station_name, lat, lon) VALUES (%s, %s, %s)",
                    (loc['name'], loc['coordinates']['latitude'], loc['coordinates']['longitude'])
                )

            conn.commit()
            print(f"Success! Saved {len(locations)} stations to the database.")
            cur.close()
            conn.close()
        else:
            print(f"API Error {response.status_code}: {response.text}")

    except Exception as e:
        print(f"Script Error: {e}")

if __name__ == "__main__":
    fetch_and_save()