import psycopg2
import folium
import json
import os
import pandas as pd
from dotenv import load_dotenv
from folium import plugins

# 1. Load Environment Variables
load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")

def generate_chennai_dashboard():
    try:
        # 2. Database Connection
        print("🔗 Connecting to PostGIS...")
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password=DB_PASS
        )
        cur = conn.cursor()

        # 3. Query A: Average PM2.5 per Zone (for coloring the map)
        # We join the GeoJSON shapes (chennai_wards) with the readings (chennai_measurements)
        zone_query = """
        SELECT 
            w."zone name" as zone_name, 
            AVG(m.value) as avg_value
        FROM chennai_wards w
        JOIN chennai_measurements m ON ST_Contains(w.wkb_geometry, m.geom)
        WHERE m.pollutant = 'pm25'
        GROUP BY w."zone name";
        """
        df_zones = pd.read_sql(zone_query, conn)

        # 4. Query B: Individual Sensor Locations (for the blue dots)
        sensor_query = """
        SELECT 
            station_name, 
            value, 
            ST_Y(geom) as lat, 
            ST_X(geom) as lon 
        FROM chennai_measurements 
        WHERE pollutant = 'pm25';
        """
        cur.execute(sensor_query)
        sensors = cur.fetchall()

        conn.close()

        if df_zones.empty:
            print("⚠️ No spatial overlap found. Ensure your sensors are within the ward boundaries!")
            return

        # 5. Initialize the Map
        print("🗺️ Building the interactive map...")
        # Centered on Chennai (Central Station area)
        m = folium.Map(location=[13.0827, 80.2707], zoom_start=11, tiles="cartodbpositron")

        # 6. Load GeoJSON for the Choropleth
        with open("chennai_wards.geojson", "r") as f:
            geo_data = json.load(f)

        # 7. Add Choropleth Layer (The Color Map)
        choropleth = folium.Choropleth(
            geo_data=geo_data,
            name="Pollution Heatmap",
            data=df_zones,
            columns=["zone_name", "avg_value"],
            key_on="feature.properties.Zone Name", # Matches the key in your GeoJSON file
            fill_color="YlOrRd", # Yellow to Red
            fill_opacity=0.6,
            line_opacity=0.3,
            legend_name="Average PM2.5 (µg/m³)",
            highlight=True
        ).add_to(m)

        # 8. Add Hover Tooltips to the Zones
        # We create a dictionary for quick lookup: { "Zone Name": Value }
        zone_data_dict = df_zones.set_index('zone_name')['avg_value'].to_dict()

        for feature in choropleth.geojson.data['features']:
            z_name = feature['properties']['Zone Name']
            val = zone_data_dict.get(z_name, "No Data")
            # Format the value if it's a number
            display_val = f"{val:.2f} µg/m³" if isinstance(val, float) else val
            feature['properties']['current_pm25'] = display_val

        choropleth.geojson.add_child(
            folium.features.GeoJsonTooltip(
                fields=['Zone Name', 'current_pm25'],
                aliases=['Zone:', 'Avg PM2.5:'],
                style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
            )
        )

        # 9. Add Individual Sensor Markers
        # These show exactly where the OpenAQ stations are located
        marker_cluster = plugins.MarkerCluster(name="Sensor Locations").add_to(m)
        for name, val, lat, lon in sensors:
            folium.CircleMarker(
                location=[lat, lon],
                radius=6,
                popup=folium.Popup(f"<b>Station:</b> {name}<br><b>PM2.5:</b> {val} µg/m³", max_width=300),
                color="blue",
                fill=True,
                fill_color="cyan",
                fill_opacity=0.8
            ).add_to(marker_cluster)

        # 10. Final Touches
        folium.LayerControl().add_to(m)
        output_name = "chennai_environmental_dashboard.html"
        m.save(output_name)
        
        print(f"✨ Success! Your dashboard is ready: {output_name}")

    except Exception as e:
        print(f"❌ Error building map: {e}")

if __name__ == "__main__":
    generate_chennai_dashboard()