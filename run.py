import os
import time
import pytz
import requests
import logging
import pandas as pd
import itertools
import geopandas as gpd
import sqlite3 as sl
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely import wkt
from datetime import datetime
from timezonefinder import TimezoneFinder
from dotenv import load_dotenv
import docker

# Import custom libraries
from util.isochrones import Isochrones
from util.fetch_transitland_gtfs import Downloader
from util.extract_osm import extract_osm

# Set logging and variables
logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN

load_dotenv()
DROOT = './1-data'
CACHE = os.path.join(DROOT, '3-interim', 'graphhopper.db')

# Read a test city to be processed.
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'), index_col=0)
logging.info(f"Total cities to be done: {cities.shape[0]}")

# TMP
cities = cities[cities.City == 'Amsterdam']

# Initialise Graphhopper client.
client = Isochrones(graphhopper_url="http://localhost:8989", db=CACHE)

for pid, city in cities.iterrows():
    
    # Get pickle and load into GeoDataFrame.
    file = f'{city.ID_HDC_G0}.pcl'
    df = pd.read_pickle(os.path.join(DROOT, '3-interim', 'populationmasks', file))
    gdf = gpd.GeoDataFrame(df)
    
    # Create OSM extracts
    osm_src = os.path.join(DROOT, '2-gh', 'osm', 'src', 'europe-latest.osm.pbf')
    osm_out = os.path.join(DROOT, '2-gh', 'osm', 'out', '{city.ID_HDC_G0}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=5000, overwrite=False)
    
    # Fetch GTFS files
    gtfs_client = Downloader(os.environ.get("TRANSITLAND_KEY"))
    gtfs_client.set_search(bbox.centroid, bbox, 10000)
    feed_ids = gtfs_client.search_feeds()
    gtfs_client.download_feeds(feed_ids, os.path.join(DROOT, '2-gh', 'gtfs'), city.ID_HDC_G0)
    
    # Create Graphhopper based on created files. # TODO Change YML file.
    dclient = docker.from_env()
    os.system('docker run -p 8989:8989 -d -v ${PWD}/1-data:/1-data --entrypoint /bin/bash israelhikingmap/graphhopper -c "java -Xmx10g -Xms10g -jar /graphhopper/*.jar server /1-data/2-gh/config-duttv2.yml"')
    
    # Wait for building to finish.
    online = False
    while not online:
        try:
            response = requests.get("http://localhost:8989/health")
            if response.status_code == 200:
                online = True
        except ConnectionRefusedError:
            time.sleep(10)

    # Start querying Grasshopper. 
    origins  = enumerate(gdf.centroid.to_crs("EPSG:4326"))
    times    = [5] # [5, 15, 25, 35, 45]
    modes_dt = [
        # ('driving', 'driving-peak', datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        # ('driving', 'driving-off',  datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 37)), 
        ('transit', 'transit-peak', datetime(2023, 6, 13, 8, 30, 37)),
        ('transit', 'transit',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('cycling', 'cycling',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('walking', 'walking',      datetime(2023, 6, 13, 8, 30, 37))
    ]

    batch      = list(itertools.product(origins, times, modes_dt))
    isochrones = client.get_isochrones(city.ID_HDC_G0, batch)