import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import traceback
import pandas as pd
import geopandas as gpd
from datetime import datetime
from dotenv import load_dotenv
import warnings
from urllib import error

# Import custom libraries
DROOT = '../1-data'
sys.path.append(os.path.realpath('../'))
from util.isochrones import Isochrones
from util.graphhopper import Graphhopper
from util.extract_urbancenter import ExtractCenters
from util.fetch_transitland_gtfs import GtfsDownloader
from util.extract_osm import extract_osm

# Create a file handler and set the level to DEBUG
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s', datefmt='%Y%m%d,%H:%M:%S')
logging.getLogger().setLevel(logging.DEBUG)
file_handler = RotatingFileHandler(os.path.join(DROOT, 'debug.log'), mode='a', maxBytes=2*1024*1024, backupCount=2)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

load_dotenv()

# Read a test city to be processed.
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.latest.xlsx'))
# cities = cities[cities.city_name == 'Paris']
logging.info(f"Total cities to be done: {cities.shape[0]}")

# Initialise Graphhopper client.
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(graphhopper_url="http://localhost:8989", db=CACHE, bing_key=os.environ['BING_API_KEY'])
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), target_dir=os.path.join(DROOT, '2-popmasks'), res=1000)
gtfs_client        = GtfsDownloader(os.environ.get("TRANSITLAND_KEY"))

for pid, city in cities.iterrows():

    logging.info(f"{f'{city.city_name} ({city.city_id})' :=^20}")
    
    # Extract urban center and read in as pickle
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    peak_dt = datetime(2023, 8, 22, 8, 30, 0)
    off_dt  = datetime(2023, 8, 22, 13, 30, 0)
    isochrone_config = [
        ('transit_off',        [15, 30], off_dt,  'g'),
        ('transit_peak',       [15, 30], peak_dt, 'g'),
        ('transit_bike_off',   [15, 30], off_dt,  'g'),
        ('transit_bike_peak',  [15, 30], peak_dt, 'g'),
        ('driving_off',        [10, 25], off_dt,  'g'),
        ('driving_peak',       [10, 25], peak_dt, 'g'),
        ('cycling',            [15, 30], peak_dt, 'g'), 
        ('walking',            [15, 30], peak_dt, 'g')
    ]
    
    # Check if records are all done 
    result, info_tuple = isochrone_client.get_isochrones(
        city_id=city.city_id, 
        points=gdf.centroid.to_crs("EPSG:4326"),
        config=isochrone_config,
        dry_run=True
    )
    warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
    if (result.cache_avail.sum() / result.shape[0]) == 1.0:
        continue
    
    # Create OSM extracts
    osm_src = os.environ.get('OSM_PLANET_PBF', os.path.join(DROOT, '2-osm', 'src', 'planet-latest.osm.pbf'))
    osm_out = os.path.join(DROOT, '2-osm', 'out', f'{city.city_id}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=20000)
    
    try:
    
        # Fetch GTFS files
        gtfs_client.set_search(bbox.centroid, bbox, 10000)
        feed_ids = gtfs_client.search_feeds()
        feeds = gtfs_client.download_feeds(feed_ids, os.path.join(DROOT, '2-gtfs'), city.city_id, [peak_dt, off_dt])
        
        # Boot Graphhopper instance
        graphhopper = Graphhopper(droot=DROOT, city=city.city_id)
        graphhopper.set_osm(osm_out)
        graphhopper.set_gtfs(feeds)
        graphhopper.build()
        
        # Try to calibrate example build.
        sample = gdf.centroid.to_crs('EPSG:4326').sample(15, random_state=10)
        sample = sample.apply(lambda x: graphhopper.nearest(x))
        graphhopper.calibrate(sample)
        
        # Fetch isochrones.
        points = gdf.centroid.to_crs("EPSG:4326").apply(lambda x: graphhopper.nearest(x))
        isochrones = isochrone_client.get_isochrones(
            city_id=city.city_id, 
            points=points,
            config=isochrone_config
        )

    except:
        logging.critical("Problem, continuing with next city.")
        logging.critical(traceback.print_exc())