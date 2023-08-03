import os
import logging
import traceback
import pandas as pd
import geopandas as gpd
from datetime import datetime
from dotenv import load_dotenv
import warnings
from util.extract_urbancenter import ExtractCenters
from urllib import error
from util.graphhopper import Graphhopper

# Import custom libraries
from util.isochrones import Isochrones
from util.fetch_transitland_gtfs import GtfsDownloader
from util.extract_osm import extract_osm

# Create a file handler and set the level to DEBUG
# formatter = logging.Formatter('%(name)-6s: %(levelname)-8s %(message)s')
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s', datefmt='%Y%m%d,%H:%M:%S')
logging.getLogger().setLevel(logging.DEBUG)
file_handler = logging.FileHandler('debug.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

DROOT = './1-data'
load_dotenv()

# Read a test city to be processed.
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'))
# cities = cities[cities.city_name == 'Stockholm']
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
    
    peak_dt = datetime(2023, 6, 13, 8, 30, 0)
    off_dt  = datetime(2023, 6, 13, 13, 30, 0)
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
    if (result.isochrone.notna().sum() / result.shape[0]) == 1.0:
        continue
    
    # Create OSM extracts
    osm_src = os.environ.get('OSM_PLANET_PBF', os.path.join(DROOT, '2-osm', 'src', 'europe-latest.osm.pbf'))
    osm_out = os.path.join(DROOT, '2-osm', 'out', f'{city.city_id}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=20000)
    
    # Fetch GTFS files
    gtfs_client.set_search(bbox.centroid, bbox, 10000)
    feed_ids = gtfs_client.search_feeds()
    gtfs_out_dir = os.path.join(DROOT, '2-gtfs')
    feed_paths = gtfs_client.download_feeds(feed_ids, gtfs_out_dir, city.city_id)
    
    for attempt in range(5):
    
        # Flag for recreating if exception
        force = False
        
        # Start GraphHopper instance.
        try:
            # Boot Graphhopper instance
            graphhopper = Graphhopper(droot=DROOT, city=city.city_id)
            graphhopper.set_osm(osm_out)
            graphhopper.set_gtfs(feed_paths)
            graphhopper.build()
            
            # Try to calibrate example build.
            sample = gdf.centroid.to_crs('EPSG:4326').sample(15, random_state=10)
            sample = sample.apply(lambda x: graphhopper.nearest(x))
            graphhopper.calibrate(sample, force=force)
            
            # Fetch isochrones.
            points = gdf.centroid.to_crs("EPSG:4326").apply(lambda x: graphhopper.nearest(x))
            isochrones = isochrone_client.get_isochrones(
                city_id=city.city_id, 
                points=points,
                config=isochrone_config
            )
            break

        except Exception:
            logging.critical("Something happened that stopped the querying:")
            logging.critical(traceback.format_exc())
            force=True
            pass