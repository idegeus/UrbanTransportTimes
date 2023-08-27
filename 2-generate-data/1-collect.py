import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import traceback
import pandas as pd
import geopandas as gpd
import datetime
from dotenv import load_dotenv
import warnings

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
cities_path = os.path.join(DROOT, '1-research', 'cities.latest.csv')
cities = pd.read_csv(cities_path, index_col=False)
cities = cities.sort_values(['priority', 'country_id', 'city_name'])
logging.info(f"Total cities to be done: {cities.shape[0]}")

# Initialise Graphhopper client.
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(graphhopper_url="http://localhost:8989", db=CACHE, bing_key=os.environ['BING_API_KEY'])
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), target_dir=os.path.join(DROOT, '2-popmasks'), res=1000)
gtfs_client        = GtfsDownloader(os.environ.get("TRANSITLAND_KEY"))

filter = (cities.country_id == 'ESP')
for pid, city in cities.iterrows():

    logging.info(f"{f'{city.city_name} ({city.city_id})' :=^30}")
    if city.n_req == city.n_req_ok and city.frac_req_ok == 1.0:
        logging.info(f"Already completed, skipping.")
        continue
    
    # Extract urban center and read in as pickle
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))

    # Create OSM extracts
    osm_src = os.environ.get('OSM_PLANET_PBF', os.path.join(DROOT, '2-osm', 'src', 'planet-latest.osm.pbf'))
    osm_out = os.path.join(DROOT, '2-osm', 'out', f'{int(city.city_id)}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=20000)
    
    try:
    
        # Set departure times
        peak_dt = datetime.datetime(2023, 9, 12, 8, 30, 0)
        off_dt  = datetime.datetime(2023, 9, 12, 13, 30, 0)
    
        # Fetch GTFS files
        gtfs_client.set_search(bbox.centroid, bbox, 10000)
        feed_ids = gtfs_client.search_feeds()
        feeds = gtfs_client.download_feeds(feed_ids, os.path.join(DROOT, '2-gtfs'), 
                                           city.city_id, [peak_dt, off_dt])
        
        # Conditionally fetch transit information. 
        isochrone_config = [
            ('driving_off',        [10, 25], off_dt,  'g'),
            ('driving_peak',       [10, 25], peak_dt, 'g'),
            ('cycling',            [15, 30], peak_dt, 'g'), 
            ('walking',            [15, 30], peak_dt, 'g')
        ]
        
        if len(feeds) == 0:
            logging.warning(f"No fitting feeds for {city.city_name} ({city.city_id}) were found.")
        else:
            isochrone_config += [
                ('transit_off',        [15, 30], off_dt,  'g'),
                ('transit_peak',       [15, 30], peak_dt, 'g'),
                ('transit_bike_off',   [15, 30], off_dt,  'g'),
                ('transit_bike_peak',  [15, 30], peak_dt, 'g')
            ]
        
        # Boot Graphhopper instance
        graphhopper = Graphhopper(droot=DROOT, city=city.city_id)
        graphhopper.set_osm(osm_out)
        graphhopper.set_gtfs(feeds)
        graphhopper.build()
        
        # Try to calibrate example build.
        sample = gdf.centroid.to_crs('EPSG:4326').sample(15, random_state=10)
        sample = sample.apply(lambda x: graphhopper.nearest(x))
        graphhopper.calibrate(sample, peak_dt=peak_dt, off_dt=off_dt)
        
        # Fetch isochrones.
        points = gdf.centroid.to_crs("EPSG:4326").apply(lambda x: graphhopper.nearest(x))
        isochrones, (batch_n, batch_n_done, frac_done) = isochrone_client.get_isochrones(
            city_id=city.city_id, 
            points=points,
            config=isochrone_config
        )
        
        # Write most recent city info out.
        cities.loc[pid, 'n_cells'] = len(points)
        cities.loc[pid, 'n_req'] = batch_n
        cities.loc[pid, 'n_req_ok'] = batch_n_done
        cities.loc[pid, 'frac_req_ok'] = frac_done
        cities.to_csv(cities_path, index_col=False)
        
    except:
        logging.critical("Problem, continuing with next city.")
        logging.critical(traceback.print_exc())
        