import os
import logging
import pandas as pd
import numpy as np
import itertools
import geopandas as gpd
from datetime import datetime
from dotenv import load_dotenv
import docker
import yaml
import warnings
from util.extract_urbancenter import ExtractCenters
from urllib import request, error

# Import custom libraries
from util.isochrones import Isochrones
from util.fetch_transitland_gtfs import GtfsDownloader
from util.extract_osm import extract_osm
import shutil

# Set logging and variables
logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler and set the level to DEBUG
file_handler = logging.FileHandler('debug.log')
file_handler.setLevel(logging.DEBUG)

# Create a console handler and set the level to WARNING
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

DROOT = './1-data'
load_dotenv()

# Read a test city to be processed.
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'))
logger.info(f"Total cities to be done: {cities.shape[0]}")

# Initialise Graphhopper client.
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(graphhopper_url="http://localhost:8989", db=CACHE, bing_key=os.environ['BING_API_KEY'])
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), target_dir=os.path.join(DROOT, '2-popmasks'), res=1000)
gtfs_client        = GtfsDownloader(os.environ.get("TRANSITLAND_KEY"))

# cities = cities[cities.City == 'Rotterdam [The Hague]']
cities = cities[cities.City == 'Amsterdam']

for pid, city in cities.iterrows():

    logging.info(f"=== Starting {city.City} ({city.ID_HDC_G0}) ===")
    
    # Extract urban center and read in as pickle
    pcl_path = urbancenter_client.extract_city(city.City, city.ID_HDC_G0)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    # Check if records are all done    
    isochrone_config = [
        # ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 0),  'b'),
        ('driving', 'driving',      datetime(2023, 6, 13, 13, 30, 0),  'g'),
        # ('transit', 'transit',      datetime(2023, 6, 13, 8, 30, 37), 'g'),
        # ('transit', 'transit-off',      datetime(2023, 6, 13, 13, 0, 37), 'b'), 
        ('cycling', 'cycling',      datetime(2023, 6, 13, 8, 30, 0),  'g'), 
        ('walking', 'walking',      datetime(2023, 6, 13, 8, 30, 0),  'g')
    ]
    
    isochrones = isochrone_client.get_isochrones(
        city_id=city.ID_HDC_G0, 
        points=enumerate(gdf.centroid.to_crs("EPSG:4326")),
        tt_mnts=[10, 15, 25], #35, 45],
        config=isochrone_config,
        fetch=False
    )

    warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
    if (isochrones.isochrone.notna().sum() / isochrones.shape[0]) == 1.0:
        continue
    
    # Create OSM extracts
    osm_src = os.path.join(DROOT, '2-osm', 'src', 'europe-latest.osm.pbf')
    osm_out = os.path.join(DROOT, '2-osm', 'out', f'{city.ID_HDC_G0}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=20000, overwrite=False)
    
    # # Fetch GTFS files
    gtfs_client.set_search(bbox.centroid, bbox, 10000)
    feed_ids = gtfs_client.search_feeds()
    gtfs_out_dir = os.path.join(DROOT, '2-gtfs')
    feed_paths = gtfs_client.download_feeds(feed_ids, gtfs_out_dir, city.ID_HDC_G0)
    
    # Check if cache should be removed
    config_src = os.path.join(DROOT, '2-gh', 'config-duttv2.src.yml')
    config_out = os.path.join(DROOT, '2-gh', 'config-duttv2.yml')
    if os.path.exists(config_out):
        with open(config_out, 'r') as f:
            config_old = yaml.safe_load(f)
        if config_old['graphhopper']['datareader.file'] == osm_out:
            logger.info("Cache already exists, not rebuilding.")
        else:
            logger.info("OSM changed, cleaning cache.")
            shutil.rmtree("./1-data/2-gh/graph-cache", ignore_errors=True)
    
    # Update configuration yaml file
    with open(config_src, 'r') as f:
        config = yaml.safe_load(f)
    config['graphhopper']['datareader.file'] = osm_out
    config['graphhopper']['gtfs.file'] = ",".join(feed_paths)
    with open(config_out, 'w',) as f:
        yaml.dump(config, f, sort_keys=False, default_flow_style=None)
    
    
    # Try couple times if docker is unexpectidely closed
    for x in range(5):
        
        # Start querying Grasshopper. 
        try:
            
            # Create Graphhopper based on created files.
            mem = os.environ.get('MEMORY', 8)
            dclient = docker.from_env()
            for d in dclient.containers.list():
                logging.info(d)
                d.stop()
                d.remove(force=True)
            
            logger.info(f"Starting docker now with {mem}g memory...")
            graphhopper = dclient.containers.run(
                image="israelhikingmap/graphhopper", 
                detach=True,
                init=True,
                command=f'"cd ../ && java -Xmx{mem}g -Xms{mem}g -jar ./graphhopper/*.jar server ./1-data/2-gh/config-duttv2.yml"',
                environment={"JAVA_OPTS": f"-Xmx{mem}g -Xms{mem}g"},
                volumes={os.path.realpath(DROOT): {'bind': '/1-data', 'mode': 'rw'}}, 
                entrypoint='/bin/bash -c',
                ports={'8989/tcp':'8989/tcp'}
            )
            
            # Wait for Graphhopper to actually finish preparing the graph.
            for line in graphhopper.logs(stream=True):
                line = str(line)
                logger.debug(line)
                if "org.eclipse.jetty.server.Server - Started" in line:
                    break

            # Check if container is built successfully and didn't stop yet.
            if graphhopper.status == "exited":
                raise RuntimeError("Graphhopper didn't load successfully, please check logs.")
            
            isochrones = isochrone_client.get_isochrones(
                city_id=city.ID_HDC_G0, 
                points=enumerate(gdf.centroid.to_crs("EPSG:4326")),
                tt_mnts=[10, 15, 25], #35, 45],
                config=isochrone_config
            )
            logger.info("Finished fetching items")
            graphhopper.stop()
            graphhopper.remove()
            break
        
        except ConnectionError as error:
            logger.critical("Seems like docker unexpectidly quit. Retrying..")
            logger.critical(error)
        
        except Exception as error:
            logger.critical("Something happened that stopped the querying:")
            logger.critical(error)