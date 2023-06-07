import os
import logging
import pandas as pd
import itertools
import geopandas as gpd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import docker
import yaml
from util.extract_urbancenter import ExtractCenters

# Import custom libraries
from util.isochrones import Isochrones
from util.fetch_transitland_gtfs import GtfsDownloader
from util.extract_osm import extract_osm

# Set logging and variables
logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
DROOT = './1-data'
load_dotenv()

# Read a test city to be processed.
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'), index_col=0)
logging.info(f"Total cities to be done: {cities.shape[0]}")

# Initialise Graphhopper client.
CACHE = os.path.join(DROOT, '3-interim', 'graphhopper.db')
isochrone_client   = Isochrones(graphhopper_url="http://localhost:8989", db=CACHE)
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'))
gtfs_client        = GtfsDownloader(os.environ.get("TRANSITLAND_KEY"))

for pid, city in cities.iterrows():
    
    # Extract urban center and read in as pickle
    pcl_path = urbancenter_client.extract_city(
        city.City, 
        city.ID_HDC_G0, 
        os.path.join(DROOT, '3-interim', 'populationmasks'))
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    # Create OSM extracts
    osm_src = os.path.join(DROOT, '2-gh', 'osm', 'src', 'europe-latest.osm.pbf')
    osm_out = os.path.join(DROOT, '2-gh', 'osm', 'out', f'{city.ID_HDC_G0}.osm.pbf')
    bbox = gdf.to_crs('EPSG:4326').unary_union
    extract_osm(osm_src, osm_out, bbox, buffer_m=5000, overwrite=False)
    
    # Fetch GTFS files
    gtfs_client.set_search(bbox.centroid, bbox, 10000)
    feed_ids = gtfs_client.search_feeds()
    gtfs_out_dir = os.path.join(DROOT, '2-gh', 'gtfs')
    feed_paths = gtfs_client.download_feeds(feed_ids, gtfs_out_dir, city.ID_HDC_G0)
    
    # Update configuration yaml file
    config_src = os.path.join(DROOT, '2-gh', 'config-duttv2.src.yml')
    config_out = os.path.join(DROOT, '2-gh', 'config-duttv2.yml')
    with open(config_src, 'r') as f:
        config = yaml.safe_load(f)
    config['graphhopper']['datareader.file'] = osm_out
    config['graphhopper']['gtfs.file'] = ",".join(feed_paths)
    with open(config_out, 'w',) as f:
        yaml.dump(config, f, sort_keys=False, default_flow_style=None)
    
    # Create Graphhopper based on created files.
    logging.info("Starting docker now...")
    mem = os.environ.get('MEMORY', 8)
    
    dclient = docker.from_env()
    graphhopper = dclient.containers.run(
        image="israelhikingmap/graphhopper", 
        detach=True,
        command=f'"cd ../ && rm -rf ./1-data/2-gh/graph-cache && sleep 120 && java -Xmx{mem}g -Xms{mem}g -jar ./graphhopper/*.jar server ./1-data/2-gh/config-duttv2.yml"',
        environment={"JAVA_OPTS": f"-Xmx{mem}g -Xms{mem}g"},
        volumes={os.path.realpath(DROOT): {'bind': '/1-data', 'mode': 'rw'}}, 
        entrypoint='/bin/bash -c',
        ports={'8989/tcp':'8989/tcp'}
    )
    
    # Wait for Graphhopper to actually finish preparing the graph.
    start_ts = datetime.utcnow()
    max_minutes = 30
    for line in graphhopper.logs(stream=True):
        line = str(line)
        logging.info(line)
        if "org.eclipse.jetty.server.Server - Started" in line:
            break

    # Check if container is built successfully and didn't stop yet.
    if graphhopper.status == "exited":
        raise RuntimeError("Graphhopper didn't load successfully, please check logs.")
    
    # Start querying Grasshopper. 
    origins  = enumerate(gdf.centroid.to_crs("EPSG:4326"))
    times    = [5, 15, 25, 35, 45]
    modes_dt = [
        # ('driving', 'driving-peak', datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        # ('driving', 'driving-off',  datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 37)), 
        # ('transit', 'transit-peak', datetime(2023, 6, 13, 8, 30, 37)), 
        # ('transit', 'transit',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('cycling', 'cycling',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('walking', 'walking',      datetime(2023, 6, 13, 8, 30, 37))
    ]

    batch      = list(itertools.product(origins, times, modes_dt))
    try:
        isochrones = isochrone_client.get_isochrones(city.ID_HDC_G0, batch)
    except:
        graphhopper.stop()
        graphhopper.remove()