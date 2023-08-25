import pandas as pd
import geopandas as gpd
import os
import sys
from datetime import datetime
from shapely.geometry import Polygon
import logging
logging.getLogger().setLevel(logging.INFO)

from dotenv import load_dotenv
load_dotenv()

from tqdm import tqdm
tqdm.pandas()

# Import custom libraries
sys.path.append(os.path.realpath('../'))
from util.isochrones import Isochrones
from util.extract_urbancenter import ExtractCenters

# Start processing cities.
DROOT = '../1-data/'
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.latest.xlsx'))
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(bing_key=os.environ['BING_API_KEY'], db=CACHE)
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), 
                                    target_dir=os.path.join(DROOT, '2-popmasks'))

isochrones_together = []
for pid, city in cities.iterrows():
    
    isochrone_pickle_path = os.path.join(DROOT, '3-traveltime-cities', f'{city.city_id}.isochrones.pcl')
    if not os.path.exists(isochrone_pickle_path):
        logging.warning(f"Isochrones for {city.city_name} ({city.city_id}) do not exist, skipping.")
        continue
    
    logging.info(f"Loading {city.city_name} ({city.city_id})...")
    isochrones_together.append(pd.read_pickle(isochrone_pickle_path))
    
isochrones = pd.concat(isochrones_together)
isochrones.to_pickle(os.path.join(DROOT, '3-traveltime-cities', f'latest.isochrones.pcl'))