import pandas as pd
import geopandas as gpd
import os
import sys
from datetime import datetime
from shapely.geometry import Polygon
import logging
logging.getLogger().setLevel(logging.INFO)
import json

from dotenv import load_dotenv
load_dotenv()

# Import custom libraries
sys.path.append(os.path.realpath('../'))
from util.isochrones import Isochrones
from util.extract_urbancenter import ExtractCenters

# Write out status of city progress.
DROOT = '../1-data/'
cities = pd.read_csv(os.path.join(DROOT, '1-research', 'cities.latest.csv'))
cities_dict = {region:cities[cities.region == region].sort_values('city_name').to_dict(orient='records') for region in cities.region.unique()}

json.dump(cities_dict, open('../3-web/src/assets/data/cities.json', 'w'))