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

# Import custom libraries
sys.path.append(os.path.realpath('../'))
from util.isochrones import Isochrones
from util.extract_urbancenter import ExtractCenters

# Start processing cities.
DROOT = './1-data/'
cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'))
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(bing_key=os.environ['BING_API_KEY'], db=CACHE)
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), 
                                    target_dir=os.path.join(DROOT, '2-popmasks'))

for pid, city in cities.iterrows():
    
    logging.info(f"=== Starting {city.city_name} ({city.city_id}) ===")
    
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
    isochrones, (_, _, frac_done) = isochrone_client.get_isochrones(
        city_id=city.city_id, 
        points=gdf.centroid.to_crs("EPSG:4326"),
        config=isochrone_config,
        dry_run=True
    )
    
    isochrone_pickle_path = os.path.join(DROOT, '3-traveltime-cities', f'{city.city_id}.isochrones.pcl')
    if os.path.exists(isochrone_pickle_path):
        continue
    
    if frac_done < 1:
        logging.info("Not completed yet, skipping")
        continue
    
    isochrones = isochrones.merge(gdf, left_on='pid', right_index=True)
    isochrones.isochrone = isochrones.isochrone.fillna(Polygon())
    isochrones.pid = isochrones.pid.astype(str)
    
    # Load in population density from a wider area, not corresponding with the above point_ids.
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id, buffer=15000)
    pop = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    pop = pop.rename(columns={'geometry': 'raster'}).set_geometry('raster').set_crs(pop.crs)
    pop['raster_km2'] = pop.raster.area
    pop = pop.to_crs(isochrones.crs)

    def reach_func(row):
        reach_bools = pop.intersects(row.isochrone)
        return {
            'reach_n':   reach_bools.sum(),
            'reach_km2': pop[reach_bools].raster_km2.sum(),
            'reach_pop': pop[reach_bools].cell_pop.sum(),
            'reach_geo': pop[reach_bools].unary_union
        }
    
    reach = isochrones.progress_apply(reach_func, axis='columns', result_type='expand')
    isochrones = pd.concat([isochrones, reach], axis='columns')
    
    isochrones.to_pickle(isochrone_pickle_path)