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
cities = cities[cities.country_id == 'NLD']
CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.main.v2.db')
isochrone_client   = Isochrones(bing_key=os.environ['BING_API_KEY'], db=CACHE)
urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, '2-external'), 
                                    target_dir=os.path.join(DROOT, '2-popmasks'))

for pid, city in cities.iterrows():
    
    isochrone_pickle_path = os.path.join(DROOT, '3-traveltime-cities', f'{city.city_id}.isochrones.pcl')
    if os.path.exists(isochrone_pickle_path):
        logging.info(f"Extract for {city.city_name} already exists, skipping")
        continue
    
    logging.info(f"Starting {city.city_name} ({city.city_id}) ===")
    
    # Extract urban center and read in as pickle
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    gdf = gdf.rename(columns={'geometry': 'raster'}).set_geometry('raster')

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
    _, (_, _, frac_done) = isochrone_client.get_isochrones(
        city_id=city.city_id, 
        points=gdf.centroid.to_crs("EPSG:4326"),
        config=isochrone_config,
        dry_run=True)
    
    if frac_done < 1:
        logging.info("Not completed yet, skipping")
        continue
    
    # Fetch including geometry.
    isochrones, (_, _, frac_done) = isochrone_client.get_isochrones(
        city_id=city.city_id, 
        points=gdf.centroid.to_crs("EPSG:4326"),
        config=isochrone_config,
        dry_run=True,
        dry_run_geometry=True,
    )
    
    # Merge raster information in isochrones
    isochrones = isochrones.merge(gdf, left_on='pid', right_index=True)
    isochrones.raster = isochrones.raster.to_crs(isochrones.isochrone.crs)
    
    # Add buffer to isochrones and calculate km2
    isochrones['isochrone']     = isochrones.isochrone.to_crs(isochrones.isochrone.estimate_utm_crs())
    isochrones['isochrone_buf'] = isochrones.isochrone.buffer(300)
    isochrones['isochrone_km2']     = isochrones.isochrone.to_crs(isochrones.isochrone.estimate_utm_crs()).area
    isochrones['isochrone']     = isochrones.isochrone.to_crs('EPSG:4326')
    isochrones['isochrone_buf'] = isochrones.isochrone_buf.to_crs('EPSG:4326')
    
    # Fill empty items and set item types.
    isochrones.isochrone = isochrones.isochrone.fillna(Polygon())
    isochrones.pid = isochrones.pid.astype(str)
    
    # Load in population density from a wider area, not corresponding with the above point_ids.
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id, buffer=15000)
    pop_gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    pop_gdf = pop_gdf.rename(columns={'geometry': 'raster'}).set_geometry('raster').to_crs(pop_gdf.estimate_utm_crs())
    pop_gdf = pop_gdf.to_crs('EPSG:4326')

    # Calculate reach
    def reach_func(isochrone_buf):
        reach_bools = pop_gdf.intersects(isochrone_buf)
        return {
            'reach_n':   reach_bools.sum(),
            'reach_km2': pop_gdf[reach_bools].raster_km2.sum(),
            'reach_pop': pop_gdf[reach_bools].cell_pop.sum(),
            'reach_geo': pop_gdf[reach_bools].unary_union
        }
    reach = isochrones.isochrone_buf.progress_apply(reach_func).apply(pd.Series)
    
    # Append to isochrones and write out
    isochrones = pd.concat([isochrones, reach], axis='columns')
    isochrones.to_pickle(isochrone_pickle_path)
    logging.info(isochrones.head(10))