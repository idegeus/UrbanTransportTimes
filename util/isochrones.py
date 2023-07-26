import os
import sys
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
from tqdm import tqdm
from dotenv import load_dotenv

sys.path.append(os.path.realpath('../'))
from util.extract_urbancenter import ExtractCenters

class Isochrones:
    """Facilitates interaction with Isochrones and caches results."""
    
    def __init__(self, bing_key=None, graphhopper_url=None, db='cache.db'): 
        self.bing_key = bing_key
        self.graphhopper_url = graphhopper_url
        self.db = db
        self.response = ""
        
        self.con = sl.connect(db)
        with self.con:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS isochrone (
                    uid        TEXT NOT NULL PRIMARY KEY,
                    city_id    TEXT NOT NULL,
                    pid        INTEGER NOT NULL,
                    pt_lat     REAL NOT NULL,
                    pt_lon     REAL NOT NULL,
                    
                    tt_mnts    INTEGER NOT NULL,
                    dep_dt     TIMESTAMP NOT NULL,
                    mode       TEXT NOT NULL,
                    source     TEXT NOT NULL,
                    
                    geometry   BLOB NOT NULL
                );
            """)
        logging.debug(f'Started new Isochrones object...')
        
    def _check_caches(self, city_id, batch):
        """Reads cache with polygons in a SQLite database."""
        
        assert isinstance(batch, pd.DataFrame)
        batch = batch.set_index('uid')
        
        logging.debug("Finding isochrones from cache..")
        with self.con:
            qry = f"SELECT uid, geometry FROM isochrone WHERE city_id='{city_id}'"
            cached = pd.read_sql_query(qry, self.con)
            
        logging.debug("Loading in geometry..")
        cached['geometry'] = cached['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else None)
        cached = cached.rename(columns={'geometry': 'isochrone'})
        cached = gpd.GeoDataFrame(cached, crs='EPSG:4326', geometry='isochrone')
        
        result = batch.merge(cached, how='left', on='uid')
        result = result.set_geometry('isochrone')
        
        return result
    
    def _save_cache(self, item, polygon):
        """Saves cache with multipolygon in a SQLite database."""
        polygon = gpd.GeoSeries([polygon], crs='EPSG:4326')
        if polygon.iloc[0].area > 0.0001:
            polygon = polygon.to_crs(polygon.estimate_utm_crs()).simplify(100).to_crs('EPSG:4326')
        polygon = polygon.iloc[0].wkt
        try:
            with self.con:
                sql = """
                    INSERT INTO isochrone (uid, city_id, pid, pt_lat, pt_lon, tt_mnts, dep_dt, mode, source, geometry)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                result = self.con.execute(sql, (
                    item.uid, 
                    item.city_id,
                    item.pid,
                    item.startpt.y,
                    item.startpt.x,
                    item.tt_mnts, 
                    item.dep_dt.to_pydatetime(), 
                    item['trmode'],
                    item.source,
                    polygon))
        except sl.IntegrityError:
            raise sl.IntegrityError(f"Constraint failed, check above with UID '{item.uid}'")
        
        return result
    
    def nearest(self, point):
        url = f"{self.graphhopper_url}/nearest"
        response = requests.request("GET", url, params={"point": f"{point.y},{point.x}"}).json()
        coords = response['coordinates']
        return Point(coords[0], coords[1])

    def _get_isochrones_bing(self, to_fetch):
        assert len(self.bing_key) > 0
            
        # Fetch in groups, first requesting it and then asking asynchronously for it.
        groupsize = 50
        iterator = tqdm(range(0, len(to_fetch), groupsize), total=len(to_fetch)//50)
        for x in iterator:
            
            # Send async start request.
            callbacks = {}
            for i, item in to_fetch.iloc[x:x+groupsize].iterrows():
            
                logging.debug(           f'Requesting {item.uid}')
                iterator.set_description(f'Requesting {item.uid}')
                
                # Optimise for best result at departure time. This was done wrongly initially, so now fixed.
                optimise = 'timeWithTraffic' if 'driving' in item['trmode'] else 'time'

                # Format date string.
                dep_dt_str = item.dep_dt.strftime("%d/%m/%Y %H:%M:%S")

                # Fetch polygon from Bing Maps
                params = {
                    'waypoint': f"{item.startpt.y},{item.startpt.x}", # LatLng
                    'maxTime': item.tt_mnts,
                    'timeUnit': 'minute',
                    'distanceUnit': 'kilometer',
                    'optimise': optimise,
                    'dateTime': dep_dt_str, # Example: 03/01/2011 05:42:00
                    'travelMode': item['trmode'],
                    'key': self.bing_key
                }
                endpoint = 'https://dev.virtualearth.net/REST/v1/Routes/IsochronesAsync'
                response_json = requests.get(endpoint, params=params).json()
                if len(response_json['resourceSets']) == 1:
                    callbacks[item.uid] = response_json['resourceSets'][0]['resources'][0]['callbackUrl']
                else:
                    callbacks[item.uid] = False
            
            # We assume that sending these requests took longer than 4 seconds..
            for i, item in to_fetch.iloc[x:x+groupsize].iterrows(): 
                
                if not callbacks[item.uid]:
                    continue
                
                logging.debug(           f'Fetching {item.uid}')
                iterator.set_description(f'Fetching {item.uid}')
                callback_json = requests.get(callbacks[item.uid]).json()
                callback_result_url = callback_json['resourceSets'][0]['resources'][0]['resultUrl']
                response_json = requests.get(callback_result_url).json()
                
                # Extract polygons to MultiPolygon
                polygons = []
                if ((len(response_json['resourceSets']) == 0) or 
                    ('polygons' not in response_json['resourceSets'][0]['resources'][0])): 
                    logging.warning(f"No resourceSets found for: {item.uid}")
                else:
                    for l1 in response_json['resourceSets'][0]['resources'][0]['polygons']:
                        for l2 in l1['coordinates']:
                            polygons.append(Polygon([[e[1], e[0]] for e in l2]))
                result = MultiPolygon(polygons)
                
                self._save_cache(item, result)
    
    def _get_isochrones_graphhopper(self, to_fetch):

        # Check if graphhopper url is actually set.
        assert len(self.graphhopper_url) > 0
        
        iterator = tqdm(to_fetch.iterrows(), total=to_fetch.shape[0], smoothing=0)
        for pid, item in iterator:

            # Get timezone estimation adoption so time is in local time, and format date string.
            dep_dt_str = item.dep_dt.astimezone(pytz.utc).isoformat().replace("+00:00", 'Z')
            
            # Translate standardised string to graphhopper version.
            gh_mode = {
                "driving_off": 'car_cbr_off',
                "driving_peak": 'car_cbr_peak',
                "walking": 'foot',
                "cycling": 'bike',
                'transit_off': 'pt',
                'transit_peak': 'pt',
                'transit_off_bike': 'pt',
                'transit_peak_bike': 'pt'
            }

            # Set required parameters.
            endpoint = f'{self.graphhopper_url}/isochrone'
            params = {
                'point': f"{item.startpt.y},{item.startpt.x}", # LatLng
                'time_limit': item.tt_mnts * 60,
                'profile': gh_mode[item['trmode']]
            }
                
            # Extra parameters are necessary if it is a public transport query.
            if gh_mode[item['trmode']] == 'pt':
                params = params | {
                    "pt.access_profile": "foot",
                    "pt.egress_profile": "foot",
                    "pt.profile": 'true',
                    "pt.earliest_departure_time": dep_dt_str,
                    "pt.arrive_by": 'false',
                    "reverse_flow": "false",
                    'profile': 'pt',
                }
        
            # Execute query.
            iterator.set_description(f'Requesting {item.uid}')
            self.response = response_json = requests.get(endpoint, params=params).json()
            
            # If polygons are in the response, calculate area and give some suggestion. 
            if 'polygons' in response_json:
                # Check if there are indeed 1 multipolygon in here.
                result = gpd.GeoDataFrame.from_features(response_json['polygons'], crs="EPSG:4326")
                assert len(result) == 1
                
                # Check area size.
                result_utm = result.to_crs(result.estimate_utm_crs())
                area = result_utm.area[0]
                if os.environ.get('ENVIRON', '') == 'dev' and area < 100:
                    logging.warning(f"Result for {item.uid} area is small: {area:.1f}m2.")
                
                # Remove unneccesary detail and convert back to geometry to be saved.
                result_utm.geometry = result_utm.buffer(10)
                result = result_utm.to_crs("EPSG:4326")
                geometry = result.iloc[0].geometry
                
            # If not in the response, give a warning and continue with an empty polygon. 
            else:
                logging.warning(self.response)
                geometry = Polygon()
                
            # Save cache
            self._save_cache(item, geometry)

    def get_isochrones(self, city_id, points, config, dry_run=False):
        """
        Gets isochrones for specific points. 
        
        Args:
        city_id (str):      City ID to store requests under.
        points (list):      Iterable of tuples with an id and a Shapely origin point.
        config (list):      Iter with configurations to get for each point.
        dry_run (bool):     Fetch new items, or only return cache. Defaults to False.
        
        Returns:
        result (gdf):       Reachable areas in GeoDataFrame with previous options, MultiPolygon.
        info_tuple (tuple): Info with respective batch size, rows cached and fraction completed
        
        Param config is subject to the following:
        :param (
            trmode          Travel mode in [driving, walking, cycling, transit]
            tt_mnts         List of minutes of which to fetch config setups.
            dep_dt          Departure datetime object.
            source          Either Bing or Graphhopper
        )
        """
        
        # Input checks
        batch = []
        for (pid, point) in enumerate(points):
            for trmode, tt_mnts_list, dep_dt, source in config:
                for tt_mnts in tt_mnts_list:
                    batch.append((pid, point, trmode, tt_mnts, dep_dt, source))
        batch = gpd.GeoDataFrame(
            batch, columns=['pid', 'startpt', 'trmode', 'tt_mnts', 'dep_dt', 'source'], 
            geometry='startpt', crs='EPSG:4326')
        batch['city_id'] = city_id
        batch['uid'] = batch.apply(lambda x: f"{x.city_id}-{x.pid}-{x.trmode}-{x.tt_mnts}m-{x.source}", axis=1)
        
        # Type checking
        assert batch.trmode.isin(['driving_off', 'driving_peak', 'transit_off', 'transit_peak', 'walking', 'cycling']).all()
        assert batch.source.isin(['g', 'b', 'h']).all() # GraphHopper, Bing, Here
        logging.debug(batch.head(15))
        
        # Localised batch to timezone-aware.
        center = batch.unary_union.centroid
        tz = TimezoneFinder().timezone_at(lng=center.x, lat=center.y)
        batch.dep_dt = batch.dep_dt.dt.tz_localize(tz)
        logging.debug(f"Converted batch to timezone {tz}.")
        
        # Check cache
        batch_cached = self._check_caches(city_id, batch)
        to_fetch = batch_cached[(batch_cached.geometry.isna()) & (~batch_cached.geometry.is_empty)]
        frac_done = 1 - (len(to_fetch)/len(batch))
        logging.info(f"Out of total {len(batch)}, {frac_done*100:.1f}% cached.")
        
        # Return if a dry run.
        if dry_run:
            if frac_done < 1.0:
                logging.info("Dry run: not fetching possible unavailable geometry due to flag.")
            return batch_cached, (len(batch), len(batch)-len(to_fetch), frac_done)
            
        # Fetch uncached isochrones
        bing_fetch = to_fetch[to_fetch.source == 'b']
        grph_fetch = to_fetch[to_fetch.source == 'g']
        if len(bing_fetch) > 0:
            self._get_isochrones_bing(bing_fetch)
        if len(grph_fetch) > 0:
            self._get_isochrones_graphhopper(grph_fetch)
        
        # To guarantee safety, we only pull out our queries from the (now filled) database.
        result = self._check_caches(city_id, batch)
        
        # Calculate again
        to_fetch = batch_cached[(batch_cached.geometry.isna()) & (~batch_cached.geometry.is_empty)]
        frac_done = 1 - (len(to_fetch)/len(batch))
        
        return result, (len(batch), len(batch)-len(to_fetch), frac_done)

def test():
    
    load_dotenv()
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    DROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../1-data')
    urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, "2-external"), target_dir=os.path.join(DROOT, "2-popmasks"), res=1000)
    
    CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.test.db')
    isochrone_client = Isochrones(
        graphhopper_url="http://localhost:8989", 
        bing_key=os.environ['BING_API_KEY'],
        db=CACHE)
    
    # Read a test city to be processed.
    cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'))
    city = cities[cities.City == 'Amsterdam'].iloc[0]
    pcl_path = urbancenter_client.extract_city(city.City, city.ID_HDC_G0)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    # Set queries and
    isochrone_config = [
        # ('driving', [10, 25], datetime(2023, 6, 13, 8, 30, 0),  'b'),
        # ('transit', [15, 30], datetime(2023, 6, 13, 8, 30, 37), 'b'),
        # ('transit', [15, 30], datetime(2023, 6, 13, 13, 0, 37), 'b'), 
        ('transit_off', [15, 30], datetime(2023, 6, 13, 13, 30, 37), 'g'),
        ('transit_peak', [15, 30], datetime(2023, 6, 13, 8, 30, 37), 'g'),
        ('driving_off', [10, 25], datetime(2023, 6, 13, 8, 30, 0), 'g'),
        ('driving_peak', [10, 25], datetime(2023, 6, 13, 8, 30, 0),  'g'),
        ('cycling', [15, 30], datetime(2023, 6, 13, 8, 30, 0),  'g'), 
        ('walking', [15, 30], datetime(2023, 6, 13, 8, 30, 0),  'g')
    ]
    
    isochrones = isochrone_client.get_isochrones(
        city_id=city.ID_HDC_G0, 
        points=gdf.centroid.to_crs("EPSG:4326"),
        config=isochrone_config
    )

if __name__ == "__main__":
    test()