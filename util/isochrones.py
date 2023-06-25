import os
import time
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
                    item['mode'],
                    item.source,
                    polygon))
        except sl.IntegrityError:
            raise sl.IntegrityError(f"Constraint failed, check above with UID '{item.uid}'")
        
        return result
    
    def snap_coordinates(self, coordinate):
        """
        Some Centroids are not close to a road, and will return a bad result. 
        This function snaps the coordinates to the nearest road.
        """
        url = "https://dev.virtualearth.net/REST/v1/Routes/SnapToRoad"
        params = {
            'points': f"{coordinate.y},{coordinate.x}",
            'travelMode': 'driving',
            'key': self.api_key
        }
        response = requests.get(url, params=params)
        self.response = response
        response_json = response.json()
        
        coords = response_json['resourceSets'][0]['resources']['snappedPoints'][0]['coordinate']
        return Point( (coords['longitude'], coords['latitude']) )

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
                optimise = 'timeWithTraffic' if 'driving' in item['mode'] else 'time'

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
                    'travelMode': item['mode'],
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
        
        iterator = tqdm(to_fetch.iterrows(), total=to_fetch.shape[0])
        for pid, item in iterator:

            # Get timezone estimation adoption so time is in local time, and format date string.
            dep_dt_str = item.dep_dt.astimezone(pytz.utc).isoformat().replace("+00:00", 'Z')
            
            # Translate standardised string to graphhopper version.
            gh_mode = {
                "driving": 'car',
                "walking": 'foot',
                "cycling": 'bike',
                'transit': 'pt'
            }

            # Set required parameters.
            endpoint = f'{self.graphhopper_url}/isochrone'
            params = {
                'point': f"{item.startpt.y},{item.startpt.x}", # LatLng
                'time_limit': item.tt_mnts * 60,
                'profile': gh_mode[item['mode']]
            }
                
            # Extra parameters are necessary if it is a public transport query.
            if item['mode'] == 'transit':
                params = params | {
                    "pt.access_profile": "foot",
                    "pt.profile": 'false',
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
                result = gpd.GeoDataFrame.from_features(response_json['polygons'], crs="EPSG:4326")
                assert len(result) == 1
                geometry = result.iloc[0].geometry
                
                if os.environ.get('ENVIRON', '') == 'dev':
                    area = result.to_crs(result.estimate_utm_crs()).area[0]
                    if area < 100:
                        logging.warning(f"Result for {item.uid} area is small: {area:.1f}m2.")
            
            # If not in the response, give a warning and continue with an empty polygon. 
            else:
                logging.warning(self.response)
                geometry = Polygon()
                
            # Save cache
            self._save_cache(item, geometry)

    def get_isochrones(self, city_id, points, tt_mnts, config, fetch=True):        
        """
        Gets isochrones for specific points. 
        
        Args:
        city_id (str):      City ID to store requests under.
        points (list):      Iterable of tuples with an id and a Shapely origin point.
        tt_mnts (list):     List of minutes of which to fetch config setups.
        config (list):      Iter with configurations to get for each point
        fetch (bool):       Fetch new items, or only return cache. Defaults to True.
        
        Returns:
        result (gdf):       Reachable areas in GeoDataFrame with previous options, MultiPolygon.
        
        Param config is subject to the following:
        :param (
            mode            Travel mode in [driving, walking, cycling, transit]
            modetime        Travel mode identification in [driving, walking, cycling, transit]
            dep_dt          Departure datetime object.
            source          Either Bing or Graphhopper
        )
        """
        
        # Input checks
        batch = list(itertools.product(points, tt_mnts, config))
        for (pid, startpt), tt_mnts, (mode, modetime, dep_dt, source) in batch:
            assert isinstance(startpt, Point)
            assert isinstance(startpt, Point)
            assert isinstance(dep_dt, datetime) or dep_dt == None
            assert isinstance(tt_mnts, int)
            assert tt_mnts >= 5
            assert tt_mnts <= 60
            assert mode in ['driving', 'walking', 'cycling', 'transit']
            assert source in ['g', 'b', 'h'] # GraphHopper, Bing, Here
            assert isinstance(tt_mnts, int)
        
        # Generate combinations with ((pid,point), (mode/modetime/date), minutes, uid).
        batch = [(pid, startpt, tt_mnts, mode, modetime, dep_dt, source)
                 for (pid, startpt), tt_mnts, (mode, modetime, dep_dt, source) in batch]
        batch = gpd.GeoDataFrame(list(batch), 
                                 columns=['pid', 'startpt', 'tt_mnts', 'mode', 'modetime', 'dep_dt', 'source'], 
                                 geometry='startpt', 
                                 crs='EPSG:4326')
        batch['city_id'] = city_id
        batch['uid'] = batch.apply(lambda x: f"{x.city_id}-{x.pid}-{x.modetime}-{x.tt_mnts}m-{x.source}", axis=1)
        
        # Localised batch to timezone-aware.
        center = batch.unary_union.centroid
        tz = TimezoneFinder().timezone_at(lng=center.x, lat=center.y)
        batch.dep_dt = batch.dep_dt.dt.tz_localize(tz)
        logging.info(f"Converted batch to timezone {tz}.")
        
        # Check cache
        batch_cached = self._check_caches(city_id, batch)
        to_fetch = batch_cached[(batch_cached.geometry.isna()) & (~batch_cached.geometry.is_empty)]
        logging.info(f"Out of total {len(batch)}, {100-len(to_fetch)/len(batch)*100:.1f}% cached.")
        
        # Return if not fetching
        if not fetch:
            if to_fetch.shape[0] > 0:
                logging.info("Not fetching unavailable geometry due to flag.")
            return batch_cached
            
        # Fetch uncached isochrones
        if len(to_fetch[to_fetch.source == 'b']) > 0:
            self._get_isochrones_bing(to_fetch[to_fetch.source == 'b'])
        if len(to_fetch[to_fetch.source == 'g']) > 0:
            self._get_isochrones_graphhopper(to_fetch[to_fetch.source == 'g'])
        
        # To guarantee safety, we only pull out our queries from the (now filled) database.
        result = self._check_caches(city_id, batch)
        
        return result

def test():
    
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    load_dotenv()
    DROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../1-data')
    
    CACHE = os.path.join(DROOT, '3-traveltime-cache', 'cache.test.db')
    client = Isochrones(
        graphhopper_url="http://localhost:8989", 
        bing_key=os.environ['BING_API_KEY'],
        db=CACHE)
    
    # Read a test city to be processed.
    cities = pd.read_excel(os.path.join(DROOT, '1-targets', 'cities.xlsx'), index_col=0)
    city = cities[cities.City == 'Amsterdam'].iloc[0]
    
    # Get pickle and load into GeoDataFrame.
    file = f'{city.ID_HDC_G0}.pcl'
    df = pd.read_pickle(os.path.join(DROOT, '2-popmasks', file))
    gdf = gpd.GeoDataFrame(df)
    
    # Set queries and
    config = [
        ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 0),  'b'),
        ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 0),  'g'),
        # ('transit', 'transit-peak', datetime(2023, 6, 13, 8, 30, 37), 'b'),
        # ('transit', 'transit',      datetime(2023, 6, 13, 13, 0, 37), 'b'), 
        ('cycling', 'cycling',      datetime(2023, 6, 13, 8, 30, 0),  'g'), 
        ('walking', 'walking',      datetime(2023, 6, 13, 8, 30, 0),  'g')
    ]
    
        # ('driving', 'driving-off',  datetime(2023, 6, 13, 13, 30, 0), 'b'),
    
    isochrones = client.get_isochrones(
        city_id=city.ID_HDC_G0, 
        points=enumerate(gdf.centroid.to_crs("EPSG:4326")),
        tt_mnts=[15], # [5, 15, 25, 35, 45],
        config=config
    )

if __name__ == "__main__":
    test()