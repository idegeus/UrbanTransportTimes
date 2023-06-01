import os
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
                    id       INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    uid      TEXT NOT NULL UNIQUE,
                    tt_mnts  INTEGER NOT NULL,
                    dep_dt   TIMESTAMP NOT NULL,
                    mode     TEXT NOT NULL,
                    geometry BLOB NOT NULL
                );
            """)
        logging.debug(f'Started new Isochrones object...')
        
    def _check_caches(self, ID_HDC_G0, uids):
        """Reads cache with polygons in a SQLite database."""
        
        assert isinstance(uids, pd.DataFrame)
        uids = uids.set_index('uid')
        
        with self.con:
            qry = f"SELECT uid, geometry FROM isochrone WHERE uid LIKE '{ID_HDC_G0}%'; "
            result = uids.merge(pd.read_sql_query(qry, self.con), how='left', on='uid')
            result.geometry = result.geometry.apply(lambda x: wkt.loads(x) if isinstance(x, str) else None)
            result = gpd.GeoDataFrame(result, crs="EPSG:4326")
        return result
            
    def _save_cache(self, uid, tt_mnts, dep_dt, mode, polygon):
        """Saves cache with multipolygon in a SQLite database."""
        try:
            with self.con:
                sql = """
                    INSERT INTO isochrone (uid, tt_mnts, dep_dt, mode, geometry) 
                    values(?, ?, ?, ?, ?)
                """
                result = self.con.execute(sql, (uid, tt_mnts, dep_dt, mode, polygon.iloc[0].wkt))
        except sl.IntegrityError:
            raise sl.IntegrityError(f"Please make sure the UID is unique, '{uid}' is not.")
        
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
        groupsize = 100
        for x in range(0, len(to_fetch), groupsize):
            
            # Send async start request.
            callbacks = {}
            for i, item in to_fetch.iloc[x:x+groupsize].iterrows():
            
                logging.info(f'Requesting {item.uid}')
                
                # Optimise for best result at departure time. This was done wrongly initially, so now fixed.
                optimise = 'time'  # 'timeWithTraffic' if 'driving' in item['mode'] else 'time'

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
                self.response = response_json
                if len(response_json['resourceSets']) == 1:
                    callbacks[item.uid] = response_json['resourceSets'][0]['resources'][0]['callbackUrl']
                else:
                    callbacks[item.uid] = False
            
            # We assume that sending these requests took longer than 4 seconds..
            for i, item in to_fetch.iloc[x:x+groupsize].iterrows(): 
                
                #iterator.set_description(f'Fetching {item.uid}')
                if not callbacks[item.uid]:
                    continue
                
                logging.info(f'Fetching {item.uid}')
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
                result = gpd.GeoSeries(MultiPolygon(polygons)).set_crs("EPSG:4326")
                
                self._save_cache(item.uid, item.tt_mnts, item.dep_dt.to_pydatetime(), item['mode'], result)
    
    def _get_isochrones_graphhopper(self, to_fetch):
        
        # Check if graphhopper url is actually set.
        assert len(self.graphhopper_url) > 0
        
        for pid, item in to_fetch.iterrows():    

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
            logging.debug(f'Requesting {item.uid}')
            self.response = response_json = requests.get(endpoint, params=params).json()
            if 'polygons' not in response_json:
                raise sl.OperationalError(self.response)
                
            # Parse as geometry
            result = gpd.GeoDataFrame.from_features(response_json['polygons'], crs="EPSG:4326")
            area = result.to_crs(result.estimate_utm_crs()).area[0]
            logging.info(f"Fetched {item.uid} with area={area:.1f}m2.")
            if area < 100:
                logging.warning(f"Result for {item.uid} area is small: {area:.1f}m2.")
                
            # Save cache
            self._save_cache(item.uid, item.tt_mnts, item.dep_dt.to_pydatetime(), item['mode'], result.geometry)

    def get_isochrones(self, city_id, batch, source='graphhopper'):
        """
        Gets isochrones for specific points. 
        :ID_HDC_G0       City ID to be searched.
        :batch           Iter with each [(starpt, uid, tt_mnts, (mode, dep_dt))]
        :source          Data source (defaults to GraphHopper, but can be configured to be Bing Maps)
        
        :return          Reachable areas in row in format MultiPolygon.
        
        Param batch is subject to the following:
        :param starpt    A Shapely point which is the origin.
        :param uid       Unique location ID for saving in the caching database.
        :param tt_mnts   Maximum travel time
        :param mode      Travel mode in [driving, walking, cycling, transit]
        :param dep_dt    Departure datetime object.
        """
            
        # Input checks
        for (pid, startpt), tt_mnts, (mode, modetime, dep_dt) in batch:
            assert isinstance(startpt, Point)
            assert isinstance(dep_dt, datetime)
            assert isinstance(tt_mnts, int)
            assert tt_mnts >= 5
            assert tt_mnts <= 60
            assert mode in ['driving', 'walking', 'cycling', 'transit']
            assert isinstance(tt_mnts, int)
        
        # Generate combinations with ((pid,point), (mode/modetime/date), minutes, uid).
        batch = [(pid, startpt, tt_mnts, mode, modetime, dep_dt)
                 for (pid, startpt), tt_mnts, (mode, modetime, dep_dt) in list(batch)]
        batch = gpd.GeoDataFrame(batch, columns=['pid', 'startpt', 'tt_mnts', 'mode', 'modetime', 'dep_dt'], 
                                 geometry='startpt', crs='EPSG:4326')
        batch['uid'] = batch.apply(lambda x: f"{city_id}-{x.pid}-{x.modetime}-{x.tt_mnts}m-v0", axis=1)
        
        # Localised batch to timezone-aware.
        center = batch.unary_union.centroid
        tz = TimezoneFinder().timezone_at(lng=center.x, lat=center.y)
        batch.dep_dt = batch.dep_dt.dt.tz_localize(tz)
        logging.info(f"Converted batch to timezone {tz}.")
        
        # Check cache
        fetched = self._check_caches(city_id, batch)
        to_fetch = batch[fetched.geometry.isna()]
        logging.info(f"Out of total {len(batch)}, {100-len(to_fetch)/len(batch)*100:.2f}% cached.")
        
        # Fetch uncached isochrones
        if source == 'bing':
            self._get_isochrones_bing(to_fetch)
        if source == 'graphhopper':
            self._get_isochrones_graphhopper(to_fetch)
        
        # To guarantee safety, we only pull out our queries from the (now filled) database.
        result = self._check_caches(city_id, batch)
        
        return result

def test():
    
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    logging.info("Hello!")
    DROOT = '../1-data'
    
    BING_KEY = os.environ.get('BING_API_KEY')
    CACHE = os.path.join(DROOT, '0-tmp', 'test.db')
    client = Isochrones(graphhopper_url="http://localhost:8989", bing_key=BING_KEY, db=CACHE)
    
    # Read a test city to be processed.
    cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'), index_col=0)
    city = cities[cities.City == 'Amsterdam'].iloc[0]
    
    # Get pickle and load into GeoDataFrame.
    file = f'{city.ID_HDC_G0}.pcl'
    df = pd.read_pickle(os.path.join(DROOT, '3-interim', 'populationmasks', file))
    gdf = gpd.GeoDataFrame(df)
    gdf = gdf.iloc[200:]
    
    # Set queries and
    origins  = enumerate(gdf.centroid.to_crs("EPSG:4326"))
    times    = [5] # [5, 15, 25, 35, 45]
    modes_dt = [
        # ('driving', 'driving-peak', datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        # ('driving', 'driving-off',  datetime(2023, 6, 13, 8, 30, 37)), # Graphhopper doesn't do traffic.
        ('driving', 'driving',      datetime(2023, 6, 13, 8, 30, 37)), 
        ('transit', 'transit-peak', datetime(2023, 6, 13, 8, 30, 37)),
        ('transit', 'transit',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('cycling', 'cycling',      datetime(2023, 6, 13, 13, 0, 37)), 
        ('walking', 'walking',      datetime(2023, 6, 13, 8, 30, 37))
    ]
    
    batch      = list(itertools.product(origins, times, modes_dt))
    isochrones = client.get_isochrones(city.ID_HDC_G0, batch)
    print(isochrones)

if __name__ == "__main__":
    test()
    