import os
import requests
import pandas as pd
import geopandas as gpd
import sqlite3 as sl
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely import wkt
from datetime import datetime
import logging

class Isochrones:
    """Facilitates interaction with Isochrones and caches results."""
    
    def __init__(self, bing_key=None, valhalla_url=None, db = 'cache.db'): 
        self.bing_key = bing_key
        self.valhalla_url = valhalla_url
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
        logging.debug(f'Started new Bing object with key {self.bing_key[:10]}...')
        
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

    def get_isochrones_bing(self, to_fetch):
        
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
    
    def get_isochrones_valhalla(to_fetch):
        
        print('wew')


    def get_isochrones(self, city_id, batch, buf_m=0):
        """
        Gets isochrones for specific points. 
        :ID_HDC_G0       City ID to be searched.
        :batch           Iter with each [(starpt, uid, tt_mnts, (mode, dep_dt))]
        :param buf_m     [Optional] Amount of padding in polygon in meters.
        :param tqdm      [Optional] Show loading indicator of the points.
        :return          Reachable areas in row in format MultiPolygon.
        
        Param batch is subject to the following:
        :param starpt    A Shapely point which is the origin.
        :param uid       Unique location ID for saving in the caching database.
        :param tt_mnts   Maximum travel time
        :param mode      Travel mode in [driving, walking, transit]
        :param dep_dt    Departure datetime object.
        """
            
        # Input checks
        for (pid, startpt), tt_mnts, (mode, modetime, dep_dt) in batch:
            assert isinstance(startpt, Point)
            assert isinstance(dep_dt, datetime)
            assert isinstance(tt_mnts, int)
            assert tt_mnts >= 5
            assert tt_mnts <= 45
            assert mode in ['driving', 'walking', 'transit']
            assert isinstance(tt_mnts, int)
        assert buf_m >= 0
        
        # Generate combinations with ((pid,point), (mode/modetime/date), minutes, uid).
        batch = [(pid, startpt, tt_mnts, mode, modetime, dep_dt)
                 for (pid, startpt), tt_mnts, (mode, modetime, dep_dt) in list(batch)]
        batch = pd.DataFrame(batch, columns=['pid', 'startpt', 'tt_mnts', 'mode', 'modetime', 'dep_dt'])
        batch['uid'] = batch.apply(lambda x: f"{city_id}-{x.pid}-{x.modetime}-{x.tt_mnts}m-v0", axis=1)
    
        # Check cache
        fetched = self._check_caches(city_id, batch)
        to_fetch = batch[fetched.geometry.isna()]
        logging.info(f"Out of total {len(batch)}, {100-len(to_fetch)/len(batch)*100:.2f}% cached.")
        
        # Fetch uncached isochrones using bing.
        self.get_isochrones_bing(to_fetch)
        
        # To guarantee safety, we only pull out our queries from the (now filled) database.
        result = self._check_caches(city_id, batch)
        
        # Add padding as per requirement
        if buf_m > 0:
            result = result.to_crs(result.estimate_utm_crs())
            result.geometry = result.geometry.buffer(buf_m)
            result = result.to_crs("EPSG:4326")
        
        return result
    
    
if __name__ == "__main__":
    logging.warning("Hello!")