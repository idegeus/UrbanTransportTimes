import datetime
import os
import logging
import stat
import time
import pandas as pd
import numpy as np
import itertools
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point, LineString
from timezonefinder import TimezoneFinder
from pytz import timezone
import docker
import yaml
import json
import sys
import numpy as np
import requests
from scipy.optimize import minimize
import shutil
import traceback

# Import custom libraries
sys.path.append(os.path.realpath('../'))
from util.extract_urbancenter import ExtractCenters

class Graphhopper:
    droot = ''
    config = {}
    config_src = {}
    config_original = {}
    container = None
    calibrated = False
    city = ''
    fetched_google = 0
    lockfile_path = ''
    
    def __init__(self, droot, city):
        self.droot = droot
        self.city = str(city)
        self.config_src_path   = os.path.join(self.droot, '2-gh', 'config-duttv2.src.yml')
        self.config_out_path   = os.path.join(self.droot, '2-gh', 'config-duttv2.yml')
        self.factor_cache_path = os.path.join(self.droot, '2-gh', 'factor-cache.json')
        self.lockfile_path     = os.path.join(self.droot, '2-gh', 'lockfile.json')
        
        if os.path.exists(self.factor_cache_path):
            self.factor_cache = json.load(open(self.factor_cache_path, 'r'))
        else:
            self.factor_cache = {}
        
        self.config_src = yaml.safe_load(open(self.config_src_path, 'r'))
        self.config     = yaml.safe_load(open(self.config_src_path, 'r'))
        
        if os.path.exists(self.config_out_path):
            self.config_original = yaml.safe_load(open(self.config_out_path, 'r'))
    
    def set_osm(self, osm_path):
        self.config['graphhopper']['datareader.file'] = osm_path
        self.set_config()
    
    def set_gtfs(self, gtfs_paths):
        assert isinstance(gtfs_paths, (list, np.array, pd.Series))
        if len(gtfs_paths) > 0:
            self.config['graphhopper']['gtfs.file'] = ",".join(gtfs_paths)
        else:
            self.config['graphhopper'].pop('gtfs.file')
        self.set_config()
    
    def set_factors(self, profile, factors):
        assert profile in ['car_cbr_off', 'car_cbr_peak']
        
        # Save cached factors in cache path.
        if self.city not in self.factor_cache:
            self.factor_cache[self.city] = {}
        self.factor_cache[self.city][profile] = factors
        json.dump(self.factor_cache, open(self.factor_cache_path, 'w'))
        
        # Get index of customizable profile and set config to it.
        ctm = [i for i,c in enumerate(self.config['graphhopper']['profiles']) if c['name'] == profile][0]
        self.config['graphhopper']['profiles'][ctm]['custom_model'] = {
            "speed": [
                {"if": "road_environment == FERRY", "multiply_by": "0.1"},
                {"if": "road_class==MOTORWAY",      "multiply_by": str(factors[0])},
                {"if": "road_class==PRIMARY",       "multiply_by": str(factors[1])},
                {"if": "road_class==SECONDARY",     "multiply_by": str(factors[2])},
                {"if": "road_class==TERTIARY",      "multiply_by": str(factors[3])},
                {"if": "road_class==RESIDENTIAL",   "multiply_by": str(factors[4])}
            ]
        }
        logging.info(f"Set factors for {profile} to {str(factors)}")
        self.set_config()
    
    def get_factors(self):
        
        # Check for cached speed correction factors.
        if (self.city in self.factor_cache
            and 'car_cbr_off'  in self.factor_cache[self.city]
            and 'car_cbr_peak' in self.factor_cache[self.city]):
            logging.info("Set previously known correction factors.")
            self.set_factors('car_cbr_off',  self.factor_cache[self.city]['car_cbr_off'])
            self.set_factors('car_cbr_peak', self.factor_cache[self.city]['car_cbr_peak'])
            self.calibrated = True
    
    def set_config(self, config=None):
        if not config:
            config = self.config
            
        with open(self.config_out_path, 'w',) as f:
            yaml.dump(self.config, f, sort_keys=False, default_flow_style=None)
        return
    
    def build(self, force=False, attempts=5):
        
        self.dclient = docker.from_env()
        
        # Check for cached speed correction factors.
        if (not force):
            self.get_factors()
        
        # Check if config is correct, and whether rebuilding is necessary.
        if self.config == self.config_original and not os.path.exists(self.lockfile_path):
            logging.info("Cache already exists, not rebuilding.")
            clean_cache_folder = False
        else:
            if self.config == self.config_src:
                logging.info("This is a default example build.")
            elif force:
                logging.info("Force cleaning cache.")
            else:
                logging.info("GH config changed, cleaning cache.")
            
            cache_path = os.path.join(self.droot, "2-gh/graph-cache")
            if os.path.exists(cache_path):
                clean_cache_folder = True
            else:
                clean_cache_folder = False
                logging.info("Cache path didn't exist, probably first execution. Continuing.")
            
            if os.path.exists(self.lockfile_path):
                os.remove(self.lockfile_path)
        
            # If config is the example configuration, use the pre-compiled graph-cache to speed up startup.
            example_path = os.path.join(self.droot, "2-gh/example/graph-cache")
            build_path = os.path.join(self.droot, "2-gh/graph-cache")
            if self.config == self.config_src and os.path.exists(example_path):
                shutil.copytree(example_path, build_path)
        
        # Remove other dockers
        mem = os.environ.get('MEMORY', 8)
        docker_image = os.environ.get('DOCKER_IMG', "ivotje50/graphhopper")
        for d in self.dclient.containers.list(filters={"label": "DUTTv2_container=yes"}):
            logging.info(d)
            d.stop()
            d.remove(force=True)

        for attempt in range(attempts):
            
            # Start querying Grasshopper. 
            json.dump({}, open(self.lockfile_path, 'w'))
            try:
                # Starting docker
                logging.info(f"Starting docker build, time estim. ~5min. (mem={mem}g, attempt=#{attempt+1})")
                clean_cache_folder = "&& rm -rf 1-data/2-gh/graph-cache/" if clean_cache_folder == True else ""
                self.container = self.dclient.containers.run(
                    image=docker_image, 
                    detach=True,
                    init=True,
                    labels={'DUTTv2_container': 'yes'},
                    command=f'"cd ../ {clean_cache_folder} && java -Xmx{mem}g -Xms{mem}g -jar ./graphhopper/*.jar server ./1-data/2-gh/config-duttv2.yml"',
                    environment={"JAVA_OPTS": f"-Xmx{mem}g -Xms{mem}g"},
                    volumes={os.path.realpath(self.droot): {'bind': '/1-data', 'mode': 'rw'}}, 
                    entrypoint='/bin/bash -c',
                    ports={'8989/tcp':'8989/tcp'}
                )

                # Wait for Graphhopper to actually finish preparing the graph.
                for line in self.container.logs(stream=True):
                    line = str(line)
                    if not "Invalid reference" in line:
                        logging.debug(line)
                    if "org.eclipse.jetty.server.Server - Started" in line:
                        return
                    if "custom speed <= maxSpeed" in line:
                        logging.critical("Factor above max speed, graphhopper quitting.")
                    if "Profiles do not match" in line:
                        clean_cache_folder = True
                
                return True
                
                # If stream ends without showing Server Started line, it's a problem. Try again.
                # raise InterruptedError("Problem while successfully creating docker, please try again.") #TODO: Re-enable this.
        
            except ConnectionError:
                logging.critical("Seems like docker unexpectedly quit. Retrying..")
                logging.critical(traceback.format_exc())
            
            except Exception:
                logging.critical("Something happened that stopped the querying:")
                logging.critical(traceback.format_exc())

        return False
    
    def stop(self):
        self.container.stop()
        os.remove(self.lockfile_path)
        return
    
    def route_gh(self, point1, point2, factors):

        # TODO: During evaluation, routes should be the same as recommended by Google Maps, instead of being shifted to other routes.

        headers = {'Accept': 'application/json','Content-Type': 'application/json'}
        json_data = {
            'points': [[point1.x, point1.y], [point2.x, point2.y]],
            'profile': 'car_ctm',
            'elevation': True,
            'debug': False,
            'instructions': False,
            'locale': 'ca',
            'optimize': 'false',
            'points_encoded': False,
            'ch.disable': True,
            'details': [
                # 'road_class',
                # 'road_environment',
                # 'max_speed',
                # 'average_speed',
            ],
            'custom_model': {
                "distance_influence": 15,
                "speed": [
                    {"if": "road_environment == FERRY", "multiply_by": "0.1"},
                    {'if': 'road_class==MOTORWAY',      'multiply_by': factors[0]},
                    {'if': 'road_class==PRIMARY',       'multiply_by': factors[1]},
                    {'if': 'road_class==SECONDARY',     'multiply_by': factors[2]},
                    {'if': 'road_class==TERTIARY',      'multiply_by': factors[3]},
                    {'if': 'road_class==RESIDENTIAL',   'multiply_by': factors[4]},
                ],
            }
        }
        response = requests.post('http://localhost:8989/route', headers=headers, json=json_data)
        return response.json()
    
    def route_google(self, point1, point2, timestamp):        
        assert isinstance(timestamp, datetime.datetime)
        assert timestamp.tzinfo is not None
        dp_datetime = timestamp.isoformat()
        
        google_key = os.environ.get("GOOGLE_KEY")
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': google_key,
            'X-Goog-FieldMask': 'routes.duration,routes.travelAdvisory,routes.staticDuration,routes.distanceMeters,routes.polyline',
        }
        json_data = {
            'origin': {'location': {'latLng': {
                'latitude': point1.y, 
                'longitude': point1.x,
            }}},
            'destination': {'location': {'latLng': {
                'latitude': point2.y, 
                'longitude': point2.x,
            }}},
            'travelMode': 'DRIVE',
            'routingPreference': 'TRAFFIC_AWARE',
            'departureTime': dp_datetime,
            'computeAlternativeRoutes': False,
            'polylineEncoding': 'GEO_JSON_LINESTRING',
            'routeModifiers': {
                'avoidTolls': False,
                'avoidHighways': False,
                'avoidFerries': False,
            },
            'languageCode': 'en-US',
            'units': 'METRIC',
        }
        response = requests.post('https://routes.googleapis.com/directions/v2:computeRoutes', headers=headers, json=json_data)
        self.fetched_google += 1
        return response.json()
    
    def get_comparison(self, points, factors, timestamp, cache_dir=None):
        
        if not cache_dir:
            cache_dir = os.path.join(self.droot, '2-gh/calibrate-cache/')
        os.makedirs(cache_dir, exist_ok=True)
        
        points = enumerate(points)
        sample_routes = list(itertools.combinations(points, 2))
    
        self.fetched_google = 0
        results = []
        for ((p1_pid, p1), (p2_pid, p2)) in sample_routes:
            google_cache = os.path.join(cache_dir, f"{self.city}-{p1_pid}-{p2_pid}-google-{timestamp.strftime('%h%m')}.json")

            response_google = {}
            if os.path.exists(google_cache):
                response_google = json.load(open(google_cache, 'r'))
            if not 'routes' in response_google: # In case something went wrong initially. 
                response_google = self.route_google(p1, p2, timestamp=timestamp)
                json.dump(response_google, open(google_cache, 'w'))
                logging.debug(f"Fetched {p1_pid}->{p2_pid} from Google for {timestamp.strftime('%h%m')}.")
            
            # Check if there's a route now.
            if not 'routes' in response_google:
                logging.warning(f'Fetched {p1}-{p2} had no google route: {str(response_google)}')
            route_google = response_google['routes'][0]
        
            response_gh = self.route_gh(p1, p2, factors)
            if 'paths' not in response_gh:
                logging.warning(f'Fetched {p1}-{p2} had no GH route: {str(response_gh)}')
                continue
            
            results.append([
                p1_pid, p2_pid,
                int(route_google['staticDuration'][:-1]), response_gh['paths'][0]['time']//1000, 
                route_google['distanceMeters'], response_gh['paths'][0]['distance'],
                LineString(route_google['polyline']['geoJsonLinestring']['coordinates']),
                LineString(response_gh['paths'][0]['points']['coordinates'])
            ])
            
        if self.fetched_google > 0:
            logging.info(f"Had to fetch {self.fetched_google} entries online which were not cached.")
        
        results = pd.DataFrame(results, columns=[
            'from', 'to',
            'google_s', 'gh_s', 
            'google_d', 'gh_d', 
            'route_google', 'route_gh'])
        
        results['google_m'] = results.google_s // 60
        results['gh_m'] = results.gh_s // 60
        results['error'] = pd.Series.abs(results.google_m - results.gh_m)
        results['distance_d'] = pd.Series.abs(results.google_d - results.gh_d)
        results['error_corrected'] = results.error ** 2 + results.distance_d ** (0.5)
        logging.info(f"mean(e): {results.error.mean():.2f}, MSE:{(results.error ** 2).mean():.2f} with factors {str(factors)}")
        
        return results
    
    def calibrate(self, points, peak_dt, off_dt, start_factors = np.full(5, 0.8), force=False, max=1.0):
        
        if self.calibrated and not force:
            logging.info('Already calibrated earlier based on cached factors, skipping. Add calibrate(force=True) to force.')
            return
        
        if len(points) > 20:
            logging.warning(f"With > 20 points, calibration is much slower while not much better. You gave {len(points)}.")
        
        # Localise timezones
        tz = timezone(TimezoneFinder().timezone_at(lng=points.iloc[0].x, lat=points.iloc[0].y))
        peak_dt = peak_dt.replace(tzinfo=tz)
        off_dt = off_dt.replace(tzinfo=tz)
        logging.info(f"Formatted datetime to localised {peak_dt} and {off_dt}.")
    
        # Move points to actual roads.
        points = points.apply(lambda x: self.nearest(x))
        bounds = [(0.5, max)] * 5
        timestamp = ''
        def error_function(factors):
            results = self.get_comparison(points, factors=factors, timestamp=timestamp)
            return results.error_corrected.sum()
        
        try:
            # Peak-hour
            logging.info("Start calibration for peak-hour")
            timestamp = peak_dt
            res_peak = minimize(error_function, start_factors, tol=5, bounds=bounds, method='Nelder-Mead', options={'maxfev': 40})
            self.set_factors(profile='car_cbr_peak', factors=res_peak.x.tolist())
            
            # Off-peak-hour
            logging.info("Start calibration for off-peak-hour")
            timestamp = off_dt
            res_off = minimize(error_function, start_factors, tol=5, bounds=bounds, method='Nelder-Mead', options={'maxfev': 40})
            self.set_factors(profile='car_cbr_off', factors=res_off.x.tolist())
            
            self.stop()
            self.build(force=True)
        
        except Exception as e:
            self.stop()
            raise e

    def nearest(self, point):
        
        for attempt in range(5):
            try:
                url = "http://localhost:8989/nearest"
                response = requests.request("GET", url, params={"point": f"{point.y},{point.x}"}).json()
                if not 'coordinates' in response:
                    logging.warning(f"GH Nearest no resolve: {response}")
                    return point
                
                coords = response['coordinates']
                return Point(coords[0], coords[1])
            
            except ConnectionError as e:
                if attempt < 5:
                    logging.error("Couldn't connect to GraphHopper, waiting 1 second and trying again.")
                    time.sleep(1)
                    pass
                else:
                    logging.error("Couldn't connect to GraphHopper, raising error.")
                    raise e
        

def test():
    
    # Set logging and variables, define data working folder
    load_dotenv()
    logging.getLogger().setLevel(logging.DEBUG) # DEBUG, INFO or WARN
    DROOT = '../1-data'
    urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, "2-external"), target_dir=os.path.join(DROOT, "2-popmasks"), res=1000)
    
    # Read a test city to be processed.
    cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.latest.xlsx'))
    city = cities[cities.city_name == 'Stockholm'].iloc[0]
    pcl_path = urbancenter_client.extract_city(city.city_name, city.city_id)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    # Run default example build. 
    graphhopper = Graphhopper(droot=DROOT, city=city.city_id)
    graphhopper.set_osm("/1-data/2-osm/example/stockholm.osm.pbf") # 2973
    graphhopper.set_gtfs([]) #"/1-data/2-gtfs/example/2973-f-u-flixbus.gtfs.zip", "/1-data/2-gtfs/example/2973-f-u-se.gtfs.zip"])
    graphhopper.build()
    
    # Try to calibrate example build.
    sample = gdf.centroid.to_crs('EPSG:4326').sample(15, random_state=10)
    sample = sample.apply(lambda x: graphhopper.nearest(x))
    peak_dt = datetime.datetime(2023, 9, 12, 8, 30, 0)
    off_dt  = datetime.datetime(2023, 9, 12, 13, 30, 0)
    graphhopper.calibrate(sample, peak_dt=peak_dt, off_dt=off_dt, force=True)

    # === Here you run the actual fetching. 

    graphhopper.stop()
    
if __name__ == "__main__":
    test()