import os
import logging
import pandas as pd
import numpy as np
import itertools
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point, LineString
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
    fetched_google = 0
    
    def __init__(self, droot):
        self.dclient = docker.from_env()
        self.droot = droot
        self.config_src_path = os.path.join(self.droot, '2-gh', 'config-duttv2.src.yml')
        self.config_out_path = os.path.join(self.droot, '2-gh', 'config-duttv2.yml')
        
        self.config_src = yaml.safe_load(open(self.config_src_path, 'r'))
        self.config     = yaml.safe_load(open(self.config_src_path, 'r'))
        
        if os.path.exists(self.config_out_path):
            self.config_original = yaml.safe_load(open(self.config_out_path, 'r'))
    
    def set_osm(self, osm_path):
        self.config['graphhopper']['datareader.file'] = osm_path
        self.set_config()
    
    def set_gtfs(self, gtfs_paths):
        assert isinstance(gtfs_paths, (list, np.array, pd.Series))
        self.config['graphhopper']['gtfs.file'] = ",".join(gtfs_paths)
        self.set_config()
        
    def set_factors(self, profile, factors):
        assert profile in ['car_cbr_off', 'car_cbr_peak']
        self.factors = factors
        
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
        
    def set_config(self, config=None):
        if not config:
            config = self.config
            
        with open(self.config_out_path, 'w',) as f:
            yaml.dump(self.config, f, sort_keys=False, default_flow_style=None)
        return
    
    def build(self, force=False, attempts=5):
        
        if self.config == self.config_original:
            logging.info("Cache already exists, not rebuilding.")
        else:
            if self.config == self.config_src:
                logging.info("This is a default example build.")
            elif force:
                logging.info("Force cleaning cache.")
            else:
                logging.info("GH config changed, cleaning cache.")
            shutil.rmtree(os.path.join(self.droot, "2-gh/graph-cache"), ignore_errors=True)
        
            # If config is the example configuration, just use the pre-compiled graph-cache to speed up startup.
            if self.config == self.config_src:
                shutil.copytree(os.path.join(self.droot, "2-gh/example/graph-cache"),
                                os.path.join(self.droot, "2-gh/graph-cache"))
        
        # Remove other dockers
        mem = os.environ.get('MEMORY', 8)
        for d in self.dclient.containers.list(filters={"label": "DUTTv2_container=yes"}):
            logging.info(d)
            d.stop()
            d.remove(force=True)

        for attempt in range(attempts):
            
            # Start querying Grasshopper. 
            try:
                # Starting docker
                logging.info(f"Starting docker now with {mem}g memory, attempt #{attempt+1}.")
                self.container = self.dclient.containers.run(
                    image="israelhikingmap/graphhopper", 
                    detach=True,
                    init=True,
                    labels={'DUTTv2_container': 'yes'},
                    command=f'"cd ../ && java -Xmx{mem}g -Xms{mem}g -jar ./graphhopper/*.jar server ./1-data/2-gh/config-duttv2.yml"',
                    environment={"JAVA_OPTS": f"-Xmx{mem}g -Xms{mem}g"},
                    volumes={os.path.realpath(self.droot): {'bind': '/1-data', 'mode': 'rw'}}, 
                    entrypoint='/bin/bash -c',
                    ports={'8989/tcp':'8989/tcp'}
                )

                # Wait for Graphhopper to actually finish preparing the graph.
                for line in self.container.logs(stream=True):
                    line = str(line)
                    logging.info(line)
                    if "org.eclipse.jetty.server.Server - Started" in line:
                        break
                return
        
            except ConnectionError as error:
                logging.critical("Seems like docker unexpectedly quit. Retrying..")
                logging.critical(error)
            
            except Exception as error:
                logging.critical("Something happened that stopped the querying:")
                logging.critical(error)
    
    def stop(self):
        self.container.stop()
        self.container.remove()
        return
    
    def route_gh(self, point1, point2, factors):

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
    
    def route_google(self, point1, point2, period):
        assert period in ['peak', 'off']
        
        if period == 'peak':
            dp_datetime = '2023-07-26T06:30:00.045123456Z'
        else:
            dp_datetime = '2023-07-26T11:30:00.045123456Z'
        
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
    
    def get_comparison(self, points, factors, city, period, cache_dir=None):
        
        if not cache_dir:
            cache_dir = os.path.join(self.droot, '2-gh/calibrate-cache/')
        os.makedirs(cache_dir, exist_ok=True)
        
        points = points.apply(lambda x: self.nearest(x))
        points = enumerate(points)
        sample_routes = list(itertools.combinations(points, 2))
    
        self.fetched_google = 0
        results = []
        for ((p1_pid, p1), (p2_pid, p2)) in sample_routes:
            google_cache = os.path.join(cache_dir, f"{city}-{p1_pid}-{p2_pid}-google-{period}.json")

            if os.path.exists(google_cache):
                response_google = json.load(open(google_cache, 'r'))
            else:
                response_google = self.route_google(p1, p2, period=period)
                json.dump(response_google, open(google_cache, 'w'))
                logging.debug(f'Fetched {p1_pid}->{p2_pid} from Google for {period}.')
            route_google = response_google['routes'][0]
        
            response_gh = self.route_gh(p1, p2, factors)
            
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
    
    def calibrate(self, points, city, start_factors = np.full(5, 0.75)):
        if len(points) > 20:
            logging.warning(f"With > 20 points, calibration is much slower while not much better. You gave {len(points)}.")
        
        period = ''
        def error_function(factors):
            results = self.get_comparison(points, factors=factors, city=city, period=period)
            return results.error_corrected.sum()
        bounds = [(0.6, 1.1)] * 5
        
        try:
            # Peak-hour
            logging.info("Start calibration for peak-hour")
            period = 'peak'
            res_peak = minimize(error_function, start_factors, tol=5, bounds=bounds, method='Nelder-Mead', options={'maxfev': 40})
            self.set_factors(profile='car_cbr_peak', factors=res_peak.x)
            
            # Off-peak-hour
            logging.info("Start calibration for off-peak-hour")
            period = 'off'
            res_off = minimize(error_function, start_factors, tol=5, bounds=bounds, method='Nelder-Mead', options={'maxfev': 40})
            self.set_factors(profile='car_cbr_off', factors=res_off.x)
            
            self.stop()
            self.build(force=True)
        
        except Exception as e:
            self.stop()
            raise e

    def nearest(self, point):
        url = "http://localhost:8989/nearest"
        response = requests.request("GET", url, params={"point": f"{point.y},{point.x}"}).json()
        coords = response['coordinates']
        return Point(coords[0], coords[1])

def test():
    
    # Set logging and variables, define data working folder
    load_dotenv()
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    DROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../1-data')
    urbancenter_client = ExtractCenters(src_dir=os.path.join(DROOT, "2-external"), target_dir=os.path.join(DROOT, "2-popmasks"), res=1000)
    
    # Read a test city to be processed.
    cities = pd.read_excel(os.path.join(DROOT, '1-research', 'cities.xlsx'))
    city = cities[cities.City == 'Amsterdam'].iloc[0]
    pcl_path = urbancenter_client.extract_city(city.City, city.ID_HDC_G0)
    gdf = gpd.GeoDataFrame(pd.read_pickle(pcl_path))
    
    # Run default example build. 
    graphhopper = Graphhopper(droot=DROOT)
    graphhopper.set_osm("/1-data/2-osm/example/amsterdam.osm.pbf")
    graphhopper.set_gtfs(["1-data/2-gtfs/example/2167-f-u-nl.gtfs.zip","1-data/2-gtfs/example/2167-f-u-flixbus.gtfs.zip"])
    graphhopper.build()
    
    # Try to calibrate example build.
    sample = gdf.centroid.to_crs('EPSG:4326').sample(15, random_state=10)
    sample = sample.apply(lambda x: graphhopper.nearest(x))
    graphhopper.calibrate(sample, city.ID_HDC_G0, start_factors=np.ones(5))

    # === Here you run the actual fetching. 

    # graphhopper.stop()
    
if __name__ == "__main__":
    test()