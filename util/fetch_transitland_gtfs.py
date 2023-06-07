import os
from shapely.geometry import Point, Polygon
import pandas as pd
import geopandas as gpd
import requests
import gtfs_kit as gk
import logging

class GtfsDownloader:
    
    def __init__(self, tl_key):
        self.tl_key = tl_key
        
    def set_search(self, point, bbox, radius=10000):
        """Set search info for downloading GTFS files

        Args:
            point (Point):  Shapely Point as center for searching agency feeds.
            radius (int):   Integer with radius to search Transitland for feeds within
            bbox (Polygon): Shapely Polygon, GTFS feeds within or intersecting will remain. 
        """
        assert isinstance(point, Point)
        assert isinstance(radius, int)
        assert isinstance(bbox, Polygon)
        
        self.point = point
        self.radius = radius
        self.bbox_polygon = bbox
        
        bbox_gdf = gpd.GeoDataFrame(geometry=[bbox], crs="EPSG:4326")
        bbox_gdf.geometry = bbox_gdf.to_crs(bbox_gdf.estimate_utm_crs()).buffer(5000).to_crs(bbox_gdf.crs)
        self.bbox_gdf = bbox_gdf
        
    def search_feeds(self):
        
        # Fetch agencies operating in the area around the constructed centroid. 
        logging.info(f"Fetching agencies & trips {self.radius} meters around {str(self.point)}.")
        url = 'https://transit.land/api/v2/rest/agencies'
        params = {
            "lat": self.point.y,
            "lon": self.point.x,
            "radius": self.radius,
            "apikey": self.tl_key
        }
        response = requests.get(url, params=params)
        res = response.json()

        # Filter the TransitLand One-IDs for each suggested feed from agencies operating there. 
        feeds = [agency['feed_version'] for agency in res['agencies']]
        feed_ids = list(set([feed['feed']['onestop_id'] for feed in feeds]))
        logging.info(f"Fetched {len(feed_ids)} relevant feeds {str(feed_ids)}")
        
        return feed_ids
        
    def download_feeds(self, feed_ids, target_dir, city_id, merge=False, force_recreate=False):
        """Downloads feeds from TransitLand from feed_ids to target_id, cutting 
        out by set bbox. Can merge all feeds into one zip. 
        

        Args:
            feed_ids (list): Transitland O-Ids
            target_dir (Path): Working directory for files. Input will end up in subdir src, output in subdir out.
            city_id (str): Unique id for the cutout.
            merge (bool, optional): Merges all feeds into one zipfile. Defaults to False.
            force_recreate (bool, optional): Force recreation of all feed files. Defaults to False.

        Returns:
            list: List of paths for the merged sets. 
        """
        
        assert os.path.exists(target_dir)
        assert isinstance(city_id, str) or isinstance(city_id, int)
        
        # Create in- and output directories.
        os.makedirs(os.path.join(target_dir, 'src'), exist_ok=True)
        os.makedirs(os.path.join(target_dir, 'out'), exist_ok=True)
        
        # Download relevant source feeds.
        for feed_id in feed_ids:
            
            # Construct source GTFS path and check for existence.
            gtfs_out = os.path.join(target_dir, 'src', f'{feed_id}.gtfs.zip')
            if os.path.exists(gtfs_out) and not force_recreate:
                logging.info(f"Skipping already downloaded {feed_id}")
                continue
            
            # If does not exist, download to gtfs_out.
            logging.info(f"Downloading {gtfs_out}")
            feed = f"https://transit.land/api/v2/rest/feeds/{feed_id}/download_latest_feed_version"
            params = {"apikey": self.tl_key}
            response = requests.get(feed, params=params)
            with open(gtfs_out, 'wb') as f:
                f.write(response.content)
        
        # Trim GTFS size to bounding box
        feed_list = []
        feeds = []
        for feed_id in feed_ids:

            # Declare the directory path for the GTFS zip file
            gtfs_in = os.path.join(target_dir, 'src', f'{feed_id}.gtfs.zip')
            gtfs_out = os.path.join(target_dir, 'out', f'{city_id}-{feed_id}.gtfs.zip')
            
            # Check if it already exists, if so, read in (if needed) and skip.
            if os.path.exists(gtfs_out) and not force_recreate:
                logging.info(f"Already extracted: {feed_id}")
                feed = gk.read_feed(gtfs_out, dist_units='km')
            else:
                # Read the feed with gtfs-kit, restrict to bounding box, and write out.
                logging.info(f"Creating GTFS extract at {gtfs_out}")
                feed = gk.read_feed(gtfs_in, dist_units='km')
                feed = feed.restrict_to_area(self.bbox_gdf).restrict_to_dates(['20230613'])
                feed.write(gtfs_out)
                
            # Check for validity, and if good, write out. TODO: Repair feeds if possible.
            if (not isinstance(feed.routes, pd.DataFrame) 
                or feed.stops.shape[0] == 0 
                or feed.trips.shape[0] == 0 
                or feed.assess_quality().iloc[14].value == 'bad feed'):
                logging.warning(f'Not adding {feed_id}, bad feed, please repair. Continuing without...')
            else:
                feeds.append(gtfs_out)
        
        return feeds

# Test        
if __name__ == "__main__":

    # Finalise Amsterdam
    from dotenv import load_dotenv
    load_dotenv()
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN

    # Example: Amsterdam
    bbox = [(4.62314, 52.51905), (5.15805, 52.16525)]
    bbox = Polygon([bbox[0], (bbox[0][0], bbox[1][1]), bbox[1], (bbox[1][0], bbox[0][1])])
    centroid = Point(4.804538, 52.785071)
    radius = 10000
    
    gtfs_client = GtfsDownloader(tl_key=os.environ['TRANSITLAND_KEY'])
    gtfs_client.set_search(centroid, bbox, radius)
    feed_ids = gtfs_client.search_feeds()
    logging.info(feed_ids)
    gtfs_client.download_feeds(feed_ids, '../1-data/2-gh/gtfs/', 12345)