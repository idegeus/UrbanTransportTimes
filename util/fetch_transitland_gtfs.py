import os
import traceback
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
        logging.debug("Parameters for GTFSDownloader:")
        logging.debug(f"Pt: {point} with radius {radius}")
        logging.debug(f"Bounding box: {bbox}")
        
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
        res = requests.get(url, params=params).json()
        agencies = res['agencies'] if 'agencies' in res else []
        
        # Fetch more records.
        while 'meta' in res and 'next' in res.get('meta', {}):
            res = requests.get(res['meta']['next']).json()
            agencies += res['agencies']
        
        # Filter the TransitLand One-IDs for each suggested feed from agencies operating there. 
        feeds = [agency['feed_version'] for agency in agencies]
        feed_ids = list(set([feed['feed']['onestop_id'] for feed in feeds]))
        logging.info(f"Fetched {len(feed_ids)} relevant feeds {str(feed_ids)}")
        
        return feed_ids
        
    def download_feeds(self, feed_ids, target_dir, city_id, merge=False, force_dl=False, force_extr=False):
        """Downloads feeds from TransitLand from feed_ids to target_id, cutting 
        out by set bbox. Can merge all feeds into one zip. 
        

        Args:
            feed_ids (list): Transitland O-Ids
            target_dir (Path): Working directory for files. Input will end up in subdir src, output in subdir out.
            city_id (str): Unique id for the cutout.
            merge (bool, optional): Merges all feeds into one zipfile. Defaults to False.
            force_dl (bool, optional): Force refresh of all source feed files. Defaults to False.
            force_extr (bool, optional): Force extraction of all source feed files. Defaults to False.

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
            if os.path.exists(gtfs_out) and not force_dl:
                logging.debug(f"Skipping already downloaded {feed_id}")
                continue
            
            # If does not exist, download to gtfs_out.
            logging.info(f"Downloading {gtfs_out}")
            feed = f"https://transit.land/api/v2/rest/feeds/{feed_id}/download_latest_feed_version"
            params = {"apikey": self.tl_key}
            response = requests.get(feed, params=params)
            with open(gtfs_out, 'wb') as f:
                f.write(response.content)
        
        # Trim GTFS size to bounding box
        feeds = []
        for feed_id in feed_ids:

            try:
                # Declare the directory path for the GTFS zip file
                gtfs_in = os.path.join(target_dir, 'src', f'{feed_id}.gtfs.zip')
                gtfs_out = os.path.join(target_dir, 'out', f'{city_id}-{feed_id}.gtfs.zip')
                
                # Check if it already exists, if so, read in (if needed) and skip.
                if os.path.exists(gtfs_out) and not force_extr:
                    logging.debug(f"Already extracted: {feed_id}")
                    newfeed = gk.read_feed(gtfs_out, dist_units='km')
                else:
                    # Read the feed with gtfs-kit, restrict to bounding box, and write out.
                    logging.info(f"Creating GTFS extract at {gtfs_out}")
                    feed = gk.read_feed(gtfs_in, dist_units='km')
                    
                    logging.debug(f'===== Feed {feed_id} before limiting:')
                    logging.debug(feed.routes.head(10))
                    logging.debug(f'Routes: {feed.routes.shape}')
                    logging.debug(f'Stops: {feed.stops.shape}')
                    logging.debug(f'Trips: {feed.trips.shape}')
                    logging.debug(feed.assess_quality())
                    
                    newfeed = feed.restrict_to_area(self.bbox_gdf) # Limit geographic range
                    newfeed = newfeed.create_shapes(all_trips=True) # Recreate shapes for accuracy.
                    newfeed.write(gtfs_out)
                    
                # Check for validity, and if good, write out. TODO: Repair feeds if possible.
                logging.debug(f'===== Feed {feed_id} after restrictions.')
                logging.debug(newfeed.routes.head(10))
                logging.debug(f'Routes: {newfeed.routes.shape}')
                logging.debug(f'Stops: {newfeed.stops.shape}')
                logging.debug(f'Trips: {newfeed.trips.shape}')
                try:
                    logging.debug(newfeed.assess_quality())
                except:
                    logging.critical(traceback.print_exc())
                
                if (not isinstance(newfeed.routes, pd.DataFrame)
                    or newfeed.stops.shape[0] == 0 
                    or newfeed.trips.shape[0] == 0 
                    or newfeed.assess_quality().iloc[14].value == 'bad feed'
                ):
                    logging.warning(f'Not adding {feed_id}, bad feed, please repair. Continuing without...')
                else:
                    feeds.append(gtfs_out)
            
            # Sometimes there's a read error. 
            except pd.errors.ParserError:
                logging.warning(f'Not adding {feed_id}, bad feed, read exception. Continuing without...')
                logging.warning(traceback.format_exc())
        
        return feeds

# Test        
if __name__ == "__main__":

    from dotenv import load_dotenv
    load_dotenv()
    logging.getLogger().setLevel(logging.DEBUG)

    # Example: Stockholm
    centroid = Point(18.000423079602378, 59.33170104820296)
    bbox = centroid.buffer(1)
    radius = 10000
    
    gtfs_client = GtfsDownloader(tl_key=os.environ['TRANSITLAND_KEY'])
    gtfs_client.set_search(centroid, bbox, radius)
    feed_ids = gtfs_client.search_feeds()
    logging.info(feed_ids)
    gtfs_client.download_feeds(feed_ids, '../1-data/2-gtfs/', 12345, force_extr=True)