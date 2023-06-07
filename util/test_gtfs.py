import os
from shapely.geometry import Point, Polygon
import pandas as pd
import gtfs_kit as gk
import numpy as np
import logging
logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
from glob import glob

feed_paths = np.array(glob('../1-data/2-gh/gtfs/out2/2167-*.zip'))

logging.info(f'Starting loading of {len(feed_paths)} feeds.')
feeds = [gk.read_feed(f, dist_units='km') for f in feed_paths]

valid = [(fid, f) for fid, f in zip(feed_paths, feeds) if isinstance(f.routes, pd.DataFrame)]
logging.info(f'Out of all feeds, {len(valid)} are not empty routes.')

goodquality = [(fid, f) for fid, f in valid if f.assess_quality().iloc[14].value != 'bad feed']
logging.info(f'Out of all remaining feeds, {", ".join([fid for fid, f in goodquality])} are of good quality. Continuing...')

main_feed = feeds[0]
logging.info(f'Starting merger with {len(feeds)} feeds...')
main_feed.agency             = pd.concat([f.agency           for f in feeds if isinstance(f.agency,          pd.DataFrame)])
main_feed.stops              = pd.concat([f.stops            for f in feeds if isinstance(f.stops,           pd.DataFrame)])
main_feed.routes             = pd.concat([f.routes           for f in feeds if isinstance(f.routes,          pd.DataFrame)])
main_feed.trips              = pd.concat([f.trips            for f in feeds if isinstance(f.trips,           pd.DataFrame)])
main_feed.stop_times         = pd.concat([f.stop_times       for f in feeds if isinstance(f.stop_times,      pd.DataFrame)])
main_feed.calendar           = pd.concat([f.calendar         for f in feeds if isinstance(f.calendar,        pd.DataFrame)])
main_feed.calendar_dates     = pd.concat([f.calendar_dates   for f in feeds if isinstance(f.calendar_dates,  pd.DataFrame)])
main_feed.shapes             = pd.concat([f.shapes           for f in feeds if isinstance(f.shapes,          pd.DataFrame)])
main_feed.transfers          = pd.concat([f.transfers        for f in feeds if isinstance(f.transfers,       pd.DataFrame)])
# main_feed.attributions       = pd.concat([f.attributions     for f in feeds if isinstance(f.attributions,    pd.DataFrame)])
# main_feed.frequencies        = pd.concat([f.frequencies      for f in feeds if isinstance(f.frequencies,     pd.DataFrame)])
# main_feed.fare_attributes    = pd.concat([f.fare_attributes  for f in feeds if isinstance(f.fare_attributes, pd.DataFrame)])
# main_feed.fare_rules         = pd.concat([f.fare_rules       for f in feeds if isinstance(f.fare_rules,      pd.DataFrame)])

 # Replaced as some feeds don't have it, problem for GH.
main_feed.feed_info          = pd.read_csv("../1-data/2-gh/gtfs/feed_info.template.csv")
    
main_feed.write('../1-data/2-gh/gtfs/out/2167-merged.gtfs.zip')

