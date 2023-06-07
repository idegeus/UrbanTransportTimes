import os
from shapely.geometry import Point, Polygon
import pandas as pd
import gtfs_kit as gk
import logging
logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
from glob import glob

feed_paths = glob('../1-data/2-gh/gtfs/out2/2167-f-*.zip')

logging.info('Starting loading of data')
# feed = gk.read_feed('../1-data/2-gh/gtfs/out/2167-f-germany~long~distance~rail.gtfs.zip', dist_units='km')
# feed = gk.read_feed('../1-data/2-gh/gtfs/out/2167-f-germany~regional~rail.gtfs.zip', dist_units='km')
# feed = gk.read_feed('../1-data/2-gh/gtfs/src/latest.zip', dist_units='km')
# feed = gk.read_feed('../1-data/2-gh/gtfs/out/2167-merged.gtfs.zip', dist_units='km')
feed = gk.read_feed('../1-data/2-gh/gtfs/out2/2167-f-u-nl.gtfs.zip', dist_units='km')
# feed = feed.drop_zombies()
# feed = feed.clean_route_short_names()
# feed = feed.clean_times()
# feed = feed.clean_ids()
print(feed.assess_quality())

print(feed.assess_quality().iloc[14].value == 'bad feed')

quit()

logging.info(f'Starting merger with {len(feed_paths)} feeds...')
main_feed.agency             = pd.concat([f.agency           for f in feeds if isinstance(f.agency,          pd.DataFrame)])
main_feed.stops              = pd.concat([f.stops            for f in feeds if isinstance(f.stops,           pd.DataFrame)])
main_feed.routes             = pd.concat([f.routes           for f in feeds if isinstance(f.routes,          pd.DataFrame)])
main_feed.trips              = pd.concat([f.trips            for f in feeds if isinstance(f.trips,           pd.DataFrame)])
main_feed.stop_times         = pd.concat([f.stop_times       for f in feeds if isinstance(f.stop_times,      pd.DataFrame)])
# main_feed.calendar           = pd.concat([f.calendar         for f in feeds if isinstance(f.calendar,        pd.DataFrame)])
main_feed.calendar_dates     = pd.concat([f.calendar_dates   for f in feeds if isinstance(f.calendar_dates,  pd.DataFrame)])
main_feed.shapes             = pd.concat([f.shapes           for f in feeds if isinstance(f.shapes,          pd.DataFrame)])
main_feed.transfers          = pd.concat([f.transfers        for f in feeds if isinstance(f.transfers,       pd.DataFrame)])

 # Replaced as some feeds don't have it, problem for GH.
main_feed.feed_info          = pd.read_csv("../1-data/2-gh/gtfs/feed_info.template.csv")

# main_feed.attributions       = pd.concat([f.attributions     for f in feeds if isinstance(f.attributions,    pd.DataFrame)])
# main_feed.frequencies        = pd.concat([f.frequencies      for f in feeds if isinstance(f.frequencies,     pd.DataFrame)])
# main_feed.fare_attributes    = pd.concat([f.fare_attributes  for f in feeds if isinstance(f.fare_attributes, pd.DataFrame)])
# main_feed.fare_rules         = pd.concat([f.fare_rules       for f in feeds if isinstance(f.fare_rules,      pd.DataFrame)])

