import os
import subprocess
from shapely.geometry import Polygon
import logging

def extract_osm(osm_src, osm_out, bbox):
    """Builds smaller extracts from OSM Source file

    Args:
        osm_src (path): Source path like europe-latest or planet.osm
        osm_out (path): Destination path, preferably with city_id. 
        bbox (Polygon): Shapely polygon, which will be covered in extract.

    Returns 0 if successful. 
    """
    
    assert os.path.exists(osm_src)
    assert isinstance(bbox, Polygon)
    
    # Skip if already exists. 
    if os.path.exists(osm_out):
        logging.info(f'Extract already exists: {osm_out}')
    
    logging.info(f"Starting extraction to {osm_out}, this might take some time.")
    bounds = bbox.bounds
    bbox_str = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
    
    result = subprocess.run(['osmium', 'extract', '--bbox', bbox_str, '-o', osm_out, osm_src])
    logging.info(result)
    return 0
    
# Test        
if __name__ == "__main__":
    
    # Import and make sure we see some info.
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    
    # Test for Amsterdam
    osm_src = '../1-data/2-gh/osm/europe-latest.osm.pbf'
    osm_out = '../1-data/2-gh/osm/amsterdam-test.osm.pbf'
    
    bbox = [(4.62314, 52.51905), (5.15805, 52.16525)]
    bbox = Polygon([bbox[0], (bbox[0][0], bbox[1][1]), bbox[1], (bbox[1][0], bbox[0][1])])
    
    extract_osm(osm_src, osm_out, bbox)