import os
import subprocess
from shapely.geometry import Polygon
import geopandas as gpd
import logging

def extract_osm(osm_src, osm_out, bbox, buffer_m=0, force=False):
    """Builds smaller extracts from OSM Source file

    Args:
        osm_src (path): Source path like europe-latest or planet.osm
        osm_out (path): Destination path, preferably with city_id. 
        bbox (Polygon): Shapely polygon, which will be covered in extract.
        buffer_m (int): Meters of buffer around the bounding box. 
        force (bool): Whether or not to recreate file if already exists. 

    Returns 0 if successful. 
    """
    
    assert os.path.exists(osm_src)
    assert isinstance(bbox, Polygon)
    
    # Skip if already exists. 
    if os.path.exists(osm_out) and not force:
        logging.info(f'Extract already exists: {osm_out}')
        return 0
    
    # Define bounding box and add buffer if required
    bbox_gdf = gpd.GeoSeries(data=[bbox], crs="EPSG:4326")
    if buffer_m > 0:
        bbox_gdf = bbox_gdf.to_crs(bbox_gdf.estimate_utm_crs()).buffer(buffer_m).to_crs(bbox_gdf.crs)
    
    # Extract string coordinates. 
    bounds = bbox_gdf.geometry[0].bounds
    bbox_str = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
    
    # Run extraction. 
    logging.info(f"Starting extraction to {osm_out}, this might take some time.")
    result = subprocess.run(['osmium', 'extract', '--bbox', bbox_str, '-o', osm_out, osm_src, '--overwrite'])
    logging.info(result)
    return 0
    
# Test        
if __name__ == "__main__":
    
    # Import and make sure we see some info.
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    
    # Test for Amsterdam
    osm_src = '../1-data/2-osm/src/planet-latest.osm.pbf'
    osm_out = '../1-data/2-osm/example/amsterdam.osm.pbf'
    
    bbox = [(4.62314, 52.51905), (5.15805, 52.16525)]
    bbox = Polygon([bbox[0], (bbox[0][0], bbox[1][1]), bbox[1], (bbox[1][0], bbox[0][1])])
    
    extract_osm(osm_src, osm_out, bbox, buffer_m=20e3, overwrite=True)
