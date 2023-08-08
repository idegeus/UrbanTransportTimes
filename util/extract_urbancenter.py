import os
import os.path
import numpy as np
import pandas as pd
import geopandas as gpd
import json
from shapely.geometry import shape

import rasterio
from rasterio.mask import mask
from rasterio.features import shapes

import logging

class ExtractCenters:
    
    def __init__(self, src_dir, target_dir, res = 1000):
        self.src_dir = src_dir
        self.target_dir = target_dir
        self.res = res
        self.initialised = False
        
        assert self.res == 1000 or self.res == 100
        
    def _load_rasters(self):
        
        # Get population data for the whole world.
        pop_path = os.path.join(
            self.src_dir,
            f'GHS_POP_E2020_GLOBE_R2022A_54009_{self.res}_V1_0',
            f'GHS_POP_E2020_GLOBE_R2022A_54009_{self.res}_V1_0.tif')
        urbancenter_path = os.path.join(
            self.src_dir, 
            'GHS_STAT_UCDB2015MT_GLOBE_R2019A',
            'GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2.gpkg')
        
        if not os.path.exists(pop_path) or not os.path.exists(urbancenter_path):
            logging.critical(f"Please download GHS-pop and UCDB (2020, resolution={self.res}m) from https://ghsl.jrc.ec.europa.eu/download.php")
            raise FileExistsError()
        
        logging.info("Initialising population raster..")
        self.pop = rasterio.open(pop_path)
        
        logging.info("Initialising urban_center raster..")
        self.urbancenter_gdf = gpd.read_file(urbancenter_path).to_crs(self.pop.crs)
        
        return 0
        
    def _mask_raster_to_tiff(self, gdf_entry, raster, tiff_out):
        """Takes raster, masking it using a gpd dataframe and writes to a TIFF out.

        Args:
            gdf_entry (GeoDataFrame): Used as mask for raster
            raster (Path): Path to a RasterIO tiff file
            tiff_out (Path): to write the out-tiff
        """
    
        out_img, out_transform = mask(
            dataset=raster, 
            shapes=self._get_mask_coords(gdf_entry), 
            crop=True)

        # Write out for usage a bit further onwards due to limitations in rasterio.
        out_meta = self.pop.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_img.shape[1],
            "width": out_img.shape[2],
            "transform": out_transform,
            "crs": self.pop.crs
        })

        with rasterio.open(tiff_out, "w", **out_meta) as dest:
            dest.write(out_img)
        
    # Convert single GeoDataFrame Polygon to GeoJSON
    def _get_mask_coords(self, gdf): 
        """Get the first polygon in a GeoDataFrame as GeoJSON."""
        return [json.loads(gdf.to_json())['features'][0]['geometry']]
        
    def extract_city(self, city_name, city_id, buffer=0):
        """Creates GeoDataFrames and GeoTIFF extracts from Population Rasters. 

        Args:
            city_name (_type_): _description_
            city_id (_type_): _description_
            buffer (integer): adds meters of buffer around zone.
        """
        
        # Paths
        tiff_path = os.path.join(self.target_dir, f"{city_id}.buf{buffer}.res{self.res}.tiff")
        pcl_path  = os.path.join(self.target_dir, f"{city_id}.buf{buffer}.res{self.res}.pcl")
        
        # Log, and if city already done, skip this.
        if(os.path.exists(tiff_path) and os.path.exists(pcl_path)):
            logging.debug(f"Population raster extract already exists: {city_name} ({city_id}.buf{buffer}.res{self.res})")
            return pcl_path
        else:
            logging.info(f"Creating population extract for city: {city_name} ({city_id}.buf{buffer}.res{self.res})")
        
        # Only initialise rasters if we know we have to do some work, e.g., now.
        if not self.initialised:
            self._load_rasters()
        
        # Write out a masked selection with city population.
        center_gdf = self.urbancenter_gdf[self.urbancenter_gdf.city_id == city_id]
        center_gdf = center_gdf.to_crs(center_gdf.estimate_utm_crs()).buffer(buffer).to_crs(center_gdf.crs)
        self._mask_raster_to_tiff(
            gdf_entry=center_gdf,
            raster=self.pop,
            tiff_out=tiff_path
        )
        
        # Convert the masked tiff to geojson for GeoPandas to use.
        # This is doing the heavy lifting!
        with rasterio.open(tiff_path) as raster:
            image = raster.read(1).astype("float32")
            
            # Add tiny random deviation (<1) so units don't join together
            #   in the next shapes() method. 
            rand_add = np.random.random(image.shape)
            rand_add[image == -200] = 0
            image += rand_add
            
            crs = raster.crs
            list_pop = [
                {'cell_pop': value, 'geometry': shape(shp)}
                for i, (shp, value) 
                in enumerate(shapes(image, transform=raster.transform))
                if value > raster.nodata
            ]
        
        # Read in as a GeoPandas dataset and write out. 
        gdf_pop = gpd.GeoDataFrame(list_pop, crs=crs)
        gdf_pop.cell_pop = np.maximum(gdf_pop.cell_pop, 0)
        gdf_pop.to_pickle(pcl_path)
        
        return pcl_path

if __name__ == "__main__":

    from dotenv import load_dotenv
    load_dotenv()
    logging.getLogger().setLevel(logging.INFO) # DEBUG, INFO or WARN
    
    droot = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../1-data')
    
    city_list_df = pd.read_excel(os.path.join(droot, '1-research', 'cities.xlsx'))
    urbancenter_client = ExtractCenters(src_dir=os.path.join(droot, '2-external'), 
                                        target_dir=os.path.join(droot, '2-popmasks'))

    # Get masked population dataframes for all mentioned cities.
    for city in city_list_df.itertuples():
        urbancenter_client.extract_city(
            city_name=city.City, 
            city_id=city.city_id, 
            buffer=0)