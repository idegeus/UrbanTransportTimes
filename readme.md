# DUTTv2 Automation
Software to automatically import population densities, GTFS feeds and mapping data in order to calculate transport & accessibility inequalities based on isochrones. 

<img src='https://media.tenor.com/SSY2V0RrU3IAAAAM/rick-roll-rick-rolled.gif' width=400>

# Data and Creation Flow
Creation of the necessary data to make calculations is done in the following steps. 

### Extraction of population rasters which serve as centroids for isochrones.
1. For every GHS Urban Center as specified by `1-data/1-research/cities.xlsx`, make an extract from GHS-pop, giving population estimates.
2. Export these to GeoDataFrame Pickles and GeoTIFF files in `3-interim/populationmasks`

### Setup of isochrone generator in a Graphhopper route-calculator instance. 
1. Download OSM map file directly from OpenStreetMap
2. Convert `map.osm` data to `map.osm.pbf` using `Osmium` so it can be read by GraphHopper.
3. Download transit GTFS feed data from `https://www.transit.land/documentation/`.
4. Update GraphHopper Config file (if necessary)
5. Create Graphhopper instance, wait for it to finish parsing all data.
   1. Comment: profiles and points have a big influence on the outcome of these isochrones.

### Go over the grid in the extracted population mask, fetching isochrones from GraphHopper
   1. Run loop and cache in cache.db