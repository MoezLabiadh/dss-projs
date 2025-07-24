"""
Purpose: 
    This script creates a dataset of aeroway features from OpenStreetMap (OSM)
    for Human Disturbance project.

Description:
    - Downloads OSM aeroway polygons and lines within an AOI GeoJSON
    - Filters lines outside polygons, buffers by aeroway tag
    - Merges polygons and buffered lines, dissolves to one geometry
    - Writes to an ESRI Shapefile in BC Albers (EPSG:3005)

Dependencies:
    - osmnx
    - geopandas
    - shapely
    - pandas

Notes:
    - "osmnx" is installed on the GTS central python clone (python_geospatial). 
       run this script on "python_geospatial" clone

Author: Moez Labiadh - GeoBC

"""
import warnings
warnings.simplefilter(action='ignore')

import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
import timeit

def download_osm_features():
    """
    Reads AOI from INPUT_AOI, reprojects to WGS84 for OSM query,
    downloads aeroway polygons and lines, returns both in BC Albers.
    """
    aoi = gpd.read_file(INPUT_AOI)
    aoi_wgs = aoi.to_crs('EPSG:4326')
    aoi_polygon = aoi_wgs.geometry.union_all()
    
    # get aeroway OSM data within the AOI polygon
    osm_gdf = ox.features.features_from_polygon(
        aoi_polygon, 
        {"aeroway": True}
    )
    
    # Check if any features were returned
    if osm_gdf.empty:
        print("No aeroway features found in the specified area")
        return gpd.GeoDataFrame(columns=['geometry'], crs=BC_ALBERS), gpd.GeoDataFrame(columns=['geometry'], crs=BC_ALBERS)
    
    polys = osm_gdf[osm_gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    lines = osm_gdf[osm_gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()
    
    polys = polys.to_crs(BC_ALBERS)
    lines = lines.to_crs(BC_ALBERS)
    
    return polys, lines

def filter_and_buffer_lines(lines, polys):
    """
    Keeps only lines outside polys, buffers by aeroway tag:
      - runway: 15m
      - taxiway/other: 7.5m
    Returns buffered GeoDataFrame in BC Albers.
    """
    if lines.empty or polys.empty:
        return gpd.GeoDataFrame(columns=['geometry'], crs=BC_ALBERS)
    
    union_poly = polys.geometry.union_all()
    outside = lines[~lines.geometry.intersects(union_poly)].copy()
    
    if outside.empty:
        return gpd.GeoDataFrame(columns=['geometry'], crs=BC_ALBERS)
    
    def buf(gdf, dist):
        if gdf.empty:
            return gdf
        out = gdf.copy()
        out.geometry = out.geometry.buffer(dist, cap_style=2)
        return out
    
    # Handle cases where aeroway column might not exist or have different values
    runway_mask = outside.get("aeroway", pd.Series(index=outside.index)) == "runway"
    taxiway_mask = outside.get("aeroway", pd.Series(index=outside.index)) == "taxiway"
    
    run = buf(outside[runway_mask], 15)
    taxi = buf(outside[taxiway_mask], 7.5)
    other = buf(outside[~(runway_mask | taxiway_mask)], 7.5)
    
    # Filter out empty GeoDataFrames
    parts = [df for df in [run, taxi, other] if not df.empty]
    
    if not parts:
        return gpd.GeoDataFrame(columns=['geometry'], crs=BC_ALBERS)
    
    combined = pd.concat(parts, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry='geometry', crs=BC_ALBERS)

def merge_and_dissolve(polys, buffered, TODAY):
    """
    Merges polys and buffered lines, removing all overlapping areas.
    Uses unary_union to eliminate overlaps while preserving separate polygons.
    """
    # Handle empty dataframes
    if polys.empty and buffered.empty:
        print("No data to merge")
        return
    
    if polys.empty:
        # Only buffered data exists
        final_gdf = buffered.copy()
    elif buffered.empty:
        # Only polygon data exists
        final_gdf = polys.copy()
    else:
        # Both exist - need to handle overlaps
        # Create union of all polygon geometries
        poly_union = polys.geometry.union_all()
        
        # Clip buffered geometries to remove areas that overlap with polygons
        buffered_clipped = buffered.copy()
        buffered_clipped.geometry = buffered_clipped.geometry.difference(poly_union)
        
        # Remove any resulting empty geometries
        buffered_clipped = buffered_clipped[~buffered_clipped.geometry.is_empty]
        
        # Combine polygons with clipped buffered geometries
        if buffered_clipped.empty:
            final_gdf = polys.copy()
        else:
            final_gdf = pd.concat([polys, buffered_clipped], ignore_index=True)
            final_gdf = gpd.GeoDataFrame(final_gdf, geometry='geometry', crs=BC_ALBERS)
    
    # Ensure we have a proper GeoDataFrame
    if not isinstance(final_gdf, gpd.GeoDataFrame):
        final_gdf = gpd.GeoDataFrame(final_gdf, geometry='geometry', crs=BC_ALBERS)
    
    # Use unary_union to remove overlaps while preserving individual polygons
    # Create union of all geometries to eliminate overlaps
    union_geom = final_gdf.geometry.union_all()
    
    # Convert the union result back to individual polygons
    if isinstance(union_geom, Polygon):
        # Single polygon result
        result_gdf = gpd.GeoDataFrame([{'geometry': union_geom}], crs=BC_ALBERS)
    elif isinstance(union_geom, MultiPolygon):
        # Multiple polygons - extract each as separate feature
        individual_polys = []
        for poly in union_geom.geoms:
            individual_polys.append({'geometry': poly})
        result_gdf = gpd.GeoDataFrame(individual_polys, crs=BC_ALBERS)
    else:
        # Fallback to original if union fails
        result_gdf = final_gdf

    result_gdf['export_date'] = TODAY
    
    return result_gdf


if __name__ == "__main__":
    start_t = timeit.default_timer()

    WKS = r'Q:\dss_workarea\mlabiadh\workspace\20250611_HD_2025\osm_aeroway'
    INPUT_AOI = os.path.join(WKS, "bc.geojson")
    TODAY = datetime.now().strftime("%Y%m%d")
    OUTPUT_SHP = os.path.join(WKS, f"osm_data_{TODAY}.shp")
    BC_ALBERS = 'EPSG:3005'
    
    print ('Downloading OSM aeroway features...')
    polygons, lines = download_osm_features()

    print ('Processing lines features...')
    buffered = filter_and_buffer_lines(lines, polygons)

    print ('Producing the final aeroway dataset...')
    result_gdf = merge_and_dissolve(polygons, buffered, TODAY)

    print ('Exporting the dataset to Shapefile...')
    result_gdf.to_file(OUTPUT_SHP, driver='ESRI Shapefile' )


    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')