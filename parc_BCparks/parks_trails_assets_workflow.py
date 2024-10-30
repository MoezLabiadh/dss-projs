#-------------------------------------------------------------------------------
# Name:        Update BC Parks data in AGO
#
# Purpose:     This script updates the BC Parks Assets and Trails Layers in AGO
#              
# Input(s):    (1) Paths to Assets (excel) and Trails (shp) files.
#              (2) AGO credentials.           
#
# Author:      Moez Labiadh - GeoBC-DSS
#
# Created:     2024-10-30
# Updated:     
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import json
import pandas as pd
import geopandas as gpd
from io import BytesIO
from datetime import datetime
from arcgis.gis import GIS

import logging
import timeit


def process_assets (assets_xlsx: str) -> gpd.GeoDataFrame:
    '''
    Returns a geodataframe of the BCparks Assets dataset
    '''
    # read the assets xlsx into a dataframe
    logging.info('..reading data from spreadsheet')
    df = pd.read_excel(assets_xlsx)
    logging.info(f'....retrieved {df.shape[0]} entries')
    
    logging.info('..cleaning up the dataset')
    # remove rows with missing coordinates
    missing_coords = df[['GIS Latitude', 'GIS Longitude']].isnull().any(axis=1).sum()
    df.dropna(subset=['GIS Latitude', 'GIS Longitude'], inplace=True)
    
    logging.warning(f'....removed {missing_coords} rows with missing coordinates')

    # check for out-of-range coordinates
    lat_min, lat_max = 47, 60
    lon_min, lon_max = -145, -113
    df_outbc = df[
        (df['GIS Latitude'] < lat_min) | (df['GIS Latitude'] > lat_max) |
        (df['GIS Longitude'] < lon_min) | (df['GIS Longitude'] > lon_max)
    ]
    logging.warning(f'....{df_outbc.shape[0]} entries are outside of BC!')

    logging.info('..generating a geodataframe')
    # convert to geodataframe
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['GIS Longitude'], df['GIS Latitude']),
        crs="EPSG:4326"
    )
    
    # convert object cols to strings (objects not supported by fiona)
    gdf = gdf.astype(
        {col: 'str' for col in gdf.select_dtypes(include=['object']).columns}
    )
    
    logging.info(f'..the final spatial dataset has {gdf.shape[0]} rows and {gdf.shape[1]} columns')


    return gdf



def process_trails (trails_shp: str) -> gpd.GeoDataFrame:
    '''
    Returns a geodataframe of the BCparks Trails dataset
    '''
    
    logging.info('..reading data from shapefile')
    gdf= gpd.read_file(trails_shp)
    
    # rename Trails columns to match with Assets dataset (if matches!) 
    # and/or assign readable names
    logging.info('....cleaning-up column names')
    gdf.rename(
        columns={"assetid": "Asset ID", 
                "gisid": "GIS ID",
                "asset_cate": "Category - Classification",
                "asset_type": "Asset Type",
                "park": "Park",
                "park_subar": "Park Subarea",
                "trail_surf": "Trail Surface",
                "length_m": "Length Meters",
                "trail_name": "Trail Name",
                "osmid": "OSM ID",
                "descriptio": "Description",
                "verified_b": "Verified By",
                "accessible": "Is Accessible",
                "route_acce": "Is Route Accessible",
                }, 
        inplace= True
    )
    
    # reproject trails gdf to wgs84
    logging.info('....repojecting coordinates')
    gdf.to_crs(
        crs= 4326,
        inplace= True
    )
    
    logging.info(f'..the final spatial dataset has {gdf.shape[0]} rows and {gdf.shape[1]} columns')
    
    
    return gdf


def connect_to_AGO (HOST: str, USERNAME: str, PASSWORD: str) -> GIS:
    """ 
    Return a connection to AGO
    """     
    gis = GIS(HOST, USERNAME, PASSWORD, verify_cert=False)

    # Test if the connection is successful
    if gis.users.me:
        logging.info(f'..successfully connected to AGOL as {gis.users.me.username}')
    else:
        logging.error('..connection to AGOL failed.')
    
    return gis


def publish_feature_layer(gis, gdf, title, geojson_name, item_desc, folder):
    """
    Publishes a gdf to AGO as Feature Layer, overwriting if it already exists.
    """
    #format null values
    gdf = gdf.replace(['nan'], '')

    logging.info("..converting data to geojson.")
    def gdf_to_geojson(gdf):
            features = []
            for _, row in gdf.iterrows():
                feature = {
                    "type": "Feature",
                    "properties": {},
                    "geometry": row['geometry'].__geo_interface__
                }
                for column, value in row.items():
                    if column != 'geometry':
                        if isinstance(value, (datetime, pd.Timestamp)):
                            feature['properties'][column] = value.isoformat() if not pd.isna(value) else ''
                        else:
                            feature['properties'][column] = value
                features.append(feature)
            
            geojson_dict = {
                "type": "FeatureCollection",
                "features": features
            }
            return geojson_dict

    # Convert GeoDataFrame to GeoJSON
    geojson_dict = gdf_to_geojson(gdf)

    try:
        #search for an existing GeoJSON
        existing_items = gis.content.search(
            f"title:\"{title}\" AND owner:{gis.users.me.username}",
            item_type="GeoJson"
        )
        
        existing_items = [item for item in existing_items if item.title == title]
        
        # if an existing GeoJSON is found, Delete it
        for item in existing_items:
            if item.type == 'GeoJson':
                item.delete(force=True)
                logging.info(f"..existing GeoJSON item '{item.title}' deleted.")

        # Create a new GeoJSON item
        geojson_item_properties = {
            'title': title,
            'type': 'GeoJson',
            'tags': 'BCparks data',
            'description': item_desc,
            'fileName': f'{geojson_name}.geojson'
        }
        geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))
        new_geojson_item = gis.content.add(item_properties=geojson_item_properties, data=geojson_file, folder=folder)

        # Overwrite the existing feature layer or create a new one if it doesn't exist
        new_geojson_item.publish(overwrite=True)
        logging.info(f"..feature layer '{title}' published successfully.")


    except Exception as e:
        error_message = f"..error publishing/updating feature layer: {str(e)}"
        raise RuntimeError(error_message)



if __name__ == "__main__":
    start_t = timeit.default_timer() #start time
    
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    #wks= r'Q:\dss_workarea\mlabiadh\workspace\20241015_Park_assets_script'
    wks= r'Q:\dss_workarea\mlabiadh\workspace\20241015_Park_assets_script'
    assets_xlsx= os.path.join(wks, 'data', 'PARC_citywide_export.xlsx')
    trails_shp= os.path.join(wks, 'data', 'trails.shp')
    
    logging.info('Processing the Assets dataset')
    gdf_ast= process_assets(assets_xlsx)
    
    logging.info('\nProcessing the Trails dataset')
    gdf_trl= process_trails(trails_shp)
    
    logging.info('\nLogging to AGO')
    AGO_HOST = os.getenv('AGO_HOST')
    AGO_USERNAME = os.getenv('AGO_USERNAME') ###########change this###########
    AGO_PASSWORD = os.getenv('AGO_PASSWORD') ###########change this###########
    gis = connect_to_AGO(AGO_HOST, AGO_USERNAME, AGO_PASSWORD)
    
    logging.info('\nPublishing the Assets dataset to AGO')
    title= 'PARC_L1G_Park_Asset_Data_Feature_Layer_v2'
    folder= 'DSS Protected Areas Resource Catalogue (PARC) - Resource Analysis'
    geojson_name= 'bcparks_assets_v2'
    item_desc= f'Point dataset - Park assets (updated on {datetime.today().strftime("%B %d, %Y")})'
    
    
    logging.info('\nPublishing the Trails dataset to AGO')
    title= 'PARC_L1G_Park_Trail_Data_Feature_Layer_v2'
    folder= 'DSS Protected Areas Resource Catalogue (PARC) - Resource Analysis'
    geojson_name= 'bcparks_trails_v2'
    item_desc= f'Line dataset - Park trails (updated on {datetime.today().strftime("%B %d, %Y")})'
    publish_feature_layer(gis, gdf_trl, title, geojson_name, item_desc, folder)

    

    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    logging.info('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
    
