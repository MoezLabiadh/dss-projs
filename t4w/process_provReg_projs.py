import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import numpy as np
import geopandas as gpd

from shapely.geometry import Point

import timeit
start_t = timeit.default_timer() #start time

wks= r'Q:\dss_workarea\mlabiadh\workspace\20241029_together_for_wildlife\data'

print ('Reading inputs')
gdb= os.path.join(wks, 'working_data.gdb')

df_org= pd.read_excel(os.path.join(wks, 'T4W_Goal_Actions_2023_Data_Oct28th2024.xlsx'))

df_pvrg= df_org[
    df_org['Spatial_Scope'].isin(['Province-wide', 'Region-wide'])
    ]

gdf_bndr= gpd.read_file(gdb, layer='resource_regions_and_prov')
gdf_bndr= gdf_bndr.to_crs(4326)

print ('\nGenerating Random points')
def generate_random_point (polygon):
    min_x, min_y, max_x, max_y = polygon.bounds
    while True:
        random_point = Point(
            np.random.uniform(min_x, max_x),
            np.random.uniform(min_y, max_y)
        )
        if polygon.contains(random_point):
            return random_point


for idx, row in df_pvrg.iterrows():
    region = row['Region_Branch']
    polygon = gdf_bndr[gdf_bndr['Region'] == region].geometry.iloc[0]
    random_point = generate_random_point(polygon)
    df_pvrg.at[idx, 'Latitude'] = random_point.y
    df_pvrg.at[idx, 'Longitude'] = random_point.x
    

print ('\nSaving files')    
gdf_pvrg_pts = gpd.GeoDataFrame(
                 df_pvrg,
                 geometry=gpd.points_from_xy(df_pvrg['Longitude'], df_pvrg['Latitude']),
                 crs="EPSG:4326"
             )  

gdf_pvrg_pts= gdf_pvrg_pts.to_crs(epsg=3005)

gdf_pvrg_pts.to_file(gdb, layer= 'regProv_points', driver="OpenFileGDB") 

finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))