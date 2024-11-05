import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import geopandas as gpd
from shapely import wkt

import timeit
start_t = timeit.default_timer() #start time

wks= r'Q:\dss_workarea\mlabiadh\workspace\20241029_together_for_wildlife\data'

print ('Reading inputs')
gdb= os.path.join(wks, 'working_data.gdb')

gdf_pnt= gpd.read_file(gdb, layer='T4W_Goal_Action_points_with_Attributes_20240422')
gdf_pol= gpd.read_file(gdb, layer='T4W_Goal_Action_polygons_with_Attributes_20240422')

df_org= pd.read_excel(os.path.join(wks, 'T4W_Goal_Actions_2023_Data_Oct28th2024.xlsx'))


print ('identifying projects requiring Plogons')
df_sub= df_org[df_org['Spatial_Scope']=='Sub-regional']

gdf_sub = pd.merge(
    df_sub.drop(columns=['Latitude', 'Longitude']),
    gdf_pol[['Project_ID', 'Latitude', 'Longitude', 'geometry']],
    how='left',
    on='Project_ID'
)

gdf_sub_missgeo= gdf_sub[gdf_sub['geometry'].isnull()]
    

print ('\nProcessing missing shapes')
projID_dict={
    '2023-T4W-083':'DryInterior_polygons', 
    '2023-T4W-064':'Ariana McKay_Christina Waddle_2022_23',  
    '2023-T4W-049':'Hab_GMU_Extent2022_23',  
    '2023-T4W-028':'Matt_Scheideman_1_22_23', 
    '2023-T4W-087':'WHR_Model_v2_31Mar2021_WCR_extent_2020_21',  
    '2023-T4W-066':'Mary Toews_Darryn McConkey_22_23',  
    '2023-T4W-091':'Karine Pigeon_Anne-Marie Roberts_1_22_23',  
    '2023-T4W-090':'Winter_Study_WCoast',
    '2023-T4W-018':'opening_1765312',
    '2023-T4W-099':'opening_1765312'
    }

poly_dict={}

miss_folder= os.path.join(wks, 'missing_boundaries')
for root, dirs, files in os.walk(miss_folder):
    for file in files:
        if file.endswith('.shp'):
            file_path = os.path.join(root, file)
            file_key = os.path.splitext(file)[0]
            print (f'...processing {file_key}')
            gdf = gpd.read_file(file_path)
            gdf= gdf.to_crs(epsg=3005)

            if len(gdf) > 1:
                merged_geometry = gdf.geometry.unary_union
                gdf = gpd.GeoDataFrame(geometry=[merged_geometry], crs=gdf.crs)
                gdf['geometry'] = gdf['geometry'].apply(
                    lambda geom: wkt.loads(
                        wkt.dumps(geom, output_dimension=2)
                        )
                    )
            
            gdf['Project_ID'] = file_key
            gdf = gdf[['Project_ID', 'geometry']]
            
            for proj_key, proj_value in projID_dict.items():
                if file_key == proj_value:
                    gdf['Project_ID'] = proj_key
                    poly_dict[proj_key] = gdf[['Project_ID', 'geometry']]
                    
                    
#create a gdf with all missing boundaries
gdf_pol_all = pd.concat(poly_dict.values(), ignore_index=True)


print ('\nCalulcating centroids')
#create a gdf of points/centroids
gdf_pnt_all = gdf_pol_all.copy()
gdf_pnt_all['geometry'] = gdf_pnt_all.centroid

centroids_wgs84 = gdf_pnt_all.to_crs(epsg=4326)
gdf_pnt_all['Longitude'] = centroids_wgs84.geometry.x
gdf_pnt_all['Latitude'] = centroids_wgs84.geometry.y

gdf_pol_all= pd.merge(
    gdf_pol_all,
    gdf_pnt_all[['Project_ID','Longitude', 'Latitude']],
    how= 'left'
)


print ('\nAdding missing shapes')
for idx, row in gdf_sub.iterrows():
    if row['geometry'] is None:
        project_id = row['Project_ID']
        
        matching_row = gdf_pol_all[gdf_pol_all['Project_ID'] == project_id]
        
        if not matching_row.empty:
            gdf_sub.at[idx, 'geometry'] = matching_row.iloc[0].geometry
            gdf_sub.at[idx, 'Latitude'] = matching_row.iloc[0].Latitude
            gdf_sub.at[idx, 'Longitude'] = matching_row.iloc[0].Longitude
            
gdf_sub = gpd.GeoDataFrame(gdf_sub, geometry='geometry', crs=gdf_pol_all.crs)    


#create a points dataset
gdf_sub_att= gdf_sub.drop(columns=['geometry'])
gdf_sub_pts = gpd.GeoDataFrame(
                 gdf_sub_att,
                 geometry=gpd.points_from_xy(gdf_sub_att['Longitude'], gdf_sub_att['Latitude']),
                 crs="EPSG:4326"
             )  

gdf_sub_pts= gdf_sub_pts.to_crs(epsg=4326)


#save to file
print ('\nSaving files')
#gdf_sub_pts.to_file(gdb, layer= 'subregional_points', driver="OpenFileGDB") 
#gdf_sub.to_file(os.path.join(wks, 'subregional_polys.geojson'), driver='GeoJSON')     


finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))