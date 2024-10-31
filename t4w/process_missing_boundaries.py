import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import geopandas as gpd
from shapely import wkt

wks= r'Q:\dss_workarea\mlabiadh\workspace\20241029_together_for_wildlife\data'

gdb= os.path.join(wks, 'working_data.gdb')

print ('Reading inputs')
gdf_pnt= gpd.read_file(gdb, layer='T4W_Goal_Action_points_with_Attributes_20240422')
gdf_pol= gpd.read_file(gdb, layer='T4W_Goal_Action_polygons_with_Attributes_20240422')

gdf_pnt_missgeo= gdf_pnt[gdf_pnt['geometry'].isnull()]
gdf_pol_missgeo= gdf_pol[gdf_pol['geometry'].isnull()]

print ('\nProcessing missing shapes')
projID_dict={
    '2023-T4W-067':'caribou_region', 
    '2023-T4W-064':'Ariana McKay_Christina Waddle_2022_23',  
    '2023-T4W-049':'Hab_GMU_Extent2022_23',  
    '2023-T4W-028':'Matt_Scheideman_1_22_23', 
    '2023-T4W-087':'WHR_Model_v2_31Mar2021_WCR_extent_2020_21',  
    '2023-T4W-066':'Mary Toews_Darryn McConkey_22_23',  
    '2023-T4W-091':'Karine Pigeon_Anne-Marie Roberts_1_22_23',  
    '2023-T4W-090':'Winter_Study_WCoast'
    }

#shp_dict={}
gdf_poly_dict={}

miss_folder= os.path.join(wks, 'missing_boundaries')
for root, dirs, files in os.walk(miss_folder):
    for file in files:
        if file.endswith('.shp'):
            file_path = os.path.join(root, file)
            file_key = os.path.splitext(file)[0]
            print (f'...processing {file_key}')
            gdf = gpd.read_file(file_path)
            #shp_dict[file_key] = gdf

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
                    gdf_poly_dict[proj_key] = gdf[['Project_ID', 'geometry']]
                    

print ('\nConcatinating gdfs')
#create a gdf with all missing boundaries
gdf_pol_all = pd.concat(gdf_poly_dict.values(), ignore_index=True)


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


#verify
gdf_pnt_all_nogeom= gdf_pnt_all[['Project_ID', 'Longitude', 'Latitude']]
gdf_pol_all_nogeom= gdf_pol_all[['Project_ID', 'Longitude', 'Latitude']]


#export to file
gdf_pnt_all.to_file(gdb, layer= 'missing_boundaries_points', driver="OpenFileGDB") 
gdf_pol_all.to_file(gdb, layer= 'missing_boundaries_polys', driver="OpenFileGDB") 

#gdf_pol_all.to_file(os.path.join(wks, 'missing_boundaries_polys.shp'))
#gdf_pnt_all.to_file(os.path.join(wks, 'missing_boundaries_points.shp'))
