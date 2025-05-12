"""
This script produces an Interim IPCA dataset for the AST tool 
"""
import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import geopandas as gpd
import timeit


def esri_to_gdf(file_path)-> gpd.GeoDataFrame:
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    if '.shp' in file_path.lower():
        gdf = gpd.read_file(file_path)
    elif '.gdb' in file_path:
        l = file_path.split('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(file_path)
        gdf = gpd.read_file(filename=gdb, layer=fc)
    else:
        raise Exception('..format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def show_boundaries(df_ruls, gdf_ipca) -> gpd.GeoDataFrame:
    """
    Returns a gdf of IPCA features whose geometries will be included in the interim dataset
    """
    df_bndr = df_ruls[
        df_ruls['Interim approach'] == 'Show boundary'
    ]
    list_bndr = df_bndr['FEATURE_ID'].tolist()
    gdf_bndr = gdf_ipca[gdf_ipca['FEATURE_ID'].isin(list_bndr)]

    gdf_bndr = gdf_bndr[
        ['FEATURE_ID', 'FIRST_NATION_GROUP','PROJECT_ID','PROJECT_NAME','geometry']
    ]

    print (f'..Show boudaries has {len(gdf_bndr)} features')

    return gdf_bndr


def cnslt_boundaries(df_ruls, gdf_pip) -> gpd.GeoDataFrame:
    """
    Returns a gdf of IPCA features whose geometries will be replaced with Consultation Boundaries
    """
    df_cnslt = df_ruls[
        df_ruls['Interim approach'] == 'Consultation boundary'
    ]
    
    gdf_pip = gdf_pip.drop_duplicates(
        subset='CNSLTN_AREA_NAME',
        keep='first'
    )

    gdf_cnslt = pd.merge(
        df_cnslt[['FEATURE_ID', 'FIRST_NATION_GROUP','CNSLTN_AREA_NAME']],
        gdf_pip[['CNSLTN_AREA_NAME', 'geometry']],
        how='left',
        on='CNSLTN_AREA_NAME'
    )

    print (f'..Cnslt boudaries has {len(gdf_cnslt)} features')

    return gdf_cnslt


def produce_interim (gdf_bndr, gdf_cnslt, gdf_f107) -> gpd.GeoDataFrame:
    """
    Returns a gdf of of interim IPCA dataset
    """
    gdf_intr = gpd.GeoDataFrame(
        pd.concat([gdf_bndr, gdf_cnslt, gdf_f107], ignore_index=True),
        crs=gdf_bndr.crs
    )

    gdf_intr.drop(
        columns=['CNSLTN_AREA_NAME'], 
        inplace=True
    )

    gdf_intr.sort_values(
        by='FEATURE_ID',     
        ascending=True, 
        inplace=True,       
        ignore_index=True     
    )   

    print (f'..IPCA interim dataset has {len(gdf_intr)} features')

    return gdf_intr


if __name__ == "__main__":
    start_t = timeit.default_timer()

    wks = r"Q:\projects\Mwlrs\Land Use Planning\Master_Data"
    rules_xls = os.path.join(wks, '2025-04-11-Interim IPCA AST Layer - FNnames.xlsx')
    ipca_gdb = os.path.join(wks, 'IPCA.gdb')
    intr_gdb = os.path.join(wks, 'interim_IPCA_AST.gdb')

    print ("Reading IPCA interim rules...")
    df_ruls = pd.read_excel(rules_xls)

    print ("\nReading IPCA dataset...")
    gdf_ipca = esri_to_gdf(os.path.join(ipca_gdb, 'IPCA'))

    print ("\nReading PIP dataset...")
    gdf_pip= esri_to_gdf(os.path.join(intr_gdb, 'IPCA_CNSLT_AREAS'))

    print("\nCreating a 'Show Boundaries' dataset...")
    gdf_bndr = show_boundaries(df_ruls, gdf_ipca)

    print("\nCreating a 'Consultation Boundaries' dataset...")
    gdf_cnslt = cnslt_boundaries(df_ruls, gdf_pip) 

    print("\nReading special features...")
    gdf_f107 = esri_to_gdf(os.path.join(intr_gdb, 'F107'))
    gdf_f107 = gdf_f107[['FEATURE_ID', 'FIRST_NATION_GROUP','geometry']]

    print("\nProducing the IPCA Interim dataset...")
    gdf_intr = produce_interim (gdf_bndr, gdf_cnslt, gdf_f107)

    gdf_intr.to_file(
        filename=intr_gdb,    
        driver="OpenFileGDB",                 
        layer="AST_interim_IPCA"    
    )


    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')