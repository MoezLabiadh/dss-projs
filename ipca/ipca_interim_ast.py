"""
Purpose: 
    This script produces an Interim IPCA dataset for the AST tool.
    Rules on how each IPCA feature is included in the Iterim dataset are read from an Excel file. 
    Make sure  to update this file before running the script.

    The script date stamps and archives the existing interim dataset before processing the new one.
    
    The script exports the interim dataset to gdb/featureclass, Shapefile, and KML formats 
    in both the main and shared directories.

Dependencies:
    - geopandas
    - pandas
    (This script must be run in the Python environment with geopandas installed:
     P:\corp\central_clones\python_geopandas)

Last updated on: 
    2025-07-24

Author: 
    Moez Labiadh - GeoBC
"""

import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import geopandas as gpd
import timeit
from datetime import datetime


def archive_existing_dataset(ast_gdb_main: str, ast_gdb_arch: str) -> None:
    """
    Archives the existing interim IPCA feature class
    """
    main_fc= 'AST_interim_IPCA'
    try:
        gdf_tmp = esri_to_gdf(os.path.join(ast_gdb_main, main_fc))
    except:
        print(f'..No existing feature class found. Nothing to archive.')

    today = datetime.now().strftime("%Y%m%d")
    gdf_tmp.to_file(
        filename=ast_gdb_arch,    
        driver="OpenFileGDB",                 
        layer=f"AST_interim_IPCA_{today}"
    )
    

def esri_to_gdf(path: str) -> gpd.GeoDataFrame:
    """Returns a GeoDataFrame for a .shp or .gdb feature class."""
    if path.lower().endswith('.shp'):
        return gpd.read_file(path)
    elif '.gdb' in path:
        gdb = path.split('.gdb')[0] + '.gdb'
        layer = os.path.basename(path)
        return gpd.read_file(filename=gdb, layer=layer)
    else:
        raise Exception('Provide a .shp or .gdb feature class')


def show_boundaries(df_rules: pd.DataFrame, gdf_ipca: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Selects IPCA features marked 'Show boundary'."""
    ids = df_rules.loc[df_rules['Interim approach'] == 'Show boundary', 'FEATURE_ID']
    gdf = gdf_ipca[gdf_ipca['FEATURE_ID'].isin(ids)]
    cols = ['FEATURE_ID', 'FIRST_NATION_GROUP', 'PROJECT_ID', 'PROJECT_NAME', 'geometry']
    print(f'..Show boundaries: {len(gdf)} features')
    return gdf[cols]


def cnslt_boundaries(df_rules: pd.DataFrame, gdf_pip: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Substitutes geometries with Consultation Boundaries."""
    df_sel = df_rules[df_rules['Interim approach'] == 'Consultation boundary']
    gdf_pip = gdf_pip.drop_duplicates(subset='CNSLTN_AREA_NAME')
    merged = pd.merge(
        df_sel[['FEATURE_ID', 'FIRST_NATION_GROUP', 'CNSLTN_AREA_NAME']],
        gdf_pip[['CNSLTN_AREA_NAME', 'geometry']],
        on='CNSLTN_AREA_NAME', how='left'
    )
    gdf_merged = gpd.GeoDataFrame(merged, geometry='geometry', crs=gdf_pip.crs)
    print(f'..Consultation boundaries: {len(gdf_merged)} features')
    return gdf_merged


def produce_interim(
    gdf_bndr: gpd.GeoDataFrame,
    gdf_cnslt: gpd.GeoDataFrame,
    gdf_f107: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Concatenates all parts into the interim GeoDataFrame."""
    gdf_intr = gpd.GeoDataFrame(
        pd.concat([gdf_bndr, gdf_cnslt, gdf_f107], ignore_index=True),
        crs=gdf_bndr.crs
    )
    gdf_intr.drop(columns=['CNSLTN_AREA_NAME'], inplace=True, errors='ignore')
    gdf_intr.sort_values('FEATURE_ID', inplace=True, ignore_index=True)
    print(f'..Interim dataset: {len(gdf_intr)} features')
    return gdf_intr


def export_interim_ipca(interim_dir: str, gdb: str, gdf_intr: gpd.GeoDataFrame) -> None:
    """
    Exports the interim IPCA GeoDataFrame to to OpenFileGDB, 
    Shapefile, and KML
    """
    # Paths
    shapefile = os.path.join(interim_dir, 'AST_interim_IPCA.shp')
    kml_file = os.path.join(interim_dir, 'AST_interim_IPCA.kml')

    # Overwrite main feature class
    gdf_intr.to_file(
        filename=gdb,    
        driver="OpenFileGDB",                 
        layer=f"AST_interim_IPCA"  # Overwrite the main dataset
    )
    print(f'..Overwrote main feature class')

    # Shapefile export
    gdf_intr.to_file(shapefile)
    print(f'..Exported shapefile')

    # KML export
    gdf_intr.to_file(kml_file, driver='KML')
    print(f'..Exported KML')



if __name__ == '__main__':
    start = timeit.default_timer()

    # Base directories
    BASE_DIR = r"W:\ilmb\dss\projects\Mwlrs\Land Use Planning\Master_Data"
    INTERIM_DIR = os.path.join(BASE_DIR, 'interim_IPCA')
    INTERIM_DIR_SHARE = r'W:\!Shared_Access\IPCA'

    # File paths
    rules_xls = os.path.join(BASE_DIR, '2025-06-Interim IPCA AST Layer - FNnames.xlsx')
    ipca_gdb = os.path.join(BASE_DIR, 'IPCA.gdb')
    ast_gdb_main = os.path.join(INTERIM_DIR, 'interim_IPCA_AST_main.gdb')
    ast_gdb_arch = os.path.join(INTERIM_DIR, 'interim_IPCA_AST_archive.gdb')
    pip_table = os.path.join(ast_gdb_main, 'IPCA_CNSLT_AREAS')
    f107_fc = os.path.join(ast_gdb_main, 'F107')

    print('Archiving existing IPCA interim dataset...')
    archive_existing_dataset(ast_gdb_main, ast_gdb_arch)

    print('\nReading IPCA interim rules...')
    df_rules = pd.read_excel(rules_xls)

    print('\nReading IPCA dataset...')
    gdf_ipca = esri_to_gdf(os.path.join(ipca_gdb, 'IPCA'))

    print('\nReading PIP consultation boundaries...')
    gdf_pip = esri_to_gdf(pip_table)

    print("\nSelecting 'Show Boundaries'...")
    gdf_bndr = show_boundaries(df_rules, gdf_ipca)

    print("\nSelecting 'Consultation Boundaries'...")
    gdf_cnslt = cnslt_boundaries(df_rules, gdf_pip)

    print("\nReading special features (F107)...")
    gdf_f107 = esri_to_gdf(f107_fc)[['FEATURE_ID', 'FIRST_NATION_GROUP', 'geometry']]

    print("\nBuilding interim dataset...")
    gdf_intr = produce_interim(gdf_bndr, gdf_cnslt, gdf_f107)

    # Export outputs
    print("\nExporting files to main folder...")
    export_interim_ipca(INTERIM_DIR, ast_gdb_main, gdf_intr)

    print("\nExporting files to Share folder...")
    share_gdb = os.path.join(INTERIM_DIR_SHARE, 'IPCA_AST.gdb')
    export_interim_ipca(INTERIM_DIR_SHARE, share_gdb, gdf_intr)

    elapsed = round(timeit.default_timer() - start)
    m, s = divmod(elapsed, 60)
    print(f'\nProcessing completed in {m}m {s}s')