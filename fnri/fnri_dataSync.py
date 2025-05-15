"""
This script synchronizes records between the local FNRI working and 
staging datasets (gdb/featureclass) and the FNRI Feature Layer on AGOL.

The script executes the following workflow:
    0) Create a backup copy of the Working and Staging Geodatabases

    1) Detect new Records from the Working GDB and append them to the Working AGOL Feature Layer

    2) Delete Records marked as "To Be Retired" from the Working AGOL and GDB datasets

    3) Detect modified attributes in Working AGOL layer and edit them in the Working GDB

    4) Flag records that are marked Complete (FNLT and MIRR) 
       but have null values in any of the required attribute columns.

    5) Move records marked 'Ready To Publish' from the Working GDB into the Staging GDB
       Flag moved records overlapping with existing records in staging 
       When a record is moved and its unique ID matches an existing one in staging , the existing record will be overwritten.
       DATE_CREATED is populated with today's date
       Records moved to staging GDB will have their Publish_Status set to "Published"

    6) Remove deleted records (Ready To Publish and manually deleted from Working GDB) from the Working AGOL Feature Layer

    7) Overwrite the Staging AGOL feature layer with data from the Staging GDB .
       Staging records that fail to publish to AGOL are flagged in the Log file (usually due to topology issues with the geometry) 

Created on: 2025-04-23
Author(s): Moez Labiadh
"""

import warnings
warnings.simplefilter(action='ignore')

import os
import json
import arcpy
import pandas as pd
from arcgis.gis import GIS
from datetime import datetime
import timeit
import time
import zipfile


class AGOConnector:
    def __init__(self, host, username, password):
        """
        Initialize the AGOConnector instance.
        """
        self.host = host
        self.username = username
        self.password = password
        self.gis = None 
    
    def connect(self):
        """
        Establish a connection to AGO and store the GIS object.
        """
        self.gis = GIS(self.host, self.username, self.password, verify_cert=False)
        if self.gis.users.me:
            print(f'..successfully connected to AGOL as {self.gis.users.me.username}')
            return self.gis
        else:
            raise ConnectionError("Failed to connect to AGOL.")
      
    def disconnect(self):
        """
        Disconnect from AGO by clearing the GIS connection.
        """
        if self.gis:
            self.gis = None
            print("\nDisconnected from AGOL")
        else:
            print("\nNo active AGOL connection to disconnect.")


class AGOSyncManager:
    def __init__(
        self, gis, working_fc, staging_fc, unique_id_field, 
        today_date, agol_item_id_working, agol_item_id_staging
    ):
        """
        Initialize the AGOSyncManager instance.
        """
        self.gis = gis
        self.working_fc = working_fc
        self.staging_fc = staging_fc
        self.unique_id_field = unique_id_field
        self.today_date = today_date
        self.agol_item_id_working = agol_item_id_working
        self.agol_item_id_staging = agol_item_id_staging
        self.agol_df = None
        self.local_df = None
        # Dictionary to hold detected changes
        self.change_log = {
            "append_new_areas_to_agol": [],
            "retired_records_deleted" : [],
            "modify_edited_agol_attributes": [],
            "flag_missing_attributes": [],
            "move_published_to_staging": [],
            "flag_spatial_overlaps_staging": [],
            "delete_removed_records_from_agol": [],
            "flag_failed_to_ago_publish_staging": []
        }


    def get_agol_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of an AGOL feature layer.
        """
        item = self.gis.content.get(self.agol_item_id_working)
        agol_layer = item.layers[0] 
        agol_features = agol_layer.query(
            where="1=1", 
            out_fields="*", 
            return_geometry=True
        ).features

        self.agol_df = pd.DataFrame([{
            **f.attributes,
            "geometry": f.geometry
        } for f in agol_features])

        for col in self.agol_df.columns:
            if 'date' in col.lower():
                self.agol_df[col] = pd.to_datetime(
                    self.agol_df[col], unit='ms', utc=True
                ).dt.tz_convert('America/Vancouver').dt.tz_localize(None)

        return self.agol_df


    def get_local_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of a gdb/featureclass.
        """
        self.local_df = pd.DataFrame.spatial.from_featureclass(self.working_fc)
        for col in self.local_df.columns:
            if 'date' in col.lower():
                self.local_df[col] = pd.to_datetime(self.local_df[col], unit='ms')
                self.local_df[col] = self.local_df[col].dt.floor('S')
                
        return self.local_df


    def append_new_areas_to_agol(self) -> None:
        """
        Detects new records from the master dataset that are not present in the AGOL dataset and appends them.
        (Logs: New Areas added to AGOL)
        """

        # Identify new records in the master dataset that are not in the AGOL dataset,
        local_new = self.local_df[~self.local_df[self.unique_id_field].isin(self.agol_df[self.unique_id_field])]

        if not local_new.empty:
            print(f"..found {len(local_new)} new records in local dataset to append to AGOL.")

            # Retrieve the target AGOL feature layer.
            item = self.gis.content.get(self.agol_item_id_working)
            if not item:
                raise ValueError("..AGOL feature layer not found.")
            agol_layer = item.layers[0]

            features = []
            for _, row in local_new.iterrows():
                # Prepare attributes, replacing NaN with None
                attributes = {
                    col: (row[col] if pd.notna(row[col]) else None)
                    for col in self.local_df.columns if col.lower() != 'shape'
                }
                # Prepare geometry from the 'SHAPE' field
                geometry = row["SHAPE"]
                if isinstance(geometry, dict):
                    # Force valid JSON format
                    geometry = json.loads(json.dumps(geometry))
                features.append({
                    "attributes": attributes,
                    "geometry": geometry
                })

            # Append features to AGOL
            result = agol_layer.edit_features(adds=features)
            if result.get('addResults', []):
                print(f"..successfully added {len(result['addResults'])} records to AGOL.")
                # Log the appended record feature IDs
                new_ids = local_new[self.unique_id_field].tolist()
                self.change_log["append_new_areas_to_agol"].extend(new_ids)
                print(f"..New Areas added: {new_ids}")
            else:
                print("..no records were added to AGOL. Check for errors.")
        else:
            print("..no new records to append to AGOL.")


    def delete_retired_working_records(self) -> None:
            """
            Deletes records from both the Working AGOL and GDB
            whose Publish_Status field is set to 'To be Retired from Working Data'.
            (Logs: Retired records deleted)
            """
            # Identify records in AGOL marked for deletion
            agol_to_delete = self.agol_df[self.agol_df['Publish_Status'] == 'To be Retired from Working Data']
            
            if agol_to_delete.empty:
                print("..no records marked for deletion found.")
                return
            
            unique_ids = agol_to_delete[self.unique_id_field].tolist()

            # Delete from AGOL Feature Layer
            item = self.gis.content.get(self.agol_item_id_working)
            layer = item.layers[0]
            
            def format_id(val):
                return f"'{val}'" if isinstance(val, str) else str(val)
            
            ids_str = ", ".join(format_id(val) for val in unique_ids)
            where_clause = f"{self.unique_id_field} in ({ids_str})"
            result = layer.delete_features(where=where_clause)
            
            if result.get("deleteResults"):
                if all(r.get("success") for r in result["deleteResults"]):
                    print(f"..successfully deleted {len(unique_ids)} records from Working AGOL: {unique_ids}")
                else:
                    print("..failed to delete some records from AGOL.")
            else:
                print("..failed to delete records from AGOL.")
            
            # Delete from the Master dataset feature class
            deleted_ids = []
            with arcpy.da.UpdateCursor(self.working_fc, [self.unique_id_field]) as cursor:
                for row in cursor:
                    if row[0] in unique_ids:
                        deleted_ids.append(row[0])
                        cursor.deleteRow()

            print(f"..successfully deleted {len(deleted_ids)} record(s) from Working GDB: {deleted_ids}")
            
            self.change_log["retired_records_deleted"].extend(unique_ids)



    def modify_edited_agol_attributes(self) -> None:
            """
            Detect modified attributes in AGOL and edit them in the working dataset.
            (Logs: Records modified in AGOL)
            """
            edit_fields = [
                'FIRST_NATION', 'AGREEMENT_TYPE', 'AGREEMENT_STAGE',
                'AGREEMENT_LINK', 'CONTACT_NAME', 'CONTACT_EMAIL', 'DATE_CREATED',
                'Review_Status_FNLT', 'Review_Status_MIRR', 'Publish_Status'
            ]
            uid = self.unique_id_field

            # build a lookup of local records
            local_lookup = (
                self.local_df
                    .drop(columns=['SHAPE'], errors='ignore')
                    .set_index(uid)[edit_fields]
                    .to_dict(orient='index')
            )

            # index AGOL by uid and focus on our edit fields
            agol_indexed = self.agol_df.set_index(uid)
            agol_to_check = agol_indexed[edit_fields]

            modified_ids = []
            for record_id, agol_row in agol_to_check.iterrows():
                local_row = local_lookup.get(record_id)
                if not local_row:
                    continue
                for field in edit_fields:
                    a = agol_row[field]
                    l = local_row[field]
                    if pd.isna(a) and pd.isna(l):
                        continue
                    if a != l:
                        modified_ids.append(record_id)
                        break

            if not modified_ids:
                print("..no edits detected in AGOL.")
                return

            unique_ids = set(modified_ids)
            print(f"..found {len(unique_ids)} records with modified attributes in AGOL: {modified_ids}.")

            self.change_log.setdefault("modify_edited_agol_attributes", [])

            # apply updates back to the master feature class
            fields = [uid] + edit_fields
            # explicit=True ensures that None becomes a true NULL in the FGDB
            with arcpy.da.UpdateCursor(self.working_fc, fields, explicit=True) as cursor:
                for row in cursor:
                    fid = row[0]
                    if fid in unique_ids:
                        updated = False
                        for idx, field in enumerate(edit_fields, start=1):
                            new_val = agol_indexed.at[fid, field]
                            # if the AGOL value is NaT (datetime) or NaN, convert to None
                            if pd.isna(new_val):
                                new_val = None
                            if row[idx] != new_val:
                                row[idx] = new_val
                                updated = True
                        if updated:
                            cursor.updateRow(row)

            self.change_log["modify_edited_agol_attributes"].extend(unique_ids)



    def flag_missing_attributes(self) -> None:
        """
        Flags Parcel_Name(s) that are marked Complete (FNLT or MIRR)
        but have null values in any of the required attribute columns.
        (Logs: [QC/QA!] -  Ready-to-Publish Records with missing Attributes)
        """
        required = [
            'FIRST_NATION', 'AGREEMENT_TYPE', 'AGREEMENT_STAGE',
            'AGREEMENT_LINK'
        ]

        df = self.local_df
        
        # condition: both review complete AND any required column is null
        mask = (
            (df['Review_Status_FNLT'] == 'Review Complete') &
            (df['Review_Status_MIRR'] == 'Review Complete')
        ) & df[required].isnull().any(axis=1)

        missing = df.loc[mask, self.unique_id_field].tolist()
        if missing:
            print(f"..flagged {len(missing)} ready-to-publish records missing attributes: {missing}")
            # add to change log
            self.change_log.setdefault('flag_missing_attributes', []).extend(missing)
        else:
            print("..no ready-to-publish records missing attributes.")


    def move_published_to_staging(self) -> None:
        """
        Move records marked Ready To Publish from working_fc into staging_fc.
        If a record already exists in staging_fc (same unique_id), delete it
        before inserting the up-to-date copy from working_fc. Then flag any
        moved records whose geometries overlap existing staging features.
        (Logs: Ready-to-Publish Records moved to staging dataset;
               [QC/QA!] - Records overlapping in staging)
        """
        df = self.local_df
        mask = (
            (df['Review_Status_FNLT'] == 'Review Complete') &
            (df['Review_Status_MIRR'] == 'Review Complete') &
            (df['Publish_Status']     == 'Ready to Publish')
        )
        to_move = df.loc[mask]
        if to_move.empty:
            print("..no records ready to publish to staging.")
            return

        self.change_log.setdefault("move_published_to_staging", [])

        # 1) Delete duplicates in staging
        staging_df   = pd.DataFrame.spatial.from_featureclass(self.staging_fc)
        existing_ids = set(staging_df[self.unique_id_field])
        ids_to_move  = set(to_move[self.unique_id_field])
        dup_ids = existing_ids & ids_to_move
        if dup_ids:
            clause = f"{self.unique_id_field} IN ({','.join(repr(i) for i in dup_ids)})"
            with arcpy.da.UpdateCursor(self.staging_fc, [self.unique_id_field], clause) as cursor:
                for _ in cursor:
                    cursor.deleteRow()
            print(f"..removed {len(dup_ids)} existing records from staging: {sorted(dup_ids)}")

        # 2) Insert fresh copies
        staging_fields = [
            f.name
            for f in arcpy.ListFields(self.staging_fc)
                if f.type not in ('Geometry', 'OID') and f.name.upper() != 'SHAPE'
        ]
        # only keep those fields that exist in to_move
        attr_fields = [col for col in to_move.columns if col in staging_fields]
        # build the full field list including geometry
        fields      = attr_fields + ['SHAPE@JSON']

        with arcpy.da.InsertCursor(self.staging_fc, fields) as inserter:
            for _, row in to_move.iterrows():
                attrs = []
                for f in attr_fields:
                    if f == 'DATE_CREATED':
                        # stamp creation date
                        attrs.append(self.today_date)
                    elif f == 'Publish_Status':
                        # override to “Published” when moving
                        attrs.append('Published')
                    else:
                        attrs.append(row[f] if pd.notna(row[f]) else None)
                geom = json.dumps(row['SHAPE'])
                inserter.insertRow(attrs + [geom])
        
        # 3) Delete moved records from working_fc
        delete_clause = f"{self.unique_id_field} IN ({','.join(repr(i) for i in ids_to_move)})"
        with arcpy.da.UpdateCursor(self.working_fc, [self.unique_id_field], delete_clause) as cursor:
            for _ in cursor:
                cursor.deleteRow()

        # Log moved IDs
        moved_ids = sorted(ids_to_move)
        self.change_log["move_published_to_staging"].extend(moved_ids)
        print(f"..moved {len(moved_ids)} records to staging: {moved_ids}")

        # 4) Spatial overlap check
        if arcpy.Exists("staging_lyr"):
            arcpy.Delete_management("staging_lyr")
        arcpy.management.MakeFeatureLayer(self.staging_fc, "staging_lyr")

        # Read geometries of just-moved records
        geom_lookup = {}
        with arcpy.da.SearchCursor(self.staging_fc, [self.unique_id_field, 'SHAPE@']) as cursor:
            for uid, geometry in cursor:
                if uid in ids_to_move:
                    geom_lookup[uid] = geometry

        overlap_ids = []
        for uid, geom in geom_lookup.items():
            if arcpy.Exists("others_lyr"):
                arcpy.Delete_management("others_lyr")
            where_clause = f"{self.unique_id_field} <> {uid}"
            arcpy.management.MakeFeatureLayer(self.staging_fc, "others_lyr", where_clause)

            arcpy.management.SelectLayerByLocation("others_lyr", "INTERSECT", geom)
            count = int(arcpy.management.GetCount("others_lyr")[0])
            if count > 0:
                overlap_ids.append(uid)

        if overlap_ids:
            self.change_log.setdefault("flag_spatial_overlaps_staging", []).extend(sorted(overlap_ids))
            print(f"..flagged records overlapping in staging: {sorted(overlap_ids)}")


    def delete_removed_local_records_from_agol(self) -> None:
        """
        Deletes records from the working AGO feature layer that are no longer present 
        in the working local dataset.
        (Logs: Records removed in AGOL)
        """
        agol_deleted = self.agol_df[
            ~self.agol_df[self.unique_id_field].isin(self.local_df[self.unique_id_field])
        ]
        if not agol_deleted.empty:
            unique_ids = agol_deleted[self.unique_id_field].tolist()
            self.change_log["delete_removed_records_from_agol"].extend(unique_ids)
            print(f"..deleted {len(unique_ids)} records from AGOL : {unique_ids}")

            item = self.gis.content.get(self.agol_item_id_working)
            layer = item.layers[0]

            def format_id(val):
                return f"'{val}'" if isinstance(val, str) else str(val)
            ids_str = ", ".join(format_id(val) for val in unique_ids)
            where_clause = f"{self.unique_id_field} in ({ids_str})"
            result = layer.delete_features(where=where_clause)
            if result.get("deleteResults"):
                if not all(r.get("success") for r in result["deleteResults"]):
                    print("..failed to delete some records from AGO.")
            else:
                print("..failed to delete records from AGO.")
        else:
            print("..no records in AGO were removed (delete_removed_local_records_from_agol).")


    def overwrite_feature_layer(self, fc, agol_item_id: str, where_clause: str) -> None:
            """
            Overwrites a feature layer on AGO by truncating it, then uploading
            each record individually.  Any Poly_Unique_ID that fail to load
            are collected and reported.
            """
            print(f"..retrieving target layer with ID: {agol_item_id}")
            item = self.gis.content.get(agol_item_id)
            if not item:
                raise ValueError(f"Feature layer with ID {agol_item_id} not found.")
            
            layer = item.layers[0]  
            print(f"..found feature layer: {item.title}")
            
            total_rows = int(arcpy.GetCount_management(fc)[0])
            print(f"..source feature class contains {total_rows} rows")
            
            print("..truncating the feature layer...")
            layer.manager.truncate()
            print("..feature layer truncated successfully.")
            
            # determine which fields to upload
            fields = [
                f.name for f in arcpy.ListFields(fc)
                if f.name in self.agol_df.columns and f.type not in ("OID", "Geometry")
            ]

            # build the search cursor
            search_fields = ["SHAPE@"] + fields
            failed_ids = []

            print("..uploading records one by one...")
            with arcpy.da.SearchCursor(fc, search_fields, where_clause) as cursor:
                for row in cursor:
                    geom = row[0]
                    if geom is None:
                        print("..skipping record with no geometry")
                        continue

                    # build the attributes dict
                    attrs = {fields[i]: row[i + 1] for i in range(len(fields))}
                    unique_id = attrs[self.unique_id_field]

                    feature = {
                        "geometry": json.loads(geom.JSON),
                        "attributes": attrs
                    }

                    # try to add the single feature
                    try:
                        result = layer.edit_features(adds=[feature])
                        add_res = result.get("addResults", [])
                        if not add_res or not add_res[0].get("success", False):
                            raise RuntimeError(add_res[0].get("error", "Unknown error"))
                        print(f"..successfully added record {unique_id}")
                    except Exception as e:
                        print(f"..FAILED to add record {unique_id}: {e}")
                        failed_ids.append(unique_id)

                    # small pause to avoid hammering the service
                    time.sleep(1)

            if failed_ids:
                print(f"\n..upload complete with failures ({len(failed_ids)}): {failed_ids}")
                self.change_log["flag_failed_to_ago_publish_staging"].extend(failed_ids)
            else:
                print("\n..all records uploaded successfully.")

    def export_change_log(self, log_file_path: str) -> None:
        """
        Exports the collected change log information to a text file.
        """
        header_mapping = {
            "append_new_areas_to_agol"        : "New Areas added to AGOL:",
            "retired_records_deleted"         : "Retired records deleted",
            "modify_edited_agol_attributes"   : "Records modified in AGOL:",
            "flag_missing_attributes"         : "[QC/QA!] -  Ready-to-Publish Records with missing Attributes:",
            "move_published_to_staging"       : "Ready-to-Publish Records moved to staging dataset:",
            "flag_spatial_overlaps_staging"   : "[QC/QA!] -Records overlapping in staging",
            "delete_removed_records_from_agol": "Records removed in Working AGOL",
            "flag_failed_to_ago_publish_staging" : "[QC/QA!] -Staging records failed to publish to AGO"
        }

        try:
            with open(log_file_path, 'w') as f:
                header = f"Change Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f.write(header)
                f.write("=" * (len(header)-1) + "\n\n")
                any_changes = False
                for key, title in header_mapping.items():
                    changes = self.change_log.get(key, [])
                    if changes:
                        any_changes = True
                        # Remove duplicates and sort for clarity
                        unique_ids = sorted(set(changes))
                        f.write(f"{title}\n")
                        f.write(f"   Number of changes: {len(unique_ids)}\n")
                        f.write(f"   Area ID(s): {', '.join(map(str, unique_ids))}\n\n")
                if not any_changes:
                    f.write("..no changes detected.\n")

        except Exception as e:
            print(f"..failed to export change log: {e}")



def archive_geodatabase(gdb_path, archive_folder, today_date_str) -> None:
    """
    Walks the ESRI fgdb folder and zips up everything it can.
    Any file that’s locked (PermissionError) will be skipped.
    """
    if not os.path.exists(gdb_path):
        print(f"..archive skipped, gdb not found: {gdb_path}")
        return

    os.makedirs(archive_folder, exist_ok=True)
    gdb_name = os.path.basename(gdb_path)
    zip_path = os.path.join(
        archive_folder,
        f"{gdb_name}_archive_{today_date_str}.zip"
    )

    print(f"..creating archive: {gdb_name}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        # root_dir: parent of the .gdb folder so arcname keeps the .gdb name
        root_dir = os.path.dirname(gdb_path)
        for dirpath, dirnames, filenames in os.walk(gdb_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                # build path inside zip so it mirrors the .gdb structure
                arcname = os.path.relpath(full_path, root_dir)
                
                try:
                    zf.write(full_path, arcname)
                except PermissionError:
                    pass
                except Exception as e:
                    pass



if __name__ == "__main__":
    start_t = timeit.default_timer()

    today_date = datetime.now()
    today_date_str = today_date.strftime("%Y%m%d")

    ###### TEMPORARY DATA #######
    wks = r"Q:\dss_workarea\shbeatti\for_Chloe"
    archive_wks = os.path.join(wks, 'archive_test')

    main_gdb = os.path.join(wks, 'Sample_FNRI_working.gdb')
    archive_gdb = os.path.join(wks, 'Sample_FNRI_backup.gdb')

    working_fc = os.path.join(main_gdb, 'fnt_Areas_of_Interest_test_working_v2') 
    staging_fc = os.path.join(main_gdb, 'fnt_Areas_of_Interest_test_staging_v2') 

    ###### TREAL DATA #######
    #wks = r'N:\projects\firstnat\First_Nations_Area_of_Interest_for_BCGW\data'

    #working_gdb = os.path.join(wks, 'working_fnt_Areas_of_Interest.gdb') 
    #staging_gdb = os.path.join(wks, 'fnt_Areas_of_Interest.gdb')         

    #working_fc = os.path.join(working_gdb, 'working_fnt_Areas_of_Interest') 
    #staging_fc = os.path.join(staging_gdb, 'fnt_Areas_of_Interest')    
    '''     
    print('Backing up the working and staging GDBs...')
    for gdb in (main_gdb, archive_gdb):
        archive_geodatabase(gdb, archive_wks, today_date_str)
    '''
    try:
        print('\nLogging to AGO...')
        AGO_HOST = 'https://governmentofbc.maps.arcgis.com'
        AGO_USERNAME_DSS = 'XXX' 
        AGO_PASSWORD_DSS = 'XXX' 
        ago = AGOConnector(AGO_HOST, AGO_USERNAME_DSS, AGO_PASSWORD_DSS)
        gis = ago.connect()

        unique_id_field = 'Poly_Unique_ID' 

        agol_item_id_working = 'af906c27d8384a059928e232fed5d376' ###### TEMPORARY DATA #######
        agol_item_id_staging = 'b0fd7e08b95d4ca7a29c2f65c97be2c5' ###### TEMPORARY DATA #######  

        #agol_item_id_working = '8e871ec48e1f40d18ffeed647e1ec1e2' ###### TREAL DATA #######
        #agol_item_id_staging = 'f6dd8b7196ca45188f4b19201a2dabc3' ###### TREAL DATA ####### 

        agol_sync_manager = AGOSyncManager(
            gis=gis,
            working_fc=working_fc,
            staging_fc = staging_fc,
            unique_id_field=unique_id_field,
            today_date=today_date,
            agol_item_id_working=agol_item_id_working,
            agol_item_id_staging=agol_item_id_staging
        )

        print('\nRetrieving Working AGOL layer records..')
        agol = agol_sync_manager.get_agol_data()
        print(f'..AGOL layer contains {len(agol_sync_manager.agol_df)} rows')

        print('\nRetrieving the Master dataset records..')
        loc= agol_sync_manager.get_local_data()
        print(f'..Master dataset contains {len(agol_sync_manager.local_df)} rows')


        print('\nAppending new Areas to Working AGOL layer..')
        agol_sync_manager.append_new_areas_to_agol()

        print('\nDeleting Records marked for retirement in the Working AGOL..')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.delete_retired_working_records()

        print('\nUpdating attributes modified in Working AGOL layer..')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.modify_edited_agol_attributes()
        
        print('\nFlagging ready-to-publish records with missing attributes…')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.flag_missing_attributes()

        print('\nMoving ready-to-publish records to Staging GDB…')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.move_published_to_staging()

        print('\nDelete removed records from Working AGOL layer…')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.delete_removed_local_records_from_agol()  


        print('\nUpdating the Staging AGO layer...')
        agol_sync_manager.overwrite_feature_layer(
            fc= staging_fc,
            agol_item_id=agol_sync_manager.agol_item_id_staging, 
            where_clause="1=1"
        )      

        
        print('\nExporting the change log…')
        today_date_f = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(wks, 'FNRI_dataSync_log' ,f"change_log_{today_date_f}.txt")
        agol_sync_manager.export_change_log(log_file)
  

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        ago.disconnect()


    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')