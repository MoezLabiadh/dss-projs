"""
This script synchronizes records between the local FNRI working and 
staging datasets (gdb/featureclass) and the FNRI Feature Layers on AGOL.

The script exports a log file detailing the number of changes performed and the IDs affected.  

The script executes the following workflow:
    0) Create a backup copy of the Working and Staging Geodatabases

    1) Detect new Records added to the Working GDB and append them to the Working AGOL Feature Layer

    2) Delete Records marked as "To Be Retired" from the Working AGOL and GDB datasets

    3) Detect modified attributes in Working AGOL layer and edit them in the Working GDB

    4) Flag records that are marked Review Complete (FNLT and MIRR) 
       but have null values in any of the required attribute columns.

    5) Move records marked 'Ready To Publish' from the Working GDB into the Staging GDB
       When a record is moved and its unique ID matches an existing one in staging, the existing record will be overwritten.
       Flag moved records overlapping with existing records in the Staging GDB 
       DATE_CREATED is populated with today's date for moved records
       Set the Publish_Status of records moved to staging GDB to "Published"

    6) Overwrite the Staging AGOL feature layer with data from the Staging GDB .
       Staging records that fail to publish to AGOL are flagged in the Log file (usually due to topology issues with the geometry) 
    
    7) Overwrites the BCGW staging GDB with the content of the local staging GDB


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
                    print(f"..successfully deleted {len(unique_ids)} record(s) from Working AGOL: {unique_ids}")
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
                'Parcel_Name', 'Review_Status_FNLT', 'Review_Status_MIRR', 'Publish_Status'
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
        Flags records set to 'Ready to Publish' that either
        - have incomplete review status, or
        - are missing any required attribute.
        (Logs: [QC/QA!] Working Data Records set to Ready-to-Publish with missing mandatory Attributes)
        """
        required = [
            'FIRST_NATION', 'AGREEMENT_TYPE', 'AGREEMENT_STAGE',
            'AGREEMENT_LINK'
        ]
        df = self.local_df

        # only look at records ready to publish
        ready = df['Publish_Status'] == 'Ready to Publish'

        # review-status violation: either FNLT or MIRR is not complete
        bad_review = (
            (df['Review_Status_FNLT'] != 'Review Complete') |
            (df['Review_Status_MIRR']   != 'Review Complete')
        )

        # missing-attribute violation: any required column is null
        missing_attr = df[required].isnull().any(axis=1)

        # combine both checks
        mask = ready & (bad_review | missing_attr)

        # collect IDs
        flagged_ids = df.loc[mask, self.unique_id_field].tolist()

        if flagged_ids:
            print(f"..flagged {len(flagged_ids)} ready-to-publish records with issues: {flagged_ids}")
            self.change_log.setdefault('flag_missing_attributes', []).extend(flagged_ids)
        else:
            print("..no ready-to-publish records with missing attributes or incomplete review.")


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
            df['Publish_Status'] == 'Ready to Publish'
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
        
        # 3) Delete moved records from Working GDB
        delete_clause = f"{self.unique_id_field} IN ({','.join(repr(i) for i in ids_to_move)})"
        with arcpy.da.UpdateCursor(self.working_fc, [self.unique_id_field], delete_clause) as cursor:
            for _ in cursor:
                cursor.deleteRow()

        # 4) Delete moved records from Working AGO
        item = self.gis.content.get(self.agol_item_id_working)
        layer = item.layers[0]

        def format_id(val):
            return f"'{val}'" if isinstance(val, str) else str(val)
        
        ids_str = ", ".join(format_id(val) for val in ids_to_move)
        where_clause = f"{self.unique_id_field} in ({ids_str})"
        result = layer.delete_features(where=where_clause)
        if result.get("deleteResults"):
            if not all(r.get("success") for r in result["deleteResults"]):
                print("..failed to delete some records from AGO.")
        else:
            print("..failed to delete records from AGO.")


        # Log moved IDs
        moved_ids = sorted(ids_to_move)
        self.change_log["move_published_to_staging"].extend(moved_ids)
        print(f"..moved {len(moved_ids)} records to staging: {moved_ids}")

        # 5) Spatial overlap check
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




    def overwrite_feature_layer(self, fc, agol_item_id: str, where_clause: str) -> None:
        """
        Overwrites a feature layer on AGOL by truncating it, then uploading
        each record individually. Any Poly_Unique_ID that fail to load
        are retried with a 5 m geometry simplification; final failures  
        are logged in flag_failed_to_ago_publish_staging".
        """
        print(f"..retrieving target layer with ID: {agol_item_id}")
        item = self.gis.content.get(agol_item_id)
        if not item:
            raise ValueError(f"Feature layer with ID {agol_item_id} not found.")

        layer = item.layers[0]
        print(f"..found feature layer: {item.title}")

        print("..truncating the feature layer...")
        layer.manager.truncate()
        print("..feature layer truncated successfully.")

        # pick only those fields present in both AGOL and the FC, excluding OID/Geometry
        fields = [
            f.name for f in arcpy.ListFields(fc)
            if f.name in self.agol_df.columns and f.type not in ("OID", "Geometry")
        ]

        search_fields = ["SHAPE@"] + fields
        failed_ids = []

        def try_add(feat):
            resp = layer.edit_features(adds=[feat])
            results = resp.get("addResults", [])
            if results and results[0].get("success"):
                return
            err = results[0].get("error", "unknown") if results else "no result"
            raise RuntimeError(err)

        print("..uploading records one by one...")
        with arcpy.da.SearchCursor(fc, search_fields, where_clause) as cursor:
            for row in cursor:
                geom = row[0]
                if geom is None:
                    print("..skipping record with no geometry")
                    continue

                attrs = {fields[i]: row[i+1] for i in range(len(fields))}
                uid = attrs[self.unique_id_field]

                feature = {
                    "geometry": json.loads(geom.JSON),
                    "attributes": attrs
                }

                # first attempt
                try:
                    try_add(feature)
                    print(f"...added {uid}")
                except Exception as e1:
                    print(f"...FAILED {uid}: {e1}")
                    # retry with simplified geometry
                    print(f"...simplifying {uid} by 5 m and retrying")
                    simple = geom.generalize(5)
                    feature["geometry"] = json.loads(simple.JSON)
                    try:
                        try_add(feature)
                        print(f"...added simplified {uid}")
                    except Exception as e2:
                        print(f"...STILL FAILED {uid}: {e2}")
                        failed_ids.append(uid)

                time.sleep(1)

        # report
        if failed_ids:
            print(f"\n..upload complete with {len(failed_ids)} final failures: {failed_ids}")
            self.change_log.setdefault("flag_failed_to_ago_publish_staging", []).extend(failed_ids)
        else:
            print("\n..all records uploaded successfully.")



    def export_change_log(self, log_file_path: str) -> None:
        """
        Exports the collected change log information to a text file.
        """
        header_mapping = {
            "append_new_areas_to_agol"          : "New Records/Areas added to the Working Data:",
            "retired_records_deleted"           : "To-be-Retired records deleted from the Working Data:",
            "modify_edited_agol_attributes"     : "Records with Attributes modified in the Working Data:",
            "flag_missing_attributes"           : "[QC/QA!] Working Data Records set to Ready-to-Publish with Attribute issues:",
            "move_published_to_staging"         : "Working Data Records set to Ready-to-Publish moved to Published Data:",
            "flag_spatial_overlaps_staging"     : "[QC/QA!] New Published records overlapping existing records in Published Data:",
            "flag_failed_to_ago_publish_staging": "[QC/QA!] Staging Data failed to publish to Staging AGO:"
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
        f"{today_date_str}_backup_{gdb_name}.zip"
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


def overwrite_bcgw_staging(staging_fc, bcgw_staging_fc):
    """
    Overwrites the BCGW staging GDB with the contents of the local staging GDB
    1) Truncates (clears) the target feature class
    2) Appends all features from staging_fc    
    """
    # 1) remove all existing features from the BCGW staging FC
    arcpy.management.TruncateTable(bcgw_staging_fc)
    print("....target truncated") 

    # 2) append everything in staging_fc into bcgw_staging_fc
    arcpy.management.Append(
        inputs=[staging_fc],
        target=bcgw_staging_fc,
        schema_type="NO_TEST"
    )
    print("....features appended")  



if __name__ == "__main__":
    start_t = timeit.default_timer()

    today_date = datetime.now()
    today_date_str = today_date.strftime("%Y%m%d")


    wks = r'N:\projects\firstnat\First_Nations_Area_of_Interest_for_BCGW\data'

    working_gdb = os.path.join(wks, 'working_fnt_Areas_of_Interest.gdb') 
    staging_gdb = os.path.join(wks, 'fnt_Areas_of_Interest.gdb')         

    working_fc = os.path.join(working_gdb, 'working_fnt_Areas_of_Interest')
    staging_fc = os.path.join(staging_gdb, 'fnt_Areas_of_Interest')        

    backup_wks = os.path.join(wks, '_archive', 'gdb_backups')
    logs_wks = os.path.join(wks, '_archive', 'script_logs')   
   
    print('Backing up the working and staging GDBs...')
    for gdb in (working_gdb, staging_gdb):
        archive_geodatabase(gdb, backup_wks, today_date_str)

    try:
        print('\nLogging to AGO...')
        AGO_HOST = 'https://governmentofbc.maps.arcgis.com'
        AGO_USERNAME_DSS = 'XXXX' 
        AGO_PASSWORD_DSS = 'XXXX' 
        ago = AGOConnector(AGO_HOST, AGO_USERNAME_DSS, AGO_PASSWORD_DSS)
        gis = ago.connect()

        unique_id_field = 'Poly_Unique_ID' 

        agol_item_id_working = '8e871ec48e1f40d18ffeed647e1ec1e2' 
        agol_item_id_staging = 'f6dd8b7196ca45188f4b19201a2dabc3' 

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


        print('\nUpdating the Staging AGO layer...')
        agol_sync_manager.overwrite_feature_layer(
            fc= staging_fc,
            agol_item_id=agol_sync_manager.agol_item_id_staging, 
            where_clause="1=1"
        )      


    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        ago.disconnect()

        print('\nExporting the change log…')
        today_date_f = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(logs_wks, f"change_log_{today_date_f}.txt")
        agol_sync_manager.export_change_log(log_file)

        print("\nSyncing local staging to BCGW…")
        bcgw_staging_gdb = r'Z:\administrative_boundaries\fnt_Areas_of_Interest.gdb'
        bcgw_staging_fc = os.path.join(bcgw_staging_gdb, 'fnt_Areas_of_Interest')
        overwrite_bcgw_staging(staging_fc, bcgw_staging_fc)



    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')