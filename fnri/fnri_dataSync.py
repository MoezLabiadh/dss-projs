"""
This script synchronizes records between the local FNRI working and 
staging datasets (gdb/featureclass) and the FNRI Feature Layer on AGOL.

The script executes the following workflow:
    0) Create a backup copy of the Working and Staging featureclasses
    1) Detect new Areas from the Working GDB and append them to the Working AGOL Feature Layer
    2) Detect modified attributes in Working AGOL layer and edit them in the Working GDB
    3) Flag records that are marked Complete (FNLT and MIRR) 
       but have null values in any of the required attribute columns.
    4) Move records marked 'Ready To Publish' from the Working GDB into the Staging GDB
       Flag moved records overlapping with existing records in staging 
    5) Remove deleted records (Ready To Publish and manually deleted from working) from the Working AGOL Feature Layer
    6) Update the Staging AGOL feature layer.


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
            "modify_edited_agol_attributes": [],
            "flag_missing_attributes": [],
            "move_published_to_staging": [],
            "flag_spatial_overlaps_staging": [],
            "delete_removed_records_from_agol": []
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
            'AGREEMENT_LINK', 'CONTACT_NAME', 'CONTACT_EMAIL', 'DATE_CREATED'
        ]
        df = self.local_df
        
        # condition: both review complete AND any required column is null
        mask = (
            (df['Review_Status_FNLT'] == 'Complete') &
            (df['Review_Status_MIRR'] == 'Complete')
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
            (df['Review_Status_FNLT'] == 'Complete') &
            (df['Review_Status_MIRR'] == 'Complete') &
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
        attr_fields = [c for c in to_move.columns if c != 'SHAPE']
        fields      = attr_fields + ['SHAPE@JSON']
        with arcpy.da.InsertCursor(self.staging_fc, fields) as inserter:
            for _, row in to_move.iterrows():
                attrs = [row[f] if pd.notna(row[f]) else None for f in attr_fields]
                geom  = json.dumps(row['SHAPE'])
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


    def export_change_log(self, log_file_path: str) -> None:
        """
        Exports the collected change log information to a text file.
        """
        header_mapping = {
            "append_new_areas_to_agol"        : "New Areas added to AGOL:",
            "modify_edited_agol_attributes"   : "Records modified in AGOL:",
            "flag_missing_attributes"         : "[QC/QA!] -  Ready-to-Publish Records with missing Attributes:",
            "move_published_to_staging"       : "Ready-to-Publish Records moved to staging dataset:",
            "flag_spatial_overlaps_staging"   : "[QC/QA!] -Records overlapping in staging",
            "delete_removed_records_from_agol": "Records removed in AGOL"

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


    def overwrite_feature_layer(self, fc, agol_item_id: str, where_clause: str) -> None:
        """
        Overwrites a feature layer on AGO.
        (No logging is performed in this function.)
        """
        print(f"..retrieving target layer with ID: {agol_item_id}")
        item = self.gis.content.get(agol_item_id)
        if not item:
            raise ValueError(f"Feature layer with ID {agol_item_id} not found.")
        
        print(f"..found feature layer: {item.title}")
        layer = item.layers[0]  
        row_count = int(arcpy.GetCount_management(fc)[0])
        print(f"..source feature class contains {row_count} rows")
        print("..truncating the feature layer...")
        layer.manager.truncate()
        print("..feature layer truncated successfully.")
        print("..adding new features to the layer...")

        fields = [
            field.name for field in arcpy.ListFields(fc) 
                if field.name in self.agol_df.columns and field.type not in ["OID", "Geometry"]
        ]
        features = []
        chunk_size = 5
        with arcpy.da.SearchCursor(fc, ["SHAPE@"] + fields, where_clause) as cursor:
            for row in cursor:
                geom = row[0]
                if geom is None:
                    continue
                geom_json = json.loads(geom.JSON)
                attributes = {fields[i]: row[i + 1] for i in range(len(fields))}
                features.append({"geometry": geom_json, "attributes": attributes})
        for i in range(0, len(features), chunk_size):
            chunk = features[i:i + chunk_size]
            try:
                result = layer.edit_features(adds=chunk)
                if "addResults" in result and all(res["success"] for res in result["addResults"]):
                    print(f"....successfully added {len(chunk)} features in batch {i//chunk_size + 1}")
                else:
                    raise ValueError(f"....failed to add features in batch {i//chunk_size + 1}.")
            except Exception as e:
                print(f"...error in batch {i//chunk_size + 1}: {e}")
            time.sleep(2)



def copy_featureclass(source_fc, target_gdb, target_fc_name, where_clause="") -> None:
    """
    Makes a copy of a featureclass based on a where_clause.
    """
    destination_fc = os.path.join(target_gdb, target_fc_name)
    if arcpy.Exists(destination_fc):
        arcpy.Delete_management(destination_fc)
    if where_clause:
        arcpy.Select_analysis(source_fc, destination_fc, where_clause)
    else:
        arcpy.CopyFeatures_management(source_fc, destination_fc)
    print("...dataset copied successfully!")



if __name__ == "__main__":
    start_t = timeit.default_timer()

    wks = r"Q:\dss_workarea\shbeatti\for_Chloe"
    main_gdb = os.path.join(wks, 'Sample_FNRI_working.gdb')
    archive_gdb = os.path.join(wks, 'Sample_FNRI_backup.gdb')
    working_fc = os.path.join(main_gdb, 'Sample_FNRI_working_testScript_v2')
    staging_fc = os.path.join(main_gdb, 'Sample_FNRI_staging_testScript_v2')

    print('Backing up the working and staging datasets...')
    today_date_f = datetime.now().strftime("%Y%m%d")

    backup_fc_name = f"FNRI_working_{today_date_f}"
    copy_featureclass(
        working_fc, 
        archive_gdb, 
        backup_fc_name
    )  

    backup_fc_name = f"FNRI_staging_{today_date_f}"
    copy_featureclass(
        staging_fc, 
        archive_gdb, 
        backup_fc_name
    )  

    try:
        print('\nLogging to AGO...')
        AGO_HOST = 'https://governmentofbc.maps.arcgis.com'
        AGO_USERNAME_DSS = 'XXX' 
        AGO_PASSWORD_DSS = 'XXX' 
        ago = AGOConnector(AGO_HOST, AGO_USERNAME_DSS, AGO_PASSWORD_DSS)
        gis = ago.connect()

        unique_id_field = 'Poly_Unique_ID' 

        today_date = datetime.now()

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