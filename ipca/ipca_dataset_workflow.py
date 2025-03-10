"""
This script synchronizes records between the local IPCA master dataset (gdb/featureclass) 
and the IPCA Feature Layer on AGOL.

The script executes the following workflow:
    1) Create a backup copy of the master IPCA dataset in the Archive gdb
    2) Check for new records in the master dataset and and appends them to the AGOL Feature Layer
    3) Check for deleted records in the master dataset and remove them from the AGOL Feature Layer
    4) Check for new records in the AGOL Feature Layer and appends them to the master dataset
    5) Check for deleted records in the AGOL Feature Layer and remove from the master dataset
    6) Check for updated records (attributes only) in the AGOL Feature Layer and modify them in the master dataset
    7) Detect changes in the Master dataset- For change tracking only
    8) Overwrites the AGOL Feature Layer with records from the master dataset.
    9) Updates the custom IPCA dataset and AGOL Feature Layer ('Unrestricted' and 'Internal only' records)
    10) Generate a change log file (change_log.txt)


Created on: 2025-03-03
By: Moez Labiadh
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
        self, gis, master_fc, unique_id_field, last_modified_field, 
        today_date, agol_item_id_main, agol_item_id_cust
    ):
        """
        Initialize the AGOSyncManager instance.
        """
        self.gis = gis
        self.master_fc = master_fc
        self.unique_id_field = unique_id_field
        self.last_modified_field = last_modified_field
        self.today_date = today_date
        self.agol_item_id_main = agol_item_id_main
        self.agol_item_id_cust = agol_item_id_cust
        self.agol_df = None
        self.local_df = None
        # Dictionary to hold lists of FEATURE_IDs for each category:
        self.change_log = {
            "delete_removed_local_records_from_agol": [],
            "append_new_local_records_to_agol": [],
            "append_new_agol_records": [],
            "delete_removed_agol_records": [],
            "append_edited_agol_records": [],
            "detect_modified_local_records": []
        }


    def get_agol_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of an AGOL feature layer.
        """
        item = self.gis.content.get(self.agol_item_id_main)
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
            if 'date' in col.lower() and col != 'UPDATE_LOG':
                self.agol_df[col] = pd.to_datetime(
                    self.agol_df[col], unit='ms', utc=True
                ).dt.tz_convert('America/Vancouver').dt.tz_localize(None)

        return self.agol_df


    def get_local_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of a gdb/featureclass.
        """
        self.local_df = pd.DataFrame.spatial.from_featureclass(self.master_fc)
        for col in self.local_df.columns:
            if 'date' in col.lower() and col != 'UPDATE_LOG':
                self.local_df[col] = pd.to_datetime(self.local_df[col], unit='ms')
                self.local_df[col] = self.local_df[col].dt.floor('S')
        return self.local_df

    
    def delete_removed_local_records_from_agol(self) -> None:
        """
        Deletes records from the AGO feature layer that are no longer present 
        in the master dataset.
        (Logs: Records removed in AGOL)
        """
        agol_deleted = self.agol_df[
            ~self.agol_df[self.unique_id_field].isin(self.local_df[self.unique_id_field])
        ]
        if not agol_deleted.empty:
            unique_ids = agol_deleted[self.unique_id_field].tolist()
            print(f"..found {len(unique_ids)} record(s) from AGO that no longer exist in the master dataset.")
            self.change_log["delete_removed_local_records_from_agol"].extend(unique_ids)
            print(f"..logged deleted records: {unique_ids}")

            item = self.gis.content.get(self.agol_item_id_main)
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


    def append_new_local_records_to_agol(self) -> None:
        """
        Detects new records from the master dataset that are not present in the AGOL dataset and appends them.
        (Logs: New records added in the master dataset)
        """

        # Identify new records in the master dataset that are not in the AGOL dataset,

        local_new = self.local_df[~self.local_df[self.unique_id_field].isin(self.agol_df[self.unique_id_field])]

        if not local_new.empty:
            print(f"..found {len(local_new)} new records in local dataset to append to AGOL.")

            # Retrieve the target AGOL feature layer.
            item = self.gis.content.get(self.agol_item_id_main)
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
                self.change_log["append_new_local_records_to_agol"].extend(new_ids)
                print(f"..logged new records: {new_ids}")
            else:
                print("..no records were added to AGOL. Check for errors.")
        else:
            print("..no new records to append to AGOL.")


    def append_new_agol_records(self) -> None:
        """
        Appends new records from AGOL to the master dataset.
        (Logs: New records added in AGOL)
        """
        agol_new = self.agol_df[
            ~self.agol_df[self.unique_id_field].isin(self.local_df[self.unique_id_field])
        ]
        agol_new = agol_new[agol_new["geometry"].notnull()]
        field_mappings = {
            col: col for col in self.local_df.columns 
                if col in agol_new.columns and col.lower() != 'shape'
        }

        if len(agol_new) > 0:
            print(f"..appending {len(agol_new)} row(s) to the Master dataset/featureclass.")
            added_records = []  
            with arcpy.da.InsertCursor(self.master_fc, ['SHAPE@'] + list(field_mappings.values())) as cursor:
                for _, row in agol_new.iterrows():
                    esri_geometry = arcpy.AsShape(row["geometry"], True) if row["geometry"] else None
                    attributes = []
                    for col in field_mappings.keys():
                        value = row[col]
                        if col == self.last_modified_field:
                            if pd.notnull(row[self.last_modified_field]):
                                value = row[self.last_modified_field]
                            elif pd.notnull(row['EditDate']):
                                value = row['EditDate']
                            else:
                                value = self.today_date
                        if col == 'AGO_PUBLISH_DATE' and pd.isnull(value):
                            value = self.today_date
                        if col == 'AGO_PUBLISH_YN':
                            value = 'Yes'
                        attributes.append(value)
                    cursor.insertRow([esri_geometry] + attributes)
                    added_records.append(row[self.unique_id_field])
            print(f"..successfully added {len(added_records)} row(s) to the Master dataset/featureclass.")
            self.change_log["append_new_agol_records"].extend(added_records)
        else:
            print("..no new rows from AGOL to append (append_new_agol_records).")


    def delete_removed_agol_records(self) -> None:
        """
        Deletes records from both the AGOL feature layer and the master dataset
        whose UPDATE_LOG field is set to 'Delete Shape'.
        (Logs: Records deleted from AGOL and Master dataset)
        """
        # Identify records in AGOL marked for deletion
        agol_to_delete = self.agol_df[self.agol_df['UPDATE_LOG'] == 'Delete Shape']
        
        if agol_to_delete.empty:
            print("..no records with UPDATE_LOG='Delete Shape' found in AGOL.")
            return
        
        unique_ids = agol_to_delete[self.unique_id_field].tolist()
        print(f"..deleting {len(unique_ids)} record(s) marked with 'Delete Shape' from both AGOL and the Master dataset.")
        
        # Delete from AGOL Feature Layer
        item = self.gis.content.get(self.agol_item_id_main)
        layer = item.layers[0]
        
        def format_id(val):
            return f"'{val}'" if isinstance(val, str) else str(val)
        
        ids_str = ", ".join(format_id(val) for val in unique_ids)
        where_clause = f"{self.unique_id_field} in ({ids_str})"
        result = layer.delete_features(where=where_clause)
        
        if result.get("deleteResults"):
            if all(r.get("success") for r in result["deleteResults"]):
                print("..successfully deleted records from AGOL.")
            else:
                print("..failed to delete some records from AGOL.")
        else:
            print("..failed to delete records from AGOL.")
        
        # Delete from the Master dataset feature class
        deleted_ids = []
        with arcpy.da.UpdateCursor(self.master_fc, [self.unique_id_field]) as cursor:
            for row in cursor:
                if row[0] in unique_ids:
                    deleted_ids.append(row[0])
                    cursor.deleteRow()
                    print(f"...- DELETED FEATURE_ID: {row[0]}")
        print(f"..successfully deleted {len(deleted_ids)} record(s) from the Master dataset/featureclass.")
        
        self.change_log["delete_removed_agol_records"].extend(unique_ids)


    def append_edited_agol_records(self) -> None:
        """
        Appends edited records from AGOL to the master dataset.
        """
        editCols = [
            'PROJECT_ID', 'FIRST_NATION_GROUP', 'PROJECT_NAME', 'PRIVACY_LEVEL',
            'DATA_SOURCE', 'SPATIAL_ACCURACY', 'LAST_MODIFIED_DATE', 'UPDATE_LOG', 'EDITOR_NAME'
        ]

        if self.unique_id_field not in self.agol_df.columns or self.unique_id_field not in self.local_df.columns:
            raise KeyError(f"..the unique_id_field '{self.unique_id_field}' is missing in one of the datasets.")

        local_dict = (
            self.local_df.drop(columns=['SHAPE'], errors='ignore')
            .set_index(self.unique_id_field)
            .to_dict(orient='index')
        )

        edited_rows = []
        for _, row in self.agol_df.iterrows():
            row_id = row[self.unique_id_field]
            if row_id in local_dict:
                for col in editCols:
                    if col in row and col in local_dict[row_id]:
                        agol_value = row[col]
                        local_value = local_dict[row_id][col]
                        if pd.isnull(agol_value) and pd.isnull(local_value):
                            continue
                        if agol_value != local_value:
                            # Retrieve and convert AGOL modified date to a Timestamp
                            agol_modified_date = row.get(self.last_modified_field)
                            if pd.isnull(agol_modified_date):
                                agol_modified_date = row.get('EditDate')
                            agol_modified_date = pd.Timestamp(agol_modified_date)
                            
                            # Retrieve and convert local modified date to a Timestamp if available
                            local_modified_date = local_dict[row_id].get(self.last_modified_field)
                            if pd.notnull(local_modified_date):
                                local_modified_date = pd.Timestamp(local_modified_date)
                            
                            # Compare the two Timestamps
                            if pd.isnull(local_modified_date) or (pd.notnull(agol_modified_date) and agol_modified_date >= local_modified_date):
                                edited_rows.append(row_id)
                                break

        if edited_rows:
            print(f"..found {len(edited_rows)} edited record(s) in AGOL.")
            # Use a set to keep FEATURE_IDs unique
            modified_ids = set(edited_rows)
            with arcpy.da.UpdateCursor(self.master_fc, [self.unique_id_field] + editCols + ['AGO_PUBLISH_YN', 'AGO_PUBLISH_DATE']) as cursor:
                for row in cursor:
                    feature_id = row[0]
                    if feature_id in modified_ids:
                        updated = False
                        for i, col in enumerate(editCols, start=1):
                            agol_value = self.agol_df.loc[self.agol_df[self.unique_id_field] == feature_id, col].values[0]
                            local_value = row[i]
                            if col == self.last_modified_field:
                                if pd.notnull(agol_value):
                                    # Convert to date
                                    agol_value = pd.Timestamp(agol_value)
                                elif pd.notnull(self.agol_df.loc[self.agol_df[self.unique_id_field] == feature_id, 'EditDate'].values[0]):
                                    agol_value = pd.Timestamp(self.agol_df.loc[self.agol_df[self.unique_id_field] == feature_id, 'EditDate'].values[0])
                                else:
                                    agol_value = self.today_date
                                if local_value != agol_value:
                                    row[i] = agol_value
                                    updated = True
                            elif agol_value != local_value:
                                row[i] = agol_value
                        if 'AGO_PUBLISH_DATE' in [f.name for f in arcpy.ListFields(self.master_fc)]:
                            row[-1] = self.today_date
                            updated = True
                        if 'AGO_PUBLISH_YN' in [f.name for f in arcpy.ListFields(self.master_fc)]:
                            row[-2] = 'Yes'
                            updated = True
                        if updated:
                            cursor.updateRow(row)
                            print(f"Updated FEATURE_ID {feature_id}")
            self.change_log["append_edited_agol_records"].extend(list(modified_ids))
        else:
            print("..no modifications detected in AGOL (append_edited_agol_records).")


    def detect_modified_local_records(self) -> None:
        """
        Detects records in the local dataset that have been modified compared to AGOL.
        (Logs: Records modified in the master dataset)
        """
        editCols = [
            'PROJECT_ID', 'FIRST_NATION_GROUP', 'PROJECT_NAME', 'PRIVACY_LEVEL',
            'DATA_SOURCE', 'SPATIAL_ACCURACY', 'LAST_MODIFIED_DATE', 'UPDATE_LOG', 'EDITOR_NAME'
        ]
        if self.unique_id_field not in self.agol_df.columns or self.unique_id_field not in self.local_df.columns:
            raise KeyError(f"..the unique_id_field '{self.unique_id_field}' is missing in one of the datasets.")

        agol_dict = self.agol_df.set_index(self.unique_id_field).to_dict(orient='index')
        local_dict = self.local_df.set_index(self.unique_id_field).to_dict(orient='index')
        modified_ids = []
        for feature_id, local_row in local_dict.items():
            if feature_id in agol_dict:
                agol_row = agol_dict[feature_id]
                for col in editCols:
                    if col in local_row and col in agol_row:
                        agol_value = agol_row[col]
                        local_value = local_row[col]
                        if pd.isnull(agol_value) and pd.isnull(local_value):
                            continue
                        if agol_value != local_value:
                            agol_modified_date = agol_row.get(self.last_modified_field) or agol_row.get('EditDate')
                            local_modified_date = local_row.get(self.last_modified_field)
                            if pd.notnull(local_modified_date) and (pd.isnull(agol_modified_date) or local_modified_date > agol_modified_date):
                                modified_ids.append(feature_id)
                                break
        if modified_ids:
            print(f"..detected {len(modified_ids)} modified record(s) in the local dataset.")
            self.change_log["detect_modified_local_records"].extend(modified_ids)
        else:
            print("..no modified local records detected (detect_modified_local_records).")

    def update_agol_fields(self) -> None:
        """
        Updates AGO_PUBLISH_YN and AGO_PUBLISH_DATE fields in the source feature class.
        (No logging is performed in this function.)
        """
        with arcpy.da.UpdateCursor(self.master_fc, ["AGO_PUBLISH_YN", "AGO_PUBLISH_DATE"]) as cursor:
            for row in cursor:
                if row[0] == "No" or row[0] is None:  
                    row[0] = "Yes" 
                    row[1] = self.today_date  
                    cursor.updateRow(row)
                    print(f"update_agol_fields: Updated AGO_PUBLISH_YN to 'Yes' and AGO_PUBLISH_DATE to {self.today_date}")


    def overwrite_feature_layer(self, agol_item_id: str, where_clause: str) -> None:
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
        row_count = int(arcpy.GetCount_management(self.master_fc)[0])
        print(f"...Source feature class contains {row_count} rows")
        print("..truncating the feature layer...")
        layer.manager.truncate()
        print("..feature layer truncated successfully.")
        print("..adding new features to the layer...")

        fields = [
            field.name for field in arcpy.ListFields(self.master_fc) 
                if field.name in self.agol_df.columns and field.type not in ["OID", "Geometry"]
        ]
        features = []
        chunk_size = 5
        with arcpy.da.SearchCursor(self.master_fc, ["SHAPE@"] + fields, where_clause) as cursor:
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
                    print(f"...successfully added {len(chunk)} features in batch {i//chunk_size + 1}")
                else:
                    raise ValueError(f"...failed to add features in batch {i//chunk_size + 1}.")
            except Exception as e:
                print(f"...error in batch {i//chunk_size + 1}: {e}")
            time.sleep(2)

    
    def export_change_log(self, log_file_path: str) -> None:
        """
        Exports the collected change log information to a text file.
        """
        header_mapping = {
            "delete_removed_local_records_from_agol": "Records removed in AGOL:",
            "append_new_local_records_to_agol": "New records added in the master dataset:",
            "append_new_agol_records": "New records added in AGOL:",
            "delete_removed_agol_records": "Records deleted from the master dataset:",
            "append_edited_agol_records": "Records modified in AGOL:",
            "detect_modified_local_records": "Records modified in the master dataset:"
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
                        f.write(f"   FEATURE_IDs: {', '.join(map(str, unique_ids))}\n\n")
                if not any_changes:
                    f.write("No changes detected.\n")
            print(f"Change log successfully exported to: {log_file_path}")
        except Exception as e:
            print(f"Failed to export change log: {e}")


def copy_master_dataset(source_fc, target_gdb, target_fc_name, where_clause="") -> None:
    """
    Makes a copy of the main IPCA featureclass based on a where_clause.
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

    wks = r"\\spatialfiles.bcgov\work\ilmb\dss\projects\Mwlrs\Land Use Planning\Master_Data"
    main_gdb = os.path.join(wks, 'IPCA.gdb')
    archive_gdb = os.path.join(wks, 'archived data', 'IPCA_Archive.gdb')
    master_fc = os.path.join(main_gdb, 'IPCA')

    print('Backing up the master IPCA dataset...')
    today_date_f = datetime.now().strftime("%Y%m%d")
    backup_fc_name = f"IPCA_{today_date_f}"
    copy_master_dataset(
        master_fc, 
        archive_gdb, 
        backup_fc_name
    )

    try:
        print('\nLogging to AGO...')
        AGO_HOST = 'https://governmentofbc.maps.arcgis.com'
        AGO_USERNAME_DSS = 'PX.GeoBC.DSS.Creator' 
        AGO_PASSWORD_DSS = 'maps_are_gr8!' 
        ago = AGOConnector(AGO_HOST, AGO_USERNAME_DSS, AGO_PASSWORD_DSS)
        gis = ago.connect()

        unique_id_field = 'FEATURE_ID'
        last_modified_field = 'LAST_MODIFIED_DATE'
        today_date = datetime.now()

        agol_item_id_main = '4127c4fc28774bfa87dccbcd7bfb145b'  
        agol_item_id_cust = '4e417b4b55f74d8e87cc106b1fe5328d'  

        agol_sync_manager = AGOSyncManager(
            gis=gis,
            master_fc=master_fc,
            unique_id_field=unique_id_field,
            last_modified_field=last_modified_field,
            today_date=today_date,
            agol_item_id_main=agol_item_id_main,
            agol_item_id_cust=agol_item_id_cust
        )

        print('\nRetrieving AGO layer records..')
        agol_sync_manager.get_agol_data()
        print(f'..AGOL layer contains {len(agol_sync_manager.agol_df)} rows')

        print('\nRetrieving the Master dataset records..')
        agol_sync_manager.get_local_data()
        print(f'..Master dataset contains {len(agol_sync_manager.local_df)} rows')


        print('\nRemoving deleted Master dataset records from AGOL..')
        agol_sync_manager.delete_removed_local_records_from_agol()

        print('\nRemoving deleted AGOL records from the Master dataset..')
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.delete_removed_agol_records()

        print('\nAppending new Master dataset records to the AGOL..')
        #agol_sync_manager.get_local_data() # re-read local records
        #agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.append_new_local_records_to_agol()

        print('\nDetecting modifications in the Master dataset..- For change tracking only')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.detect_modified_local_records()

        print('\nAppending edited AGOL records to the Master dataset..')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.append_edited_agol_records()


        print('\nAppending new AGOL records to the Master dataset..')
        agol_sync_manager.get_local_data() # re-read local records
        agol_sync_manager.get_agol_data()  # re-read agol records
        agol_sync_manager.append_new_agol_records()

        print('\nUpdating the AGO main layer...')
        agol_sync_manager.update_agol_fields()
        agol_sync_manager.overwrite_feature_layer(agol_item_id=agol_sync_manager.agol_item_id_main, 
                                                  where_clause="1=1")

        print('\nUpdating the AGO custom layer...')
        custom_clause = "PRIVACY_LEVEL IN ('Unrestricted', 'Internal only')"
        agol_sync_manager.overwrite_feature_layer(agol_item_id=agol_sync_manager.agol_item_id_cust, 
                                                  where_clause=custom_clause)
        
        print('\nUpdating the IPCA custom featureclass...')
        copy_master_dataset(
            master_fc, 
            main_gdb, 
            target_fc_name='IPCA_custom',
            where_clause=custom_clause
        )
        
        # Export the change log to a text file
        log_file = os.path.join(wks, 'IPCA_dataSync_log' ,f"change_log_{today_date_f}.txt")
        agol_sync_manager.export_change_log(log_file)
  
    except Exception as e:
        print(f"Error occurred: {e}") 
    finally:
        ago.disconnect()


    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')