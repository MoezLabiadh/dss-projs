"""
This script synchronizes records between the local IPCA dataset (gdb/featureclass) 
and the IPCA Feature Layer on AGOL

Author: Moez Labiadh

"""

import warnings
warnings.simplefilter(action='ignore')

import os
import pandas as pd
import numpy as np

import arcpy
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

from datetime import datetime
import timeit
import time

class AGOConnector:
    def __init__(self, host, username, password):
        """
        Initialize the AGOManager instance 
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
        today_date, agol_item_id_review, agol_item_id_ovr
    ):
        """
        Initialize the AGOLDataManager instance 
        """
        self.gis = gis
        self.master_fc = master_fc
        self.unique_id_field = unique_id_field
        self.last_modified_field = last_modified_field
        self.today_date = today_date
        self.agol_item_id_review = agol_item_id_review
        self.agol_item_id_ovr = agol_item_id_ovr
        self.agol_df = None
        self.local_df = None


    def get_agol_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of an AGOL feature layer 
        """
        # Connect to the feature layer
        item = self.gis.content.get(self.agol_item_id_review)
        agol_layer = item.layers[0] 

        agol_features = agol_layer.query(
            where="1=1", 
            out_fields="*", 
            return_geometry=True
        ).features

        # Load records in df
        self.agol_df = pd.DataFrame([{
            **f.attributes,
            "geometry": f.geometry
        } for f in agol_features])

        # Format datetime columns correctly
        for col in self.agol_df.columns:
            if 'date' in col.lower() and col != 'UPDATE_LOG':
                self.agol_df[col] = pd.to_datetime(self.agol_df[col], unit='ms')

        return self.agol_df


    def get_local_data(self) -> pd.DataFrame:
        """
        Returns a dataframe containing the records of a gdb/featureclass 
        """
        self.local_df = pd.DataFrame.spatial.from_featureclass(self.master_fc)

        # Format datetime columns correctly
        for col in self.local_df.columns:
            if 'date' in col.lower() and col != 'UPDATE_LOG':
                self.local_df[col] = pd.to_datetime(self.local_df[col], unit='ms')
                self.local_df[col] = self.local_df[col].dt.floor('S')

        return self.local_df


    def append_new_agol_records(self) -> None:
        """
        Appends new records from AGOL to the master dataset
        """
        # New rows in AGOL
        agol_new = self.agol_df[~self.agol_df[self.unique_id_field].isin(self.local_df[self.unique_id_field])]
        agol_new = agol_new[agol_new["geometry"].notnull()]  # Remove null geometries

        # Define field mappings
        field_mappings = {
            col: col for col in self.local_df.columns 
                if col in agol_new.columns 
                    and col.lower() != 'shape'
        }

        if len(agol_new) > 0:
            print(f"..appending {len(agol_new)} rows to the Master dataset/featureclass.")
            added_records = []  # Track unique IDs of the new rows
            
            with arcpy.da.InsertCursor(self.master_fc, ['SHAPE@'] + list(field_mappings.values())) as cursor:
                for _, row in agol_new.iterrows():
                    # Convert geometry
                    esri_geometry = arcpy.AsShape(row["geometry"], True) if row["geometry"] else None

                    # Prepare attributes
                    attributes = []
                    for col in field_mappings.keys():
                        value = row[col]

                        # Handle LAST_MODIFIED_DATE logic
                        if col == self.last_modified_field:
                            if pd.notnull(row[self.last_modified_field]):
                                value = row[self.last_modified_field]
                            elif pd.notnull(row['EditDate']):
                                value = row['EditDate']
                            else:
                                value = self.today_date

                        # Handle AGO_PUBLISH_DATE
                        if col == 'AGO_PUBLISH_DATE':
                            if pd.isnull(value):  # Check for NaT/null/None
                                value = self.today_date

                        # Handle AGO_PUBLISH_YN
                        if col == 'AGO_PUBLISH_YN':
                            value = 'Yes'

                        attributes.append(value)

                    # Insert the row into the feature class
                    cursor.insertRow([esri_geometry] + attributes)

                    # Track the unique ID
                    added_records.append(row[self.unique_id_field])

            # Print summary of added rows
            print(f"..successfully added {len(added_records)} rows to the Master dataset/featureclass.")
            for record_id in added_records:
                print(f"...- FEATURE_ID: {record_id}")
        else:
            print("..no new rows from AGOL to append.")


    def append_edited_agol_records(self) -> None:
        """
        Appends edited records from AGOL to the master dataset
        """
        # Edited Attributes in AGO - Step 1: search for modifications
        editCols = [
            'PROJECT_ID', 'FIRST_NATION_GROUP', 'PROJECT_NAME', 'PRIVACY_LEVEL',
            'DATA_SOURCE', 'SPATIAL_ACCURACY', 'UPDATE_LOG', self.last_modified_field, 'EDITOR_NAME',
        ]

        # Ensure the unique_id_field exists in both dataframes
        if self.unique_id_field not in self.agol_df.columns or self.unique_id_field not in self.local_df.columns:
            raise KeyError(f"..the unique_id_field '{self.unique_id_field}' is missing in one of the datasets.")

        # Remove the SHAPE column and create a lookup dictionary
        local_dict = (
            self.local_df.drop(columns=['SHAPE'], errors='ignore')
            .set_index(self.unique_id_field)
            .to_dict(orient='index')
        )

        # Find rows with edits and track modifications
        edited_rows = []
        modifications = []  # To store details of changes

        for _, row in self.agol_df.iterrows():
            row_id = row[self.unique_id_field]
            if row_id in local_dict:
                # Compare each column in editCols
                for col in editCols:
                    if col in row and col in local_dict[row_id]:  # Ensure column exists in both
                        agol_value = row[col]
                        local_value = local_dict[row_id][col]

                        # Handle null/NaT comparison for date fields
                        if pd.isnull(agol_value) and pd.isnull(local_value):
                            continue  # Both are null, skip comparison

                        # Check if values differ
                        if agol_value != local_value:
                            # Check LAST_MODIFIED_DATE condition
                            agol_modified_date = row.get(self.last_modified_field)
                            local_modified_date = local_dict[row_id].get(self.last_modified_field)

                            # Consider edited if LAST_MODIFIED_DATE in agol_df is greater or either is null
                            if (pd.isnull(agol_modified_date) or pd.isnull(local_modified_date)) or (
                                not pd.isnull(agol_modified_date) and not pd.isnull(local_modified_date) and 
                                agol_modified_date > local_modified_date
                            ):
                                edited_rows.append(row_id)
                                modifications.append({
                                    "FEATURE_ID": row_id,
                                    "COLUMN": col,
                                    "AGOL_VALUE": agol_value,
                                    "LOCAL_VALUE": local_value
                                })
                                break  # Exit loop after detecting a modification for this row

        # Extract rows with edits from agol_df
        agol_edited = self.agol_df[self.agol_df[self.unique_id_field].isin(edited_rows)]

        # Print detected modifications, if any
        if len(modifications) > 0:
            print(f'..found {len(modifications)} edited rows')
            for mod in modifications:
                print(f"FEATURE_ID: {mod['FEATURE_ID']}, COLUMN: {mod['COLUMN']}, "
                    f"AGOL_VALUE: {mod['AGOL_VALUE']}, LOCAL_VALUE: {mod['LOCAL_VALUE']}")
        else:
            print('..no modifications detected')
        
        # Edited Attributes in AGOL - Step 2: Push modifications to the FeatureClass
        updateCols = [col for col in editCols if col in agol_edited.columns]

        # Ensure the local feature class has the unique ID field and the columns to update
        local_fields = [f.name for f in arcpy.ListFields(self.master_fc)]
        missing_fields = [col for col in updateCols if col not in local_fields]
        if missing_fields:
            raise KeyError(f"..the following fields are missing in the local feature class: {missing_fields}")

        # Create a lookup dictionary for easy access to updates from agol_edited
        agol_updates = agol_edited.set_index(self.unique_id_field).to_dict(orient="index")

        # Update local feature class using an UpdateCursor
        with arcpy.da.UpdateCursor(self.master_fc, [self.unique_id_field] + updateCols + ['AGO_PUBLISH_YN', 'AGO_PUBLISH_DATE']) as cursor:
            for row in cursor:
                feature_id = row[0]  # First field is the unique_id_field
                if feature_id in agol_updates:
                    updated = False
                    for i, col in enumerate(updateCols, start=1):  # Start at 1 since 0 is the unique_id_field
                        agol_value = agol_updates[feature_id][col]
                        local_value = row[i]

                        # Handle LAST_MODIFIED_DATE
                        if col == self.last_modified_field:
                            if pd.notnull(agol_updates[feature_id].get(self.last_modified_field)):
                                agol_value = agol_updates[feature_id][self.last_modified_field]
                            elif pd.notnull(agol_updates[feature_id].get('EditDate')):
                                agol_value = agol_updates[feature_id]['EditDate']
                            else:
                                agol_value = self.today_date  # Use today's date if both are null

                            if local_value != agol_value:
                                row[i] = agol_value
                                updated = True
                        elif agol_value != local_value:
                            row[i] = agol_value  # Update other fields
                            updated = True

                    # Always set AGO_PUBLISH_DATE to today's date
                    if 'AGO_PUBLISH_DATE' in local_fields:
                        row[-1] = self.today_date
                        updated = True

                    # Set AGO_PUBLISH_YN to 'Yes'
                    if 'AGO_PUBLISH_YN' in local_fields:
                        row[-2] = 'Yes'
                        updated = True

                    if updated:
                        cursor.updateRow(row)  # Commit changes to the row
                        print(f"Updated FEATURE_ID {feature_id}")

        print("..master dataset/featureclass updated successfully.")


    def append_new_local_records(self) -> None:
        """
        Appends new records from the master dataset to AGOL
        Currently Disabled. 
        """
        local_new = self.local_df[~self.local_df[self.unique_id_field].isin(self.agol_df[self.unique_id_field])]
        print(f"..found {len(local_new)} new records in local dataset.")
        new_features = local_new[self.unique_id_field].to_list()
        for feature_id in new_features:
            print(f"...- FEATURE_ID: {feature_id}")


    def overwrite_feature_layer(self) -> None:
        """
        Overwrites a feature layer on AGO.
        """
        print(f"..retrieving target layer with ID: {self.agol_item_id_ovr}")
        item = self.gis.content.get(self.agol_item_id_ovr)
        if not item:
            raise ValueError(f"Feature layer with ID {self.agol_item_id_ovr} not found.")
        
        print(f"..found feature layer: {item.title}")
        layer = item.layers[0]  

        row_count = int(arcpy.GetCount_management(self.master_fc)[0])
        print(f"...Source feature class contains {row_count} rows")

        # Update AGO_PUBLISH_YN and AGO_PUBLISH_DATE in the source feature class
        print("..updating source feature class fields...")
        with arcpy.da.UpdateCursor(self.master_fc, ["AGO_PUBLISH_YN", "AGO_PUBLISH_DATE"]) as cursor:
            for row in cursor:
                if row[0] == "No":  
                    row[0] = "Yes" 
                    row[1] = self.today_date  
                    cursor.updateRow(row)

        # Process to update AGOL
        print("..truncating the feature layer...")
        layer.manager.truncate()  # Clears all existing features in the layer
        print("..feature layer truncated successfully.")

        print("..adding new features to the layer...")

        fields = [
            field.name for field in arcpy.ListFields(self.master_fc) 
                if field.name in self.agol_df.columns 
                    and field.type not in ["OID", "Geometry"]
        ]

        features = []
        chunk_size = 5  # Set batch size to 5

        # Loop through rows in the feature class and convert them to the required format
        with arcpy.da.SearchCursor(self.master_fc, ["SHAPE@"] + fields) as cursor:
            for row in cursor:
                geom = row[0]  # SHAPE@ gives the geometry object
                if geom is None:
                    continue  # Skip rows with no geometry
                geom_json = geom.JSON  # Convert geometry to JSON format
                attributes = {fields[i]: row[i + 1] for i in range(len(fields))}
                features.append({"geometry": geom_json, "attributes": attributes})

        # Upload features in chunks of 5. to avoid large payload (Error 413: Max retries exceeded with url)
        for i in range(0, len(features), chunk_size):
            chunk = features[i:i + chunk_size]
            
            try:
                result = layer.edit_features(adds=chunk)

                if "addResults" in result and all(res["success"] for res in result["addResults"]):
                    print(f"...successfully added {len(chunk)} features in batch {i//chunk_size + 1}")
                else:
                    raise ValueError(f"...failed to add features in batch {i//chunk_size + 1}. Result: {result}")

            except Exception as e:
                print(f"...error in batch {i//chunk_size + 1}: {e}")

            time.sleep(2)  # 2 seconds Pause to avoid hitting API rate limits



def backup_master_dataset(main_gdb, archive_gdb, source_fc_name) -> None:
    """
    Makes a backup copy of the main IPCA dataset.
    """
    source_fc = os.path.join(main_gdb, source_fc_name)
    today_date = datetime.now().strftime("%Y%m%d")
    backup_fc_name = f"IPCA_{today_date}"
    destination_fc = os.path.join(archive_gdb, backup_fc_name)

    arcpy.CopyFeatures_management(source_fc, destination_fc)
    print("..backup successful!")




if __name__ == "__main__":
    start_t = timeit.default_timer()

    wks = r"Q:\dss_workarea\mlabiadh\workspace\20241126_IPCA\work\Master_Data"
    main_gdb = os.path.join(wks, 'IPCA.gdb')
    archive_gdb = os.path.join(wks, 'archived data', 'IPCA_Archive.gdb')

    master_fc = os.path.join(main_gdb, 'IPCA_working')

    # Uncomment if backup is needed
    '''
    print('Backing up the master IPCA dataset...')
    backup_master_dataset(
        main_gdb, 
        archive_gdb, 
        master_fc
    )
    '''
    
    try:
        print('\nLogging to AGO...')
        AGO_HOST = 'https://governmentofbc.maps.arcgis.com'
        AGO_USERNAME_DSS = 'XXX'  # Replace with actual username
        AGO_PASSWORD_DSS = 'XXX' # Replace with actual password
        ago = AGOConnector(AGO_HOST, AGO_USERNAME_DSS, AGO_PASSWORD_DSS)
        gis = ago.connect()

        # Initialize AGOLDataManager with required parameters
        unique_id_field = 'FEATURE_ID'
        last_modified_field = 'LAST_MODIFIED_DATE'
        today_date = datetime.now()
        agol_item_id_review = '66190589c7fa484ab4d0ca8058702aaf'  # Replace with actual AGOL item ID
        agol_item_id_ovr = '911c500c9f2c4076b307c34275d9bed0'  # Replace with actual AGOL item ID

        agol_sync_manager = AGOSyncManager(
            gis=gis,
            master_fc=master_fc,
            unique_id_field=unique_id_field,
            last_modified_field=last_modified_field,
            today_date=today_date,
            agol_item_id_review=agol_item_id_review,
            agol_item_id_ovr=agol_item_id_ovr
        )

        print('\nRetrieving AGO layer records..')
        agol_df = agol_sync_manager.get_agol_data()
        print(f'AGOL layer contains {len(agol_df)} rows')

        print('\nRetrieving the Master dataset records..')
        local_df = agol_sync_manager.get_local_data()
        print(f'Master dataset contains {len(local_df)} rows')

        print('\nAppending new AGOL records to the Master dataset..')
        agol_sync_manager.append_new_agol_records()

        print('\nAppending edited AGOL records to the Master dataset..')
        agol_sync_manager.append_edited_agol_records()

        print('\nAppending new Master dataset records to AGOL ..')
        agol_sync_manager.append_new_local_records()

        print('\nUpdating the AGO layer...')
        agol_sync_manager.overwrite_feature_layer()

    except Exception as e:
        print(f"Error occurred: {e}") 

    finally:
        ago.disconnect()

    finish_t = timeit.default_timer()
    t_sec = round(finish_t - start_t)
    mins, secs = divmod(t_sec, 60)
    print(f'\nProcessing Completed in {mins} minutes and {secs} seconds')