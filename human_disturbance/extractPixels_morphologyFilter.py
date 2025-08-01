"""
This script extracts urban class (value 17) from a land cover raster and removes road-like noise
using a morphological opening with a single disk kernel.

Last updated on: 2025-08-01
Author: Moez Labiadh
"""

import rasterio
import numpy as np
from skimage import morphology
import os


def _prepare_output_profile(src_profile):
    """
    Adjust profile so that 0 is treated as NoData and output is uint8 with only 1 as valid.
    """
    profile = src_profile.copy()
    profile.update(
        dtype=rasterio.uint8,
        count=1,
        nodata=0,  # 0 will be interpreted as NoData
        compress="lzw"
    )
    return profile


def extract_urban_class(input_path, out_path, urban_value=17):
    """
    Extract pixels equal to urban_value from the input raster and save a binary (1, NoData) raster.

    Parameters
    ----------
    input_path : str
        Path to the original land cover raster.
    out_path : str
        Path where the binary urban-only raster will be written.
    urban_value : int
        Value in the source raster that represents urban.
    """
    print(f"[1] Extracting urban class (value={urban_value}) from: {input_path}")
    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = _prepare_output_profile(src.profile)

        urban_mask = (data == urban_value).astype(np.uint8)
        count = np.sum(urban_mask == 1)
        print(f"Found {count} urban pixels before any filtering.")

    if count == 0:
        print("Warning: No urban pixels found with the specified value.")

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(urban_mask, 1)
    print(f"Binary urban raster saved to: {out_path}")
    return out_path  # return path to the extracted raster


def remove_roads_single_kernel(input_path, output_path, kernel_size=2, urban_value=1):
    """
    Remove road-like pixels from an urban binary mask using morphological opening.

    Assumes the input raster encodes urban as 'urban_value' and others as 0.

    Output raster has value 1 for retained urban and NoData elsewhere.

    Parameters:
    -----------
    input_path : str
        Path to input GeoTIFF file (urban mask or full raster if urban_value is set accordingly)
    output_path : str
        Path to output GeoTIFF file
    kernel_size : int
        Radius of disk structuring element for opening
    urban_value : int
        Value representing urban in the input (default 1 for the binary mask)
    """
    print(f"[2] Opening raster for road cleanup: {input_path}")

    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = _prepare_output_profile(src.profile)
        pre_count = np.sum(data == urban_value)
        print(f"Urban pixels before opening: {pre_count}")

    # Binary mask: urban vs non-urban
    urban_mask = (data == urban_value).astype(np.uint8)

    print(f"Applying morphological opening with disk kernel size: {kernel_size}")
    selem = morphology.disk(kernel_size)
    opened = morphology.opening(urban_mask, selem)

    post_count = np.sum(opened == 1)
    removed = pre_count - post_count
    print(f"Urban pixels after opening: {post_count}")
    print(f"Removed {removed} pixels")

    out_array = opened.astype(np.uint8)

    print(f"Saving cleaned urban raster to: {output_path}")
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(out_array, 1)

    print("Single-kernel processing complete.")
    return out_array


if __name__ == "__main__":
    # --- user-adjustable inputs ---
    kernel_size = 2          # disk radius in pixels
    urban_value_source = 17  # urban value in original landcover
    urban_value_mask = 1     # after extraction urban is represented as 1

    workspace = r'Q:\projects\GeoBC\Human Disturbance\data\Landclass'
    landcover_raster = os.path.join(workspace, "landcover-2020-classification_BC.tif")
    intermediate_urban = os.path.join(workspace, "NRcan_urban_processing", "urban_extracted_binary.tif")
    final_output = os.path.join(workspace, "NRcan_urban_processing", f"urban_morph_single_kernel_size{kernel_size}_binary.tif")

    # --- sanity checks and setup ---
    if not os.path.exists(landcover_raster):
        print(f"Error: Land cover source '{landcover_raster}' does not exist.")
        exit(1)

    if not os.path.isdir(workspace):
        os.makedirs(workspace, exist_ok=True)
    '''
    # Step 1: extract urban class (value 17) into binary mask
    extract_urban_class(
        input_path=landcover_raster,
        out_path=intermediate_urban,
        urban_value=urban_value_source
    )
    '''
    # Step 2: apply single-kernel opening to clean roads
    remove_roads_single_kernel(
        input_path=intermediate_urban,
        output_path=final_output,
        kernel_size=kernel_size,
        urban_value=urban_value_mask
    )