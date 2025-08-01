"""
This script removes road pixels from urban classification raster data using morphological opening filter.
It can use either a single kernel size or multiple kernel sizes for more aggressive filtering.

Last updated on: 2025-07-31
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
        nodata=0,
        compress="lzw"
    )
    return profile

def remove_roads_single_kernel(input_path, output_path, kernel_size=1, urban_value=1):
    """
    Remove road pixels from urban classification using morphological opening.

    Output raster has value 1 for kept urban and NoData elsewhere.

    Parameters:
    -----------
    input_path : str
        Path to input GeoTIFF file
    output_path : str
        Path to output GeoTIFF file
    kernel_size : int
        Size of the morphological kernel (disk radius in pixels)
    urban_value : int
        Pixel value representing urban areas in the input raster
    """

    print(f"Opening raster: {input_path}")

    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = _prepare_output_profile(src.profile)
        print(f"Raster shape: {data.shape}")
        print(f"Raster dtype (input): {data.dtype}")
        pre_count = np.sum(data == urban_value)
        print(f"Urban pixels before filtering: {pre_count}")

    # Binary mask of urban (1) vs others (0)
    urban_mask = (data == urban_value).astype(np.uint8)

    print(f"Applying morphological opening with disk kernel size: {kernel_size}")
    selem = morphology.disk(kernel_size)

    # Apply opening
    opened = morphology.opening(urban_mask, selem)

    post_count = np.sum(opened == 1)
    print(f"Urban pixels after filtering: {post_count}")
    print(f"Removed pixels: {pre_count - post_count}")

    # Prepare output: keep 1 as-is; other pixels are 0 which will be interpreted as NoData
    out_array = opened.astype(np.uint8)

    print(f"Saving result to: {output_path}")
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(out_array, 1)

    print("Single-kernel processing complete!")
    return out_array


def remove_roads_multi_kernel(input_path, output_path, kernel_sizes=[1, 2, 3], urban_value=1):
    """
    Apply multiple morphological opening operations with different kernel sizes.
    Output raster has value 1 for kept urban and NoData elsewhere.
    """

    print(f"Opening raster: {input_path}")

    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = _prepare_output_profile(src.profile)
        pre_count = np.sum(data == urban_value)
        print(f"Urban pixels before filtering: {pre_count}")

    urban_mask = (data == urban_value).astype(np.uint8)
    result = urban_mask.copy()

    for kernel_size in kernel_sizes:
        print(f"Applying morphological opening with disk kernel size: {kernel_size}")
        selem = morphology.disk(kernel_size)
        opened = morphology.opening(result, selem)
        # intersect (logical AND)
        result = result & opened

    post_count = np.sum(result == 1)
    print(f"Urban pixels after filtering: {post_count}")
    print(f"Removed pixels: {pre_count - post_count}")

    out_array = result.astype(np.uint8)

    print(f"Saving result to: {output_path}")
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(out_array, 1)

    print("Multi-kernel processing complete!")
    return out_array


if __name__ == "__main__":
    # Input and output file paths
    workspace = r'Q:\dss_workarea\mlabiadh\workspace\20250611_HD_2025\urban'
    input_file = os.path.join(workspace, "urban.tif")
    output_file = os.path.join(workspace, "urban_morph_single_kernel_binary.tif") 

    # Parameters
    kernel_size = 1          # Adjust kernel size as needed (1-4 typical range)
    urban_value = 17         # Pixel value representing urban areas in the input raster
    use_multi_kernel = False  # Set to True to use multiple kernel sizes

    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        print("Please update the input_file path in the script.")
        exit(1)

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    try:
        if use_multi_kernel:
            kernel_sizes = [1, 2]  # add kernels for aggressiveness (e.g., [1, 2, 3])
            remove_roads_multi_kernel(input_file, output_file, kernel_sizes, urban_value)
        else:
            remove_roads_single_kernel(input_file, output_file, kernel_size, urban_value)
    except Exception as e:
        print(f"Error processing file: {e}")
        exit(1)
