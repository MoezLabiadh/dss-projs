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

def remove_roads_single_kernel(input_path, output_path, kernel_size=1, urban_value=1):
    """
    Remove road pixels from urban classification using morphological opening.
    
    Parameters:
    -----------
    input_path : str
        Path to input GeoTIFF file
    output_path : str
        Path to output GeoTIFF file
    kernel_size : int
        Size of the morphological kernel (disk radius in pixels)
        For 30m pixels: 1-2 for narrow roads, 3-4 for wider roads
    urban_value : int
        Pixel value representing urban areas in the raster
    """
    
    print(f"Opening raster: {input_path}")
    
    # Open the input raster
    with rasterio.open(input_path) as src:
        # Read the data
        data = src.read(1)  # Read first band
        
        # Get metadata for output file
        profile = src.profile.copy()
        
        print(f"Raster shape: {data.shape}")
        print(f"Raster dtype: {data.dtype}")
        print(f"Urban pixels before filtering: {np.sum(data == urban_value)}")
    
    # Create binary mask for urban areas
    urban_mask = (data == urban_value).astype(np.uint8)
    
    # Create morphological structuring element (disk)
    print(f"Applying morphological opening with disk kernel size: {kernel_size}")
    selem = morphology.disk(kernel_size)
    
    # Apply morphological opening
    # This removes small objects and narrow connections (like roads)
    result = morphology.opening(urban_mask, selem)
    
    print(f"Urban pixels after filtering: {np.sum(result == urban_value)}")
    print(f"Removed pixels: {np.sum(data == urban_value) - np.sum(result == urban_value)}")
    
    # Save the result
    print(f"Saving result to: {output_path}")
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(result, 1)
    
    print("Processing complete!")
    
    return result


def remove_roads_multi_kernel(input_path, output_path, kernel_sizes=[1, 2, 3], urban_value=1):
    """
    Apply multiple morphological opening operations with different kernel sizes.
    This can be more effective for roads of varying widths.
    """
    
    print(f"Opening raster: {input_path}")
    
    with rasterio.open(input_path) as src:
        data = src.read(1)
        profile = src.profile.copy()
        print(f"Urban pixels before filtering: {np.sum(data == urban_value)}")
    
    # Create binary mask for urban areas
    urban_mask = (data == urban_value).astype(np.uint8)
    result = urban_mask.copy()
    
    # Apply opening with multiple kernel sizes
    for kernel_size in kernel_sizes:
        print(f"Applying morphological opening with disk kernel size: {kernel_size}")
        selem = morphology.disk(kernel_size)
        
        # Apply opening and intersect with previous result
        opened = morphology.opening(result, selem)
        result = result & opened
    
    print(f"Urban pixels after filtering: {np.sum(result == urban_value)}")
    print(f"Removed pixels: {np.sum(data == urban_value) - np.sum(result == urban_value)}")
    
    # Save the result
    print(f"Saving result to: {output_path}")
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(result, 1)
    
    print("Multi-kernel processing complete!")
    return result



if __name__ == "__main__":
    # Input and output file paths
    workspace = r'\\spatialfiles.bcgov\work\ilmb\dss\dss_workarea\mlabiadh\workspace\20250611_HD_2025\urban'
    input_file = os.path.join(workspace, "urban.tif")
    output_file = os.path.join(workspace, "urban_morph_single_kernel_rerun.tif") 
    
    # Parameters
    kernel_size = 1      # Adjust kernel size as needed (1-4 typical range)
    urban_value = 17      # Pixel value representing urban areas
    use_multi_kernel = False  # Set to False for single kernel approach
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        print("Please update the input_file path in the script.")
        exit()
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        if use_multi_kernel:
            # Use multiple kernel sizes for better results
            kernel_sizes = [1,2] #increase kernel for more agressive filtering, e.g., [1, 2, 3, 4]
            remove_roads_multi_kernel(input_file, output_file, kernel_sizes, urban_value)
        else:
            # Use single kernel size
            remove_roads_single_kernel(input_file, output_file, kernel_size, urban_value)
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        exit()