import os
import subprocess
import shutil

# Define paths
venv_path = "venv"
layer_dir = "python"
zip_file = "layer.zip"

# Create a directory for the layer
if os.path.exists(layer_dir):
    shutil.rmtree(layer_dir)
os.makedirs(layer_dir)

# Copy the site-packages from the virtual environment to the layer directory
site_packages = os.path.join(venv_path, "lib", "python3.9", "site-packages")
shutil.copytree(site_packages, os.path.join(layer_dir, "lib", "python3.9", "site-packages"))

# Zip the layer directory
shutil.make_archive("layer", 'zip', layer_dir)

# Clean up
shutil.rmtree(layer_dir)